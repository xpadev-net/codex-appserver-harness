package product

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestCloudCompletionReaderMeasuresAssignedResponseWithoutCurrentApproval(t *testing.T) {
	for _, mode := range []string{"commented", "closed", "new_head", "new_actor_config", "approved", "changes_requested", "dismissed", "legacy", "wrong_actor", "wrong_sha", "old_review", "pending", "future_review", "missing_comment", "edited_comment", "duplicate_comment", "duplicate_review", "changed_head", "changed_repository", "unknown_mapping", "unknown_request", "revoked", "read_error", "report_error"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			r, requestAPI, saved, id := requestFixture(t)
			actors := []int64{91}
			if mode == "legacy" {
				actors = nil
			}
			r.Gate = func(context.Context, storage.ProductTargetSnapshot) (ReviewRequest, error) {
				return ReviewRequest{Trigger: "@review please review", RequesterID: 71, CloudReviewerIDs: actors}, nil
			}
			if _, err := r.Request(ctx, id, "completion-owner"); err != nil {
				t.Fatal(err)
			}
			round, err := r.Store.ReadProductCloudRound(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			pr := *requestAPI.pr
			pr.Base.SHA = saved.Value.Candidate.BaseSHA
			api := &cloudReadAPI{repository: Repository{ID: 17, Name: saved.Value.Repository, DefaultBranch: saved.Value.BaseBranch}, pr: pr, comments: append([]github.Comment{}, requestAPI.comments...), reviews: []github.Review{{ID: 101, User: github.User{ID: 91}, State: "COMMENTED", CommitID: saved.Value.Candidate.SHA, SubmittedAt: time.Now().UTC()}}, fail: "never"}
			base := contract.NewReport(saved.Value.Repository)
			base.RepositoryID, base.DefaultBranch = 17, saved.Value.BaseBranch
			base.ObservedAt = round.RequestedAt.Add(-time.Hour)
			for _, key := range []string{"cloud_request", "review_sha"} {
				base.Capabilities[key] = contract.Capability{State: contract.Verified, Evidence: "fixture verified route contract"}
			}
			originalDate := base.ObservedAt
			wantCompletion := false
			switch mode {
			case "commented", "new_actor_config":
				wantCompletion = true
			case "closed":
				api.pr.State = "closed"
				api.pr.Merged = true
				wantCompletion = true
			case "new_head":
				current, err := r.Store.ReadProductTarget(ctx, saved.Value.Target, saved.Value.WorkItem.ID)
				if err != nil {
					t.Fatal(err)
				}
				next := current.Value
				next.Candidate.ID = "next"
				next.Candidate.SHA = strings.Repeat("c", 40)
				next.WorkItem.Candidate = next.Candidate.ID
				next.WorkItem.Version++
				if _, err := r.Store.SetProductTarget(ctx, current.Digest, next); err != nil {
					t.Fatal(err)
				}
				api.pr.Head.SHA = next.Candidate.SHA
				wantCompletion = true
			case "approved", "changes_requested", "dismissed":
				api.reviews[0].State = strings.ToUpper(mode)
				wantCompletion = true
			case "wrong_actor":
				api.reviews[0].User.ID = 92
			case "wrong_sha":
				api.reviews[0].CommitID = strings.Repeat("c", 40)
			case "old_review":
				api.reviews[0].SubmittedAt = round.RequestedAt.Add(-time.Second)
			case "pending":
				api.reviews[0].State = "PENDING"
			case "future_review":
				api.reviews[0].SubmittedAt = time.Now().Add(time.Hour)
			case "missing_comment":
				api.comments = nil
			case "edited_comment":
				api.comments[0].Body += " changed"
			case "duplicate_comment":
				api.comments = append(api.comments, api.comments[0])
			case "duplicate_review":
				api.reviews = append(api.reviews, api.reviews[0])
			case "changed_head":
				api.onFinal = func() { api.pr.Head.SHA = strings.Repeat("c", 40) }
			case "changed_repository":
				api.onFinal = func() { api.repository.DefaultBranch = "other" }
			case "unknown_mapping":
				base.Capabilities["review_sha"] = contract.Capability{State: contract.Unknown}
			case "unknown_request":
				base.Capabilities["cloud_request"] = contract.Capability{State: contract.Unknown}
			case "revoked":
				api.onFinal = func() { base.Capabilities["review_sha"] = contract.Capability{State: contract.Unknown} }
			case "read_error":
				api.fail = "pulls/42/reviews?per_page=100"
			case "report_error":
			}
			configuredActors := []int64{91}
			if mode == "new_actor_config" {
				configuredActors = []int64{92}
			}
			reportReads := 0
			reader := CloudCompletionReader{Store: r.Store, API: func(storage.ProductTarget) ResourceAPI { return api }, Routes: map[int64]CloudReviewRoute{17: {CloudReviewerIDs: configuredActors, ReadReport: func(readCtx context.Context) (contract.Report, error) {
				reportReads++
				if deadline, ok := readCtx.Deadline(); !ok || time.Until(deadline) > 30*time.Second {
					return base, errors.New("unbounded completion lookup")
				}
				if mode == "report_error" {
					return base, errors.New("fixture report unavailable")
				}
				return base, nil
			}}}}
			before, err := r.Store.ReadProductTarget(ctx, saved.Value.Target, saved.Value.WorkItem.ID)
			if err != nil {
				t.Fatal(err)
			}
			completed, err := reader.Reconcile(ctx, id)
			if wantCompletion {
				if err != nil || completed.Digest == "" {
					t.Fatal(completed, err)
				}
				if completed.Value.Report.ObservedAt.Before(round.RequestedAt) || !strings.Contains(completed.Value.Report.Capabilities["review_sha"].Evidence, "native-review:101") || !strings.Contains(completed.Value.Report.Capabilities["cloud_request"].Evidence, round.Digest) {
					t.Fatal("completion report lacked measured request-specific facts", completed)
				}
				if !base.ObservedAt.Equal(originalDate) || completed.Value.Report.Capabilities["cloud_native_approval"].State != contract.Unknown {
					t.Fatal("old report was redated or approval was manufactured")
				}
				if api.pulls != 2 || api.metadata != 2 || reportReads != 2 {
					t.Fatal("completion reads not bookended", api.pulls, api.metadata, reportReads)
				}
				api.fail = "" // A successful replay must not reach this failure.
				again, err := reader.Reconcile(ctx, id)
				if err != nil || !reflect.DeepEqual(again, completed) || api.metadata != 2 || reportReads != 2 {
					t.Fatal("completed work rescanned or changed", again, err)
				}
			} else if err == nil {
				t.Fatal("unknown provider work was completed", mode, completed)
			}
			seat, e := r.Store.OperationReservation(ctx, id)
			if e != nil || (seat.State == "released") != wantCompletion {
				t.Fatal(seat, e)
			}
			after, e := r.Store.ReadProductTarget(ctx, saved.Value.Target, saved.Value.WorkItem.ID)
			if e != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("completion adopted current Product", after, e)
			}
			var approvals int
			if e := r.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&approvals); e != nil || approvals != 0 {
				t.Fatal("provider response created authority", approvals, e)
			}
		})
	}
}
