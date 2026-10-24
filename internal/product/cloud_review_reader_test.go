package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type cloudReadAPI struct {
	repository      Repository
	pr              github.PR
	comments        []github.Comment
	reviews         []github.Review
	pulls, metadata int
	onFinal         func()
	fail            string
}

func (a *cloudReadAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if path == a.fail {
		return nil, errors.New("fixture read failed")
	}
	if path == "" {
		a.metadata++
		return json.Marshal(a.repository)
	}
	if path == fmt.Sprintf("pulls/%d", a.pr.Number) {
		a.pulls++
		if a.pulls == 2 && a.onFinal != nil {
			a.onFinal()
		}
		return json.Marshal(a.pr)
	}
	return nil, errors.New("unexpected Cloud GET")
}
func (a *cloudReadAPI) ResourcePages(ctx context.Context, path, field string) ([]json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if path == a.fail {
		return nil, errors.New("fixture read failed")
	}
	var values []any
	switch path {
	case fmt.Sprintf("issues/%d/comments?per_page=100", a.pr.Number):
		for _, v := range a.comments {
			values = append(values, v)
		}
	case fmt.Sprintf("pulls/%d/reviews?per_page=100", a.pr.Number):
		for _, v := range a.reviews {
			values = append(values, v)
		}
	default:
		return nil, errors.New("unexpected Cloud collection")
	}
	var raw []json.RawMessage
	for _, v := range values {
		data, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		raw = append(raw, data)
	}
	return raw, nil
}

func TestCloudReviewReaderRequiresCurrentFormalApprovalAndFreshCapability(t *testing.T) {
	for _, mode := range []string{"approved", "commented", "old_sha", "old_round", "untrusted", "dismissed", "changes_requested", "unknown_capability", "unknown_request", "unknown_sha", "future_report", "future_final_report", "unsupported_capability", "missing_evidence", "host_disabled", "revoked_during_read", "report_error", "missing_comment", "edited_comment", "wrong_requester", "duplicate_comment", "wrong_base", "changed_head", "changed_repository", "future_review", "duplicate_review"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			r, requestAPI, saved, id := requestFixture(t)
			if _, err := r.Request(ctx, id, "cloud-owner"); err != nil {
				t.Fatal(err)
			}
			round, err := r.Store.ReadProductCloudRound(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			pr := *requestAPI.pr
			pr.Base.SHA = saved.Value.Candidate.BaseSHA
			api := &cloudReadAPI{repository: Repository{ID: 17, Name: saved.Value.Repository, DefaultBranch: saved.Value.BaseBranch}, pr: pr, comments: append([]github.Comment{}, requestAPI.comments...), reviews: []github.Review{{ID: 101, User: github.User{ID: 91}, State: "APPROVED", CommitID: saved.Value.Candidate.SHA, SubmittedAt: time.Now().UTC()}}, fail: "never"}
			report := contract.NewReport(saved.Value.Repository)
			report.RepositoryID, report.DefaultBranch = 17, saved.Value.BaseBranch
			for _, key := range []string{"cloud_request", "cloud_native_approval", "review_sha"} {
				report.Capabilities[key] = contract.Capability{State: contract.Verified, Evidence: "fixture observed " + key}
			}
			policy := review.Policy{VerifiedRequest: true, VerifiedNativeApproval: true, VerifiedSHAMapping: true, TrustedReviewerIDs: []int64{91}, RequiredApprovals: 1}
			wantError := false
			switch mode {
			case "commented":
				api.reviews[0].State = "COMMENTED"
			case "old_sha":
				api.reviews[0].CommitID = strings.Repeat("c", 40)
			case "old_round":
				api.reviews[0].SubmittedAt = round.RequestedAt.Add(-time.Second)
			case "untrusted":
				api.reviews[0].User.ID = 92
			case "dismissed", "changes_requested":
				later := api.reviews[0]
				later.ID++
				later.SubmittedAt = time.Now().UTC()
				later.State = strings.ToUpper(mode)
				if mode == "dismissed" {
					later.State = "DISMISSED"
				}
				api.reviews = append(api.reviews, later)
			case "unknown_capability":
				report.Capabilities["cloud_native_approval"] = contract.Capability{State: contract.Unknown, Evidence: "not verified"}
			case "unknown_request":
				report.Capabilities["cloud_request"] = contract.Capability{State: contract.Unknown}
			case "unknown_sha":
				report.Capabilities["review_sha"] = contract.Capability{State: contract.Unknown}
			case "future_final_report":
				api.onFinal = func() { report.ObservedAt = time.Now().Add(time.Hour) }
				wantError = true
			case "future_report":
				report.ObservedAt = time.Now().Add(time.Hour)
				wantError = true
			case "unsupported_capability":
				report.Capabilities["cloud_native_approval"] = contract.Capability{State: contract.Unsupported, Evidence: "unsupported"}
			case "missing_evidence":
				report.Capabilities["review_sha"] = contract.Capability{State: contract.Verified, Evidence: " \n"}
			case "host_disabled":
				policy.VerifiedNativeApproval = false
			case "revoked_during_read":
				api.onFinal = func() { report.Capabilities["cloud_native_approval"] = contract.Capability{State: contract.Unknown} }
			case "report_error":
				wantError = true
			case "missing_comment":
				api.comments = nil
				wantError = true
			case "edited_comment":
				api.comments[0].Body += "edited"
				wantError = true
			case "wrong_requester":
				api.comments[0].User.ID++
				wantError = true
			case "duplicate_comment":
				api.comments = append(api.comments, api.comments[0])
				wantError = true
			case "wrong_base":
				api.pr.Base.SHA = strings.Repeat("c", 40)
				wantError = true
			case "changed_head":
				api.onFinal = func() { api.pr.Head.SHA = strings.Repeat("c", 40) }
				wantError = true
			case "changed_repository":
				api.onFinal = func() { api.repository.DefaultBranch = "other" }
				wantError = true
			case "future_review":
				api.reviews[0].SubmittedAt = time.Now().Add(time.Hour)
				wantError = true
			case "duplicate_review":
				api.reviews = append(api.reviews, api.reviews[0])
				wantError = true
			}
			reads := 0
			reader := CloudReviewReader{Store: r.Store, API: func(storage.ProductTarget) ResourceAPI { return api }, Repositories: map[int64]CloudReviewPolicy{17: {Policy: policy, ReadReport: func(context.Context) (contract.Report, error) {
				reads++
				if mode == "report_error" {
					return report, errors.New("report unavailable")
				}
				return report, nil
			}}}}
			result, err := reader.Read(ctx, id)
			if wantError {
				if err == nil {
					t.Fatal("invalid observation admitted", mode)
				}
			} else {
				if err != nil {
					t.Fatal(result, err)
				}
				if result.Decision.Approved != (mode == "approved") {
					t.Fatal("native approval misclassified", mode, result.Decision)
				}
				if reads != 2 || api.pulls != 2 || api.metadata != 2 {
					t.Fatal("fresh bookends missing", reads, api.pulls, api.metadata)
				}
			}
			seat, err := r.Store.OperationReservation(ctx, id)
			if err != nil || seat.State != "held" {
				t.Fatal("read released unfinished Cloud capacity", seat, err)
			}
			var approvals int
			if err := r.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&approvals); err != nil || approvals != 0 {
				t.Fatal("read persisted approval authority", approvals, err)
			}
		})
	}
}
