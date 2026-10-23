package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type fakeReviewAPI struct {
	*fakeAPI
	comments          []github.Comment
	guard             func(context.Context, string, string) error
	onRequest         func()
	afterRequestGuard func()
	loseRequest       bool
	omitGuard         bool
	requestWrites     int
}

func (f *fakeReviewAPI) ReviewComments(context.Context, int) ([]github.Comment, error) {
	return f.comments, nil
}
func (f *fakeReviewAPI) RequestReview(ctx context.Context, n int, round, trigger string, author int64) (int64, error) {
	if f.onRequest != nil {
		f.onRequest()
	}
	for _, c := range f.comments {
		if c.User.ID == author && strings.Contains(c.Body, "<!-- harness-review:"+round+" -->") {
			return c.ID, nil
		}
	}
	if !f.omitGuard {
		if err := f.guard(ctx, "POST", fmt.Sprintf("issues/%d/comments", n)); err != nil {
			return 0, err
		}
	}
	if f.afterRequestGuard != nil {
		f.afterRequestGuard()
	}
	f.requestWrites++
	c := github.Comment{ID: 18, User: github.User{ID: author}, Body: trigger + "\n\n<!-- harness-review:" + round + " -->"}
	f.comments = append(f.comments, c)
	if f.loseRequest {
		return 0, errors.New("request response lost")
	}
	return c.ID, nil
}

func TestReviewRequestRequiresGuardAndBindsOnlyExplicitExistingCommentWithoutWrite(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			r, api, _, id := requestFixture(t)
			api.omitGuard = true
			if existing {
				packet := storage.PRReviewRequestPayload{RoundID: id, Trigger: "@review please review"}
				api.comments = []github.Comment{{ID: 88, User: github.User{ID: 71}, Body: packet.Body()}}
			}
			receipt, err := r.Request(context.Background(), id, "owner")
			if existing {
				if err != nil || api.requestWrites != 0 || receipt.Transport.Disposition != "not_started" {
					t.Fatal(receipt, err)
				}
			} else if err == nil || receipt.OperationID != "" {
				t.Fatal("unguarded write accepted", receipt, err)
			}
		})
	}
}
func requestFixture(t *testing.T) (ReviewRequester, *fakeReviewAPI, storage.ProductTargetSnapshot, string) {
	t.Helper()
	ctx := context.Background()
	p, api, saved, publish := publicationFixture(t)
	if _, err := p.Publish(ctx, publish, "publisher"); err != nil {
		t.Fatal(err)
	}
	current, err := p.Store.ReadProductTarget(ctx, saved.Value.Target, saved.Value.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Store.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(contract.PublishPRRequest{SchemaVersion: 1, Target: current.Value.Target, Candidate: contract.CandidateReference{WorkItemID: current.Value.WorkItem.ID, CandidateID: current.Value.Candidate.ID, SHA: current.Value.Candidate.SHA}})
	reply, err := (contract.KernelGateway{Backend: p.Store}).Handle(ctx, contract.ToolInvocation{SessionID: b.ID, RunID: "model-run", ThreadID: b.ThreadID, TurnID: "turn", CallID: "request-review", Name: "pr_review_request", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeReviewAPI{fakeAPI: api}
	r := ReviewRequester{Store: p.Store, Gate: func(context.Context, storage.ProductTargetSnapshot) (ReviewRequest, error) {
		return ReviewRequest{Trigger: "@review please review", RequesterID: 71}, nil
	}, API: func(_ storage.ProductTarget, before func(context.Context, string, string) error) ReviewAPI {
		f.guard = before
		return f
	}}
	return r, f, current, reply.OperationID
}
func TestReviewRequesterPersistsUnknownBeforeSingleRequestAndKeepsCloudCapacity(t *testing.T) {
	r, api, _, id := requestFixture(t)
	ctx := context.Background()
	api.afterRequestGuard = func() {
		var claim storage.PRDispatchClaim
		if err := r.Store.Record(ctx, "product_pr_dispatch", id, &claim); err != nil || claim.State != "unknown" {
			t.Fatal(claim, err)
		}
	}
	receipt, err := r.Request(ctx, id, "owner")
	if err != nil || receipt.Outcome != "review_requested" || api.requestWrites != 1 {
		t.Fatal(receipt, api.requestWrites, err)
	}
	claim := storage.PRDispatchClaim{}
	if err = r.Store.Record(ctx, "product_pr_dispatch", id, &claim); err != nil {
		t.Fatal(err)
	}
	seat, err := r.Store.OperationReservation(ctx, claim.ReservationID)
	if err != nil || seat.State != "held" || seat.Kind != "cloud_review" {
		t.Fatal(seat, err)
	}
	if _, err = r.Request(ctx, id, "restart"); err != nil || api.requestWrites != 1 {
		t.Fatal("completed request repeated", err)
	}
}
func TestReviewRequesterChecksLatestCanonicalGateAndRemotePRBeforeWrite(t *testing.T) {
	for _, mode := range []string{"gate", "trigger", "default", "repo", "sha", "closed", "draft", "pause", "endpoint"} {
		t.Run(mode, func(t *testing.T) {
			r, api, saved, id := requestFixture(t)
			ctx := context.Background()
			changed := false
			if mode == "gate" || mode == "trigger" {
				r.Gate = func(context.Context, storage.ProductTargetSnapshot) (ReviewRequest, error) {
					if changed && mode == "gate" {
						return ReviewRequest{}, errors.New("unverified route")
					}
					trigger := "@review please review"
					if changed {
						trigger = "@other review"
					}
					return ReviewRequest{Trigger: trigger, RequesterID: 71}, nil
				}
			}
			api.onRequest = func() {
				changed = true
				switch mode {
				case "default":
					api.repository.DefaultBranch = "other"
				case "repo":
					api.repository.ID++
				case "sha":
					api.pr.Head.SHA = strings.Repeat("c", 40)
				case "closed":
					api.pr.State = "closed"
				case "draft":
					api.pr.Draft = true
				case "pause":
					if _, err := r.Store.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause", Target: saved.Value.Target, Action: "pause", Reason: "hold request"}); err != nil {
						t.Fatal(err)
					}
				case "endpoint":
					original := api.guard
					api.guard = func(c context.Context, _, _ string) error { return original(c, "PUT", "pulls/42/merge") }
				}
			}
			if _, err := r.Request(ctx, id, "owner"); err == nil || api.requestWrites != 0 {
				t.Fatal("held request sent", api.requestWrites, err)
			}
		})
	}
}
func TestLostReviewRequestRecoversReadOnlyAfterPauseWithoutCurrentGate(t *testing.T) {
	r, api, saved, id := requestFixture(t)
	ctx := context.Background()
	api.loseRequest = true
	if _, err := r.Request(ctx, id, "owner"); !errors.Is(err, storage.ErrPRReconciliationRequired) {
		t.Fatal(err)
	}
	if _, err := r.Request(ctx, id, "restart"); !errors.Is(err, storage.ErrPRReconciliationRequired) || api.requestWrites != 1 {
		t.Fatal("unknown request repeated", api.requestWrites, err)
	}
	if _, err := r.Store.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause", Target: saved.Value.Target, Action: "pause", Reason: "retain facts"}); err != nil {
		t.Fatal(err)
	}
	r.Gate = nil
	api.repository.DefaultBranch = "new-default"
	receipt, err := r.Reconcile(ctx, id, storage.PRTransportProof{ID: "stopped", OwnerID: "owner", Disposition: "reconciled_after_stop"})
	if err != nil || receipt.ReviewRequest == nil || api.requestWrites != 1 {
		t.Fatal(receipt, err)
	}
}
func TestReviewRequestRecoveryRejectsAbsentEditedForeignAndDuplicateComments(t *testing.T) {
	for _, mode := range []string{"absent", "body", "author", "duplicate", "edited_duplicate", "owner"} {
		t.Run(mode, func(t *testing.T) {
			r, api, _, id := requestFixture(t)
			ctx := context.Background()
			api.loseRequest = true
			if _, err := r.Request(ctx, id, "owner"); !errors.Is(err, storage.ErrPRReconciliationRequired) {
				t.Fatal(err)
			}
			proof := storage.PRTransportProof{ID: "stopped", OwnerID: "owner", Disposition: "reconciled_after_stop"}
			switch mode {
			case "absent":
				api.comments = nil
			case "body":
				api.comments[0].Body += "edited"
			case "author":
				api.comments[0].User.ID++
			case "duplicate":
				api.comments = append(api.comments, api.comments[0])
			case "edited_duplicate":
				duplicate := api.comments[0]
				duplicate.ID++
				duplicate.Body += "edited"
				api.comments = append(api.comments, duplicate)
			case "owner":
				proof.OwnerID = "other"
			}
			if _, err := r.Reconcile(ctx, id, proof); err == nil || api.requestWrites != 1 {
				t.Fatal("ambiguous recovery settled or resent", err)
			}
		})
	}
}

func TestReviewRequestRejectsMixedEditedDuplicateBeforeSending(t *testing.T) {
	for _, reversed := range []bool{false, true} {
		t.Run(fmt.Sprint(reversed), func(t *testing.T) {
			r, api, _, id := requestFixture(t)
			packet := storage.PRReviewRequestPayload{RoundID: id, Trigger: "@review please review"}
			api.comments = []github.Comment{{ID: 81, User: github.User{ID: 71}, Body: packet.Body()}, {ID: 82, User: github.User{ID: 71}, Body: packet.Body() + "edited"}}
			if reversed {
				api.comments[0], api.comments[1] = api.comments[1], api.comments[0]
			}
			if _, err := r.Request(context.Background(), id, "owner"); err == nil || api.requestWrites != 0 {
				t.Fatal("ambiguous round accepted", api.requestWrites, err)
			}
		})
	}
}
