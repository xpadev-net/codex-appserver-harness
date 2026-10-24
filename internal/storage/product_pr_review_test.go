package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func observedReviewFixture(t *testing.T, unknown bool, expected ...[]int64) (*KernelStore, string, PRDispatchClaim, PRTransportProof, PRReviewRequestObservation) {
	t.Helper()
	ctx := context.Background()
	k, _, _, call := productCallFixture(t)
	pub, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.PreparePRDispatch(ctx, pub.OperationID, "publisher")
	if err != nil {
		t.Fatal(err)
	}
	p := claim.Product.Value
	_, err = k.BindObservedPRPublication(ctx, pub.OperationID, PRTransportProof{ID: "publish", OwnerID: claim.OwnerID, Disposition: "not_started"}, PRResourceObservation{ProofID: "pr", RepositoryID: 17, HeadRepositoryID: 17, BaseRepositoryID: 17, Number: 42, MarkerWorkItemID: p.WorkItem.ID, HeadSHA: p.Candidate.SHA, HeadBranch: p.WorkItem.Branch, BaseBranch: p.BaseBranch, State: "open", ObservedAt: k.clock()})
	if err != nil {
		t.Fatal(err)
	}
	call.Name, call.CallID = "pr_review_request", "request-review"
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	claim, err = k.PreparePRDispatch(ctx, reply.OperationID, "requester")
	if err != nil {
		t.Fatal(err)
	}
	packet, err := k.SavePRReviewRequestPayload(ctx, reply.OperationID, claim.OwnerID, "@review please review", 71, expected...)
	if err != nil {
		t.Fatal(err)
	}
	proof := PRTransportProof{ID: "request-transport", OwnerID: claim.OwnerID, Disposition: "not_started"}
	if unknown {
		claim, err = k.MarkPRMayHaveSent(ctx, reply.OperationID, claim.OwnerID)
		if err != nil {
			t.Fatal(err)
		}
		proof.Disposition = "reconciled_after_stop"
	}
	o := PRReviewRequestObservation{ProofID: "comment", RepositoryID: 17, PRNumber: 42, RoundID: packet.RoundID, CommentID: 8, RequesterID: 71, Body: packet.Body(), ObservedAt: k.clock()}
	return k, reply.OperationID, claim, proof, o
}
func TestObservedReviewRequestKeepsCloudAndPRSeatsAndQueuesOnlyRequestNotice(t *testing.T) {
	k, id, claim, proof, o := observedReviewFixture(t, true)
	ctx := context.Background()
	receipt, err := k.BindObservedPRReviewRequest(ctx, id, proof, o)
	if err != nil || receipt.Outcome != "review_requested" || receipt.ReviewRequest == nil {
		t.Fatal(receipt, err)
	}
	if heldPRSlots(t, k, "cloud_review") != 1 || heldPRSlots(t, k, "pr") != 1 {
		t.Fatal("request acknowledgment released unfinished external work")
	}
	var count int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE resource=? AND state='held'", claim.MutationLease.Resource).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err = k.ReconcilePRCompletionNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	if err = k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&count); err != nil || count != 0 {
		t.Fatal("request forged quality approval", count, err)
	}
	var raw []byte
	if err = k.DB.QueryRow("SELECT payload FROM kernel_notifications WHERE subscription_id=?", receipt.SubscriptionID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "review_requested") || !strings.Contains(string(raw), "deferred") {
		t.Fatal(string(raw))
	}
	again, err := k.BindObservedPRReviewRequest(ctx, id, proof, o)
	if err != nil || again.ReviewRequest == nil || !reflect.DeepEqual(again.ReviewRequest, receipt.ReviewRequest) {
		t.Fatal(again, err)
	}
}
func TestObservedReviewRequestValidatesPacketProofAndRollsBackAllSettlement(t *testing.T) {
	for _, mode := range []string{"body", "author", "pr", "round", "repo", "comment", "owner", "no_stop", "old", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			k, id, claim, proof, o := observedReviewFixture(t, true)
			ctx := context.Background()
			switch mode {
			case "body":
				o.Body += "edited"
			case "author":
				o.RequesterID++
			case "pr":
				o.PRNumber++
			case "round":
				o.RoundID += "x"
			case "repo":
				o.RepositoryID++
			case "comment":
				o.CommentID = 0
			case "owner":
				proof.OwnerID = "other"
			case "no_stop":
				proof.Disposition = "not_started"
			case "old":
				o.ObservedAt = claim.PreparedAt.Add(-1)
			case "rollback":
				if _, err := k.DB.Exec(`CREATE TRIGGER fail_review_receipt BEFORE INSERT ON records WHEN NEW.kind='product_pr_completion' AND NEW.id='` + id + `' BEGIN SELECT RAISE(FAIL,'injected completion failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.BindObservedPRReviewRequest(ctx, id, proof, o); err == nil {
				t.Fatal("invalid or partial settlement committed")
			}
			var current PRDispatchClaim
			if err := k.Record(ctx, "product_pr_dispatch", id, &current); err != nil || current.State != "unknown" {
				t.Fatal(current, err)
			}
			if heldPRSlots(t, k, "cloud_review") != 1 {
				t.Fatal("failed settlement released cloud")
			}
		})
	}
}
func TestReviewRequestPayloadImmutableBoundedAndRecoveryAfterCancel(t *testing.T) {
	k, id, claim, proof, o := observedReviewFixture(t, false)
	ctx := context.Background()
	if _, err := k.SavePRReviewRequestPayload(ctx, id, claim.OwnerID, "different", 71); err == nil {
		t.Fatal("packet mutated")
	}
	if _, err := k.SavePRReviewRequestPayload(ctx, id, claim.OwnerID, strings.Repeat("x", 64<<10), 71); err == nil {
		t.Fatal("markers excluded from body limit")
	}
	if _, err := k.SavePRReviewRequestPayload(ctx, id, "foreign", "@review please review", 71); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal(err)
	}
	if _, err := k.MarkPRMayHaveSent(ctx, id, claim.OwnerID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel", Target: claim.Product.Value.Target, Action: "cancel", Reason: "retain prior request"}); err != nil {
		t.Fatal(err)
	}
	proof.Disposition = "reconciled_after_stop"
	path := k.Root + "/state.db"
	k.Close()
	k, err := OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err = k.BindObservedPRReviewRequest(ctx, id, proof, o); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReconcilePRCompletionNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	var state string
	var receipt PRCompletionReceipt
	if err = k.Record(ctx, "product_pr_completion", id, &receipt); err != nil {
		t.Fatal(err)
	}
	if err = k.DB.QueryRow("SELECT state FROM kernel_notifications WHERE subscription_id=?", receipt.SubscriptionID).Scan(&state); err != nil || state != "archived" {
		t.Fatal(state, err)
	}
}
