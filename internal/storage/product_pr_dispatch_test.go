package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func acceptedPRFixture(t *testing.T) (*KernelStore, ProductTargetSnapshot, string) {
	t.Helper()
	k, _, p, call := productCallFixture(t)
	reply, err := (contract.KernelGateway{Backend: k}).Handle(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}
	return k, p, reply.OperationID
}
func heldPRSlots(t *testing.T, k *KernelStore, kind string) int {
	t.Helper()
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_dispatch_intents WHERE state='held' AND json_extract(payload,'$.kind')=?", kind).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func TestPRDispatchClaimAndSendAreExclusiveAndUnknownSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	k, _, id := acceptedPRFixture(t)
	claim, err := k.PreparePRDispatch(ctx, id, "owner-a")
	if err != nil || claim.State != "prepared" || heldPRSlots(t, k, "pr") != 1 {
		t.Fatal(claim, err)
	}
	again, err := k.PreparePRDispatch(ctx, id, "owner-a")
	if err != nil || !reflect.DeepEqual(again, claim) {
		t.Fatal(again, err)
	}
	if _, err = k.PreparePRDispatch(ctx, id, "owner-b"); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal("second executor admitted", err)
	}
	if _, err = k.MarkPRMayHaveSent(ctx, id, "owner-b"); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal("foreign executor sent", err)
	}
	sent, err := k.MarkPRMayHaveSent(ctx, id, "owner-a")
	if err != nil || sent.State != "unknown" || sent.MayHaveSentAt.IsZero() {
		t.Fatal(sent, err)
	}
	if _, err = k.MarkPRMayHaveSent(ctx, id, "owner-a"); !errors.Is(err, ErrPRReconciliationRequired) {
		t.Fatal("duplicate send authorized", err)
	}
	if err = k.CancelUnsentPRDispatch(ctx, id, "owner-a", "cancel"); !errors.Is(err, ErrPRReconciliationRequired) {
		t.Fatal("discarded unknown request", err)
	}
	if _, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-after-send", Target: claim.Product.Value.Target, Action: "cancel", Reason: "keep unknown PR tracked"}); err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	k.Close()
	reopened, err := OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err = reopened.PreparePRDispatch(ctx, id, "owner-restart"); !errors.Is(err, ErrPRReconciliationRequired) {
		t.Fatal("restart repeated unknown HTTP write", err)
	}
	if heldPRSlots(t, reopened, "pr") != 1 {
		t.Fatal("unknown lost PR capacity")
	}
}
func TestPRBeforeSendRechecksLatestHoldsAndCanFenceKnownUnsent(t *testing.T) {
	for _, mode := range []string{"wave_pause", "wave_cancel", "wave_direction", "work_pause", "work_hold", "projection_version", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, p, id := acceptedPRFixture(t)
			if _, err := k.PreparePRDispatch(ctx, id, "owner"); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "wave_pause", "wave_cancel":
				action := "pause"
				if mode == "wave_cancel" {
					action = "cancel"
				}
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: mode, Target: p.Value.Target, Action: action, Reason: "before HTTP write"}); err != nil {
					t.Fatal(err)
				}
			case "wave_direction":
				if _, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "direction", Target: p.Value.Target, Instruction: "update explanation"}); err != nil {
					t.Fatal(err)
				}
			case "work_pause", "work_hold", "projection_version":
				next := p.Value
				if mode == "work_pause" {
					next.WorkItem.Paused = true
				} else if mode == "work_hold" {
					next.WorkItem.MergeHold = true
				} else {
					next.WorkItem.Version++
				}
				if _, err := k.SetProductTarget(ctx, p.Digest, next); err != nil {
					t.Fatal(err)
				}
			case "deadline":
				w, err := k.Wave(ctx, p.Value.Target.WaveID)
				if err != nil {
					t.Fatal(err)
				}
				k.Now = func() time.Time { return w.Deadline }
			}
			if _, err := k.MarkPRMayHaveSent(ctx, id, "owner"); err == nil {
				t.Fatal("stale prepared request allowed HTTP write")
			}
			if err := k.CancelUnsentPRDispatch(ctx, id, "owner", "fence unsent"); err != nil {
				t.Fatal(err)
			}
			if err := k.CancelUnsentPRDispatch(ctx, id, "owner", "fence unsent"); err != nil {
				t.Fatal("cancellation replay failed", err)
			}
			if _, err := k.MarkPRMayHaveSent(ctx, id, "owner"); !errors.Is(err, ErrPRDispatchHeld) {
				t.Fatal("delayed HTTP callback escaped cancellation", err)
			}
			if heldPRSlots(t, k, "pr") != 0 {
				t.Fatal("known unsent new PR kept slot")
			}
		})
	}
}
func TestPRClaimAndCancellationFailuresRollbackCapacityAndFences(t *testing.T) {
	ctx := context.Background()
	k, _, id := acceptedPRFixture(t)
	if _, err := k.DB.Exec(`CREATE TRIGGER reject_pr_claim BEFORE INSERT ON records WHEN NEW.kind='product_pr_dispatch' BEGIN SELECT RAISE(FAIL,'claim rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PreparePRDispatch(ctx, id, "owner"); err == nil {
		t.Fatal("partial claim admitted")
	}
	if heldPRSlots(t, k, "pr") != 0 {
		t.Fatal("failed claim retained capacity")
	}
	var leases int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE state='held' AND resource LIKE 'product-pr-mutation:%'").Scan(&leases); err != nil || leases != 0 {
		t.Fatal(leases, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER reject_pr_claim"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PreparePRDispatch(ctx, id, "owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec(`CREATE TRIGGER reject_pr_cancel BEFORE INSERT ON records WHEN NEW.kind='product_pr_unsent_cancellation' BEGIN SELECT RAISE(FAIL,'cancellation rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if err := k.CancelUnsentPRDispatch(ctx, id, "owner", "audit failure"); err == nil {
		t.Fatal("partial cancellation succeeded")
	}
	if heldPRSlots(t, k, "pr") != 1 {
		t.Fatal("failed cancellation released capacity")
	}
	claim, err := k.PreparePRDispatch(ctx, id, "owner")
	if err != nil || claim.State != "prepared" {
		t.Fatal("failed cancellation changed fence", claim, err)
	}
}
func TestPRPreparedClaimSerializesSamePRAndPreservesExistingOpenSlot(t *testing.T) {
	ctx := context.Background()
	k, p, call := func() (*KernelStore, ProductTargetSnapshot, contract.ToolInvocation) {
		k, _, p, call := productCallFixture(t)
		return k, p, call
	}()
	p.Value.PRNumber = 42
	if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
		t.Fatal(err)
	}
	gateway := contract.KernelGateway{Backend: k}
	first, err := gateway.Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	call.CallID = "second-call"
	second, err := gateway.Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.PreparePRDispatch(ctx, first.OperationID, "first"); err != nil {
		t.Fatal(err)
	}
	if _, err = k.PreparePRDispatch(ctx, second.OperationID, "second"); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("same PR got concurrent writes", err)
	}
	if err = k.CancelUnsentPRDispatch(ctx, first.OperationID, "first", "unsent update"); err != nil {
		t.Fatal(err)
	}
	if heldPRSlots(t, k, "pr") != 1 {
		t.Fatal("cancel of unsent update released existing open PR slot")
	}
	if _, err = k.PreparePRDispatch(ctx, second.OperationID, "second"); err != nil {
		t.Fatal("same open PR could not reuse its slot", err)
	}
}

func TestPRReviewClaimUsesCloudPoolAndCanonicalCandidateBudget(t *testing.T) {
	ctx := context.Background()
	k, w, p, call := productCallFixture(t)
	p.Value.PRNumber = 42
	if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
		t.Fatal(err)
	}
	call.Name = "pr_review_request"
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.PreparePRDispatch(ctx, reply.OperationID, "cloud-owner"); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal("unaccounted PR started Cloud work", err)
	}
	// Reconcile/account the already open PR through a known-unsent publication
	// preparation. Canceling that update retains the existing PR's lifetime seat.
	publication := call
	publication.Name = "pr_publish"
	publication.CallID = "account-existing-pr"
	published, err := (contract.KernelGateway{Backend: k}).Handle(ctx, publication)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.PreparePRDispatch(ctx, published.OperationID, "bootstrap-owner"); err != nil {
		t.Fatal(err)
	}
	if err = k.CancelUnsentPRDispatch(ctx, published.OperationID, "bootstrap-owner", "existing PR accounted; no update sent"); err != nil {
		t.Fatal(err)
	}
	claim, err := k.PreparePRDispatch(ctx, reply.OperationID, "cloud-owner")
	if err != nil {
		t.Fatal(err)
	}
	r, err := k.OperationReservation(ctx, claim.ReservationID)
	if err != nil || r.Kind != "cloud_review" || r.Request.ResourceKey != "github:17:pull:42" || r.Request.CandidateSHA != p.Value.Candidate.SHA {
		t.Fatal(r, err)
	}
	if heldPRSlots(t, k, "cloud_review") != 1 || heldPRSlots(t, k, "pr") != 1 {
		t.Fatal("review used wrong capacity pool")
	}
	budget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || len(budget.CandidateReviews) != 1 || budget.CandidateReviews["github:17:pull:42:"+p.Value.Candidate.SHA] != 1 {
		t.Fatal(budget, err)
	}
	if _, err = k.PreparePRDispatch(ctx, reply.OperationID, "cloud-owner"); err != nil {
		t.Fatal("replay spent another review", err)
	}
	again, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(budget, again) {
		t.Fatal("review replay charged budget twice", again, err)
	}
	if err = k.CancelUnsentPRDispatch(ctx, reply.OperationID, "cloud-owner", "request not sent"); err != nil {
		t.Fatal(err)
	}
	if heldPRSlots(t, k, "cloud_review") != 0 {
		t.Fatal("unsent review retained capacity")
	}
}

func TestPRMarkFailureDoesNotLosePreparedClaimAndUnclaimedCancellationIsAudited(t *testing.T) {
	ctx := context.Background()
	k, _, id := acceptedPRFixture(t)
	if _, err := k.PreparePRDispatch(ctx, id, "owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec(`CREATE TRIGGER reject_pr_send BEFORE UPDATE ON kernel_tool_operations WHEN NEW.state='unknown' BEGIN SELECT RAISE(FAIL,'unknown rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.MarkPRMayHaveSent(ctx, id, "owner"); err == nil {
		t.Fatal("partially marked send")
	}
	claim, err := k.PreparePRDispatch(ctx, id, "owner")
	if err != nil || claim.State != "prepared" || !claim.MayHaveSentAt.IsZero() {
		t.Fatal(claim, err)
	}
	if err = k.CancelUnsentPRDispatch(ctx, id, "owner", "unsent rollback"); err != nil {
		t.Fatal(err)
	}
	k2, _, unclaimed := acceptedPRFixture(t)
	if err = k2.CancelUnsentPRDispatch(ctx, unclaimed, "controller", "cancel before preparation"); err != nil {
		t.Fatal(err)
	}
	if _, err = k2.PreparePRDispatch(ctx, unclaimed, "delayed"); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal("canceled unclaimed operation restarted", err)
	}
}

func TestConcurrentPRClaimOwnersCannotAuthorizeTwoSends(t *testing.T) {
	ctx := context.Background()
	k, _, id := acceptedPRFixture(t)
	other, err := OpenExistingKernel(k.Root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	type result struct {
		owner string
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for index, store := range []*KernelStore{k, other} {
		owner := []string{"controller-a", "controller-b"}[index]
		go func() { <-start; _, err := store.PreparePRDispatch(ctx, id, owner); results <- result{owner, err} }()
	}
	close(start)
	winner := ""
	admitted := 0
	losers := []result{}
	for range 2 {
		r := <-results
		if r.err == nil {
			winner = r.owner
			admitted++
		} else {
			losers = append(losers, r)
		}
	}
	for _, r := range losers {
		var busy sqlite3.Error
		if !errors.Is(r.err, ErrPRDispatchHeld) && (!errors.As(r.err, &busy) || (busy.Code != sqlite3.ErrBusy && busy.Code != sqlite3.ErrLocked)) {
			t.Fatal(r.err)
		}
		// Retry the entire rolled-back operation only after competing writers
		// settle. Transient SQLite contention must still end at the winner's fence.
		if _, err = k.PreparePRDispatch(ctx, id, r.owner); !errors.Is(err, ErrPRDispatchHeld) {
			t.Fatal("contention retry stole claim", err)
		}
	}
	if admitted != 1 || heldPRSlots(t, k, "pr") != 1 {
		t.Fatal("claim not exclusive", admitted)
	}
	if _, err = k.MarkPRMayHaveSent(ctx, id, winner); err != nil {
		t.Fatal(err)
	}
	if _, err = other.MarkPRMayHaveSent(ctx, id, winner); !errors.Is(err, ErrPRReconciliationRequired) {
		t.Fatal("second connection repeated send", err)
	}
}

func TestPRUnsentCleanupStillWorksAfterVerifiedResourceClosure(t *testing.T) {
	ctx := context.Background()
	k, _, id := acceptedPRFixture(t)
	claim, err := k.PreparePRDispatch(ctx, id, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err = k.ReleaseOperationCapacity(ctx, OperationReleaseProof{OperationID: claim.ReservationID, ProofID: "observed-closed", Disposition: "closed", Reason: "trusted reconciler observed resource closure"}); err != nil {
		t.Fatal(err)
	}
	if _, err = k.MarkPRMayHaveSent(ctx, id, "owner"); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal("released resource authorized a write", err)
	}
	if err = k.CancelUnsentPRDispatch(ctx, id, "owner", "cleanup unsent after resource closure"); err != nil {
		t.Fatal(err)
	}
	var held int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE state='held' AND resource=?", claim.MutationLease.Resource).Scan(&held); err != nil || held != 0 {
		t.Fatal("orphaned mutation fence", held, err)
	}
}

func TestFreshPRPublicationAfterUnsentCancelUsesNewReservationGeneration(t *testing.T) {
	for _, changeCandidate := range []bool{false, true} {
		t.Run(fmt.Sprint(changeCandidate), func(t *testing.T) {
			ctx := context.Background()
			k, _, p, call := productCallFixture(t)
			gateway := contract.KernelGateway{Backend: k}
			first, err := gateway.Handle(ctx, call)
			if err != nil {
				t.Fatal(err)
			}
			old, err := k.PreparePRDispatch(ctx, first.OperationID, "old-owner")
			if err != nil {
				t.Fatal(err)
			}
			if err = k.CancelUnsentPRDispatch(ctx, first.OperationID, "old-owner", "no request sent"); err != nil {
				t.Fatal(err)
			}
			if changeCandidate {
				p.Value.WorkItem.Version++
				p.Value.WorkItem.Candidate = "new-candidate"
				p.Value.Candidate.ID = "new-candidate"
				p.Value.Candidate.SHA = strings.Repeat("c", 40)
				if p, err = k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
					t.Fatal(err)
				}
				call.Arguments, err = json.Marshal(contract.PublishPRRequest{SchemaVersion: 1, Target: p.Value.Target, Candidate: contract.CandidateReference{WorkItemID: p.Value.WorkItem.ID, CandidateID: p.Value.Candidate.ID, SHA: p.Value.Candidate.SHA}})
				if err != nil {
					t.Fatal(err)
				}
			}
			call.CallID = "fresh-publication"
			next, err := gateway.Handle(ctx, call)
			if err != nil {
				t.Fatal(err)
			}
			fresh, err := k.PreparePRDispatch(ctx, next.OperationID, "new-owner")
			if err != nil {
				t.Fatal("fresh publication permanently held", err)
			}
			if fresh.ReservationID == old.ReservationID || fresh.MutationLease.Generation <= old.MutationLease.Generation || heldPRSlots(t, k, "pr") != 1 {
				t.Fatal("reservation history/fence reused", old, fresh)
			}
			prior, err := k.OperationReservation(ctx, old.ReservationID)
			if err != nil || prior.State != "released" || prior.Proof == nil || prior.Proof.Disposition != "not_started" {
				t.Fatal(prior, err)
			}
			if err = k.CancelUnsentPRDispatch(ctx, first.OperationID, "old-owner", "no request sent"); err != nil {
				t.Fatal(err)
			}
			if _, err = k.MarkPRMayHaveSent(ctx, first.OperationID, "old-owner"); !errors.Is(err, ErrPRDispatchHeld) {
				t.Fatal("old callback escaped new generation", err)
			}
			if _, err = k.MarkPRMayHaveSent(ctx, next.OperationID, "new-owner"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPreparedReviewAndReplayRequireLivePRLifetimeSeat(t *testing.T) {
	ctx := context.Background()
	k, _, p, call := productCallFixture(t)
	p.Value.PRNumber = 42
	if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
		t.Fatal(err)
	}
	gateway := contract.KernelGateway{Backend: k}
	publication, err := gateway.Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.PreparePRDispatch(ctx, publication.OperationID, "bootstrap"); err != nil {
		t.Fatal(err)
	}
	if err = k.CancelUnsentPRDispatch(ctx, publication.OperationID, "bootstrap", "existing PR retained"); err != nil {
		t.Fatal(err)
	}
	call.Name, call.CallID = "pr_review_request", "review-request"
	review, err := gateway.Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.PreparePRDispatch(ctx, review.OperationID, "review-owner")
	if err != nil {
		t.Fatal(err)
	}
	if err = k.ReleaseOperationCapacity(ctx, OperationReleaseProof{OperationID: claim.PRReservationID, ProofID: "pr-observed-closed", Disposition: "closed", Reason: "external PR closure"}); err != nil {
		t.Fatal(err)
	}
	if _, err = k.PreparePRDispatch(ctx, review.OperationID, "review-owner"); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal("review replay bypassed PR closure", err)
	}
	if _, err = k.MarkPRMayHaveSent(ctx, review.OperationID, "review-owner"); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal("review send bypassed PR closure", err)
	}
	if err = k.CancelUnsentPRDispatch(ctx, review.OperationID, "review-owner", "PR closed before request"); err != nil {
		t.Fatal(err)
	}
	if heldPRSlots(t, k, "cloud_review") != 0 {
		t.Fatal("unsent Cloud request could not clean up after PR closure")
	}
}

func TestTerminalPRReleaseCannotBeReopenedAsUnsentGeneration(t *testing.T) {
	for _, disposition := range []string{"closed", "merged"} {
		t.Run(disposition, func(t *testing.T) {
			ctx := context.Background()
			k, _, _, call := productCallFixture(t)
			gateway := contract.KernelGateway{Backend: k}
			first, err := gateway.Handle(ctx, call)
			if err != nil {
				t.Fatal(err)
			}
			claim, err := k.PreparePRDispatch(ctx, first.OperationID, "owner")
			if err != nil {
				t.Fatal(err)
			}
			if err = k.ReleaseOperationCapacity(ctx, OperationReleaseProof{OperationID: claim.PRReservationID, ProofID: "terminal-pr", Disposition: disposition, Reason: "trusted terminal observation"}); err != nil {
				t.Fatal(err)
			}
			if err = k.CancelUnsentPRDispatch(ctx, first.OperationID, "owner", "cleanup unsent after terminal observation"); err != nil {
				t.Fatal(err)
			}
			call.CallID = "fresh-call"
			next, err := gateway.Handle(ctx, call)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = k.PreparePRDispatch(ctx, next.OperationID, "fresh-owner"); !errors.Is(err, ErrPRDispatchHeld) {
				t.Fatal("reopened terminal PR reservation", err)
			}
		})
	}
}
