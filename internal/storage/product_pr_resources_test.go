package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func observedPRFixture(t *testing.T, unknown bool) (*KernelStore, ProductTargetSnapshot, string, PRDispatchClaim, PRTransportProof, PRResourceObservation) {
	t.Helper()
	ctx := context.Background()
	k, p, id := acceptedPRFixture(t)
	claim, err := k.PreparePRDispatch(ctx, id, "transport-owner")
	if err != nil {
		t.Fatal(err)
	}
	proof := PRTransportProof{ID: "transport-proof", OwnerID: claim.OwnerID, Disposition: "not_started"}
	if unknown {
		claim, err = k.MarkPRMayHaveSent(ctx, id, claim.OwnerID)
		if err != nil {
			t.Fatal(err)
		}
		proof.Disposition = "reconciled_after_stop"
	}
	o := PRResourceObservation{ProofID: "observed-pr", RepositoryID: p.Value.WorkItem.RepositoryID, HeadRepositoryID: p.Value.WorkItem.RepositoryID, BaseRepositoryID: p.Value.WorkItem.RepositoryID, Number: 42, MarkerWorkItemID: p.Value.WorkItem.ID, HeadSHA: p.Value.Candidate.SHA, HeadBranch: p.Value.WorkItem.Branch, BaseBranch: p.Value.BaseBranch, State: "open", ObservedAt: k.clock()}
	return k, p, id, claim, proof, o
}
func TestObservedPRBindingCommitsIdentitySubscriptionReceiptAndProjectionAtomically(t *testing.T) {
	ctx := context.Background()
	k, p, id, claim, proof, o := observedPRFixture(t, false)
	receipt, err := k.BindObservedPRPublication(ctx, id, proof, o)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Outcome != "resource_bound" || receipt.Resource.Number != 42 || receipt.NotificationState != "pending" {
		t.Fatal(receipt)
	}
	var result []byte
	if err = k.DB.QueryRow("SELECT result FROM kernel_tool_operations WHERE id=?", id).Scan(&result); err != nil {
		t.Fatal(err)
	}
	var reply contract.ToolReply
	if err = json.Unmarshal(result, &reply); err != nil || reply.State != "completed" || reply.ResourceID != "github:17:pull:42" {
		t.Fatal(reply, err)
	}
	var watch PRSubscriptionIntent
	if err = k.Record(ctx, "product_pr_subscription_intent", receipt.SubscriptionID, &watch); err != nil || watch.PRNumber != 42 || watch.State != "bound" || watch.ResourceKey != receipt.Resource.ResourceKey {
		t.Fatal(watch, err)
	}
	var resource string
	if err = k.DB.QueryRow("SELECT resource FROM kernel_subscriptions WHERE id=?", watch.ID).Scan(&resource); err != nil || resource != "external:github:17:pull:42" {
		t.Fatal(resource, err)
	}
	current, err := k.ReadProductTarget(ctx, p.Value.Target, p.Value.WorkItem.ID)
	if err != nil || current.Value.PRNumber != 42 || current.Value.Candidate != p.Value.Candidate {
		t.Fatal(current, err)
	}
	if heldPRSlots(t, k, "pr") != 1 {
		t.Fatal("open PR lifetime seat released by publication completion")
	}
	var mutation int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE resource=? AND state='held'", claim.MutationLease.Resource).Scan(&mutation); err != nil || mutation != 0 {
		t.Fatal(mutation, err)
	}
	// A resource association is not a Wave, review approval or merge receipt.
	var quality int
	if err = k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('wave_completion','review_approval')").Scan(&quality); err != nil || quality != 0 {
		t.Fatal(quality, err)
	}
	again, err := k.BindObservedPRPublication(ctx, id, proof, o)
	if err != nil || !reflect.DeepEqual(receipt, again) {
		t.Fatal("completion replay failed", again, err)
	}
}
func TestPRBindingRejectsForeignHeadersAndUnverifiedTransportWithoutReleasingOwnership(t *testing.T) {
	for _, mode := range []string{"repository", "head_repository", "base_repository", "marker", "sha", "branch", "base", "number", "state", "merged", "old_observation", "owner", "missing_proof", "missing_stop"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, id, claim, proof, o := observedPRFixture(t, true)
			switch mode {
			case "repository":
				o.RepositoryID++
			case "head_repository":
				o.HeadRepositoryID++
			case "base_repository":
				o.BaseRepositoryID++
			case "marker":
				o.MarkerWorkItemID = "foreign"
			case "sha":
				o.HeadSHA = strings.Repeat("f", 40)
			case "branch":
				o.HeadBranch = "foreign-branch"
			case "base":
				o.BaseBranch = "foreign-base"
			case "number":
				o.Number = 0
			case "state":
				o.State = "unknown"
			case "merged":
				o.Merged = true
			case "old_observation":
				o.ObservedAt = claim.PreparedAt.Add(-time.Second)
			case "owner":
				proof.OwnerID = "another-owner"
			case "missing_proof":
				o.ProofID = ""
			case "missing_stop":
				proof.Disposition = "not_started"
			}
			if _, err := k.BindObservedPRPublication(ctx, id, proof, o); err == nil {
				t.Fatal("invalid external observation adopted", mode)
			}
			if heldPRSlots(t, k, "pr") != 1 {
				t.Fatal("invalid observation released PR slot")
			}
			var completions int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_pr_completion'").Scan(&completions); err != nil || completions != 0 {
				t.Fatal(completions, err)
			}
		})
	}
}
func TestPRBindingFailureRollsBackResourceSubscriptionProjectionAndLease(t *testing.T) {
	ctx := context.Background()
	k, p, id, claim, proof, o := observedPRFixture(t, true)
	if _, err := k.DB.Exec(`CREATE TRIGGER fail_pr_completion BEFORE INSERT ON records WHEN NEW.kind='product_pr_completion' BEGIN SELECT RAISE(FAIL,'binding rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.BindObservedPRPublication(ctx, id, proof, o); err == nil {
		t.Fatal("partial binding committed")
	}
	current, err := k.ReadProductTarget(ctx, p.Value.Target, p.Value.WorkItem.ID)
	if err != nil || !reflect.DeepEqual(current, p) {
		t.Fatal(current, err)
	}
	var bindings int
	if err = k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_pr_resource'").Scan(&bindings); err != nil || bindings != 0 {
		t.Fatal(bindings, err)
	}
	var mutation int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE resource=? AND state='held'", claim.MutationLease.Resource).Scan(&mutation); err != nil || mutation != 1 {
		t.Fatal(mutation, err)
	}
	if _, err = k.DB.Exec("DROP TRIGGER fail_pr_completion"); err != nil {
		t.Fatal(err)
	}
	if _, err = k.BindObservedPRPublication(ctx, id, proof, o); err != nil {
		t.Fatal(err)
	}
}
func TestUnknownPRBindingAfterRestartAndCancelPreservesNewerCandidateAndFlags(t *testing.T) {
	ctx := context.Background()
	k, p, id, _, proof, o := observedPRFixture(t, true)
	next := p.Value
	next.WorkItem.Version++
	next.WorkItem.Paused = true
	next.WorkItem.Candidate = "new-candidate"
	next.Candidate.ID = "new-candidate"
	next.Candidate.SHA = strings.Repeat("c", 40)
	updated, err := k.SetProductTarget(ctx, p.Digest, next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-after-send", Target: p.Value.Target, Action: "cancel", Reason: "track already sent request"}); err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	k.Close()
	reopened, err := OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	receipt, err := reopened.BindObservedPRPublication(ctx, id, proof, o)
	if err != nil {
		t.Fatal(err)
	}
	current, err := reopened.ReadProductTarget(ctx, p.Value.Target, p.Value.WorkItem.ID)
	if err != nil || current.Value.PRNumber != 42 || !current.Value.WorkItem.Paused || current.Value.Candidate != updated.Value.Candidate || current.Value.WorkItem.Version != updated.Value.WorkItem.Version {
		t.Fatal("external tracking restored stale product state", current, err)
	}
	if receipt.Observation.HeadSHA != p.Value.Candidate.SHA {
		t.Fatal("old observed candidate was relabeled current")
	}
	wave, err := reopened.Wave(ctx, p.Value.Target.WaveID)
	if err != nil || wave.State != domain.WaveCanceled {
		t.Fatal("binding reopened canceled Wave", wave, err)
	}
}
func TestObservedTerminalPRReleasesSeatAndCompletionReplayCannotChangeResource(t *testing.T) {
	ctx := context.Background()
	k, _, id, _, proof, o := observedPRFixture(t, true)
	o.State = "closed"
	o.Merged = true
	proof.Disposition = "response_received"
	if _, err := k.BindObservedPRPublication(ctx, id, proof, o); err != nil {
		t.Fatal(err)
	}
	if heldPRSlots(t, k, "pr") != 0 {
		t.Fatal("positive terminal PR observation kept slot")
	}
	o.Number++
	if _, err := k.BindObservedPRPublication(ctx, id, proof, o); err == nil {
		t.Fatal("completion replay rewrote resource")
	}
}
func TestBoundPRResourceCanBeUpdatedByFreshOperationWithoutAnotherOpenSlot(t *testing.T) {
	ctx := context.Background()
	k, p, id, _, proof, o := observedPRFixture(t, false)
	if _, err := k.BindObservedPRPublication(ctx, id, proof, o); err != nil {
		t.Fatal(err)
	}
	var intent PROperationIntent
	if err := k.Record(ctx, "product_pr_intent", id, &intent); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, intent.Source.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	var run contract.SessionRun
	if err = k.Record(ctx, "agent_run", intent.Source.RunID, &run); err != nil {
		t.Fatal(err)
	}
	request := contract.PublishPRRequest{SchemaVersion: 1, Target: p.Value.Target, Candidate: intent.Candidate}
	call := kernelCall(t, b, run, "pr_publish", request)
	call.CallID = "update-existing"
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.PreparePRDispatch(ctx, reply.OperationID, "update-owner")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Product.Value.PRNumber != 42 || heldPRSlots(t, k, "pr") != 1 {
		t.Fatal(claim)
	}
	if reply.State != "accepted" || reply.OperationID == id || claim.State != "prepared" {
		t.Fatal("fresh operation inherited old completion", reply, claim.State)
	}
}

func TestPreviouslyBoundPRCannotBeAdoptedByAnotherWorkItem(t *testing.T) {
	ctx := context.Background()
	k, p, id, _, proof, o := observedPRFixture(t, false)
	o.State = "closed"
	if _, err := k.BindObservedPRPublication(ctx, id, proof, o); err != nil {
		t.Fatal(err)
	}
	second := p.Value
	second.WorkItem.ID = "another-work"
	second.WorkItem.Candidate = "another-candidate"
	second.Candidate.ID = "another-candidate"
	if _, err := k.SetProductTarget(ctx, "", second); err != nil {
		t.Fatal(err)
	}
	var original PROperationIntent
	if err := k.Record(ctx, "product_pr_intent", id, &original); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, original.Source.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	var run contract.SessionRun
	if err = k.Record(ctx, "agent_run", original.Source.RunID, &run); err != nil {
		t.Fatal(err)
	}
	request := contract.PublishPRRequest{SchemaVersion: 1, Target: second.Target, Candidate: contract.CandidateReference{WorkItemID: second.WorkItem.ID, CandidateID: second.Candidate.ID, SHA: second.Candidate.SHA}}
	call := kernelCall(t, b, run, "pr_publish", request)
	call.CallID = "foreign-resource-attempt"
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.PreparePRDispatch(ctx, reply.OperationID, "second-owner")
	if err != nil {
		t.Fatal(err)
	}
	o.MarkerWorkItemID = second.WorkItem.ID
	o.ObservedAt = k.clock()
	o.ProofID = "second-observation"
	if _, err = k.BindObservedPRPublication(ctx, reply.OperationID, PRTransportProof{ID: "no-write", OwnerID: claim.OwnerID, Disposition: "not_started"}, o); err == nil {
		t.Fatal("PR ownership moved to another WorkItem")
	}
	var owner PRResourceBinding
	if err = k.Record(ctx, "product_pr_resource", "github:17:pull:42", &owner); err != nil || owner.WorkItemID != p.Value.WorkItem.ID {
		t.Fatal(owner, err)
	}
}

func TestCanceledUnsentPublicationCannotAcquireResourceFromLateObservation(t *testing.T) {
	k, _, id, claim, proof, o := observedPRFixture(t, false)
	if err := k.CancelUnsentPRDispatch(context.Background(), id, claim.OwnerID, "cancel unsent"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.BindObservedPRPublication(context.Background(), id, proof, o); !errors.Is(err, ErrPRDispatchHeld) {
		t.Fatal("late observation adopted for a canceled write", err)
	}
}
