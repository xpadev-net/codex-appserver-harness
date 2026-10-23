package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

func productTargetFixture(t *testing.T) (*KernelStore, domain.Wave, ProductTargetSnapshot) {
	t.Helper()
	k, request := waveStoreFixture(t)
	request.Policy.Stage, request.Policy.Product = "P", true
	w, err := k.CreateWave(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	p := ProductTarget{Target: w.Target(), Repository: "owner/repository", BaseBranch: "master", WorkItem: domain.WorkItem{ID: "work-item", RepositoryID: 17, ScopeRevision: 1, PolicyRevision: 1, Phase: domain.Reviewing, Candidate: "candidate", Branch: "harness/work", Version: 1}, Candidate: runner.Candidate{ID: "candidate", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: 1, PolicyRevision: 1}}
	saved, err := k.SetProductTarget(context.Background(), "", p)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, saved
}
func productCallFixture(t *testing.T) (*KernelStore, domain.Wave, ProductTargetSnapshot, contract.ToolInvocation) {
	t.Helper()
	k, w, p := productTargetFixture(t)
	b, run, _ := reserveBudgetMain(t, k, "product-run", w.Target())
	request := contract.PublishPRRequest{SchemaVersion: 1, Target: w.Target(), Candidate: contract.CandidateReference{WorkItemID: p.Value.WorkItem.ID, CandidateID: p.Value.Candidate.ID, SHA: p.Value.Candidate.SHA}}
	return k, w, p, kernelCall(t, b, run, "pr_publish", request)
}
func productIntentCounts(t *testing.T, k *KernelStore) (int, int, int) {
	t.Helper()
	var operations, subscriptions, receipts int
	for _, entry := range []struct {
		kind  string
		count *int
	}{{"product_pr_intent", &operations}, {"product_pr_subscription_intent", &subscriptions}} {
		if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind=?", entry.kind).Scan(entry.count); err != nil {
			t.Fatal(err)
		}
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations WHERE name LIKE 'pr_%'").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	return operations, subscriptions, receipts
}
func TestPRToolAdmissionPersistsOperationAndAutomaticSubscriptionTogether(t *testing.T) {
	k, w, p, call := productCallFixture(t)
	ctx := context.Background()
	gateway := contract.KernelGateway{Backend: k}
	reply, err := gateway.Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if reply.State != "accepted" || len(reply.SubscriptionIDs) != 1 || reply.ResourceID != p.Value.WorkItem.ID {
		t.Fatal(reply)
	}
	var operationState string
	if err = k.DB.QueryRow("SELECT state FROM kernel_tool_operations WHERE id=?", reply.OperationID).Scan(&operationState); err != nil || operationState != "accepted" {
		t.Fatal("pending PR operation became completed", operationState, err)
	}
	var intent PROperationIntent
	if err = k.Record(ctx, "product_pr_intent", reply.OperationID, &intent); err != nil {
		t.Fatal(err)
	}
	if intent.State != "accepted" || intent.Source.SessionID != call.SessionID || intent.Source.RunID != call.RunID || intent.Source.TurnID != call.TurnID || intent.Source.CallID != call.CallID || intent.Source.Target != w.Target() || intent.ProductDigest != p.Digest || intent.MarkerWorkItemID != p.Value.WorkItem.ID {
		t.Fatal(intent)
	}
	var watch PRSubscriptionIntent
	if err = k.Record(ctx, "product_pr_subscription_intent", reply.SubscriptionIDs[0], &watch); err != nil {
		t.Fatal(err)
	}
	if watch.State != "pending" || watch.RepositoryID != 17 || watch.PRNumber != 0 || watch.SessionID != call.SessionID || watch.Target != w.Target() || watch.OperationID != reply.OperationID {
		t.Fatal(watch)
	}
	if a, b, c := productIntentCounts(t, k); a != 1 || b != 1 || c != 1 {
		t.Fatal(a, b, c)
	}
	// Reopen the database and replay the exact call: no external request and no
	// duplicate PR intent, resource subscription intent, or tool receipt.
	path := k.Root + "/state.db"
	if err = k.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := (contract.KernelGateway{Backend: reopened}).Handle(ctx, call)
	if err != nil || !reflect.DeepEqual(reply, again) {
		t.Fatal(again, err)
	}
	if a, b, c := productIntentCounts(t, reopened); a != 1 || b != 1 || c != 1 {
		t.Fatal(a, b, c)
	}
}
func TestPRIntentAdmissionRollbackIncludesEarlyTurnBinding(t *testing.T) {
	k, _, _, call := productCallFixture(t)
	if _, err := k.DB.Exec(`CREATE TRIGGER reject_product_watch BEFORE INSERT ON records WHEN NEW.kind='product_pr_subscription_intent' BEGIN SELECT RAISE(FAIL,'injected resource subscription failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(context.Background(), call); err == nil {
		t.Fatal("partial admission succeeded")
	}
	if a, b, c := productIntentCounts(t, k); a != 0 || b != 0 || c != 0 {
		t.Fatal(a, b, c)
	}
	var run contract.SessionRun
	if err := k.Record(context.Background(), "agent_run", call.RunID, &run); err != nil || run.TurnID != "" || run.State != "starting" {
		t.Fatal("early turn binding survived failed admission", run.TurnID, run.State, err)
	}
	var subCount int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_subscriptions WHERE resource LIKE 'kernel:operation:%'").Scan(&subCount); err != nil || subCount != 0 {
		t.Fatal(subCount, err)
	}
}
func TestPRIntentAdmissionRejectsHeldChangedForeignAndUnboundTargets(t *testing.T) {
	for _, mode := range []string{"paused_work", "hold_work", "terminal_work", "changed_sha", "unknown_work", "review_without_pr", "paused_wave", "wrong_run", "changed_call", "K"} {
		t.Run(mode, func(t *testing.T) {
			k, w, p, call := productCallFixture(t)
			ctx := context.Background()
			switch mode {
			case "paused_work", "hold_work", "terminal_work":
				switch mode {
				case "paused_work":
					p.Value.WorkItem.Paused = true
				case "hold_work":
					p.Value.WorkItem.MergeHold = true
				case "terminal_work":
					p.Value.WorkItem.Phase = domain.Canceled
				}
				if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
					t.Fatal(err)
				}
			case "changed_sha":
				call.Arguments = []byte(strings.Replace(string(call.Arguments), p.Value.Candidate.SHA, strings.Repeat("c", 40), 1))
			case "unknown_work":
				call.Arguments = []byte(strings.Replace(string(call.Arguments), "work-item", "foreign-work", 1))
			case "review_without_pr":
				call.Name = "pr_review_request"
			case "paused_wave":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause", Target: w.Target(), Action: "pause", Reason: "hold publication"}); err != nil {
					t.Fatal(err)
				}
			case "wrong_run":
				call.RunID = "foreign-run"
			case "changed_call":
				if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err != nil {
					t.Fatal(err)
				}
				call.Name = "pr_review_request"
			case "K":
				k, w, _, b, run := kernelToolFixture(t, "task")
				call = kernelCall(t, b, run, "pr_publish", contract.PublishPRRequest{SchemaVersion: 1, Target: w.Target(), Candidate: contract.CandidateReference{WorkItemID: "work-item", CandidateID: "candidate", SHA: strings.Repeat("a", 40)}})
				if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
					t.Fatal("K admitted PR operation")
				}
				return
			}
			if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
				t.Fatal("invalid product request admitted", mode)
			}
			want := 0
			if mode == "changed_call" {
				want = 1
			}
			if a, b, c := productIntentCounts(t, k); a != want || b != want || c != want {
				t.Fatal(a, b, c)
			}
		})
	}
}
func TestProductTargetCASIncludesFlagsAndKeepsTrustedWorkItemMembership(t *testing.T) {
	k, w, saved := productTargetFixture(t)
	ctx := context.Background()
	next := saved.Value
	next.WorkItem.Paused = true
	updated, err := k.SetProductTarget(ctx, saved.Digest, next)
	if err != nil || updated.Digest == saved.Digest {
		t.Fatal(updated, err)
	}
	if _, err = k.SetProductTarget(ctx, saved.Digest, saved.Value); err == nil {
		t.Fatal("stale flag snapshot overwrote pause")
	}
	var membership struct {
		Target      contract.Target `json:"target"`
		CandidateID string          `json:"candidate_id"`
	}
	if err = k.Record(ctx, "wave_work_item", saved.Value.WorkItem.ID, &membership); err != nil || membership.Target != w.Target() || membership.CandidateID != saved.Value.Candidate.ID {
		t.Fatal(membership, err)
	}
	request := WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "another-product", OriginalRequest: "Separate product scope", Criteria: w.Criteria, Policy: w.Policy}
	other, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	foreign := saved.Value
	foreign.Target = other.Target()
	if _, err = k.SetProductTarget(ctx, "", foreign); err == nil {
		t.Fatal("WorkItem registered in two Waves")
	}
	request.SourceID = "kernel-only"
	request.Policy.Stage = "K"
	request.Policy.Product = false
	kernel, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	foreign.Target = kernel.Target()
	foreign.WorkItem.ID = "kernel-work"
	if _, err = k.SetProductTarget(ctx, "", foreign); err == nil {
		t.Fatal("K registered product target")
	}
}

func TestPRReviewIntentUsesControllerBoundPRAndTaskCannotBorrowWorkItem(t *testing.T) {
	ctx := context.Background()
	k, _, p, call := productCallFixture(t)
	p.Value.PRNumber = 42
	if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
		t.Fatal(err)
	}
	call.Name = "pr_review_request"
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	var sub PRSubscriptionIntent
	if err = k.Record(ctx, "product_pr_subscription_intent", reply.SubscriptionIDs[0], &sub); err != nil || sub.PRNumber != 42 {
		t.Fatal(sub, err)
	}
	// A separate P Wave's Task has no WorkItem assignment. Even a legitimate
	// registered candidate in its own Wave does not grant that Task access.
	k2, w2, p2 := productTargetFixture(t)
	adopted, err := k2.AdoptPlan(ctx, w2.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	task, err := k2.Task(ctx, adopted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k2, task.SessionID)
	run := contract.SessionRun{ID: "unassigned-product-run", ThreadID: b.ThreadID, State: "starting", Started: k2.clock(), BaselineKnown: true}
	if err = k2.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err != nil {
		t.Fatal(err)
	}
	request := contract.PublishPRRequest{SchemaVersion: 1, Target: w2.Target(), Candidate: contract.CandidateReference{WorkItemID: p2.Value.WorkItem.ID, CandidateID: p2.Value.Candidate.ID, SHA: p2.Value.Candidate.SHA}}
	if _, err = (contract.KernelGateway{Backend: k2}).Handle(ctx, kernelCall(t, b, run, "pr_publish", request)); err == nil {
		t.Fatal("unassigned Task borrowed product WorkItem")
	}
	if a, b, c := productIntentCounts(t, k2); a != 0 || b != 0 || c != 0 {
		t.Fatal(a, b, c)
	}
}

func TestProductTargetObservationFailureRollsBackProjectionAndMembership(t *testing.T) {
	k, _, saved := productTargetFixture(t)
	if _, err := k.DB.Exec(`CREATE TRIGGER reject_product_observation BEFORE INSERT ON records WHEN NEW.kind='product_target_observation' BEGIN SELECT RAISE(FAIL,'injected observation failure'); END`); err != nil {
		t.Fatal(err)
	}
	next := saved.Value
	next.WorkItem.Paused = true
	if _, err := k.SetProductTarget(context.Background(), saved.Digest, next); err == nil {
		t.Fatal("partial product observation accepted")
	}
	current, err := k.ReadProductTarget(context.Background(), saved.Value.Target, saved.Value.WorkItem.ID)
	if err != nil || !reflect.DeepEqual(current, saved) {
		t.Fatal(current, err)
	}
}

func TestProductProjectionCannotRewriteHistoricalCandidateOrRepositoryIdentity(t *testing.T) {
	k, _, saved := productTargetFixture(t)
	ctx := context.Background()
	next := saved.Value
	next.WorkItem.Version++
	next.WorkItem.Candidate, next.Candidate.ID = "candidate-next", "candidate-next"
	next.Candidate.SHA = strings.Repeat("c", 40)
	current, err := k.SetProductTarget(ctx, saved.Digest, next)
	if err != nil {
		t.Fatal(err)
	}
	changed := saved.Value
	changed.WorkItem.Version = next.WorkItem.Version + 1
	changed.Candidate.SHA = strings.Repeat("d", 40)
	if _, err = k.SetProductTarget(ctx, current.Digest, changed); err == nil {
		t.Fatal("rewrote an older candidate ID after another candidate")
	}
	changed = current.Value
	changed.WorkItem.RepositoryID++
	if _, err = k.SetProductTarget(ctx, current.Digest, changed); err == nil {
		t.Fatal("moved registered WorkItem to another repository")
	}
	latest, err := k.ReadProductTarget(ctx, current.Value.Target, current.Value.WorkItem.ID)
	if err != nil || !reflect.DeepEqual(latest, current) {
		t.Fatal(latest, err)
	}
}

func TestPRPendingAndUnknownReceiptsHoldReplanningWithoutDuplicateAdmission(t *testing.T) {
	k, w, _, call := productCallFixture(t)
	ctx := context.Background()
	if _, err := k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture()); err != nil {
		t.Fatal(err)
	}
	gateway := contract.KernelGateway{Backend: k}
	reply, err := gateway.Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.AdoptPlan(ctx, w.Target(), "replan-pending", storagePlanFixture()); !errors.Is(err, ErrPlanAdmissionHeld) {
		t.Fatal("replanned with unresolved PR operation", err)
	}
	// Emulate a trusted executor's unknown transport disposition, without any
	// external request. A replay returns its reconciliation ID, never a new op.
	if _, err = k.DB.Exec("UPDATE kernel_tool_operations SET state='unknown' WHERE id=?", reply.OperationID); err != nil {
		t.Fatal(err)
	}
	again, err := gateway.Handle(ctx, call)
	if err != nil || again.State != "unknown" || again.OperationID != reply.OperationID {
		t.Fatal(again, err)
	}
	if _, err = k.AdoptPlan(ctx, w.Target(), "replan-unknown", storagePlanFixture()); !errors.Is(err, ErrPlanAdmissionHeld) {
		t.Fatal("replanned with unknown PR operation", err)
	}
	if a, b, c := productIntentCounts(t, k); a != 1 || b != 1 || c != 1 {
		t.Fatal(a, b, c)
	}
}
