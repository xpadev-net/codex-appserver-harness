package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func waveToolFixture(t *testing.T) (*KernelStore, domain.Wave, contract.SessionBinding, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, "main")
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "wave-tool-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	return k, w, b, run
}
func TestWaveCreateToolResolvesOnlyRegisteredCurrentInput(t *testing.T) {
	for _, mode := range []string{"owned", "unknown", "foreign-request", "paused", "canceled", "stale"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, b, run := waveToolFixture(t)
			request := contract.CreateWaveRequest{SchemaVersion: 1, RequestID: w.SourceID}
			switch mode {
			case "unknown":
				request.RequestID = "not-a-user-request"
			case "foreign-request":
				other, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: b.ID, Source: "direct", SourceID: "another", OriginalRequest: w.OriginalRequest, Criteria: w.Criteria, Policy: w.Policy})
				if err != nil {
					t.Fatal(err)
				}
				request.RequestID = other.SourceID
			case "paused":
				w.Paused = true
			case "canceled":
				w.State = domain.WaveCanceled
			case "stale":
				w.ScopeRevision++
			}
			if mode == "paused" || mode == "canceled" || mode == "stale" {
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err = saveWave(ctx, tx, w); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			gateway := contract.KernelGateway{Backend: k}
			call := kernelCall(t, b, run, "wave_create", request)
			reply, err := gateway.Handle(ctx, call)
			if mode != "owned" {
				if err == nil {
					t.Fatal("unregistered/held Wave admitted", reply)
				}
				return
			}
			if err != nil || reply.State != "accepted" || reply.ResourceID != w.ID || len(reply.SubscriptionIDs) != 1 {
				t.Fatal(reply, err)
			}
			replay, err := gateway.Handle(ctx, call)
			if err != nil || !reflect.DeepEqual(reply, replay) {
				t.Fatal(replay, err)
			}
			var waves, operations int
			if err = k.DB.QueryRow("SELECT count(*) FROM kernel_waves").Scan(&waves); err != nil || waves != 1 {
				t.Fatal("tool invented another Wave", waves, err)
			}
			if err = k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations").Scan(&operations); err != nil || operations != 1 {
				t.Fatal(operations, err)
			}
		})
	}
}
func TestWavePlanToolAcceptsAndSubscribesAtomically(t *testing.T) {
	ctx := context.Background()
	k, w, b, run := waveToolFixture(t)
	gateway := contract.KernelGateway{Backend: k}
	call := kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: storagePlanFixture()})
	reply, err := gateway.Handle(ctx, call)
	if err != nil || reply.State != "accepted" || reply.ResourceID != w.ID || len(reply.SubscriptionIDs) != 1 {
		t.Fatal(reply, err)
	}
	var view struct {
		Target  contract.Target `json:"target"`
		Created int             `json:"created_tasks"`
	}
	if err = json.Unmarshal([]byte(reply.Content), &view); err != nil || view.Target != w.Target() || view.Created != 2 {
		t.Fatal(view, err)
	}
	after, err := k.Wave(ctx, w.ID)
	if err != nil || after.TasksCreated != 2 || after.PhasesCreated != 2 || after.MainTurns != 1 || after.Replans != 0 || after.State != domain.WaveRunning {
		t.Fatal(after, err)
	}
	replay, err := gateway.Handle(ctx, call)
	if err != nil || !reflect.DeepEqual(reply, replay) {
		t.Fatal(replay, err)
	}
	sub, err := k.Subscribe(ctx, SubscriptionRequest{SessionID: b.ID, Target: w.Target(), Key: reply.OperationID, Kind: "wave_terminal"})
	if err != nil || sub.ID != reply.SubscriptionIDs[0] {
		t.Fatal(sub, err)
	}
	// The asynchronous background Wave remains unfinished until controller gates
	// produce a terminal event. Accepted work is never a completed-Wave claim.
	if _, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel", Target: w.Target(), Action: "cancel", Reason: "observe terminal subscription"}); err != nil {
		t.Fatal(err)
	}
	n, err := k.Notifications(ctx, b.ID, 0, 100)
	if err != nil || len(n) != 1 || n[0].Kind != "wave_terminal" || n[0].WaveState != domain.WaveCanceled {
		t.Fatal(n, err)
	}
}
func TestWavePlanToolOperationFailureRollsBackPlanAndSubscription(t *testing.T) {
	ctx := context.Background()
	k, w, b, run := waveToolFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER reject_wave_tool BEFORE INSERT ON kernel_tool_operations BEGIN SELECT RAISE(ABORT,'operation failure'); END"); err != nil {
		t.Fatal(err)
	}
	call := kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: storagePlanFixture()})
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("unaudited plan accepted")
	}
	for _, table := range []string{"kernel_plans", "kernel_phases", "kernel_tasks", "kernel_subscriptions"} {
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal(table, count, err)
		}
	}
	after, err := k.Wave(ctx, w.ID)
	if err != nil || after.TasksCreated != 0 || after.PhasesCreated != 0 || after.Replans != 0 {
		t.Fatal(after, err)
	}
}
func TestWaveToolRejectsTaskNestedWaveAndInventedCriteria(t *testing.T) {
	ctx := context.Background()
	k, w, _, b, run := kernelToolFixture(t, "task")
	for name, args := range map[string]any{"wave_create": contract.CreateWaveRequest{SchemaVersion: 1, RequestID: w.SourceID}, "wave_plan": contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: storagePlanFixture()}} {
		if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, kernelCall(t, b, run, name, args)); err == nil {
			t.Fatal("Task managed Wave", name)
		}
	}
	k, w, b, run = waveToolFixture(t)
	plan := storagePlanFixture()
	plan.Phases[0].Tasks[0].Criteria = []string{"invented"}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: plan})); err == nil {
		t.Fatal("plan enlarged trusted criteria")
	}
}
func TestMainAcknowledgesSameRunToolPlanWithoutReplanOrWaitingForTask(t *testing.T) {
	ctx := context.Background()
	k, w, b, run := waveToolFixture(t)
	proposal := storagePlanFixture()
	call := kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: proposal})
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := k.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	child := prepareWakeSession(t, k, tasks[0].SessionID)
	childRun := contract.SessionRun{ID: "child-running", ThreadID: child.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, child, sessionSnapshot(), childRun); err != nil {
		t.Fatal(err)
	}
	final := contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{{Target: w.Target(), Action: "plan", Plan: &proposal, Summary: "accepted plan", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}}}}
	raw, err := json.Marshal(final)
	if err != nil {
		t.Fatal(err)
	}
	run.State = "completed"
	run.TurnID = "tool-turn"
	run.Final = string(raw)
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != "applied" || len(receipt.Plans) != 1 || receipt.Plans[0].Key != reply.OperationID {
		t.Fatal(receipt, err)
	}
	after, err := k.Wave(ctx, w.ID)
	if err != nil || after.TasksCreated != 2 || after.Replans != 0 || after.MainTurns != 1 {
		t.Fatal("final ack repeated plan effects", after, err)
	}
	if _, err = k.ApplyMainResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
}

func renamedWaveToolPlan(prefix string) contract.PlanProposal {
	p := storagePlanFixture()
	for i := range p.Phases {
		phase := &p.Phases[i]
		phase.ID = prefix + phase.ID
		for j := range phase.Tasks {
			task := &phase.Tasks[j]
			task.ID = prefix + task.ID
			for d := range task.DependsOn {
				task.DependsOn[d].TaskID = prefix + task.DependsOn[d].TaskID
			}
		}
		for j := range phase.Completion.RequiredTaskIDs {
			phase.Completion.RequiredTaskIDs[j] = prefix + phase.Completion.RequiredTaskIDs[j]
		}
	}
	return p
}
func existingWaveToolFixture(t *testing.T) (*KernelStore, domain.Wave, contract.SessionBinding, contract.SessionRun, contract.InputSnapshot) {
	t.Helper()
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture()); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, "main")
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "existing-wave-tool-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	return k, w, b, run, snapshot
}
func TestWavePlanReplanPreservesSameRunToolsReplayAndFinalScope(t *testing.T) {
	for _, action := range []string{"wait", "plan"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			k, w, b, run, snapshot := existingWaveToolFixture(t)
			priorState := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: w.Target()})
			priorState.CallID = "state-before-replan"
			priorReply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, priorState)
			if err != nil {
				t.Fatal(err)
			}
			proposal := renamedWaveToolPlan("new-")
			call := kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: proposal})
			call.CallID = "replan"
			gateway := contract.KernelGateway{Backend: k}
			accepted, err := gateway.Handle(ctx, call)
			if err != nil {
				t.Fatal(err)
			}
			var replyView struct {
				Target contract.Target `json:"target"`
			}
			if err = json.Unmarshal([]byte(accepted.Content), &replyView); err != nil || replyView.Target.PlanRevision != w.PlanRevision+1 {
				t.Fatal(replyView, err)
			}
			current := replyView.Target
			stateReplay, err := gateway.Handle(ctx, priorState)
			if err != nil || !reflect.DeepEqual(stateReplay, priorReply) {
				t.Fatal("exact earlier read replay rejected or recomputed", stateReplay, err)
			}
			changedState := priorState
			changedState.Arguments, _ = json.Marshal(contract.StateRequest{SchemaVersion: 1, Target: w.Target(), Page: &contract.StatePage{Collection: "tasks", Limit: 1}})
			if _, err = gateway.Handle(ctx, changedState); err == nil {
				t.Fatal("same call ID changed its arguments after replan")
			}

			replay, err := gateway.Handle(ctx, call)
			if err != nil || !reflect.DeepEqual(accepted, replay) {
				t.Fatal("accepted replan cannot replay", replay, err)
			}
			waitCall := kernelCall(t, b, run, "session_wait", contract.WaitRequest{SchemaVersion: 1, Target: current, SubscriptionIDs: accepted.SubscriptionIDs})
			waitCall.CallID = "wait"
			if _, err = gateway.Handle(ctx, waitCall); err != nil {
				t.Fatal("replan invalidated its own wait", err)
			}
			stale := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: w.Target()})
			stale.CallID = "stale-new-call"
			if _, err = gateway.Handle(ctx, stale); err == nil {
				t.Fatal("old scope authorized a new tool call")
			}
			stateCall := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: current})
			stateCall.CallID = "current-state"
			if _, err = gateway.Handle(ctx, stateCall); err != nil {
				t.Fatal(err)
			}
			// Original judgment bytes keep their original version label even though
			// access is authorized through this run's newly accepted current target.
			var input struct {
				Context SessionContext `json:"context"`
			}
			if err = json.Unmarshal(snapshot.Input, &input); err != nil {
				t.Fatal(err)
			}
			ref := input.Context.Waves[0].Manifest
			read := kernelCall(t, b, run, "context_read", contract.ContextReadRequest{SchemaVersion: 1, Target: current, Section: "manifest", ReferenceID: ref.ID, Limit: 2048})
			read.CallID = "original-input"
			result, err := gateway.Handle(ctx, read)
			if err != nil {
				t.Fatal(err)
			}
			var slice contract.ContextSlice
			if err = json.Unmarshal([]byte(result.Content), &slice); err != nil || slice.Target != w.Target() || slice.Digest != ref.Digest {
				t.Fatal("immutable input was relabeled as current state", slice, err)
			}
			decision := contract.MainDecision{Target: current, Action: action, Summary: "accepted replan", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}}
			if action == "wait" {
				decision.SubscriptionIDs = accepted.SubscriptionIDs
			} else {
				decision.Plan = &proposal
			}
			final := contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{decision}}
			raw, err := json.Marshal(final)
			if err != nil {
				t.Fatal(err)
			}
			run.State = "completed"
			run.TurnID = call.TurnID
			run.Final = string(raw)
			if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
				t.Fatal(err)
			}
			if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			application, err := k.ApplyMainResult(ctx, b.ID, run.ID)
			if err != nil || application.State != "applied" || !reflect.DeepEqual(application.Targets, []contract.Target{current}) {
				t.Fatal("authorized own replan archived final result", application, err)
			}
			var budget RunBudgetBinding
			if err = k.Record(ctx, "run_budget", run.ID, &budget); err != nil || !reflect.DeepEqual(budget.Targets, []contract.Target{w.Target()}) || budget.SnapshotID != snapshot.ID {
				t.Fatal("immutable budget input was rewritten", budget, err)
			}
			after, err := k.Wave(ctx, w.ID)
			if err != nil || after.MainTurns != 1 || after.Replans != 1 || after.TasksCreated != 4 {
				t.Fatal("replan acknowledgment duplicated budgets", after, err)
			}
		})
	}
}
func TestWavePlanTransitionDoesNotAuthorizeAnotherControllersReplan(t *testing.T) {
	ctx := context.Background()
	k, w, b, run, _ := existingWaveToolFixture(t)
	gateway := contract.KernelGateway{Backend: k}
	accepted, err := gateway.Handle(ctx, kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: renamedWaveToolPlan("own-")}))
	if err != nil {
		t.Fatal(err)
	}
	var view struct {
		Target contract.Target `json:"target"`
	}
	json.Unmarshal([]byte(accepted.Content), &view)
	external, err := k.AdoptPlan(ctx, view.Target, "other-controller-decision", renamedWaveToolPlan("external-"))
	if err != nil {
		t.Fatal(err)
	}
	call := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: external.Target})
	call.CallID = "unapproved-latest"
	if _, err = gateway.Handle(ctx, call); err == nil {
		t.Fatal("another decision's revision was inferred into this run")
	}
}
func TestReplanTransitionRollsBackWithItsOperationRecord(t *testing.T) {
	ctx := context.Background()
	k, w, b, run, _ := existingWaveToolFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_replan_operation BEFORE INSERT ON kernel_tool_operations BEGIN SELECT RAISE(ABORT,'operation failure'); END"); err != nil {
		t.Fatal(err)
	}
	call := kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: renamedWaveToolPlan("new-")})
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("unrecorded transition accepted")
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='run_plan_transition'").Scan(&count); err != nil || count != 0 {
		t.Fatal("rolled-back scope transition survived", count, err)
	}
	after, err := k.Wave(ctx, w.ID)
	if err != nil || after.Target() != w.Target() || after.Replans != 0 || after.TasksCreated != 2 {
		t.Fatal(after, err)
	}
}

func TestInitialWavePlanCallCanReplayAfterAnotherSameRunReplan(t *testing.T) {
	ctx := context.Background()
	k, w, b, run := waveToolFixture(t)
	gateway := contract.KernelGateway{Backend: k}
	first := kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: storagePlanFixture()})
	first.CallID = "initial-plan"
	original, err := gateway.Handle(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	second := kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: renamedWaveToolPlan("new-")})
	second.CallID = "replan"
	if _, err = gateway.Handle(ctx, second); err != nil {
		t.Fatal(err)
	}
	replay, err := gateway.Handle(ctx, first)
	if err != nil || !reflect.DeepEqual(original, replay) {
		t.Fatal("initial acceptance has no transition but must replay", replay, err)
	}
	changed := first
	changed.CallID = "new-old-target-call"
	if _, err = gateway.Handle(ctx, changed); err == nil {
		t.Fatal("new old-target plan call was accepted as replay")
	}
}
func TestWaveReplanWaitsForPendingMainOperation(t *testing.T) {
	ctx := context.Background()
	k, w, b, run, _ := existingWaveToolFixture(t)
	// A publication intent may already be durable while its file write is still
	// pending. Do not advance the plan and invalidate that authorized old-target
	// operation/subscription before it has a known outcome.
	op := kernelToolOperation{ID: "pending-publication", SessionID: b.ID, RunID: run.ID, ThreadID: b.ThreadID, TurnID: "tool-turn", CallID: "publish", Name: "artifact_publish", Target: w.Target()}
	raw, _ := json.Marshal(op)
	if _, err := k.DB.Exec("INSERT INTO kernel_tool_operations(id,session_id,thread_id,turn_id,call_id,name,argument_digest,state,result,payload) VALUES(?,?,?,?,?,?,'digest','accepted','{}',?)", op.ID, b.ID, b.ThreadID, op.TurnID, op.CallID, op.Name, raw); err != nil {
		t.Fatal(err)
	}
	call := kernelCall(t, b, run, "wave_plan", contract.PlanWaveRequest{SchemaVersion: 1, Target: w.Target(), Plan: renamedWaveToolPlan("new-")})
	call.CallID = "replan"
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); !errors.Is(err, ErrPlanAdmissionHeld) {
		t.Fatal("pending operation invalidated by a replan", err)
	}
	after, err := k.Wave(ctx, w.ID)
	if err != nil || after.Target() != w.Target() {
		t.Fatal(after, err)
	}
}
