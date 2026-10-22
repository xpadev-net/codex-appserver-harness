package storage

import (
	"context"
	"encoding/json"
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
