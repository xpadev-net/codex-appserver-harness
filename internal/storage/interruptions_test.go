package storage

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func interruptionFixture(t *testing.T) (*KernelStore, domain.Wave, contract.SessionBinding, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	k, w, b := managedTaskFixture(t)
	r := managedReserve(t, k, b, "owner", "run", "execute")
	managedStart(t, k, r, 42)
	b = managedBind(t, k, r)
	run := contract.SessionRun{ID: "run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err := k.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err != nil {
		t.Fatal(err)
	}
	return k, w, b, run
}
func interruptRequest(b contract.SessionBinding, run contract.SessionRun) SessionInterruptRequest {
	return SessionInterruptRequest{Key: "interrupt", Target: b.Target, SessionID: b.ID, RunID: run.ID, PreparationID: b.PreparationID, Reason: "explicit operator stop"}
}

func TestInterruptHoldsCapacityUntilProofAndNeverInfersRunOutcome(t *testing.T) {
	ctx := context.Background()
	k, w, b, run := interruptionFixture(t)
	if err := k.CheckManagedRunSend(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	i, err := k.RequestSessionInterrupt(ctx, interruptRequest(b, run))
	if err != nil {
		t.Fatal(err)
	}
	managedCapacity(t, k, 1)
	if err = k.CompleteSessionInterrupt(ctx, i.ID); !errors.Is(err, ErrInterruptionHeld) {
		t.Fatal("interrupt released an unconfirmed process", err)
	}
	if err = k.CheckManagedRunSend(ctx, "owner"); err == nil {
		t.Fatal("interrupt did not fence turn/start")
	}
	call := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: w.Target()})
	if _, err = (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("interrupted run retained tool authority")
	}
	if err = k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "owner", Disposition: "group_stopped", Identity: managedIdentity(42)}); err != nil {
		t.Fatal(err)
	}
	if err = k.CompleteSessionInterrupt(ctx, i.ID); err != nil {
		t.Fatal(err)
	}
	managedCapacity(t, k, 0)
	var saved contract.SessionRun
	if err = k.Record(ctx, "agent_run", run.ID, &saved); err != nil || !reflect.DeepEqual(saved, run) {
		t.Fatal("stopping fabricated a model outcome", saved, run, err)
	}
	if err = k.ArchiveSessionRun(ctx, b.ID, run.ID, "unknown interrupted history"); err == nil {
		t.Fatal("unknown run discarded without history reconciliation")
	}
	task, err := k.Task(ctx, b.TaskID)
	if err != nil || task.State != contract.TaskBlocked || task.Attempts != 1 {
		t.Fatal(task, err)
	}
	permit := sessionSnapshot()
	if err = k.AuthorizeTaskRecovery(ctx, task, "future", permit, "classified retry"); err == nil {
		t.Fatal("unreconciled interrupted run permitted another turn")
	}
	if interrupted, err := k.RunWasInterrupted(ctx, b.ID, run.ID); err != nil || !interrupted {
		t.Fatal(interrupted, err)
	}
}

func TestInterruptNormalLateCompletionIsArchivedAsBlockedAndCancelWins(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "interrupt", true: "cancel"}[cancel], func(t *testing.T) {
			ctx := context.Background()
			k, w, b, run := interruptionFixture(t)
			artifact := mustImport(t, k, ArtifactImport{SessionID: b.ID, Target: b.Target, Key: "output", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: b.TaskID}}, "late report")
			result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, PhaseID: b.PhaseID, TaskID: b.TaskID, Outcome: "result", Artifacts: []string{artifact.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "report", EvidenceIDs: []string{artifact.ID}}}}
			final, err := marshalTaskResultFixture(result)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = k.RequestSessionInterrupt(ctx, interruptRequest(b, run)); err != nil {
				t.Fatal(err)
			}
			if cancel {
				mustControl(t, k, operatorRequest(w, "cancel", "cancel", b.TaskID))
			}
			run.State, run.TurnID, run.Final = "completed", "turn", string(final)
			if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
				t.Fatal(err)
			}
			if err = k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "owner", Disposition: "group_stopped", Identity: managedIdentity(42)}); err != nil {
				t.Fatal(err)
			}
			application, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
			want := contract.TaskBlocked
			if cancel {
				want = contract.TaskCanceled
			}
			if err != nil || !application.Archived || application.State != want || application.ResultID != "" {
				t.Fatal("interrupted completion became success", application, err)
			}
			var physical contract.SessionRun
			if err := k.Record(ctx, "agent_run", run.ID, &physical); err != nil || physical.Final != run.Final {
				t.Fatal("late outcome was lost", physical, err)
			}
		})
	}
}

func TestInterruptReopenReplayAndWrongGenerationRejection(t *testing.T) {
	ctx := context.Background()
	k, _, b, run := interruptionFixture(t)
	r := interruptRequest(b, run)
	for _, change := range []func(*SessionInterruptRequest){
		func(r *SessionInterruptRequest) { r.PreparationID = "other" },
		func(r *SessionInterruptRequest) { r.RunID = "other" },
		func(r *SessionInterruptRequest) { r.SessionID = "main" },
		func(r *SessionInterruptRequest) { r.Target.ScopeRevision++ },
	} {
		bad := r
		change(&bad)
		if _, err := k.RequestSessionInterrupt(ctx, bad); err == nil {
			t.Fatal("foreign interruption source accepted", bad)
		}
	}
	first, err := k.RequestSessionInterrupt(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if replay, err := k.RequestSessionInterrupt(ctx, r); err != nil || !reflect.DeepEqual(replay, first) {
		t.Fatal(replay, err)
	}
	conflict := r
	conflict.Reason += " changed"
	if _, err := k.RequestSessionInterrupt(ctx, conflict); err == nil {
		t.Fatal("conflicting interruption replay accepted")
	}
	pending, err := k.PendingSessionInterrupts(ctx, "", 1)
	if err != nil || len(pending.Interruptions) != 1 || pending.Interruptions[0].ID != first.ID {
		t.Fatal(pending, err)
	}
}

func TestInterruptEventFailureRollsBackTaskAndAllStopFences(t *testing.T) {
	ctx := context.Background()
	k, _, b, run := interruptionFixture(t)
	before, err := k.Task(ctx, b.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.ExecContext(ctx, `CREATE TRIGGER fail_interrupt BEFORE INSERT ON kernel_events WHEN NEW.kind='run_interrupt_requested' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err = k.RequestSessionInterrupt(ctx, interruptRequest(b, run)); err == nil {
		t.Fatal("interruption event failure accepted")
	}
	if err = k.CheckManagedRunSend(ctx, "owner"); err != nil {
		t.Fatal("failed request left a stop fence", err)
	}
	after, err := k.Task(ctx, b.TaskID)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal(after, before, err)
	}
	if interrupted, err := k.RunWasInterrupted(ctx, b.ID, run.ID); err != nil || interrupted {
		t.Fatal(interrupted, err)
	}
}

func TestInterruptMainRequiresItsRunTargetAndDoesNotCancelChildren(t *testing.T) {
	ctx := context.Background()
	k, w, child, _ := interruptionFixture(t)
	main, err := k.SessionBinding(ctx, w.OrchestratorSessionID)
	if err != nil {
		t.Fatal(err)
	}
	r := managedReserve(t, k, main, "main-owner", "main-run", "execute")
	managedStart(t, k, r, 88)
	main = managedBind(t, k, r)
	run := contract.SessionRun{ID: "main-run", ThreadID: main.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
	if err = k.ReserveSessionRun(ctx, main, mainBudgetSnapshot(t, "main-input", main.ID, w.Target()), run); err != nil {
		t.Fatal(err)
	}
	request := SessionInterruptRequest{Key: "main-stop", Target: w.Target(), SessionID: main.ID, RunID: run.ID, PreparationID: main.PreparationID, Reason: "stop this parent run"}
	bad := request
	bad.Target.ScopeRevision++
	if _, err = k.RequestSessionInterrupt(ctx, bad); err == nil {
		t.Fatal("foreign main target interrupted")
	}
	i, err := k.RequestSessionInterrupt(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if interrupted, err := k.RunWasInterrupted(ctx, main.ID, run.ID); err != nil || !interrupted {
		t.Fatal(interrupted, err)
	}
	task, err := k.Task(ctx, child.TaskID)
	if err != nil || task.State != contract.TaskRunning {
		t.Fatal("parent interruption cascaded to child", task, err)
	}
	if err = k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "main-owner", Disposition: "group_stopped", Identity: managedIdentity(88)}); err != nil {
		t.Fatal(err)
	}
	if err = k.CompleteSessionInterrupt(ctx, i.ID); err != nil {
		t.Fatal(err)
	}
	managedCapacity(t, k, 1)
}

func TestManagedRunSendRechecksControlsAfterTurnReservation(t *testing.T) {
	for _, action := range []string{"wave-pause", "task-pause", "wave-cancel", "task-cancel", "direction", "interrupt"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			k, w, b, run := interruptionFixture(t)
			if err := k.CheckManagedRunSend(ctx, "owner"); err != nil {
				t.Fatal(err)
			}
			switch action {
			case "direction":
				mustDirection(t, k, w)
			case "interrupt":
				if _, err := k.RequestSessionInterrupt(ctx, interruptRequest(b, run)); err != nil {
					t.Fatal(err)
				}
			default:
				verb, taskID := "pause", ""
				if action == "wave-cancel" || action == "task-cancel" {
					verb = "cancel"
				}
				if action == "task-pause" || action == "task-cancel" {
					taskID = b.TaskID
				}
				mustControl(t, k, operatorRequest(w, "control", verb, taskID))
			}
			if err := k.CheckManagedRunSend(ctx, "owner"); err == nil {
				t.Fatal("late control bypassed the send gate")
			}
			managedCapacity(t, k, 1)
			var saved contract.SessionRun
			if err := k.Record(ctx, "agent_run", run.ID, &saved); err != nil || saved.State != "starting" {
				t.Fatal("send rejection fabricated an outcome", saved, err)
			}
		})
	}
}

func TestInterruptedTaskKeepsUnprocessedWakeInputsPending(t *testing.T) {
	ctx := context.Background()
	k, w, b := managedTaskFixture(t)
	message, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: w.OrchestratorSessionID, Target: w.Target(), Key: "input", Recipient: b.TaskID, Body: "unprocessed operator input"})
	if err != nil {
		t.Fatal(err)
	}
	items, err := k.PendingWakeNotifications(ctx, b.ID, 10)
	if err != nil || len(items) != 1 {
		t.Fatal(items, err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: b.ID, Key: "wake", NotificationIDs: []string{items[0].ID}})
	if err != nil {
		t.Fatal(err)
	}
	r := managedReserve(t, k, b, "owner", wake.Intent.RunID, "execute")
	managedStart(t, k, r, 42)
	b = managedBind(t, k, r)
	run := contract.SessionRun{ID: wake.Intent.RunID, ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	if _, err = k.RequestSessionInterrupt(ctx, interruptRequest(b, run)); err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID, run.Final = "completed", "turn", `{"ignored":"interrupted result"}`
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "owner", Disposition: "group_stopped", Identity: managedIdentity(42)}); err != nil {
		t.Fatal(err)
	}
	application, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || application.State != contract.TaskBlocked || !application.Archived {
		t.Fatal(application, err)
	}
	for _, query := range []string{
		"SELECT state,run_id FROM kernel_notifications WHERE id=?",
		"SELECT state,run_id FROM kernel_mailbox WHERE message_id=?",
	} {
		id := items[0].ID
		if query == "SELECT state,run_id FROM kernel_mailbox WHERE message_id=?" {
			id = message.ID
		}
		var state, runID string
		if err = k.DB.QueryRowContext(ctx, query, id).Scan(&state, &runID); err != nil || state != "pending" || runID != "" {
			t.Fatal("interruption lost pending input", state, runID, err)
		}
	}
	set, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
	if err != nil || len(set.Tasks) != 0 {
		t.Fatal("pending input automatically retried blocked work", set, err)
	}
}
