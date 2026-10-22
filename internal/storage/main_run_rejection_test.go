package storage

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func mainRejectionFixture(t *testing.T) (*KernelStore, contract.SessionBinding, []domain.Wave, contract.SessionRun, MainRunRejectionRequest) {
	t.Helper()
	ctx := context.Background()
	k, b, waves, run, _ := mainResultFixture(t)
	run.State, run.TurnID, run.Final = "completed", "main-turn", "invalid main output"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	next, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{waves[0].Target(), waves[1].Target()})
	if err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := MainRunRejectionRequest{Key: "reject-output", Expected: b, RunID: run.ID, Snapshot: next, Reason: "operator rejected the invalid known output"}
	return k, b, waves, run, request
}
func TestKnownMainRejectionIsAtomicAuditedAndKeepsRunBudgets(t *testing.T) {
	ctx := context.Background()
	k, b, waves, run, request := mainRejectionFixture(t)
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); !errors.Is(err, ErrMainResultInvalid) {
		t.Fatal(err)
	}
	var budget RunBudgetBinding
	if err := k.Record(ctx, "run_budget", run.ID, &budget); err != nil {
		t.Fatal(err)
	}
	rejected, err := k.RejectStoppedMainRun(ctx, request)
	if err != nil || rejected.RunState != "completed" || rejected.Rotation.OldThreadID != b.ThreadID || rejected.Rotation.State != "pending" || rejected.Rotation.SnapshotID != request.Snapshot.ID {
		t.Fatal(rejected, err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil || current.ThreadID != "" || current.Generation != b.Generation+1 || current.PreparationID != "" {
		t.Fatal(current, err)
	}
	var oldRun contract.SessionRun
	if err = k.Record(ctx, "agent_run", run.ID, &oldRun); err != nil || !reflect.DeepEqual(oldRun, run) {
		t.Fatal("rejection changed physical run", oldRun, err)
	}
	var retained RunBudgetBinding
	if err = k.Record(ctx, "run_budget", run.ID, &retained); err != nil || !reflect.DeepEqual(retained, budget) {
		t.Fatal("rejection changed budget ledger", retained, err)
	}
	eligible, err := k.PlanningInputs(ctx, b.ID, "", 16)
	if err != nil || len(eligible.Targets) != 2 {
		t.Fatal("explicit decision did not release initial planning", eligible, err)
	}
	for _, wave := range waves {
		actual, err := k.Wave(ctx, wave.ID)
		if err != nil || actual.MainTurns != 1 || actual.Replans != 0 || actual.TasksCreated != 0 {
			t.Fatal(actual, err)
		}
	}
	path := filepath.Join(k.Root, "state.db")
	if err = k.Close(); err != nil {
		t.Fatal(err)
	}
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.RejectStoppedMainRun(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, rejected) {
		t.Fatal(replay, err)
	}
	changed := request
	changed.Reason = "different decision"
	if _, err = k.RejectStoppedMainRun(ctx, changed); err == nil {
		t.Fatal("conflicting replay accepted")
	}
	current = prepareWakeSession(t, k, b.ID)
	if current.ThreadID == b.ThreadID {
		t.Fatal("old thread reused")
	}
	nextRun := contract.SessionRun{ID: "new-main-run", ThreadID: current.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
	if err = k.ReserveSessionRun(ctx, current, request.Snapshot, nextRun); err != nil {
		t.Fatal(err)
	}
	for _, wave := range waves {
		actual, err := k.Wave(ctx, wave.ID)
		if err != nil || actual.MainTurns != 2 {
			t.Fatal("retry reset cumulative counter", actual, err)
		}
	}
}
func TestMainRejectionHoldsUnknownLiveForeignAndUncertainRuns(t *testing.T) {
	for _, mode := range []string{"unknown", "live", "stale_binding", "foreign_snapshot", "unreconciled_operation", "unknown_owner"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, b, waves, run, request := mainRejectionFixture(t)
			switch mode {
			case "unknown":
				run.State = "unknown"
				raw, err := json.Marshal(run)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = k.DB.ExecContext(ctx, "UPDATE records SET payload=? WHERE kind='agent_run' AND id=?", raw, run.ID); err != nil {
					t.Fatal(err)
				}
				if _, err = k.DB.ExecContext(ctx, "UPDATE kernel_run_bindings SET state='unknown' WHERE run_id=?", run.ID); err != nil {
					t.Fatal(err)
				}
			case "live":
				_, _ = k.DB.ExecContext(ctx, "UPDATE kernel_sessions SET process_stopped=0 WHERE id=?", b.ID)
			case "stale_binding":
				request.Expected.Generation++
			case "foreign_snapshot":
				request.Snapshot = mainBudgetSnapshot(t, "another-set", b.ID, waves[0].Target())
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := saveInputSnapshotTx(ctx, tx, request.Snapshot); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "unreconciled_operation":
				op := kernelToolOperation{ID: "uncertain", SessionID: b.ID, RunID: run.ID, Target: waves[0].Target()}
				raw, err := json.Marshal(op)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = k.DB.ExecContext(ctx, `INSERT INTO kernel_tool_operations(id,session_id,thread_id,turn_id,call_id,name,argument_digest,state,result,payload) VALUES(?,?,?,'turn','call','artifact_publish','digest','unknown',NULL,?)`, op.ID, b.ID, b.ThreadID, raw); err != nil {
					t.Fatal(err)
				}
			case "unknown_owner":
				process := ManagedProcess{State: "started", Request: ManagedProcessRequest{OwnerID: "another-owner", Binding: b}}
				if err := k.Put(ctx, "managed_process", "another-owner", b.ID, process); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.RejectStoppedMainRun(ctx, request); err == nil {
				t.Fatal("unreconciled recovery authorized", mode)
			}
			var applied bool
			if err := k.DB.QueryRowContext(ctx, "SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&applied); err != nil || applied {
				t.Fatal("partial disposition", applied, err)
			}
			current, err := k.SessionBinding(ctx, b.ID)
			if err != nil || current.ThreadID != b.ThreadID {
				t.Fatal("partial thread replacement", current, err)
			}
		})
	}
}

func TestMainRejectionRotationFailureRollsBackRunDisposition(t *testing.T) {
	ctx := context.Background()
	k, b, _, run, request := mainRejectionFixture(t)
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_rejection_rotation BEFORE INSERT ON kernel_rebindings BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RejectStoppedMainRun(ctx, request); err == nil {
		t.Fatal("rejection committed without rotation audit")
	}
	var active string
	var applied bool
	if err := k.DB.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&active); err != nil || active != run.ID {
		t.Fatal(active, err)
	}
	if err := k.DB.QueryRowContext(ctx, "SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&applied); err != nil || applied {
		t.Fatal(applied, err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil || !reflect.DeepEqual(current, b) {
		t.Fatal(current, err)
	}
	for _, kind := range []string{"main_result_application", "main_run_rejection", "main_thread_rotation"} {
		var count int
		if err = k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind=?", kind).Scan(&count); err != nil || count != 0 {
			t.Fatal(kind, count, err)
		}
	}
}

func TestMainRejectionReturnsSharedMessagesToPending(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	waves := []domain.Wave{}
	for _, id := range []string{"one", "two"} {
		request.SourceID = id
		wave, err := k.CreateWave(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		admission, err := k.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture())
		if err != nil {
			t.Fatal(err)
		}
		task, err := k.Task(ctx, admission.CreatedTaskIDs[0])
		if err != nil {
			t.Fatal(err)
		}
		if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: task.SessionID, Target: wave.Target(), Recipient: "parent", Key: "question", Body: "Need parent judgement"}); err != nil {
			t.Fatal(err)
		}
		waves = append(waves, wave)
	}
	notifications, err := k.PendingWakeNotifications(ctx, "main", 16)
	if err != nil || len(notifications) != 2 {
		t.Fatal(notifications, err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: "main", Key: "shared", NotificationIDs: []string{notifications[0].ID, notifications[1].ID}, IncludeSessionContext: true})
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, "main")
	run := wakeStartingRun(k, b, wake)
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID, run.Final = "completed", "known-main-turn", "invalid known output"
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	next, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{waves[0].Target(), waves[1].Target()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.RejectStoppedMainRun(ctx, MainRunRejectionRequest{Key: "reject-shared", Expected: b, RunID: run.ID, Snapshot: next, Reason: "reject invalid output and retain questions"}); err != nil {
		t.Fatal(err)
	}
	pending, err := k.PendingWakeNotifications(ctx, b.ID, 16)
	if err != nil || len(pending) != 2 {
		t.Fatal("shared input lost", pending, err)
	}
	for _, n := range pending {
		var state, owner string
		if err = k.DB.QueryRowContext(ctx, "SELECT state,run_id FROM kernel_mailbox WHERE message_id=? AND session_id=?", n.MessageID, b.ID).Scan(&state, &owner); err != nil || state != "pending" || owner != "" {
			t.Fatal(state, owner, err)
		}
	}
	retired, err := k.Wake(ctx, wake.Intent.ID)
	if err != nil || retired.Intent.State != "superseded" {
		t.Fatal(retired, err)
	}
	newer, err := k.ReserveWake(ctx, WakeRequest{SessionID: b.ID, Key: "retry-shared", NotificationIDs: []string{pending[0].ID, pending[1].ID}, IncludeSessionContext: true})
	if err != nil || newer.Intent.RunID == run.ID {
		t.Fatal(newer, err)
	}
}
