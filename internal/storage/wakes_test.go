package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func wakeFixture(t *testing.T) (*KernelStore, domain.Wave, domain.Task, Message, WakeRequest) {
	t.Helper()
	k, wave, tasks := mailboxFixture(t)
	m, err := k.SendMessage(context.Background(), MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "question", Recipient: tasks[0].ID, Body: "Please explain the source."})
	if err != nil {
		t.Fatal(err)
	}
	n, err := k.PendingWakeNotifications(context.Background(), tasks[0].SessionID, 10)
	if err != nil || len(n) != 1 {
		t.Fatal(n, err)
	}
	return k, wave, tasks[0], m, WakeRequest{SessionID: tasks[0].SessionID, Key: "resume", NotificationIDs: []string{n[0].ID}, Context: json.RawMessage(`{"original_request":"report with evidence"}`)}
}
func prepareWakeSession(t *testing.T, k *KernelStore, session string) contract.SessionBinding {
	t.Helper()
	ctx := context.Background()
	b, err := k.SessionBinding(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, k, b)
	if b.ThreadID == "" {
		if err = k.BindSessionThread(ctx, b, "thread-"+b.ID); err != nil {
			t.Fatal(err)
		}
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func wakeStartingRun(k *KernelStore, b contract.SessionBinding, w WakeReservation) contract.SessionRun {
	return contract.SessionRun{ID: w.Intent.RunID, ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
}
func completeWakeWaiting(t *testing.T, k *KernelStore, b contract.SessionBinding, run contract.SessionRun) contract.SessionRun {
	t.Helper()
	r := contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "waiting", SubscriptionIDs: []string{mailboxSubscriptionID(b.ID, b.Target, b.TaskID)}}
	raw, err := marshalTaskResultFixture(r)
	if err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID, run.Final = "completed", "turn-"+run.ID, string(raw)
	if err = k.Put(context.Background(), "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	return run
}
func TestWakeReservationIsReplayableAndInputDoesNotDependOnDeliveryAck(t *testing.T) {
	ctx := context.Background()
	k, _, task, m, request := wakeFixture(t)
	if err := k.AcknowledgeNotification(ctx, task.SessionID, request.NotificationIDs[0]); err != nil {
		t.Fatal(err)
	}
	if err := k.AcknowledgeMessage(ctx, task.SessionID, m.ID); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	group.Add(2)
	out := make([]WakeReservation, 2)
	errs := make([]error, 2)
	for i := range out {
		go func(i int) { defer group.Done(); out[i], errs[i] = k.ReserveWake(ctx, request) }(i)
	}
	group.Wait()
	if errs[0] != nil || errs[1] != nil || !reflect.DeepEqual(out[0], out[1]) {
		t.Fatal(out, errs)
	}
	w := out[0]
	if w.Intent.State != "reserved" || w.Intent.RunID == "" || w.Snapshot.ID == "" {
		t.Fatal(w)
	}
	var input wakeInput
	if err := json.Unmarshal(w.Snapshot.Input, &input); err != nil || len(input.Messages) != 1 || input.Messages[0].ID != m.ID || input.Messages[0].Body != m.Request.Body || input.Messages[0].BodyEncoding != "utf-8" || len(input.Notifications) != 1 || input.Notifications[0].ID != request.NotificationIDs[0] {
		t.Fatal(input, err)
	}
	if !input.Notifications[0].DeliveryAcknowledged || input.Notifications[0].State != "pending" {
		t.Fatal("snapshot lost delivery facts or confused them with processing", input.Notifications[0])
	}
	entries, err := k.Mailbox(ctx, task.SessionID, 0, 10)
	if err != nil || entries[0].State != "bound_to_run" || entries[0].RunID != w.Intent.RunID || !entries[0].DeliveryAcknowledged {
		t.Fatal(entries, err)
	}
	var runs, attempts int
	k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs)
	k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE kind='task_attempt'").Scan(&attempts)
	if runs != 0 || attempts != 0 {
		t.Fatal("wake reservation launched or charged a turn", runs, attempts)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.ReserveWake(ctx, request)
	if err != nil || !reflect.DeepEqual(w, replay) {
		t.Fatal("restart changed wake input", replay, err)
	}
	request.Context = json.RawMessage(`{"changed":true}`)
	if _, err = k.ReserveWake(ctx, request); err == nil {
		t.Fatal("wake key changed its snapshot")
	}
}
func TestWakeBindsOnlyItsRunAndAppliesWithTheLogicalResult(t *testing.T) {
	ctx := context.Background()
	k, _, task, _, request := wakeFixture(t)
	w, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, task.SessionID)
	run := wakeStartingRun(k, b, w)
	wrong := run
	wrong.ID = "unrelated-run"
	if err = k.ReserveSessionRun(ctx, b, w.Snapshot, wrong); err == nil {
		t.Fatal("another run bypassed a reserved wake")
	}
	if err = k.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err == nil {
		t.Fatal("wake accepted a different snapshot")
	}
	if err = k.ReserveSessionRun(ctx, b, w.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	run = completeWakeWaiting(t, k, b, run)
	before, err := k.Wake(ctx, w.Intent.ID)
	if err != nil || before.Intent.State != "bound" {
		t.Fatal("physical completion applied wake", before, err)
	}
	if _, err = k.ApplyTaskResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("unstopped process applied wake")
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec("CREATE TRIGGER inject_wake_apply BEFORE UPDATE ON kernel_notifications WHEN NEW.state='applied' BEGIN SELECT RAISE(ABORT,'wake application failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyTaskResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("Task applied without its input decision")
	}
	current, err := k.Task(ctx, task.ID)
	if err != nil || current.State != contract.TaskRunning {
		t.Fatal("partial Task decision survived", current, err)
	}
	if _, err = k.DB.Exec("DROP TRIGGER inject_wake_apply"); err != nil {
		t.Fatal(err)
	}
	applied, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || applied.State != contract.TaskWaiting {
		t.Fatal(applied, err)
	}
	after, err := k.Wake(ctx, w.Intent.ID)
	if err != nil || after.Intent.State != "applied" || !reflect.DeepEqual(w.Snapshot, after.Snapshot) {
		t.Fatal(after, err)
	}
	entries, err := k.Mailbox(ctx, b.ID, 0, 10)
	if err != nil || entries[0].State != "applied" || entries[0].RunID != run.ID || entries[0].HasReply {
		t.Fatal("input decision and reply became the same state", entries, err)
	}
	if _, err = k.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if pending, err := k.PendingWakeNotifications(ctx, b.ID, 10); err != nil || len(pending) != 0 {
		t.Fatal(pending, err)
	}
	// A later message is new input on the same unfinished Task/thread.
	if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: b.Target, Key: "next", Recipient: b.TaskID, Body: "new answer"}); err != nil {
		t.Fatal(err)
	}
	pending, err := k.PendingWakeNotifications(ctx, b.ID, 10)
	if err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	next, err := k.ReserveWake(ctx, WakeRequest{SessionID: b.ID, Key: "next", NotificationIDs: []string{pending[0].ID}})
	if err != nil || next.Intent.RunID == run.ID {
		t.Fatal(next, err)
	}
	resumed := prepareWakeSession(t, k, b.ID)
	if resumed.ThreadID != b.ThreadID {
		t.Fatal("unfinished Task got another thread")
	}
	if err = k.ReserveSessionRun(ctx, resumed, next.Snapshot, wakeStartingRun(k, resumed, next)); err != nil {
		t.Fatal(err)
	}
	current, err = k.Task(ctx, task.ID)
	if err != nil || current.Attempts != 2 {
		t.Fatal("wake retry bypassed attempt accounting", current, err)
	}
}
func TestWakeUnknownStartHoldsInputsButStoppedFailureMayRetry(t *testing.T) {
	for _, state := range []string{"unknown", "failed"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			k, _, task, _, request := wakeFixture(t)
			w, err := k.ReserveWake(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			b := prepareWakeSession(t, k, task.SessionID)
			run := wakeStartingRun(k, b, w)
			if err = k.ReserveSessionRun(ctx, b, w.Snapshot, run); err != nil {
				t.Fatal(err)
			}
			run.State = state
			if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
				t.Fatal(err)
			}
			if err = k.ArchiveSessionRun(ctx, b.ID, run.ID, "failed attempt"); err == nil {
				t.Fatal("live process released wake")
			}
			if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			if state == "unknown" {
				if err = k.ArchiveSessionRun(ctx, b.ID, run.ID, "unknown is not failure"); err == nil {
					t.Fatal("unknown run released wake")
				}
				if _, err = k.ReconcileUnstartedWake(ctx, w.Intent.ID); !errors.Is(err, ErrWakeHeld) {
					t.Fatal(err)
				}
				if pending, err := k.PendingWakeNotifications(ctx, b.ID, 10); err != nil || len(pending) != 0 {
					t.Fatal(pending, err)
				}
				request.Key = "another"
				if got, err := k.ReserveWake(ctx, request); !errors.Is(err, ErrWakeBusy) || got.Intent.ID != w.Intent.ID {
					t.Fatal(got, err)
				}
			} else {
				if err = k.ArchiveSessionRun(ctx, b.ID, run.ID, "failed attempt"); err != nil {
					t.Fatal(err)
				}
				pending, err := k.PendingWakeNotifications(ctx, b.ID, 10)
				if err != nil || len(pending) != 1 {
					t.Fatal("failure lost original input", pending, err)
				}
				request.Key = "retry"
				retry, err := k.ReserveWake(ctx, request)
				if err != nil || retry.Intent.RunID == run.ID {
					t.Fatal(retry, err)
				}
				authorizeRecoveryFixture(t, k, b.TaskID, retry.Intent.RunID, retry.Snapshot)
				b = prepareWakeSession(t, k, b.ID)
				if err = k.ReserveSessionRun(ctx, b, retry.Snapshot, wakeStartingRun(k, b, retry)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestWakeBudgetAndReservationFailuresLeaveInputPending(t *testing.T) {
	for _, failure := range []string{"snapshot", "intent", "binding", "size"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			k, wave, task, _, request := wakeFixture(t)
			if failure == "size" {
				for i := range 4 {
					if _, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: string(rune('a' + i)), Recipient: task.ID, Body: strings.Repeat("x", contract.MaxInline)}); err != nil {
						t.Fatal(err)
					}
				}
				pending, err := k.PendingWakeNotifications(ctx, task.SessionID, 10)
				if err != nil {
					t.Fatal(err)
				}
				request.NotificationIDs = nil
				for _, n := range pending {
					request.NotificationIDs = append(request.NotificationIDs, n.ID)
				}
			} else {
				table := map[string]string{"snapshot": "kernel_snapshots", "intent": "kernel_wake_intents", "binding": "kernel_notifications"}[failure]
				action := "INSERT"
				if failure == "binding" {
					action = "UPDATE"
				}
				if _, err := k.DB.Exec("CREATE TRIGGER inject_wake BEFORE " + action + " ON " + table + " BEGIN SELECT RAISE(ABORT,'wake persistence failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ReserveWake(ctx, request); err == nil {
				t.Fatal("invalid/incomplete wake was accepted")
			}
			var wakes, bound, snapshots int
			k.DB.QueryRow("SELECT count(*) FROM kernel_wake_intents").Scan(&wakes)
			k.DB.QueryRow("SELECT count(*) FROM kernel_notifications WHERE state<>'pending'").Scan(&bound)
			k.DB.QueryRow("SELECT count(*) FROM kernel_snapshots").Scan(&snapshots)
			if wakes != 0 || bound != 0 || snapshots != 0 {
				t.Fatal("partial wake effects survived", wakes, bound, snapshots)
			}
			if failure != "size" {
				if _, err := k.DB.Exec("DROP TRIGGER inject_wake"); err != nil {
					t.Fatal(err)
				}
			} else {
				request.NotificationIDs = request.NotificationIDs[:1]
			}
			if _, err := k.ReserveWake(ctx, request); err != nil {
				t.Fatal("smaller/retried batch failed", err)
			}
		})
	}
}
func TestWakePauseAndSteeringCannotLaunchReservedOldInput(t *testing.T) {
	ctx := context.Background()
	k, wave, task, _, request := wakeFixture(t)
	w, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	wave, err = k.Wave(ctx, wave.ID)
	if err != nil {
		t.Fatal(err)
	}
	wave.Paused = true
	tx, _ := k.kernelTx(ctx)
	if err = saveWave(ctx, tx, wave); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReserveWake(ctx, request); !errors.Is(err, ErrWakeHeld) {
		t.Fatal(err)
	}
	if _, err = k.ReconcileUnstartedWake(ctx, w.Intent.ID); !errors.Is(err, ErrWakeHeld) {
		t.Fatal("pause discarded input", err)
	}
	wave.Paused = false
	wave.ScopeRevision++
	tx, _ = k.kernelTx(ctx)
	if err = saveWave(ctx, tx, wave); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	retired, err := k.ReconcileUnstartedWake(ctx, w.Intent.ID)
	if err != nil || retired.Intent.State != "superseded" {
		t.Fatal(retired, err)
	}
	pending, err := k.PendingWakeNotifications(ctx, task.SessionID, 10)
	if err != nil || len(pending) != 0 {
		t.Fatal("old target became new input", pending, err)
	}
	items, err := k.Notifications(ctx, task.SessionID, 0, 10)
	if err != nil || len(items) != 1 || items[0].State != "archived" || items[0].Target == wave.Target() {
		t.Fatal(items, err)
	}
}
func TestSharedMainWakeReleasesOnlyStillCurrentWaveAfterSteering(t *testing.T) {
	ctx := context.Background()
	k, first, tasks := mailboxFixture(t)
	other, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "other", OriginalRequest: "another", Criteria: first.Criteria, Policy: first.Policy})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := k.AdoptPlan(ctx, other.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	otherTask, err := k.Task(ctx, plan.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range []domain.Task{tasks[0], otherTask} {
		if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: task.SessionID, Target: task.Target(), Key: "question", Recipient: "parent", Body: "please clarify"}); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := k.PendingWakeNotifications(ctx, "main", 10)
	if err != nil || len(pending) != 2 {
		t.Fatal(pending, err)
	}
	request := WakeRequest{SessionID: "main", Key: "batch", NotificationIDs: []string{pending[0].ID, pending[1].ID}}
	w, err := k.ReserveWake(ctx, request)
	if err != nil || len(w.Intent.Targets) != 2 {
		t.Fatal(w, err)
	}
	first, err = k.Wave(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	first.PlanRevision++
	tx, _ := k.kernelTx(ctx)
	if err = saveWave(ctx, tx, first); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReconcileUnstartedWake(ctx, w.Intent.ID); err != nil {
		t.Fatal(err)
	}
	pending, err = k.PendingWakeNotifications(ctx, "main", 10)
	if err != nil || len(pending) != 1 || pending[0].Target != other.Target() {
		t.Fatal("current Wave input was lost", pending, err)
	}
	next, err := k.ReserveWake(ctx, WakeRequest{SessionID: "main", Key: "current", NotificationIDs: []string{pending[0].ID}})
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, "main")
	if err = k.ReserveSessionRun(ctx, b, w.Snapshot, wakeStartingRun(k, b, w)); err == nil {
		t.Fatal("retired wake run bypassed the current reservation")
	}
	if err = k.ReserveSessionRun(ctx, b, next.Snapshot, wakeStartingRun(k, b, next)); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReconcileUnstartedWake(ctx, next.Intent.ID); !errors.Is(err, ErrWakeHeld) {
		t.Fatal("started run was retired", err)
	}
}

func TestWakeInputIsArchivedForInvalidOrCanceledTaskDecision(t *testing.T) {
	for _, outcome := range []string{"invalid", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			k, wave, task, _, request := wakeFixture(t)
			w, err := k.ReserveWake(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			b := prepareWakeSession(t, k, task.SessionID)
			run := wakeStartingRun(k, b, w)
			if err = k.ReserveSessionRun(ctx, b, w.Snapshot, run); err != nil {
				t.Fatal(err)
			}
			if outcome == "invalid" {
				run.State, run.TurnID, run.Final = "completed", "turn", `{"not":"a Task result"}`
				if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
					t.Fatal(err)
				}
			} else {
				run = completeWakeWaiting(t, k, b, run)
				wave, err = k.Wave(ctx, wave.ID)
				if err != nil {
					t.Fatal(err)
				}
				wave.State, wave.CompletedAt = domain.WaveCanceled, k.clock()
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err = saveWave(ctx, tx, wave); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			if _, err = k.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			saved, err := k.Wake(ctx, w.Intent.ID)
			if err != nil || saved.Intent.State != "archived" {
				t.Fatal(saved, err)
			}
			entries, err := k.Mailbox(ctx, b.ID, 0, 10)
			if err != nil || len(entries) != 1 || entries[0].State != "archived" {
				t.Fatal("invalid output marked input processed", entries, err)
			}
			if pending, err := k.PendingWakeNotifications(ctx, b.ID, 10); err != nil || len(pending) != 0 {
				t.Fatal("terminal Task became runnable", pending, err)
			}
		})
	}
}
func TestWakeRunReservationFailureDoesNotConsumeWakeOrAttempt(t *testing.T) {
	ctx := context.Background()
	k, _, task, _, request := wakeFixture(t)
	w, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, task.SessionID)
	run := wakeStartingRun(k, b, w)
	if _, err = k.DB.Exec("CREATE TRIGGER inject_wake_run BEFORE INSERT ON kernel_run_bindings BEGIN SELECT RAISE(ABORT,'run reservation failure'); END"); err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionRun(ctx, b, w.Snapshot, run); err == nil {
		t.Fatal("failed run reservation consumed wake")
	}
	saved, err := k.Wake(ctx, w.Intent.ID)
	if err != nil || saved.Intent.State != "reserved" {
		t.Fatal(saved, err)
	}
	current, err := k.Task(ctx, task.ID)
	if err != nil || current.Attempts != 0 || current.State != contract.TaskReady {
		t.Fatal(current, err)
	}
	if _, err = k.DB.Exec("DROP TRIGGER inject_wake_run"); err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionRun(ctx, b, w.Snapshot, run); err != nil {
		t.Fatal(err)
	}
}
func TestWakeReconciliationRequiresPreparedProcessStoppingProof(t *testing.T) {
	ctx := context.Background()
	k, wave, task, _, request := wakeFixture(t)
	w, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, task.SessionID)
	wave, err = k.Wave(ctx, wave.ID)
	if err != nil {
		t.Fatal(err)
	}
	wave.ScopeRevision++
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = saveWave(ctx, tx, wave); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReconcileUnstartedWake(ctx, w.Intent.ID); !errors.Is(err, ErrWakeHeld) {
		t.Fatal("prepared process reservation was released", err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReconcileUnstartedWake(ctx, w.Intent.ID); err != nil {
		t.Fatal(err)
	}
}
func TestWakeCannotBorrowAnotherSessionsNotification(t *testing.T) {
	ctx := context.Background()
	k, wave, task, _, request := wakeFixture(t)
	if _, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: task.SessionID, Target: wave.Target(), Key: "parent-message", Recipient: "parent", Body: "for parent only"}); err != nil {
		t.Fatal(err)
	}
	parent, err := k.PendingWakeNotifications(ctx, "main", 10)
	if err != nil || len(parent) != 1 {
		t.Fatal(parent, err)
	}
	request.NotificationIDs = []string{parent[0].ID}
	if _, err = k.ReserveWake(ctx, request); err == nil {
		t.Fatal("Task borrowed parent's input")
	}
	request.SessionID = "main"
	request.NotificationIDs = []string{parent[0].ID, parent[0].ID}
	if _, err = k.ReserveWake(ctx, request); err == nil {
		t.Fatal("duplicate input IDs accepted")
	}
}

func TestSingleMaximumInlineMessageAlwaysFitsWakeTransport(t *testing.T) {
	for _, character := range []string{"<", "&", "\x00", "\"", "\u2028"} {
		t.Run(fmt.Sprintf("%x", []byte(character)), func(t *testing.T) {
			ctx := context.Background()
			k, wave, tasks := mailboxFixture(t)
			body := "x" + strings.Repeat(character, (contract.MaxInline-1)/len(character))
			m, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "full-body", Recipient: tasks[0].ID, Body: body})
			if err != nil {
				t.Fatal(err)
			}
			pending, err := k.PendingWakeNotifications(ctx, tasks[0].SessionID, 10)
			if err != nil || len(pending) != 1 {
				t.Fatal(pending, err)
			}
			w, err := k.ReserveWake(ctx, WakeRequest{SessionID: tasks[0].SessionID, Key: "body", NotificationIDs: []string{pending[0].ID}})
			if err != nil {
				t.Fatal("valid single message became undeliverable", err)
			}
			if err = contract.ValidateSessionInput(w.Snapshot.Input); err != nil {
				t.Fatal(err)
			}
			var input wakeInput
			if err = json.Unmarshal(w.Snapshot.Input, &input); err != nil || len(input.Messages) != 1 {
				t.Fatal(err)
			}
			message := input.Messages[0]
			decoded := []byte(message.Body)
			if message.BodyEncoding == "base64" {
				decoded, err = base64.StdEncoding.DecodeString(strings.Join(message.EncodedBodyChunks, ""))
				if err != nil || message.Body != "" {
					t.Fatal(message, err)
				}
				for _, chunk := range message.EncodedBodyChunks {
					if len(chunk) > contract.MaxInline {
						t.Fatal("oversize encoded inline value")
					}
				}
			} else if message.BodyEncoding != "utf-8" {
				t.Fatal(message.BodyEncoding)
			}
			if string(decoded) != body || message.ID != m.ID || message.BodyDigest != fmt.Sprintf("%x", sha256.Sum256(decoded)) {
				t.Fatal("message body/provenance changed during transport")
			}
		})
	}
}

func TestTerminalWaveObservationCanWakeOnlyItsCurrentParent(t *testing.T) {
	for _, state := range []domain.WaveState{domain.WaveCompleted, domain.WaveCanceled} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			k, wave, tasks := mailboxFixture(t)
			if _, err := k.Subscribe(ctx, SubscriptionRequest{SessionID: "main", Target: wave.Target(), Key: "terminal", Kind: "wave_terminal"}); err != nil {
				t.Fatal(err)
			}
			if _, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: tasks[0].SessionID, Target: wave.Target(), Key: "parent", Recipient: "parent", Body: "ordinary parent input"}); err != nil {
				t.Fatal(err)
			}
			if _, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "child", Recipient: tasks[0].ID, Body: "ordinary Task input"}); err != nil {
				t.Fatal(err)
			}
			wave, err := k.Wave(ctx, wave.ID)
			if err != nil {
				t.Fatal(err)
			}
			wave.State, wave.CompletedAt = state, k.clock()
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = saveWave(ctx, tx, wave); err != nil {
				t.Fatal(err)
			}
			if err = k.waveEventTx(ctx, tx, wave, "wave_"+string(state), "", wave.ID+":terminal", wave.Target()); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			pending, err := k.PendingWakeNotifications(ctx, "main", 10)
			if err != nil || len(pending) != 1 || pending[0].Kind != "wave_terminal" || pending[0].WaveState != state {
				t.Fatal("parent lost terminal startup reason", pending, err)
			}
			if child, err := k.PendingWakeNotifications(ctx, tasks[0].SessionID, 10); err != nil || len(child) != 0 {
				t.Fatal("terminal Wave restarted Task work", child, err)
			}
			all, err := k.Notifications(ctx, "main", 0, 10)
			if err != nil || len(all) != 2 {
				t.Fatal(all, err)
			}
			for _, n := range all {
				if n.Kind == "message" {
					if _, err = k.ReserveWake(ctx, WakeRequest{SessionID: "main", Key: "ordinary", NotificationIDs: []string{n.ID}}); !errors.Is(err, ErrWakeHeld) {
						t.Fatal("ordinary input bypassed terminal gate", err)
					}
				}
			}
			request := WakeRequest{SessionID: "main", Key: "terminal", NotificationIDs: []string{pending[0].ID}}
			wave.Paused = true
			tx, err = k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = saveWave(ctx, tx, wave); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if _, err = k.ReserveWake(ctx, request); !errors.Is(err, ErrWakeHeld) {
				t.Fatal("terminal observation ignored pause", err)
			}
			wave.Paused = false
			tx, err = k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = saveWave(ctx, tx, wave); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			w, err := k.ReserveWake(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = k.ReconcileUnstartedWake(ctx, w.Intent.ID); !errors.Is(err, ErrWakeHeld) {
				t.Fatal("valid terminal observation was retired", err)
			}
			b := prepareWakeSession(t, k, "main")
			if err = k.ReserveSessionRun(ctx, b, w.Snapshot, wakeStartingRun(k, b, w)); err != nil {
				t.Fatal("parent could not consume terminal snapshot", err)
			}
		})
	}
}
