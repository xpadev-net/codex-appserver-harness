package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func authorizeRecoveryFixture(t *testing.T, k *KernelStore, taskID, runID string, snapshot contract.InputSnapshot) {
	t.Helper()
	task, err := k.Task(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.AuthorizeTaskRecovery(context.Background(), task, runID, snapshot, "trusted classified transient recovery"); err != nil {
		t.Fatal(err)
	}
}
func blockedRecoveryFixture(t *testing.T) (*KernelStore, domain.Task) {
	t.Helper()
	ctx := context.Background()
	k, b, run, _ := recordedTaskResultFixture(t, func(r *contract.TaskResult) {
		r.Outcome = "blocked"
		r.Blockers = []string{"need trusted recovery decision"}
	})
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if applied, err := k.ApplyTaskResult(ctx, b.ID, run.ID); err != nil || applied.State != contract.TaskBlocked {
		t.Fatal(applied, err)
	}
	task, err := k.Task(ctx, b.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	return k, task
}
func recoveryRun(k *KernelStore, b contract.SessionBinding, id string) contract.SessionRun {
	return contract.SessionRun{ID: id, ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
}
func TestBlockedRunRequiresExactDurableRecoveryPermit(t *testing.T) {
	ctx := context.Background()
	k, task := blockedRecoveryFixture(t)
	// Readiness selection is not a write authorization.
	if _, err := k.ReadyTasks(ctx, ReadyRequest{Target: task.Target(), RecoveryTaskIDs: []string{task.ID}}); err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, task.SessionID)
	if err := k.ReserveSessionRun(ctx, b, sessionSnapshot(), recoveryRun(k, b, "retry")); !errors.Is(err, ErrTaskRecoveryRequired) {
		t.Fatal(err)
	}
	current, err := k.Task(ctx, task.ID)
	if err != nil || current.Attempts != 1 || current.State != contract.TaskBlocked {
		t.Fatal(current, err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	snapshot := sessionSnapshot()
	if err = k.AuthorizeTaskRecovery(ctx, task, "retry", snapshot, "operator resolved input blocker"); err != nil {
		t.Fatal(err)
	}
	if err = k.AuthorizeTaskRecovery(ctx, task, "retry", snapshot, "operator resolved input blocker"); err != nil {
		t.Fatal(err)
	}
	b = prepareWakeSession(t, k, b.ID)
	wrong := snapshot
	wrong.ID = "different-snapshot"
	for _, trial := range []struct {
		id       string
		snapshot contract.InputSnapshot
	}{{"different-run", snapshot}, {"retry", wrong}} {
		if err = k.ReserveSessionRun(ctx, b, trial.snapshot, recoveryRun(k, b, trial.id)); !errors.Is(err, ErrTaskRecoveryRequired) {
			t.Fatal(trial.id, err)
		}
	}
	// A later failure in the run transaction must not consume the permit.
	if _, err = k.DB.Exec("CREATE TRIGGER fail_recovery BEFORE INSERT ON kernel_run_bindings WHEN NEW.run_id='retry' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionRun(ctx, b, snapshot, recoveryRun(k, b, "retry")); err == nil {
		t.Fatal("injected failure ignored")
	}
	var permit taskRecoveryPermit
	if err = k.Record(ctx, "task_recovery_permit", "retry", &permit); err != nil || permit.Consumed {
		t.Fatal(permit, err)
	}
	if _, err = k.DB.Exec("DROP TRIGGER fail_recovery"); err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionRun(ctx, b, snapshot, recoveryRun(k, b, "retry")); err != nil {
		t.Fatal(err)
	}
	if err = k.Record(ctx, "task_recovery_permit", "retry", &permit); err != nil || !permit.Consumed {
		t.Fatal(permit, err)
	}
	current, err = k.Task(ctx, task.ID)
	if err != nil || current.Attempts != 2 {
		t.Fatal(current, err)
	}
}
func TestRecoveryPermitRejectsChangedTaskAndSurvivesRestart(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			ctx := context.Background()
			k, task := blockedRecoveryFixture(t)
			authorizeRecoveryFixture(t, k, task.ID, "retry", sessionSnapshot())
			path := filepath.Join(k.Root, "state.db")
			k.Close()
			reopened, err := OpenKernel(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			k = reopened
			if changed {
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				task.Reason = "new unresolved blocker"
				if err = saveTask(ctx, tx, task); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			b := prepareWakeSession(t, k, task.SessionID)
			err = k.ReserveSessionRun(ctx, b, sessionSnapshot(), recoveryRun(k, b, "retry"))
			if changed {
				if !errors.Is(err, ErrTaskRecoveryRequired) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestWaitingTaskCannotResumeWithoutItsReservedWake(t *testing.T) {
	ctx := context.Background()
	k, _, task, _, request := wakeFixture(t)
	first, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, task.SessionID)
	run := wakeStartingRun(k, b, first)
	if err = k.ReserveSessionRun(ctx, b, first.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	completeWakeWaiting(t, k, b, run)
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	b = prepareWakeSession(t, k, b.ID)
	if err = k.ReserveSessionRun(ctx, b, sessionSnapshot(), recoveryRun(k, b, "unsolicited")); err == nil {
		t.Fatal("waiting resumed without wake")
	}
	if err = k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: task.Target(), Key: "answer", Recipient: task.ID, Body: "trusted new input"}); err != nil {
		t.Fatal(err)
	}
	pending, err := k.PendingWakeNotifications(ctx, b.ID, 10)
	if err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: b.ID, Key: "answer", NotificationIDs: []string{pending[0].ID}})
	if err != nil {
		t.Fatal(err)
	}
	set, err := k.ReadyTasks(ctx, ReadyRequest{Target: task.Target(), ResumeTaskIDs: []string{task.ID}})
	if err != nil || len(set.Tasks) != 1 {
		t.Fatal(set, err)
	}
	b = prepareWakeSession(t, k, b.ID)
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, wakeStartingRun(k, b, wake)); err != nil {
		t.Fatal(err)
	}
}
func TestReadinessPropagatesOperationalFailuresAndClassifiesEvidence(t *testing.T) {
	for _, condition := range []string{"db", "io", "integrity", "manifest_json", "manifest_type"} {
		t.Run(condition, func(t *testing.T) {
			ctx := context.Background()
			k, w, tasks := readyFixture(t)
			a := completeTemplateTask(t, k, tasks[0], "completion evidence", "", nil)
			switch condition {
			case "db":
				if _, err := k.DB.Exec("ALTER TABLE records RENAME TO unavailable_records"); err != nil {
					t.Fatal(err)
				}
			case "io": // An OS read failure must remain visible instead of a successful hold.
				if err := os.Remove(filepath.Join(k.Root, "objects", a.ID)); err != nil {
					t.Fatal(err)
				}
			case "integrity":
				if err := os.WriteFile(filepath.Join(k.Root, "objects", a.ID), []byte("tampered evidence"), 0600); err != nil {
					t.Fatal(err)
				}
			case "manifest_json", "manifest_type":
				raw := []byte(`{"id":`)
				if condition == "manifest_type" {
					raw = []byte(`{"id":7}`)
				}
				if _, err := k.DB.Exec("UPDATE kernel_artifacts SET payload=? WHERE id=?", raw, a.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := k.Artifact(ctx, a.ID); !errors.Is(err, ErrArtifactIntegrity) {
					t.Fatal(err)
				}
			}
			set, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
			if condition != "db" && condition != "io" {
				if err != nil || len(set.Tasks) != 0 || set.Reason != "drain_and_block" {
					t.Fatal(set, err)
				}
			} else if err == nil {
				t.Fatal("operational failure became a successful hold", set)
			}
		})
	}
	// Typed holds do not include cancellation or arbitrary SQL/read errors.
	if readinessHoldError(context.Canceled) || readinessHoldError(context.DeadlineExceeded) || readinessHoldError(os.ErrPermission) {
		t.Fatal("operational errors classified as domain holds")
	}
}

func TestReadinessPropagatesInputArtifactReadFailure(t *testing.T) {
	ctx := context.Background()
	k, w, request := artifactFixture(t)
	a := mustImport(t, k, request, "trusted input")
	plan := storagePlanFixture()
	plan.Phases[0].Tasks[0].InputArtifactIDs = []string{a.ID}
	if _, err := k.AdoptPlan(ctx, w.Target(), "initial", plan); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(k.Root, "objects", a.ID)); err != nil {
		t.Fatal(err)
	}
	if set, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()}); err == nil {
		t.Fatal("input read failure became a successful hold", set)
	}
}
