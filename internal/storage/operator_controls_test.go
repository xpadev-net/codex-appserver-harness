package storage

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func operatorRequest(w domain.Wave, key, action, task string) OperatorControl {
	return OperatorControl{Key: key, Target: w.Target(), TaskID: task, Action: action, Reason: "operator request"}
}
func mustControl(t *testing.T, k *KernelStore, r OperatorControl) OperatorControlReceipt {
	t.Helper()
	receipt, err := k.ApplyOperatorControl(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestOperatorPauseScopesComposeAndPreserveSavedInputs(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := readyFixture(t)
	before, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	mustControl(t, k, operatorRequest(w, "task-pause", "pause", tasks[0].ID))
	mustReady(t, k, w, tasks[1].ID)
	mustControl(t, k, operatorRequest(w, "wave-pause", "pause", ""))
	mustReady(t, k, w)
	mustControl(t, k, operatorRequest(w, "task-resume", "resume", tasks[0].ID))
	mustReady(t, k, w) // Task resume cannot bypass the Wave prohibition.
	mustControl(t, k, operatorRequest(w, "wave-resume", "resume", ""))
	mustReady(t, k, w, tasks[0].ID, tasks[1].ID)
	after, err := k.Wave(ctx, w.ID)
	if err != nil || after.OriginalRequest != before.OriginalRequest || after.Target() != before.Target() || after.TasksCreated != before.TasksCreated || after.Replans != before.Replans || after.MainTurns != before.MainTurns {
		t.Fatal(after, err)
	}
	for _, before := range tasks {
		after, err := k.Task(ctx, before.ID)
		if err != nil || after.InputDigest != before.InputDigest || after.Attempts != before.Attempts || after.Spec.Instruction != before.Spec.Instruction {
			t.Fatal(after, before, err)
		}
	}
}

func TestOperatorActionsFenceAdmittedLaunchWithoutReleasingCapacity(t *testing.T) {
	for _, action := range []string{"pause", "cancel"} {
		for _, scope := range []string{"wave", "task"} {
			t.Run(action+"/"+scope, func(t *testing.T) {
				ctx := context.Background()
				k, w, b := managedTaskFixture(t)
				r := managedReserve(t, k, b, "owner", "run", "execute")
				taskID := ""
				if scope == "task" {
					taskID = b.TaskID
				}
				mustControl(t, k, operatorRequest(w, "operation", action, taskID))
				if _, err := k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID); err == nil {
					t.Fatal("operator prohibition bypassed at launch boundary")
				}
				capacity, err := k.ExecutionCapacity(ctx)
				if err != nil || capacity.HeldProcessSlots != 1 {
					t.Fatal("operator action released an unconfirmed process", capacity, err)
				}
				if err := k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "owner", Disposition: "not_started"}); err != nil {
					t.Fatal(err)
				}
				capacity, err = k.ExecutionCapacity(ctx)
				if err != nil || capacity.HeldProcessSlots != 0 {
					t.Fatal(capacity, err)
				}
			})
		}
	}
}

func TestOperatorCancelArchivesLateResultAndKeepsRunUntilStopped(t *testing.T) {
	for _, scope := range []string{"wave", "task"} {
		t.Run(scope, func(t *testing.T) {
			ctx := context.Background()
			k, b, run, _ := recordedTaskResultFixture(t, nil)
			w, err := k.Wave(ctx, b.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			taskID := ""
			if scope == "task" {
				taskID = b.TaskID
			}
			mustControl(t, k, operatorRequest(w, "cancel", "cancel", taskID))
			var active string
			var stopped int
			if err := k.DB.QueryRowContext(ctx, "SELECT active_run_id,process_stopped FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &stopped); err != nil || active != run.ID || stopped != 0 {
				t.Fatal("cancel lost physical ownership", active, stopped, err)
			}
			if _, err := k.ApplyTaskResult(ctx, b.ID, run.ID); err == nil {
				t.Fatal("cancelled live run applied before stopping")
			}
			if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			result, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
			if err != nil || !result.Archived || result.State != contract.TaskCanceled {
				t.Fatal("late success revived canceled Task", result, err)
			}
			var saved contract.SessionRun
			if err := k.Record(ctx, "agent_run", run.ID, &saved); err != nil || saved.Final != run.Final {
				t.Fatal("sent result lost", saved, err)
			}
		})
	}
}

func TestOperatorReplayAndCancelPrioritySurviveReopen(t *testing.T) {
	ctx := context.Background()
	k, w, _ := readyFixture(t)
	r := operatorRequest(w, "pause", "pause", "")
	first := mustControl(t, k, r)
	mustControl(t, k, operatorRequest(w, "resume", "resume", ""))
	if replay := mustControl(t, k, r); !reflect.DeepEqual(replay, first) {
		t.Fatal(replay, first)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Paused {
		t.Fatal("old pause replay was applied again")
	}
	conflict := r
	conflict.Reason = "different request"
	if _, err := k.ApplyOperatorControl(ctx, conflict); err == nil {
		t.Fatal("conflicting action key accepted")
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if replay := mustControl(t, k, r); !reflect.DeepEqual(replay, first) {
		t.Fatal(replay, first)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = k.ApplyOperatorControl(ctx, operatorRequest(w, "race-resume", "resume", ""))
	}()
	go func() {
		defer wg.Done()
		if _, err := k.ApplyOperatorControl(ctx, operatorRequest(w, "race-cancel", "cancel", "")); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()
	current, err = k.Wave(ctx, w.ID)
	if err != nil || current.State != domain.WaveCanceled {
		t.Fatal("resume race revived canceled Wave", current, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, operatorRequest(w, "after-cancel", "resume", "")); err == nil {
		t.Fatal("terminal Wave resumed")
	}
}

func TestOperatorPauseRetainsPhysicalRunAndAcceptsItsNormalResult(t *testing.T) {
	ctx := context.Background()
	k, b, run, _ := recordedTaskResultFixture(t, nil)
	w, err := k.Wave(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	mustControl(t, k, operatorRequest(w, "pause", "pause", ""))
	var saved contract.SessionRun
	if err = k.Record(ctx, "agent_run", run.ID, &saved); err != nil || !reflect.DeepEqual(saved, run) {
		t.Fatal("pause changed sent run", saved, err)
	}
	var active string
	var stopped int
	if err = k.DB.QueryRowContext(ctx, "SELECT active_run_id,process_stopped FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &stopped); err != nil || active != run.ID || stopped != 0 {
		t.Fatal("pause changed physical ownership", active, stopped, err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	result, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || result.Archived || result.State != contract.TaskCompleted {
		t.Fatal("pause discarded normal run result", result, err)
	}
}

func TestOperatorRejectsStaleAndCrossWaveTaskTargets(t *testing.T) {
	ctx := context.Background()
	k, w, _ := readyFixture(t)
	other, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: w.OrchestratorSessionID, Source: w.Source, SourceID: w.SourceID + ":other", OriginalRequest: w.OriginalRequest, Criteria: w.Criteria, Policy: w.Policy})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := k.AdoptPlan(ctx, other.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*OperatorControl){
		func(r *OperatorControl) { r.Target.ScopeRevision++ },
		func(r *OperatorControl) { r.Target.PolicyRevision++ },
		func(r *OperatorControl) { r.TaskID = plan.CreatedTaskIDs[0] },
		func(r *OperatorControl) { r.Action = "interrupt" },
		func(r *OperatorControl) { r.Reason = "" },
	} {
		r := operatorRequest(w, "invalid", "pause", "")
		change(&r)
		if _, err := k.ApplyOperatorControl(ctx, r); err == nil {
			t.Fatal("invalid operator request accepted", r, other.ID)
		}
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.Paused || current.State != domain.WaveRunning {
		t.Fatal(current, err)
	}
}

func TestOperatorEventFailureRollsBackAllCancellationAndAudit(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := readyFixture(t)
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_operator_event BEFORE INSERT ON kernel_events WHEN NEW.kind='operator_control' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyOperatorControl(ctx, operatorRequest(w, "cancel", "cancel", "")); err == nil {
		t.Fatal("event failure accepted")
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.State != domain.WaveRunning {
		t.Fatal(current, err)
	}
	for _, task := range tasks {
		current, err := k.Task(ctx, task.ID)
		if err != nil || current.State != task.State {
			t.Fatal(current, err)
		}
	}
	var count int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='operator_control'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}

func TestOperatorCanceledLostThreadCanBeRetiredOnlyAfterStop(t *testing.T) {
	for _, scope := range []string{"task", "wave"} {
		t.Run(scope, func(t *testing.T) {
			ctx := context.Background()
			k, w, b := managedTaskFixture(t)
			r := managedReserve(t, k, b, "owner", "run", "execute")
			managedStart(t, k, r, 42)
			if err := k.MarkManagedThreadRPC(ctx, "owner", "thread/start"); err != nil {
				t.Fatal(err)
			}
			taskID := b.TaskID
			if scope == "wave" {
				taskID = ""
			}
			mustControl(t, k, operatorRequest(w, "cancel-lost-start", "cancel", taskID))
			before, err := k.Task(ctx, b.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			if err = k.RetireUnboundTaskSession(ctx, b.ID, "lost reply"); err == nil {
				t.Fatal("unconfirmed canceled creation retired")
			}
			if err = k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "owner", Disposition: "group_stopped", Identity: managedIdentity(42)}); err != nil {
				t.Fatal(err)
			}
			plan := storagePlanFixture()
			plan.Phases[0].ID = "replacement"
			plan.Phases[0].Tasks[0].ID = "replacement-work"
			plan.Phases[0].Completion.RequiredTaskIDs[0] = "replacement-work"
			plan.Phases = plan.Phases[:1]
			if _, err = k.AdoptPlan(ctx, w.Target(), "replacement", plan); err == nil {
				t.Fatal("unresolved creation did not hold replan")
			}
			if err = k.RetireUnboundTaskSession(ctx, b.ID, "audited lost reply after cancellation"); err != nil {
				t.Fatal(err)
			}
			after, err := k.Task(ctx, b.TaskID)
			if err != nil || !reflect.DeepEqual(after, before) || after.State != contract.TaskCanceled {
				t.Fatal("cleanup changed canceled Task", after, before, err)
			}
			var creating int
			if err = k.DB.QueryRowContext(ctx, "SELECT creating FROM kernel_sessions WHERE id=?", b.ID).Scan(&creating); err != nil || creating != 0 {
				t.Fatal(creating, err)
			}
			if _, err = k.ReserveManagedProcess(ctx, ManagedProcessRequest{OwnerID: "again", RunID: "again", Mode: "execute", Binding: b}); err == nil {
				t.Fatal("canceled Task reopened after cleanup")
			}
			_, err = k.AdoptPlan(ctx, w.Target(), "replacement", plan)
			if scope == "task" && err != nil || scope == "wave" && err == nil {
				t.Fatal("cleanup did not preserve Wave cancellation policy", scope, err)
			}
		})
	}
}

func TestOperatorPauseThenCancelAllowsOnlyParentTerminalObservation(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := mailboxFixture(t)
	if _, err := k.Subscribe(ctx, SubscriptionRequest{SessionID: "main", Target: w.Target(), Key: "terminal", Kind: "wave_terminal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: w.Target(), Key: "child", Recipient: tasks[0].ID, Body: "unsent work"}); err != nil {
		t.Fatal(err)
	}
	mustControl(t, k, operatorRequest(w, "pause", "pause", ""))
	mustControl(t, k, operatorRequest(w, "cancel", "cancel", ""))
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.State != domain.WaveCanceled || current.Paused {
		t.Fatal(current, err)
	}
	pending, err := k.PendingWakeNotifications(ctx, "main", 10)
	if err != nil || len(pending) != 1 || pending[0].Kind != "wave_terminal" || pending[0].WaveState != domain.WaveCanceled {
		t.Fatal("cancellation observation was stranded", pending, err)
	}
	if child, err := k.PendingWakeNotifications(ctx, tasks[0].SessionID, 10); err != nil || len(child) != 0 {
		t.Fatal("cancellation restarted Task inputs", child, err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: "main", Key: "terminal", NotificationIDs: []string{pending[0].ID}})
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, "main")
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, wakeStartingRun(k, b, wake)); err != nil {
		t.Fatal("parent terminal run remained paused", err)
	}
	if set := mustReady(t, k, current); set.Reason != "terminal" {
		t.Fatal("cancel did not supersede pause", set)
	}
}
