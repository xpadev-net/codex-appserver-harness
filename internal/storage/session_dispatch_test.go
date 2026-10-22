package storage

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestDispatchSessionPagesAndStatePreserveUnknownRun(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := mailboxFixture(t)
	ids := []string{"main", tasks[0].SessionID, tasks[1].SessionID}
	sort.Strings(ids)
	got := []string{}
	after := ""
	for {
		page, err := k.DispatchSessions(ctx, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.SessionIDs) != 1 {
			t.Fatal(page)
		}
		got = append(got, page.SessionIDs...)
		if page.NextAfter == "" {
			break
		}
		if page.NextAfter <= after {
			t.Fatal("nonprogressing cursor")
		}
		after = page.NextAfter
	}
	if !reflect.DeepEqual(got, ids) {
		t.Fatal(got, ids)
	}
	initial, err := k.SessionDispatchState(ctx, tasks[0].SessionID)
	if err != nil || !initial.ProcessStopped || initial.Creating || initial.Run != nil || initial.Wake != nil {
		t.Fatal(initial, err)
	}
	snapshot, err := k.BuildSessionInput(ctx, tasks[0].SessionID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	b, run := reserveInputRun(t, k, tasks[0].SessionID, snapshot)
	run.State = "unknown"
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	state, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || state.Run == nil || state.Run.State != "unknown" || !state.RunStopped || !state.ProcessStopped || state.Run.ID != run.ID {
		t.Fatal(state, err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || !reflect.DeepEqual(state, replay) {
		t.Fatal("restart changed pending unknown facts", replay, err)
	}
	for _, limit := range []int{0, 129} {
		if _, err = k.DispatchSessions(ctx, "", limit); err == nil {
			t.Fatal("unbounded status page")
		}
	}
}
func TestSessionStateKeepsWakeSeparateFromExecutionAndAppliedInput(t *testing.T) {
	ctx := context.Background()
	k, _, task, _, request := wakeFixture(t)
	request.IncludeSessionContext = true
	wake, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	before, err := k.SessionDispatchState(ctx, task.SessionID)
	if err != nil || before.Wake == nil || before.Wake.Intent.State != "reserved" || before.Run != nil || !before.ProcessStopped {
		t.Fatal(before, err)
	}
	b := prepareWakeSession(t, k, task.SessionID)
	run := wakeStartingRun(k, b, wake)
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	bound, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || bound.Wake == nil || bound.Wake.Intent.State != "bound" || bound.Run == nil || bound.ProcessStopped || bound.RunStopped {
		t.Fatal(bound, err)
	}
	run = completeWakeWaiting(t, k, b, run)
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	completed, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || completed.Run == nil || completed.Run.State != "completed" || completed.Wake == nil || !completed.RunStopped {
		t.Fatal("stopping was confused with decision application", completed, err)
	}
	if _, err = k.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	applied, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || applied.Run != nil || applied.Wake != nil || applied.Binding.TaskState != contract.TaskWaiting || !applied.ProcessStopped {
		t.Fatal(applied, err)
	}
}
func TestDispatchIndexDoesNotDecodeAnotherSessionsCorruptRun(t *testing.T) {
	ctx := context.Background()
	k, _, _, b, run := kernelToolFixture(t, "task")
	if _, err := k.DB.Exec("UPDATE kernel_run_bindings SET state='completed' WHERE run_id=?", run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.SessionDispatchState(ctx, b.ID); err == nil {
		t.Fatal("inconsistent run accepted")
	}
	page, err := k.DispatchSessions(ctx, "", 128)
	if err != nil || len(page.SessionIDs) != 3 {
		t.Fatal("one broken session blocked index traversal", page, err)
	}
	if _, err = k.SessionDispatchState(ctx, "main"); err != nil {
		t.Fatal("unrelated main cannot progress", err)
	}
}
func TestPlanningInputSelectionHasMeaningfulReasonsAndNoReplay(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	page, err := k.PlanningInputs(ctx, "main", "", 128)
	if err != nil || !reflect.DeepEqual(page.Targets, []contract.Target{w.Target()}) {
		t.Fatal(page, err)
	}
	snapshot, err := k.BuildSessionInput(ctx, "main", page.Targets)
	if err != nil {
		t.Fatal(err)
	}
	b, run := reserveInputRun(t, k, "main", snapshot)
	page, err = k.PlanningInputs(ctx, "main", "", 128)
	if err != nil || len(page.Targets) != 0 {
		t.Fatal("active run was eligible for new planning", page, err)
	}
	result := contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{{Target: w.Target(), Action: "report", Summary: "retain remaining work", SubscriptionIDs: []string{}, Remaining: []string{"needs user context"}, Blockers: []string{}}}}
	saveMainCompletion(t, k, b, run, result, true)
	if _, err = k.ApplyMainResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	page, err = k.PlanningInputs(ctx, "main", "", 128)
	if err != nil || len(page.Targets) != 0 {
		t.Fatal("same initial planning target replayed after a report", page, err)
	}
	d := mustDirection(t, k, w)
	if _, err = k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	page, err = k.PlanningInputs(ctx, "main", "", 128)
	if err != nil || !reflect.DeepEqual(page.Targets, []contract.Target{current.Target()}) {
		t.Fatal("new direction had no fresh planning reason", page, err)
	}
	if _, err = k.AdoptPlan(ctx, current.Target(), "fresh", storagePlanFixture()); err != nil {
		t.Fatal(err)
	}
	page, err = k.PlanningInputs(ctx, "main", "", 128)
	if err != nil || len(page.Targets) != 0 {
		t.Fatal("existing current plan caused unsolicited main call", page, err)
	}
}
func TestPlanningSelectionRespectsPauseDeadlineBudgetAndOwner(t *testing.T) {
	for _, mode := range []string{"pause", "deadline", "turn-budget", "closed", "wake", "other-main"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, request := waveStoreFixture(t)
			w, err := k.CreateWave(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "pause":
				w.Paused = true
			case "deadline":
				k.Now = func() time.Time { return w.Deadline }
			case "turn-budget":
				w.MainTurns = w.Policy.Limits.MainTurns
			case "closed":
				if _, err = k.DB.Exec("UPDATE kernel_sessions SET payload=json_set(payload,'$.closed',json('true')) WHERE id='main'"); err != nil {
					t.Fatal(err)
				}
			case "wake":
				if _, err = k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture()); err != nil {
					t.Fatal(err)
				}
				tasks, err := k.Tasks(ctx, w.ID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: tasks[0].SessionID, Target: w.Target(), Key: "new", Recipient: "parent", Body: "question"}); err != nil {
					t.Fatal(err)
				}
				n, err := k.PendingWakeNotifications(ctx, "main", 1)
				if err != nil || len(n) != 1 {
					t.Fatal(n, err)
				}
				if _, err = k.ReserveWake(ctx, WakeRequest{SessionID: "main", Key: "wake", NotificationIDs: []string{n[0].ID}}); err != nil {
					t.Fatal(err)
				}
			case "other-main":
				w.OrchestratorSessionID = "some-other-main"
			}
			if mode != "deadline" && mode != "closed" && mode != "wake" {
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
			page, err := k.PlanningInputs(ctx, "main", "", 128)
			if err != nil || len(page.Targets) != 0 {
				t.Fatal(mode, page, err)
			}
		})
	}
}
func TestPlanningPagesDoNotChargeOrReserve(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	for _, source := range []string{"a", "b", "c"} {
		request.SourceID = source
		if _, err := k.CreateWave(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	targets := []contract.Target{}
	after := ""
	for {
		page, err := k.PlanningInputs(ctx, "main", after, 1)
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, page.Targets...)
		if page.NextAfter == "" {
			break
		}
		after = page.NextAfter
	}
	if len(targets) != 3 {
		t.Fatal(targets)
	}
	for _, table := range []string{"kernel_snapshots", "kernel_run_bindings", "kernel_wake_intents"} {
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal(table, count, err)
		}
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE kind IN ('main_turn','tokens')").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err := k.PlanningInputs(ctx, "main", "", 0); err == nil {
		t.Fatal("invalid planning bound")
	}
}

func TestPlanningRequeuesCurrentInitialInputAfterSharedJudgmentIsSuperseded(t *testing.T) {
	ctx := context.Background()
	k, b, waves, run, _ := mainResultFixture(t)
	saveMainCompletion(t, k, b, run, mainResult(t, b, waves, "report"), true)
	direction := mustDirection(t, k, waves[0])
	application, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || application.State != "archived" {
		t.Fatal(application, err)
	}
	page, err := k.PlanningInputs(ctx, b.ID, "", 128)
	if err != nil || !reflect.DeepEqual(page.Targets, []contract.Target{waves[1].Target()}) {
		t.Fatal("other Wave's undecided initial request was lost", page, err)
	}
	if _, err = k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	page, err = k.PlanningInputs(ctx, b.ID, "", 128)
	if err != nil || len(page.Targets) != 2 {
		t.Fatal("new scope and preserved input cannot progress", page, err)
	}
}

func TestRunnableWakeRechecksSavedTargetsWithoutReservingRun(t *testing.T) {
	ctx := context.Background()
	k, w, task, _, request := wakeFixture(t)
	wake, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := k.RunnableWake(ctx, wake.Intent.ID)
	if err != nil || !reflect.DeepEqual(ready, wake) {
		t.Fatal(ready, err)
	}
	if _, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-before-model", Target: w.Target(), TaskID: task.ID, Action: "pause", Reason: "hold notification"}); err != nil {
		t.Fatal(err)
	}
	if _, err = k.RunnableWake(ctx, wake.Intent.ID); !errors.Is(err, ErrWakeHeld) {
		t.Fatal("pause ignored by ready wake", err)
	}
	var runs int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal(runs, err)
	}
	replay, err := k.Wake(ctx, wake.Intent.ID)
	if err != nil || !reflect.DeepEqual(replay, wake) {
		t.Fatal("eligibility read changed saved input", replay, err)
	}
}

func TestReleaseUnstartedWakePreservesInputsAndRequiresNoModelRun(t *testing.T) {
	ctx := context.Background()
	k, _, task, m, request := wakeFixture(t)
	wake, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, task.SessionID)
	if err = k.ReleaseUnstartedWake(ctx, wake.Intent.ID, "known non-start"); !errors.Is(err, ErrWakeHeld) {
		t.Fatal("preparation was not stopped", err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	if err = k.ReleaseUnstartedWake(ctx, wake.Intent.ID, "known non-start"); err != nil {
		t.Fatal(err)
	}
	if err = k.ReleaseUnstartedWake(ctx, wake.Intent.ID, "known non-start"); err != nil {
		t.Fatal(err)
	}
	if err = k.ReleaseUnstartedWake(ctx, wake.Intent.ID, "different reason"); err == nil {
		t.Fatal("non-start audit reason overwritten")
	}
	entries, err := k.Mailbox(ctx, b.ID, 0, 10)
	if err != nil || len(entries) != 1 || entries[0].Message.ID != m.ID || entries[0].State != "pending" || entries[0].RunID != "" {
		t.Fatal(entries, err)
	}
	request.Key = "fresh-after-non-start"
	next, err := k.ReserveWake(ctx, request)
	if err != nil || next.Intent.RunID == wake.Intent.RunID {
		t.Fatal(next, err)
	}
	b = prepareWakeSession(t, k, b.ID)
	run := wakeStartingRun(k, b, next)
	if err = k.ReserveSessionRun(ctx, b, next.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err = k.ReleaseUnstartedWake(ctx, next.Intent.ID, "pretend model did not start"); !errors.Is(err, ErrWakeHeld) {
		t.Fatal("journaled model input was replayed", err)
	}
}
