package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func terminalSubscription(b contract.SessionBinding) SubscriptionRequest {
	return SubscriptionRequest{SessionID: b.ParentSessionID, Target: b.Target, Key: "observe-" + b.TaskID, Kind: "task_terminal", TaskID: b.TaskID}
}
func TestSubscribeBeforeAfterAndConcurrentCompletion(t *testing.T) {
	for _, ordering := range []string{"before", "after", "concurrent"} {
		t.Run(ordering, func(t *testing.T) {
			ctx := context.Background()
			store, b, run, _ := recordedTaskResultFixture(t, nil)
			if err := store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			request := terminalSubscription(b)
			var sub Subscription
			var subscribeErr, applyErr error
			if ordering == "before" {
				sub, subscribeErr = store.Subscribe(ctx, request)
				prior, err := store.Notifications(ctx, request.SessionID, 0, 100)
				if err != nil || len(prior) != 0 {
					t.Fatal("not-yet-completed task notified", prior, err)
				}
				_, applyErr = store.ApplyTaskResult(ctx, b.ID, run.ID)
			} else if ordering == "after" {
				_, applyErr = store.ApplyTaskResult(ctx, b.ID, run.ID)
				sub, subscribeErr = store.Subscribe(ctx, request)
			} else {
				var group sync.WaitGroup
				group.Add(2)
				start := make(chan struct{})
				go func() { defer group.Done(); <-start; sub, subscribeErr = store.Subscribe(ctx, request) }()
				go func() { defer group.Done(); <-start; _, applyErr = store.ApplyTaskResult(ctx, b.ID, run.ID) }()
				close(start)
				group.Wait()
			}
			if subscribeErr != nil || applyErr != nil {
				t.Fatal(subscribeErr, applyErr)
			}
			notifications, err := store.Notifications(ctx, request.SessionID, 0, 100)
			if err != nil || len(notifications) != 1 {
				t.Fatal(notifications, err)
			}
			n := notifications[0]
			if n.SubscriptionID != sub.ID || n.Target != b.Target || n.TaskID != b.TaskID || n.TaskState != contract.TaskCompleted || n.ResultID == "" || n.InputDigest == "" || n.State != "pending" || n.RunID != "" || n.DeliveryAcknowledged {
				t.Fatal("incomplete terminal observation", n)
			}
			if _, err = store.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			if replay, err := store.Subscribe(ctx, request); err != nil || replay.ID != sub.ID {
				t.Fatal(replay, err)
			}
			changed := request
			changed.Kind, changed.TaskID = "wave_terminal", ""
			if _, err = store.Subscribe(ctx, changed); err == nil {
				t.Fatal("subscription key changed condition")
			}
			after, err := store.Notifications(ctx, request.SessionID, 0, 100)
			if err != nil || !reflect.DeepEqual(notifications, after) {
				t.Fatal("replay changed observation", after, err)
			}
		})
	}
}
func TestNotificationFailureRollsBackTaskResult(t *testing.T) {
	for _, failure := range []string{"insert", "cursor"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			store, b, run, _ := recordedTaskResultFixture(t, nil)
			if _, err := store.Subscribe(ctx, terminalSubscription(b)); err != nil {
				t.Fatal(err)
			}
			if err := store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			sql := "CREATE TRIGGER inject_notification BEFORE INSERT ON kernel_notifications BEGIN SELECT RAISE(ABORT,'injected notification crash'); END"
			if failure == "cursor" {
				sql = "CREATE TRIGGER inject_notification BEFORE UPDATE ON kernel_subscriptions BEGIN SELECT RAISE(ABORT,'injected cursor crash'); END"
			}
			if _, err := store.DB.Exec(sql); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ApplyTaskResult(ctx, b.ID, run.ID); err == nil {
				t.Fatal("injected notification failure ignored")
			}
			task, err := store.Task(ctx, b.TaskID)
			if err != nil || task.State != contract.TaskRunning || task.ResultID != "" {
				t.Fatal("partial Task result committed", task, err)
			}
			var events, intents, observations int
			store.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='task_result_applied'").Scan(&events)
			store.DB.QueryRow("SELECT count(*) FROM records WHERE kind='notification_intent' AND id=?", run.ID+":result_applied").Scan(&intents)
			store.DB.QueryRow("SELECT count(*) FROM kernel_notifications").Scan(&observations)
			if events != 0 || intents != 0 || observations != 0 {
				t.Fatal("partial event or notification survived", events, intents, observations)
			}
			if _, err = store.DB.Exec("DROP TRIGGER inject_notification"); err != nil {
				t.Fatal(err)
			}
			if _, err = store.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			notifications, err := store.Notifications(ctx, b.ParentSessionID, 0, 10)
			if err != nil || len(notifications) != 1 {
				t.Fatal(notifications, err)
			}
		})
	}
}
func TestNotificationDeliveryAckIsSeparateAndSurvivesReboot(t *testing.T) {
	ctx := context.Background()
	store, b, run, _ := recordedTaskResultFixture(t, nil)
	request := terminalSubscription(b)
	sub, err := store.Subscribe(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	items, err := store.Notifications(ctx, b.ParentSessionID, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatal(items, err)
	}
	if err = store.AcknowledgeNotification(ctx, b.ID, items[0].ID); err == nil {
		t.Fatal("child acknowledged parent's notification")
	}
	for range 2 {
		if err = store.AcknowledgeNotification(ctx, b.ParentSessionID, items[0].ID); err != nil {
			t.Fatal(err)
		}
	}
	store.Close()
	reopened, err := OpenKernel(filepath.Join(store.Root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	itemsAfter, err := reopened.Notifications(ctx, b.ParentSessionID, 0, 10)
	if err != nil || len(itemsAfter) != 1 || !itemsAfter[0].DeliveryAcknowledged || itemsAfter[0].State != "pending" || itemsAfter[0].RunID != "" {
		t.Fatal("delivery ack was lost or became processing", itemsAfter, err)
	}
	replay, err := reopened.Subscribe(ctx, request)
	if err != nil || replay.ID != sub.ID {
		t.Fatal(replay, err)
	}
	page, err := reopened.Notifications(ctx, b.ParentSessionID, itemsAfter[0].DeliverySequence, 10)
	if err != nil || len(page) != 0 {
		t.Fatal(page, err)
	}
	var wakes int
	if err = reopened.DB.QueryRow("SELECT count(*) FROM kernel_wake_intents").Scan(&wakes); err != nil || wakes != 0 {
		t.Fatal("delivery created a model wake", wakes, err)
	}
}
func TestNotificationReconciliationIsBoundedAndOrderIndependent(t *testing.T) {
	ctx := context.Background()
	store, b, run, _ := recordedTaskResultFixture(t, nil)
	sub, err := store.Subscribe(ctx, terminalSubscription(b))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	// Simulate a v2 intent from before fanout was installed. Reverse delivery
	// may revisit older source events after the terminal observation exists.
	if _, err = store.DB.Exec("DELETE FROM records WHERE kind='notification_fanout'"); err != nil {
		t.Fatal(err)
	}
	var previousCursor int64
	if err = store.DB.QueryRow("SELECT cursor FROM kernel_subscriptions WHERE id=?", sub.ID).Scan(&previousCursor); err != nil {
		t.Fatal(err)
	}
	total := 0
	for {
		count, err := store.ReconcileNotifications(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
		total += count
		if total > 20 {
			t.Fatal("reconciliation never converged")
		}
	}
	var currentCursor int64
	if err = store.DB.QueryRow("SELECT cursor FROM kernel_subscriptions WHERE id=?", sub.ID).Scan(&currentCursor); err != nil || currentCursor != previousCursor || total == 0 {
		t.Fatal(previousCursor, currentCursor, total, err)
	}
	items, err := store.Notifications(ctx, b.ParentSessionID, 0, 10)
	if err != nil || len(items) != 1 {
		t.Fatal("reverse replay duplicated notification", items, err)
	}
}
func TestSubscriptionAuthorityAndMultipleWaves(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	first, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := store.AdoptPlan(ctx, first.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	work, _ := store.Task(ctx, receipt.CreatedTaskIDs[0])
	review, _ := store.Task(ctx, receipt.CreatedTaskIDs[1])
	for _, invalid := range []SubscriptionRequest{
		{SessionID: work.SessionID, Target: work.Target(), Key: "sibling", Kind: "task_terminal", TaskID: review.ID},
		{SessionID: work.SessionID, Target: work.Target(), Key: "wave", Kind: "wave_terminal"},
	} {
		if _, err = store.Subscribe(ctx, invalid); err == nil {
			t.Fatal("forbidden subscription", invalid)
		}
	}
	if _, err = store.Subscribe(ctx, SubscriptionRequest{SessionID: review.SessionID, Target: review.Target(), Key: "dependency", Kind: "task_terminal", TaskID: work.ID}); err != nil {
		t.Fatal("declared dependency observation denied", err)
	}
	request.SourceID = "second"
	second, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.AdoptPlan(ctx, second.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	otherWork, _ := store.Task(ctx, other.CreatedTaskIDs[0])
	if _, err = store.Subscribe(ctx, SubscriptionRequest{SessionID: review.SessionID, Target: second.Target(), Key: "foreign", Kind: "task_terminal", TaskID: otherWork.ID}); err == nil {
		t.Fatal("cross-Wave Task subscription accepted")
	}
	for _, task := range []domain.Task{work, otherWork} {
		if _, err = store.Subscribe(ctx, SubscriptionRequest{SessionID: "main", Target: task.Target(), Key: task.ID, Kind: "task_terminal", TaskID: task.ID}); err != nil {
			t.Fatal(err)
		}
		completeTemplateTask(t, store, task, "result "+task.ID, "", nil)
	}
	items, err := store.Notifications(ctx, "main", 0, 10)
	if err != nil || len(items) != 2 || items[0].Target.WaveID == items[1].Target.WaveID {
		t.Fatal("same main lost a Wave", items, err)
	}
	foreignPage, err := store.Notifications(ctx, work.SessionID, 0, 10)
	if err != nil || len(foreignPage) != 0 {
		t.Fatal("sibling notification leaked", foreignPage, err)
	}
}
func TestNotificationCarriesCompletedBindingButNotRetiredTasks(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	initial, err := store.AdoptPlan(ctx, wave.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	work, _ := store.Task(ctx, initial.CreatedTaskIDs[0])
	completeTemplateTask(t, store, work, "work", "", nil)
	plan.Phases[1].Tasks[0].ID = "review-new"
	plan.Phases[1].ID = "evaluation-new"
	plan.Phases[1].Completion.RequiredTaskIDs = []string{"review-new"}
	next, err := store.AdoptPlan(ctx, wave.Target(), "replan", plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Subscribe(ctx, SubscriptionRequest{SessionID: "main", Target: next.Target, Key: "carry", Kind: "task_terminal", TaskID: work.ID}); err != nil {
		t.Fatal("explicit completed carry was not observable", err)
	}
	if _, err = store.Subscribe(ctx, SubscriptionRequest{SessionID: "main", Target: next.Target, Key: "retired", Kind: "task_terminal", TaskID: initial.CreatedTaskIDs[1]}); err == nil {
		t.Fatal("retired Task became current observation")
	}
	items, err := store.Notifications(ctx, "main", 0, 10)
	if err != nil || len(items) != 1 || items[0].Target != next.Target || items[0].SourceTarget != work.Target() || items[0].ResultID == "" {
		t.Fatal(items, err)
	}
}
func TestTerminalNotificationIsObservationNotSuccess(t *testing.T) {
	for _, mutation := range []string{"failure", "paused", "wave-canceled"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			store, b, run, _ := recordedTaskResultFixture(t, func(r *contract.TaskResult) {
				if mutation == "failure" {
					r.CriterionResults = nil
				}
			})
			if _, err := store.Subscribe(ctx, terminalSubscription(b)); err != nil {
				t.Fatal(err)
			}
			if mutation != "failure" {
				wave, err := store.Wave(ctx, b.Target.WaveID)
				if err != nil {
					t.Fatal(err)
				}
				wave.Paused = mutation == "paused"
				if mutation == "wave-canceled" {
					wave.State, wave.CompletedAt = domain.WaveCanceled, store.clock()
				}
				tx, err := store.kernelTx(ctx)
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
			if err := store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			items, err := store.Notifications(ctx, "main", 0, 10)
			if err != nil || len(items) != 1 {
				t.Fatal(items, err)
			}
			if mutation == "failure" && items[0].TaskState != contract.TaskFailed || mutation == "wave-canceled" && items[0].TaskState != contract.TaskCanceled {
				t.Fatal("terminal observation converted to success", items)
			}
			var wakes int
			if err = store.DB.QueryRow("SELECT count(*) FROM kernel_wake_intents").Scan(&wakes); err != nil || wakes != 0 {
				t.Fatal("observation launched a turn", wakes, err)
			}
		})
	}
}
func TestStaleNotificationIntentDoesNotBecomeCurrentInput(t *testing.T) {
	ctx := context.Background()
	store, b, run, _ := recordedTaskResultFixture(t, nil)
	if _, err := store.Subscribe(ctx, terminalSubscription(b)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	wave, err := store.Wave(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	wave.ScopeRevision++
	wave.Policy.Revision++
	raw, _ := json.Marshal(wave)
	if _, err = store.DB.Exec("UPDATE kernel_waves SET payload=? WHERE id=?", raw, wave.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec("DELETE FROM records WHERE kind='notification_fanout'"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ReconcileNotifications(ctx, 100); err != nil {
		t.Fatal(err)
	}
	items, err := store.Notifications(ctx, "main", 0, 10)
	if err != nil || len(items) != 1 || items[0].Target != b.Target || items[0].Target == wave.Target() {
		t.Fatal("old observation rewritten into current scope", items, err)
	}
}

func TestWaveTerminalSubscriptionsAndForeignParent(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	main, err := store.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.CreateOrchestrator(ctx, "other-main", main.Model, main.CWD, main.Sandbox); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Subscribe(ctx, SubscriptionRequest{SessionID: "other-main", Target: wave.Target(), Key: "foreign", Kind: "wave_terminal"}); err == nil {
		t.Fatal("another main observed an unrelated Wave")
	}
	before := SubscriptionRequest{SessionID: "main", Target: wave.Target(), Key: "before", Kind: "wave_terminal"}
	if _, err = store.Subscribe(ctx, before); err != nil {
		t.Fatal(err)
	}
	wave.State, wave.CompletedAt = domain.WaveCanceled, store.clock()
	tx, err := store.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = saveWave(ctx, tx, wave); err != nil {
		t.Fatal(err)
	}
	if err = store.waveEventTx(ctx, tx, wave, "wave_canceled", "", wave.ID+":canceled", wave.Target()); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after := before
	after.Key = "after"
	if _, err = store.Subscribe(ctx, after); err != nil {
		t.Fatal(err)
	}
	items, err := store.Notifications(ctx, "main", 0, 10)
	if err != nil || len(items) != 2 {
		t.Fatal(items, err)
	}
	if items[0].WaveState != domain.WaveCanceled || items[0].EventID != items[1].EventID || items[0].ObservedVia != "event" || items[0].SourceEventID != wave.ID+":canceled" || items[1].ObservedVia != "state_snapshot" || items[1].SourceEventID != "" {
		t.Fatal("event and snapshot observations have incorrect provenance", items)
	}
}

func TestReplanCancellationNotifiesOutgoingSubscriptionsAtomically(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range initial.CreatedTaskIDs {
		if _, err = store.Subscribe(ctx, SubscriptionRequest{SessionID: "main", Target: initial.Target, Key: id, Kind: "task_terminal", TaskID: id}); err != nil {
			t.Fatal(err)
		}
	}
	plan := storagePlanFixture()
	for i := range plan.Phases {
		phase := &plan.Phases[i]
		phase.ID += "-new"
		phase.Tasks[0].ID += "-new"
		phase.Completion.RequiredTaskIDs = []string{phase.Tasks[0].ID}
		if i > 0 {
			phase.Tasks[0].DependsOn[0].TaskID += "-new"
		}
	}
	if _, err = store.DB.Exec("CREATE TRIGGER inject_cancel_notification BEFORE INSERT ON kernel_notifications BEGIN SELECT RAISE(ABORT,'cancellation notification failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AdoptPlan(ctx, initial.Target, "replan", plan); err == nil {
		t.Fatal("plan committed without outgoing terminal notification")
	}
	current, err := store.Wave(ctx, wave.ID)
	if err != nil || current.PlanRevision != 1 || current.TasksCreated != 2 {
		t.Fatal("partial replacement plan survived", current, err)
	}
	for _, id := range initial.CreatedTaskIDs {
		task, err := store.Task(ctx, id)
		if err != nil || task.State != contract.TaskReady {
			t.Fatal("partial cancellation survived", task, err)
		}
	}
	if _, err = store.DB.Exec("DROP TRIGGER inject_cancel_notification"); err != nil {
		t.Fatal(err)
	}
	next, err := store.AdoptPlan(ctx, initial.Target, "replan", plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.ReconcileNotifications(ctx, 100); err != nil {
		t.Fatal(err)
	}
	items, err := store.Notifications(ctx, "main", 0, 100)
	if err != nil || len(items) != 2 {
		t.Fatal(items, err)
	}
	for _, item := range items {
		if item.TaskState != contract.TaskCanceled || item.Target != initial.Target || item.Target == next.Target || item.ObservedVia != "event" || item.SourceEventID != item.TaskID+":canceled" {
			t.Fatal("outgoing cancellation was lost or became new-plan input", item)
		}
	}
}
func TestOldNonterminalIntentObservesCurrentSnapshotWithCorrectBoundary(t *testing.T) {
	ctx := context.Background()
	store, b, first, result := recordedTaskResultFixture(t, func(r *contract.TaskResult) {
		r.Outcome, r.Blockers = "blocked", []string{"need input"}
	})
	if _, err := store.Subscribe(ctx, terminalSubscription(b)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkSessionStopped(ctx, b.ID, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyTaskResult(ctx, b.ID, first.ID); err != nil {
		t.Fatal(err)
	}
	b, err := store.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, store, b)
	result.Outcome, result.Blockers = "result", nil
	recordAndStopTaskResult(t, store, b, "second", result)
	applied, err := store.ApplyTaskResult(ctx, b.ID, "second")
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct the pre-fanout situation in which an older blocked event is
	// visited first, after the Task has reached a newer terminal result.
	if _, err = store.DB.Exec("DELETE FROM kernel_notifications"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec("DELETE FROM records WHERE kind='notification_fanout' AND id=?", first.ID+":result_applied"); err != nil {
		t.Fatal(err)
	}
	if count, err := store.ReconcileNotifications(ctx, 1); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	var boundary int64
	if err = store.DB.QueryRow("SELECT max(sequence) FROM kernel_events").Scan(&boundary); err != nil {
		t.Fatal(err)
	}
	items, err := store.Notifications(ctx, "main", 0, 100)
	if err != nil || len(items) != 1 || items[0].ObservedVia != "state_snapshot" || items[0].SourceEventID != "" || items[0].EventBoundary != boundary || items[0].ResultID != applied.ResultID {
		t.Fatal("nonterminal source was presented as the cause of completion", items, err)
	}
}
