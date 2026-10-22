package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func mainResultFixture(t *testing.T) (*KernelStore, contract.SessionBinding, []domain.Wave, contract.SessionRun, contract.InputSnapshot) {
	t.Helper()
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	waves := []domain.Wave{}
	for _, source := range []string{"first", "second"} {
		request.SourceID = source
		w, err := k.CreateWave(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		waves = append(waves, w)
	}
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, k, b)
	if err = k.BindSessionThread(ctx, b, "main-thread"); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := mainBudgetSnapshot(t, "main-input", b.ID, waves[0].Target(), waves[1].Target())
	run := contract.SessionRun{ID: "main-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	return k, b, waves, run, snapshot
}
func mainResult(t *testing.T, b contract.SessionBinding, waves []domain.Wave, action string) contract.MainResult {
	t.Helper()
	result := contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{}}
	for _, wave := range waves {
		d := contract.MainDecision{Target: wave.Target(), Action: action, Summary: "Bound original request remains the scope.", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}}
		if action == "plan" {
			p := storagePlanFixture()
			d.Plan = &p
		}
		result.Decisions = append(result.Decisions, d)
	}
	return result
}
func saveMainCompletion(t *testing.T, k *KernelStore, b contract.SessionBinding, run contract.SessionRun, result contract.MainResult, stop bool) contract.SessionRun {
	t.Helper()
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID, run.Final = "completed", "main-turn", string(raw)
	if err = k.Put(context.Background(), "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if stop {
		if err = k.MarkSessionStopped(context.Background(), b.ID, run.ID); err != nil {
			t.Fatal(err)
		}
	}
	return run
}
func TestMainResultAdoptsAllScopedPlansAtomicallyAndReplaysAfterRebind(t *testing.T) {
	ctx := context.Background()
	k, b, waves, run, snapshot := mainResultFixture(t)
	run = saveMainCompletion(t, k, b, run, mainResult(t, b, waves, "plan"), false)
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("live process applied main plan")
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_main_apply BEFORE INSERT ON records WHEN NEW.kind='main_result_application' BEGIN SELECT RAISE(ABORT,'injected failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("main result survived incomplete transaction")
	}
	var tasks, plans, applied int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_plans").Scan(&plans); err != nil {
		t.Fatal(err)
	}
	if err := k.DB.QueryRow("SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&applied); err != nil || tasks != 0 || plans != 0 || applied != 0 {
		t.Fatal("partial main adoption survived", tasks, plans, applied, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_main_apply"); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != "applied" || len(receipt.Plans) != 2 {
		t.Fatal(receipt, err)
	}
	for _, wave := range waves {
		current, err := k.Wave(ctx, wave.ID)
		if err != nil || current.State == domain.WaveCompleted || current.TasksCreated != 2 {
			t.Fatal("main acceptance or lost tasks", current, err)
		}
	}
	if err = k.RebindMainThread(ctx, b, "replacement-main", snapshot.ID, "safe maintenance"); err != nil {
		t.Fatal(err)
	}
	again, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || !reflect.DeepEqual(receipt, again) {
		t.Fatal("thread rotation changed receipt", again, err)
	}
	path := filepath.Join(k.Root, "state.db")
	if err = k.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err = reopened.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || !reflect.DeepEqual(receipt, again) {
		t.Fatal("restart repeated main judgment", again, err)
	}
}
func TestMainResultRejectsMissingForeignStaleAndPolicyWeakeningDecisions(t *testing.T) {
	for _, kind := range []string{"missing", "foreign", "stale", "invented-criterion", "self-complete", "wrong-session"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, b, waves, run, _ := mainResultFixture(t)
			result := mainResult(t, b, waves, "plan")
			switch kind {
			case "missing":
				result.Decisions = result.Decisions[:1]
			case "foreign":
				result.Decisions[0].Target.WaveID = "foreign"
			case "stale":
				result.Decisions[0].Target.ScopeRevision++
			case "invented-criterion":
				result.Decisions[0].Plan.Phases[0].Tasks[0].Criteria = []string{"unapproved"}
			case "self-complete":
				result.Decisions[0].Action = "complete"
			case "wrong-session":
				result.SessionID = "foreign-main"
			}
			saveMainCompletion(t, k, b, run, result, true)
			if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); !errors.Is(err, ErrMainResultInvalid) {
				t.Fatal("invalid main result adopted", err)
			}
			var plans, applied int
			k.DB.QueryRow("SELECT count(*) FROM kernel_plans").Scan(&plans)
			k.DB.QueryRow("SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&applied)
			if plans != 0 || applied != 0 {
				t.Fatal("invalid proposal changed state", plans, applied)
			}
		})
	}
}
func TestMainResultUnknownOutcomeRequiresHistoryAndPauseHoldsPlan(t *testing.T) {
	ctx := context.Background()
	k, b, waves, run, _ := mainResultFixture(t)
	run.State = "unknown"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("unknown main outcome applied")
	}
	saveMainCompletion(t, k, b, run, mainResult(t, b, waves, "plan"), true)
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Target: waves[0].Target(), Key: "pause", Action: "pause", Reason: "operator pause"}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); !errors.Is(err, ErrMainResultHeld) {
		t.Fatal("pause discarded or applied plan", err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Target: waves[0].Target(), Key: "resume", Action: "resume", Reason: "operator resume"}); err != nil {
		t.Fatal(err)
	}
	if receipt, err := k.ApplyMainResult(ctx, b.ID, run.ID); err != nil || receipt.State != "applied" {
		t.Fatal(receipt, err)
	}
}

func TestMainResultInterruptRetainsUndecidedRunAndCancelWins(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	wave, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	reservation := managedReserve(t, k, b, "main-owner", "managed-main-run", "execute")
	managedStart(t, k, reservation, 77)
	b = managedBind(t, k, reservation)
	snapshot := mainBudgetSnapshot(t, "interrupt-main-input", b.ID, wave.Target())
	run := contract.SessionRun{ID: "managed-main-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	interrupt, err := k.RequestSessionInterrupt(ctx, SessionInterruptRequest{Key: "main-stop", Target: wave.Target(), SessionID: b.ID, RunID: run.ID, PreparationID: b.PreparationID, Reason: "explicit stop"})
	if err != nil {
		t.Fatal(err)
	}
	run = saveMainCompletion(t, k, b, run, mainResult(t, b, []domain.Wave{wave}, "plan"), false)
	if err = k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "main-owner", Disposition: "group_stopped", Identity: managedIdentity(77)}); err != nil {
		t.Fatal(err)
	}
	if err = k.CompleteSessionInterrupt(ctx, interrupt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyMainResult(ctx, b.ID, run.ID); !errors.Is(err, ErrInterruptionHeld) {
		t.Fatal("late output revoked interruption", err)
	}
	var applied, plans int
	k.DB.QueryRow("SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&applied)
	k.DB.QueryRow("SELECT count(*) FROM kernel_plans").Scan(&plans)
	if applied != 0 || plans != 0 {
		t.Fatal("interrupted main input was consumed", applied, plans)
	}
	if _, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel", Target: wave.Target(), Action: "cancel", Reason: "operator cancel"}); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != "archived" || len(receipt.Plans) != 0 {
		t.Fatal(receipt, err)
	}
	current, err := k.Wave(ctx, wave.ID)
	if err != nil || current.State != domain.WaveCanceled {
		t.Fatal(current, err)
	}
}
func TestMainResultAppliesOrRequeuesSharedWake(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			ctx := context.Background()
			k, request := waveStoreFixture(t)
			waves := []domain.Wave{}
			messages := []Message{}
			for _, source := range []string{"first", "second"} {
				request.SourceID = source
				wave, err := k.CreateWave(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				plan, err := k.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture())
				if err != nil {
					t.Fatal(err)
				}
				task, err := k.Task(ctx, plan.CreatedTaskIDs[0])
				if err != nil {
					t.Fatal(err)
				}
				message, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: task.SessionID, Target: wave.Target(), Recipient: "parent", Key: "question", Body: "Need parent explanation."})
				if err != nil {
					t.Fatal(err)
				}
				waves = append(waves, wave)
				messages = append(messages, message)
			}
			notifications, err := k.PendingWakeNotifications(ctx, "main", 100)
			if err != nil || len(notifications) != 2 {
				t.Fatal(notifications, err)
			}
			ids := []string{}
			for _, n := range notifications {
				ids = append(ids, n.ID)
			}
			wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: "main", Key: "shared", NotificationIDs: ids})
			if err != nil {
				t.Fatal(err)
			}
			b := prepareWakeSession(t, k, "main")
			run := wakeStartingRun(k, b, wake)
			if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, run); err != nil {
				t.Fatal(err)
			}
			saveMainCompletion(t, k, b, run, mainResult(t, b, waves, "report"), true)
			if cancel {
				if _, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-first", Target: waves[0].Target(), Action: "cancel", Reason: "operator cancel"}); err != nil {
					t.Fatal(err)
				}
			}
			receipt, err := k.ApplyMainResult(ctx, b.ID, run.ID)
			expected := "applied"
			if cancel {
				expected = "archived"
			}
			if err != nil || receipt.State != expected {
				t.Fatal(receipt, err)
			}
			for i, message := range messages {
				var state, boundRun string
				if err = k.DB.QueryRow("SELECT state,run_id FROM kernel_mailbox WHERE message_id=? AND session_id='main'", message.ID).Scan(&state, &boundRun); err != nil {
					t.Fatal(err)
				}
				if !cancel {
					if state != "applied" || boundRun != run.ID {
						t.Fatal("input was not applied with main decision", state, boundRun)
					}
				} else if i == 0 {
					if state != "archived" || boundRun != run.ID {
						t.Fatal("obsolete input not retained", state, boundRun)
					}
				} else if state != "pending" || boundRun != "" {
					t.Fatal("current Wave input lost", state, boundRun)
				}
			}
			saved, err := k.Wake(ctx, wake.Intent.ID)
			wakeState := "applied"
			if cancel {
				wakeState = "superseded"
			}
			if err != nil || saved.Intent.State != wakeState {
				t.Fatal(saved, err)
			}
		})
	}
}
func TestMainResultWaitRequiresExactOwnedSubscriptions(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(fmt.Sprint(foreign), func(t *testing.T) {
			ctx := context.Background()
			k, b, waves, run, _ := mainResultFixture(t)
			result := mainResult(t, b, waves, "wait")
			for i, wave := range waves {
				sub, err := k.Subscribe(ctx, SubscriptionRequest{SessionID: b.ID, Target: wave.Target(), Key: "terminal-" + wave.ID, Kind: "wave_terminal"})
				if err != nil {
					t.Fatal(err)
				}
				result.Decisions[i].SubscriptionIDs = []string{sub.ID}
			}
			if foreign {
				result.Decisions[0].SubscriptionIDs = result.Decisions[1].SubscriptionIDs
			}
			saveMainCompletion(t, k, b, run, result, true)
			receipt, err := k.ApplyMainResult(ctx, b.ID, run.ID)
			if foreign {
				if !errors.Is(err, ErrMainResultInvalid) {
					t.Fatal("foreign Wave subscription accepted", err)
				}
			} else if err != nil || receipt.State != "applied" {
				t.Fatal(receipt, err)
			}
		})
	}
}

func TestMainResultCanReportAnAlreadyCanceledTerminalObservation(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	wave, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.Subscribe(ctx, SubscriptionRequest{SessionID: "main", Target: wave.Target(), Key: "terminal", Kind: "wave_terminal"}); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel", Target: wave.Target(), Action: "cancel", Reason: "operator cancel"}); err != nil {
		t.Fatal(err)
	}
	notifications, err := k.PendingWakeNotifications(ctx, "main", 10)
	if err != nil || len(notifications) != 1 {
		t.Fatal(notifications, err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: "main", Key: "terminal", NotificationIDs: []string{notifications[0].ID}})
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, "main")
	run := wakeStartingRun(k, b, wake)
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	saveMainCompletion(t, k, b, run, mainResult(t, b, []domain.Wave{wave}, "report"), true)
	receipt, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != "applied" {
		t.Fatal("terminal observation was confused with late work", receipt, err)
	}
	current, err := k.Wave(ctx, wave.ID)
	if err != nil || current.State != domain.WaveCanceled {
		t.Fatal(current, err)
	}
}

func TestMainResultWaveLimitMatchesWakeAndRunAdmission(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	targets := []contract.Target{}
	for i := range contract.MaxMainWaves + 1 {
		request.SourceID = fmt.Sprintf("terminal-%d", i)
		wave, err := k.CreateWave(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = k.Subscribe(ctx, SubscriptionRequest{SessionID: "main", Target: wave.Target(), Key: "terminal-" + wave.ID, Kind: "wave_terminal"}); err != nil {
			t.Fatal(err)
		}
		if _, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel", Target: wave.Target(), Action: "cancel", Reason: "terminal load"}); err != nil {
			t.Fatal(err)
		}
		targets = append(targets, wave.Target())
	}
	notifications, err := k.PendingWakeNotifications(ctx, "main", 100)
	if err != nil || len(notifications) != contract.MaxMainWaves+1 {
		t.Fatal(len(notifications), err)
	}
	ids := []string{}
	for _, n := range notifications {
		ids = append(ids, n.ID)
	}
	if _, err = k.ReserveWake(ctx, WakeRequest{SessionID: "main", Key: "oversize", NotificationIDs: ids}); !errors.Is(err, ErrWakeScopeLimit) {
		t.Fatal("unanswerable wake admitted", err)
	}
	var intents, pending int
	k.DB.QueryRow("SELECT count(*) FROM kernel_wake_intents").Scan(&intents)
	k.DB.QueryRow("SELECT count(*) FROM kernel_notifications WHERE state='pending'").Scan(&pending)
	if intents != 0 || pending != len(ids) {
		t.Fatal("oversized wake consumed inputs", intents, pending)
	}
	b := prepareWakeSession(t, k, "main")
	run := contract.SessionRun{ID: "too-large-main", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, b, mainBudgetSnapshot(t, "oversize", b.ID, targets...), run); err == nil {
		t.Fatal("unanswerable direct run admitted")
	}
	// The accepted subset can produce and apply a complete valid result, while
	// the remaining terminal notification stays pending for a later batch.
	if err = k.MarkSessionPreparationStopped(ctx, b.ID, b.PreparationID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: "main", Key: "bounded", NotificationIDs: ids[:contract.MaxMainWaves]})
	if err != nil {
		t.Fatal(err)
	}
	b = prepareWakeSession(t, k, "main")
	run = wakeStartingRun(k, b, wake)
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	selected := []domain.Wave{}
	for _, target := range wake.Intent.Targets {
		wave, err := k.Wave(ctx, target.WaveID)
		if err != nil {
			t.Fatal(err)
		}
		selected = append(selected, wave)
	}
	saveMainCompletion(t, k, b, run, mainResult(t, b, selected, "report"), true)
	receipt, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != "applied" {
		t.Fatal(receipt, err)
	}
	k.DB.QueryRow("SELECT count(*) FROM kernel_notifications WHERE state='pending'").Scan(&pending)
	if pending != 1 {
		t.Fatal("remaining input lost", pending)
	}
}
