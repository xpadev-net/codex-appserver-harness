package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestAutomaticTemplateProgressionHoldsDownstreamAndUsesNoMainTurn(t *testing.T) {
	ctx := context.Background()
	k, w, review, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"})
	set := mustReady(t, k, w)
	if set.Reason != "review_findings" || set.State != "held" {
		t.Fatal(set)
	}
	oldReport, err := k.Task(ctx, structureID("task-", w.ID, "report"))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.checkTaskDispatchTx(ctx, tx, oldReport); err == nil {
		t.Fatal("direct dispatch bypassed findings gate")
	}
	tx.Rollback()
	changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID)
	if err != nil || !changed {
		t.Fatal(changed, err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil || w.MainTurns != 0 || w.Replans != 0 || w.FixRounds != 1 || w.PlanRevision != 2 {
		t.Fatal(w, err)
	}
	ready, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
	if err != nil || len(ready.Tasks) != 1 || ready.Tasks[0].Task.Spec.Kind != "fix" {
		t.Fatal(ready, err)
	}
	if ready.Tasks[0].Task.SessionID == review.SessionID {
		t.Fatal("completed review session reused")
	}
	for i := 0; i < 3; i++ {
		changed, err = k.ReconcileSessionTemplates(ctx, review.SessionID)
		if err != nil || changed {
			t.Fatal("poll repeated expansion", changed, err)
		}
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
	if changed, err = reopened.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || changed {
		t.Fatal(changed, err)
	}
	current, err := reopened.Wave(ctx, w.ID)
	if err != nil || current.FixRounds != 1 || current.MainTurns != 0 {
		t.Fatal(current, err)
	}
}
func TestAutomaticTemplateProgressionDoesNotBypassUnclassifiedOrInconclusiveReview(t *testing.T) {
	for _, verdict := range []string{"findings", "inconclusive"} {
		t.Run(verdict, func(t *testing.T) {
			ctx := context.Background()
			k, w, review, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"specification_change"})
			if verdict == "inconclusive" {
				var result contract.TaskResult
				review, _ = k.Task(ctx, review.ID)
				if err := k.Record(ctx, "task_result", review.ResultID, &result); err != nil {
					t.Fatal(err)
				}
				result.Review.Verdict, result.Review.Findings = verdict, []contract.ReviewFinding{}
				if err := k.Put(ctx, "task_result", review.ResultID, review.ID, result); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID)
				if err != nil || changed {
					t.Fatal(changed, err)
				}
				set := mustReady(t, k, w)
				if set.Reason != "review_"+verdict {
					t.Fatal(set)
				}
			}
			current, err := k.Wave(ctx, w.ID)
			if err != nil || current.PlanRevision != w.PlanRevision || current.FixRounds != 0 || current.MainTurns != 0 {
				t.Fatal(current, err)
			}
		})
	}
}
func TestAutomaticTemplateProgressionWaitsForParentRunAndHonorsPause(t *testing.T) {
	for _, mode := range []string{"active", "paused", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, _, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"})
			if mode == "active" {
				b := prepareWakeSession(t, k, w.OrchestratorSessionID)
				snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
				if err != nil {
					t.Fatal(err)
				}
				run := contract.SessionRun{ID: "main-active", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
				if err = k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
					t.Fatal(err)
				}
			} else {
				action := "pause"
				if mode == "canceled" {
					action = "cancel"
				}
				mustControl(t, k, operatorRequest(w, "hold", action, ""))
			}
			if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || changed {
				t.Fatal(changed, err)
			}
			current, err := k.Wave(ctx, w.ID)
			if err != nil || current.PlanRevision != w.PlanRevision || current.FixRounds != 0 {
				t.Fatal(current, err)
			}
		})
	}
}
func TestFindingsGateRejectsPlanThatDropsGeneratedFixTasks(t *testing.T) {
	ctx := context.Background()
	k, w, review, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"})
	if _, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID}); err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a later trusted plan preserving the original result while omitting
	// its repair Tasks. The saved expansion alone must not grant quality passage.
	original := storagePlanFixture()
	original.Phases = append(original.Phases, contract.PhaseSpec{ID: "new-report", Tasks: []contract.TaskSpec{{ID: "new-report", Kind: "work", Instruction: "report", Criteria: []string{"AC"}}}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"new-report"}}, FailurePolicy: "drain_and_block"})
	original.Templates = []contract.FixReviewTemplate{kernelTemplate()}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.Policy.Limits.Replans = 1
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	admission, err := k.AdoptPlan(ctx, w.Target(), "drop-fixes", original)
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil || w.Target() != admission.Target {
		t.Fatal(w, err)
	}
	set := mustReady(t, k, w)
	if set.Reason != "review_findings" {
		raw, _ := json.Marshal(set)
		t.Fatal(string(raw))
	}
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || changed {
		t.Fatal("old expansion replay requested a busy recheck", changed, err)
	}
	if w.State == domain.WaveCompleted {
		t.Fatal("findings completed Wave")
	}
}

func TestAutomaticTemplateProgressionPreservesTerminalWait(t *testing.T) {
	ctx := context.Background()
	k, w, _, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"})
	prior, err := k.Subscribe(ctx, SubscriptionRequest{SessionID: w.OrchestratorSessionID, Target: w.Target(), Key: "background-terminal", Kind: "wave_terminal"})
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err = k.DB.QueryRowContext(ctx, "SELECT payload FROM kernel_subscriptions WHERE session_id=? AND wave_id=? AND plan_revision=? AND resource=?", w.OrchestratorSessionID, w.ID, current.PlanRevision, "kernel:wave-terminal:"+w.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var successor Subscription
	if err = json.Unmarshal(raw, &successor); err != nil || successor.Target != current.Target() || successor.ID == prior.ID {
		t.Fatal(successor, err)
	}
	original, err := subscriptionTx(ctx, k.DB, prior.ID)
	if err != nil || original.Request != prior.Request {
		t.Fatal("original terminal wait rewritten", original, err)
	}
	mustControl(t, k, operatorRequest(current, "cancel", "cancel", ""))
	notifications, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(notifications) != 1 || notifications[0].SubscriptionID != successor.ID || notifications[0].Target != current.Target() || notifications[0].WaveState != domain.WaveCanceled {
		t.Fatal(notifications, err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: w.OrchestratorSessionID, Key: "terminal-wake", NotificationIDs: []string{notifications[0].ID}, IncludeSessionContext: true})
	if err != nil || len(wake.Intent.NotificationIDs) != 1 {
		t.Fatal(wake, err)
	}
}

func TestTemplateTerminalSuccessorFailureRollsBackExpansion(t *testing.T) {
	ctx := context.Background()
	k, w, _, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"})
	if _, err := k.Subscribe(ctx, SubscriptionRequest{SessionID: w.OrchestratorSessionID, Target: w.Target(), Key: "terminal", Kind: "wave_terminal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_terminal_follow BEFORE INSERT ON kernel_subscriptions WHEN NEW.resource LIKE 'kernel:wave-terminal:%' AND NEW.plan_revision>1 BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err == nil || changed {
		t.Fatal("partial expansion accepted", changed, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.Target() != w.Target() || current.FixRounds != 0 || current.MainTurns != 0 {
		t.Fatal(current, err)
	}
	var receipts int
	if err = k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='template_expansion'").Scan(&receipts); err != nil || receipts != 0 {
		t.Fatal(receipts, err)
	}
}
