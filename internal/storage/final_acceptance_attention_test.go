package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestFinalAcceptanceAttentionPersistsAckAndDecisionWithoutRepeatedMain(t *testing.T) {
	ctx := context.Background()
	k, w, _, _, _ := acceptanceFixture(t, nil)
	changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID)
	if err != nil || !changed {
		t.Fatal(changed, err)
	}
	notes, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(notes) != 1 || notes[0].Reason != "wave_execution_completed" {
		t.Fatal(notes, err)
	}
	notification := notes[0]
	if err = k.AcknowledgeNotification(ctx, w.OrchestratorSessionID, notification.ID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for i := 0; i < 3; i++ {
		if changed, err = k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || changed {
			t.Fatal("polling repeated final attention", changed, err)
		}
	}
	notes, err = k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(notes) != 1 || notes[0].ID != notification.ID || !notes[0].DeliveryAcknowledged {
		t.Fatal(notes, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.MainTurns != 0 || current.State == domain.WaveCompleted {
		t.Fatal("attention asserted acceptance", current, err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: w.OrchestratorSessionID, Key: "final-evaluation", NotificationIDs: []string{notification.ID}, IncludeSessionContext: true})
	if err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, w.OrchestratorSessionID)
	run := wakeStartingRun(k, b, wake)
	if err = k.ReserveSessionRun(ctx, b, wake.Snapshot, run); err != nil {
		t.Fatal(err)
	}
	result := contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{{Target: w.Target(), Action: "wait", Summary: "final judgment needs operator clarification", SubscriptionIDs: []string{notification.SubscriptionID}, Remaining: []string{"clarify interpretation"}, Blockers: []string{}}}}
	saveMainCompletion(t, k, b, run, result, true)
	if _, err = k.ApplyMainResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if changed, err = k.ReconcileSessionTemplates(ctx, b.ID); err != nil || changed {
			t.Fatal("final judgment automatically repeated", changed, err)
		}
	}
	notes, err = k.PendingWakeNotifications(ctx, b.ID, 16)
	if err != nil || len(notes) != 0 {
		t.Fatal(notes, err)
	}
	current, err = k.Wave(ctx, w.ID)
	if err != nil || current.State == domain.WaveCompleted || current.MainTurns != 1 {
		t.Fatal(current, err)
	}
}
func TestFinalAcceptanceAttentionWaitsForLastPhaseAndSurvivesPause(t *testing.T) {
	ctx := context.Background()
	k, w, _, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, nil)
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || changed {
		t.Fatal("unfinished last phase emitted final judgment", changed, err)
	}
	notes, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(notes) != 0 {
		t.Fatal(notes, err)
	}
	report, err := k.Task(ctx, structureID("task-", w.ID, "report"))
	if err != nil {
		t.Fatal(err)
	}
	completeTemplateTask(t, k, report, "completed final report", "", nil)
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	notes, err = k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(notes) != 1 {
		t.Fatal(notes, err)
	}
	id := notes[0].ID
	mustControl(t, k, operatorRequest(w, "pause-final", "pause", ""))
	if pending, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16); err != nil || len(pending) != 0 {
		t.Fatal(pending, err)
	}
	mustControl(t, k, operatorRequest(w, "resume-final", "resume", ""))
	notes, err = k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(notes) != 1 || notes[0].ID != id {
		t.Fatal("pause discarded final input", notes, err)
	}
}
func TestFinalAcceptanceAttentionRollbackDoesNotLeaveUndeliverableReceipt(t *testing.T) {
	ctx := context.Background()
	k, w, _, _, _ := acceptanceFixture(t, nil)
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_final_attention BEFORE INSERT ON kernel_notifications WHEN json_extract(NEW.payload,'$.reason')='wave_execution_completed' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err == nil || changed {
		t.Fatal(changed, err)
	}
	var count int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='parent_attention'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err := k.DB.ExecContext(ctx, "DROP TRIGGER fail_final_attention"); err != nil {
		t.Fatal(err)
	}
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || !changed {
		t.Fatal("rolled-back observation suppressed retry", changed, err)
	}
}
