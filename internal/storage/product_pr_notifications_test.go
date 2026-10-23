package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func completedPRNoticeFixture(t *testing.T) (*KernelStore, ProductTargetSnapshot, string, PRCompletionReceipt) {
	t.Helper()
	k, p, id, _, proof, o := observedPRFixture(t, false)
	receipt, err := k.BindObservedPRPublication(context.Background(), id, proof, o)
	if err != nil {
		t.Fatal(err)
	}
	return k, p, id, receipt
}
func waitForPROperation(t *testing.T, k *KernelStore, id string) {
	t.Helper()
	ctx := context.Background()
	var intent PROperationIntent
	if err := k.Record(ctx, "product_pr_intent", id, &intent); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, intent.Source.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	var run contract.SessionRun
	if err = k.Record(ctx, "agent_run", intent.Source.RunID, &run); err != nil {
		t.Fatal(err)
	}
	call := kernelCall(t, b, run, "session_wait", contract.WaitRequest{SchemaVersion: 1, Target: intent.Source.Target, SubscriptionIDs: []string{intent.SubscriptionID}})
	call.CallID = "wait-" + id
	if _, err = (contract.KernelGateway{Backend: k}).Handle(ctx, call); err != nil {
		t.Fatal(err)
	}
}
func productNotice(t *testing.T, k *KernelStore, id string) Notification {
	t.Helper()
	ctx := context.Background()
	var notification string
	if err := k.DB.QueryRow("SELECT id FROM kernel_notifications WHERE json_extract(payload,'$.operation_id')=?", id).Scan(&notification); err != nil {
		t.Fatal(err)
	}
	n, err := notificationTx(ctx, k.DB, notification)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func TestPRCompletionNotificationDefersUntilExplicitWaitAndViewingDoesNotConsume(t *testing.T) {
	ctx := context.Background()
	k, _, id, receipt := completedPRNoticeFixture(t)
	queued, err := k.ReconcilePRCompletionNotifications(ctx, 16)
	if err != nil || len(queued) != 1 {
		t.Fatal(queued, err)
	}
	n := productNotice(t, k, id)
	if n.State != "deferred" || n.OperationState != "completed" || n.SubscriptionID != receipt.SubscriptionID {
		t.Fatal(n)
	}
	if pending, err := k.PendingWakeNotifications(ctx, n.SessionID, 16); err != nil || len(pending) != 0 {
		t.Fatal("ordinary completion woke main", pending, err)
	}
	if err = k.AcknowledgeNotification(ctx, n.SessionID, n.ID); err != nil {
		t.Fatal(err)
	}
	waitForPROperation(t, k, id)
	pending, err := k.PendingWakeNotifications(ctx, n.SessionID, 16)
	if err != nil || len(pending) != 1 || pending[0].ID != n.ID || !pending[0].DeliveryAcknowledged {
		t.Fatal("wait lost already viewed completion", pending, err)
	}
	if queued, err = k.ReconcilePRCompletionNotifications(ctx, 16); err != nil || len(queued) != 0 {
		t.Fatal("outbox replay duplicated notification", queued, err)
	}
	var count int
	if err = k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='agent_run'").Scan(&count); err != nil || count != 1 {
		t.Fatal("notification/view started a model", count, err)
	}
}
func TestPRWaitBeforeCompletionAndMainResultWaitBothActivateOneInput(t *testing.T) {
	for _, finalWait := range []bool{false, true} {
		t.Run(fmt.Sprint(finalWait), func(t *testing.T) {
			ctx := context.Background()
			k, p, id, _, proof, o := observedPRFixture(t, false)
			if !finalWait {
				waitForPROperation(t, k, id)
			} else {
				var intent PROperationIntent
				if err := k.Record(ctx, "product_pr_intent", id, &intent); err != nil {
					t.Fatal(err)
				}
				var run contract.SessionRun
				if err := k.Record(ctx, "agent_run", intent.Source.RunID, &run); err != nil {
					t.Fatal(err)
				}
				result := contract.MainResult{SchemaVersion: 1, SessionID: intent.Source.SessionID, Decisions: []contract.MainDecision{{Target: p.Value.Target, Action: "wait", Summary: "await accepted PR operation", SubscriptionIDs: []string{intent.SubscriptionID}, Remaining: []string{}, Blockers: []string{}}}}
				raw, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				run.State, run.Final = "completed", string(raw)
				if err = k.Put(ctx, "agent_run", run.ID, intent.Source.SessionID, run); err != nil {
					t.Fatal(err)
				}
				if err = k.MarkSessionStopped(ctx, intent.Source.SessionID, run.ID); err != nil {
					t.Fatal(err)
				}
				if _, err = k.ApplyMainResult(ctx, intent.Source.SessionID, run.ID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.BindObservedPRPublication(ctx, id, proof, o); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReconcilePRCompletionNotifications(ctx, 16); err != nil {
				t.Fatal(err)
			}
			n := productNotice(t, k, id)
			if n.State != "pending" {
				t.Fatal("explicit wait missed completion", n)
			}
			if finalWait {
				first, err := k.ReserveWake(ctx, WakeRequest{SessionID: n.SessionID, Key: "pr-resume", NotificationIDs: []string{n.ID}})
				if err != nil {
					t.Fatal(err)
				}
				again, err := k.ReserveWake(ctx, WakeRequest{SessionID: n.SessionID, Key: "pr-resume", NotificationIDs: []string{n.ID}})
				if err != nil || first.Intent.ID != again.Intent.ID {
					t.Fatal("duplicate restart intent", again, err)
				}
			}
		})
	}
}
func TestPRNoticeOutboxFailureRollsBackAndRestartRecoversOnce(t *testing.T) {
	ctx := context.Background()
	k, _, id, _ := completedPRNoticeFixture(t)
	if _, err := k.DB.Exec(`CREATE TRIGGER reject_pr_notice BEFORE INSERT ON kernel_notifications WHEN json_extract(NEW.payload,'$.operation_id')<>'' BEGIN SELECT RAISE(FAIL,'notification failure'); END`); err != nil {
		t.Fatal(err)
	}
	if queued, err := k.ReconcilePRCompletionNotifications(ctx, 16); err == nil || len(queued) != 0 {
		t.Fatal("partial outbox queue succeeded", queued, err)
	}
	var events int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='product_operation_settled'").Scan(&events); err != nil || events != 0 {
		t.Fatal(events, err)
	}
	var receipt PRCompletionReceipt
	if err := k.Record(ctx, "product_pr_completion", id, &receipt); err != nil || receipt.NotificationState != "pending" {
		t.Fatal(receipt, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER reject_pr_notice"); err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	k.Close()
	reopened, err := OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if queued, err := reopened.ReconcilePRCompletionNotifications(ctx, 16); err != nil || len(queued) != 1 {
		t.Fatal(queued, err)
	}
	if _, err := reopened.ReconcileNotifications(ctx, 128); err != nil {
		t.Fatal(err)
	}
	if queued, err := reopened.ReconcilePRCompletionNotifications(ctx, 16); err != nil || len(queued) != 0 {
		t.Fatal(queued, err)
	}
	var notices int
	if err := reopened.DB.QueryRow("SELECT count(*) FROM kernel_notifications WHERE json_extract(payload,'$.operation_id')=?", id).Scan(&notices); err != nil || notices != 1 {
		t.Fatal(notices, err)
	}
}
func TestPRNoticesArchiveCanceledScopeAndUnsentCancellationHasOwnOutcome(t *testing.T) {
	ctx := context.Background()
	k, p, id, _ := completedPRNoticeFixture(t)
	waitForPROperation(t, k, id)
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel", Target: p.Value.Target, Action: "cancel", Reason: "archive old completion"}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReconcilePRCompletionNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	if n := productNotice(t, k, id); n.State != "archived" {
		t.Fatal(n)
	}
	k2, _, unsent := acceptedPRFixture(t)
	waitForPROperation(t, k2, unsent)
	if err := k2.CancelUnsentPRDispatch(ctx, unsent, "controller", "known unsent"); err != nil {
		t.Fatal(err)
	}
	if _, err := k2.ReconcilePRCompletionNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	if n := productNotice(t, k2, unsent); n.State != "pending" || n.OperationState != "canceled" {
		t.Fatal(n)
	}
}
func TestFailedPRNoticeDoesNotStarveOtherOutboxRows(t *testing.T) {
	ctx := context.Background()
	k, p, first, _ := completedPRNoticeFixture(t)
	var intent PROperationIntent
	if err := k.Record(ctx, "product_pr_intent", first, &intent); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, intent.Source.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	var run contract.SessionRun
	if err = k.Record(ctx, "agent_run", intent.Source.RunID, &run); err != nil {
		t.Fatal(err)
	}
	call := kernelCall(t, b, run, "pr_publish", contract.PublishPRRequest{SchemaVersion: 1, Target: p.Value.Target, Candidate: intent.Candidate})
	call.CallID = "another-operation"
	second, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.CancelUnsentPRDispatch(ctx, second.OperationID, "controller", "not sent"); err != nil {
		t.Fatal(err)
	}
	bad, good := first, second.OperationID
	if bad > good {
		bad, good = good, bad
	}
	// Generated operation IDs contain only the fixed prefix and hex digest.
	if _, err = k.DB.Exec(fmt.Sprintf(`CREATE TRIGGER reject_one_pr_notice BEFORE INSERT ON kernel_notifications WHEN NEW.event_id='%s:settled' BEGIN SELECT RAISE(FAIL,'single poisoned notification'); END`, bad)); err != nil {
		t.Fatal(err)
	}
	if queued, err := k.ReconcilePRCompletionNotifications(ctx, 1); err == nil || len(queued) != 0 {
		t.Fatal(queued, err)
	}
	if queued, err := k.ReconcilePRCompletionNotifications(ctx, 1); err != nil || len(queued) != 1 || queued[0] != good {
		t.Fatal("bad row starved later completion", queued, err)
	}
	if _, err = k.DB.Exec("DROP TRIGGER reject_one_pr_notice"); err != nil {
		t.Fatal(err)
	}
	if queued, err := k.ReconcilePRCompletionNotifications(ctx, 1); err != nil || len(queued) != 1 || queued[0] != bad {
		t.Fatal("cursor wrap lost failed completion", queued, err)
	}
}

func TestPRCompletionFromPreviousScopeRemainsArchivedAfterDirection(t *testing.T) {
	ctx := context.Background()
	k, p, id, _ := completedPRNoticeFixture(t)
	waitForPROperation(t, k, id)
	var intent PROperationIntent
	if err := k.Record(ctx, "product_pr_intent", id, &intent); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, intent.Source.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	var run contract.SessionRun
	if err = k.Record(ctx, "agent_run", intent.Source.RunID, &run); err != nil {
		t.Fatal(err)
	}
	run.State = "completed"
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err = k.ArchiveSessionRun(ctx, b.ID, run.ID, "known stopped run before operator direction"); err != nil {
		t.Fatal(err)
	}
	direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "new-scope", Target: p.Value.Target, Instruction: "Explain the source with updated direction"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReconcilePRCompletionNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	if n := productNotice(t, k, id); n.State != "archived" || n.SourceTarget != p.Value.Target {
		t.Fatal("old completion acquired new scope", n)
	}
}
