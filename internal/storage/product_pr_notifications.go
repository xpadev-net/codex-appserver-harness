package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type prOperationAwait struct {
	SessionID      string          `json:"session_id"`
	RunID          string          `json:"run_id"`
	Target         contract.Target `json:"target"`
	SubscriptionID string          `json:"subscription_id"`
}

// A new plan/wait/accept decision or explicit wait replaces the wait set for this
// target. Bound inputs remain owned by their run; unbound resource updates wait
// for a new declaration instead of waking main through an obsolete subscription.
func (k *KernelStore) replacePRWaitsTx(ctx context.Context, tx *sql.Tx, session string, target contract.Target) error {
	if _, err := tx.ExecContext(ctx, `UPDATE kernel_notifications SET state='deferred',payload=json_set(payload,'$.state','deferred') WHERE session_id=? AND state='pending' AND run_id='' AND json_extract(payload,'$.kind')='external_update' AND subscription_id IN (SELECT id FROM kernel_subscriptions WHERE session_id=? AND wave_id=? AND scope_revision=? AND plan_revision=? AND json_extract(payload,'$.target.policy_revision')=?)`, session, session, target.WaveID, target.ScopeRevision, target.PlanRevision, target.PolicyRevision); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM records WHERE kind='product_pr_await' AND work_item_id=? AND json_extract(payload,'$.target.wave_id')=? AND json_extract(payload,'$.target.scope_revision')=? AND json_extract(payload,'$.target.plan_revision')=? AND json_extract(payload,'$.target.policy_revision')=?`, session, target.WaveID, target.ScopeRevision, target.PlanRevision, target.PolicyRevision)
	return err
}

// Only explicit, already-authorized wait declarations activate these one-shot
// completion inputs. Resource updates have separate subscription policy.
func (k *KernelStore) awaitPRSubscriptionsTx(ctx context.Context, tx *sql.Tx, session, run string, target contract.Target, ids []string) error {
	for _, id := range ids {
		var watch PRSubscriptionIntent
		err := completionRecord(ctx, tx, "product_pr_subscription_intent", id, &watch)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if watch.ID != id || watch.SessionID != session || watch.Target != target {
			return errors.New("PR wait is outside current session/target")
		}
		await := prOperationAwait{SessionID: session, RunID: run, Target: target, SubscriptionID: id}
		if err = kernelRecordTx(ctx, tx, "product_pr_await", watch.OperationID, session, await); err != nil {
			return err
		}
		// Viewing/ACK does not consume judgment input. The payload is updated with
		// the state column; a later wake still binds it exactly once.
		if _, err = tx.ExecContext(ctx, `UPDATE kernel_notifications SET state='pending',payload=json_set(payload,'$.state','pending') WHERE subscription_id=? AND session_id=? AND state='deferred' AND run_id=''`, id, session); err != nil {
			return err
		}
	}
	return nil
}

// ReconcilePRCompletionNotifications consumes a bounded durable completion
// outbox without model or external calls. Event/fanout/outbox state commit once.
func (k *KernelStore) ReconcilePRCompletionNotifications(ctx context.Context, limit int) ([]string, error) {
	completed := []string{}
	if limit < 1 || limit > 128 {
		return completed, errors.New("bounded PR completion batch required")
	}
	ids, err := k.nextPRCompletionNoticeBatch(ctx, limit)
	if err != nil {
		return completed, err
	}
	var failures []error
	for _, id := range ids {
		if err = k.queuePRCompletionNotice(ctx, id); err != nil {
			failures = append(failures, err)
		} else {
			completed = append(completed, id)
		}
	}
	return completed, errors.Join(failures...)
}
func (k *KernelStore) nextPRCompletionNoticeBatch(ctx context.Context, limit int) ([]string, error) {
	return k.nextPRPendingRecordBatch(ctx, "product_pr_completion", "pr_completions", limit)
}
func (k *KernelStore) nextPRPendingRecordBatch(ctx context.Context, kind, cursorKey string, limit int) ([]string, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var after string
	err = completionRecord(ctx, tx, "kernel_reconcile_cursor", cursorKey, &after)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	read := func(cursor string) ([]string, error) {
		return inputIDsTx(ctx, tx, "SELECT id FROM records WHERE kind=? AND json_extract(payload,'$.notification_state')='pending' AND id>? ORDER BY id LIMIT ?", kind, cursor, limit)
	}
	ids, err := read(after)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 && after != "" {
		ids, err = read("")
		if err != nil {
			return nil, err
		}
	}
	next := ""
	if len(ids) > 0 {
		next = ids[len(ids)-1]
	}
	if next != after {
		if err = kernelRecordTx(ctx, tx, "kernel_reconcile_cursor", cursorKey, "controller", next); err != nil {
			return nil, err
		}
	}
	return ids, tx.Commit()
}
func (k *KernelStore) queuePRCompletionNotice(ctx context.Context, id string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var receipt PRCompletionReceipt
	if err = completionRecord(ctx, tx, "product_pr_completion", id, &receipt); err != nil {
		return err
	}
	if receipt.NotificationState == "queued" {
		return nil
	}
	if receipt.NotificationState != "pending" {
		return errors.New("unsupported PR completion outbox state")
	}
	intent, state, err := prIntentTx(ctx, tx, id)
	if err != nil {
		return err
	}
	if receipt.OperationID != id || receipt.SubscriptionID != intent.SubscriptionID || state != "completed" || (((receipt.Outcome != "resource_bound" && receipt.Outcome != "review_requested") || intent.State != "completed") && (receipt.Outcome != "canceled" || intent.State != "canceled")) {
		return errors.New("PR completion outbox identity conflict")
	}
	wave, err := loadWave(ctx, tx, intent.Source.Target.WaveID)
	if err != nil {
		return err
	}
	wave.ScopeRevision, wave.PlanRevision, wave.Policy.Revision = intent.Source.Target.ScopeRevision, intent.Source.Target.PlanRevision, intent.Source.Target.PolicyRevision
	if err = k.waveEventTx(ctx, tx, wave, "product_operation_settled", intent.TaskID, id+":settled", map[string]string{"operation_id": id}); err != nil {
		return err
	}
	receipt.NotificationState = "queued"
	if err = kernelRecordTx(ctx, tx, "product_pr_completion", id, intent.Source.SessionID, receipt); err != nil {
		return err
	}
	return tx.Commit()
}

func (k *KernelStore) fanoutPROperationTx(ctx context.Context, tx *sql.Tx, event notificationIntent) error {
	if event.Kind != "product_operation_settled" {
		return nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT payload FROM kernel_events WHERE id=? AND kind='product_operation_settled'", event.EventID).Scan(&raw); err != nil {
		return err
	}
	var payload struct {
		OperationID string `json:"operation_id"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	intent, state, err := prIntentTx(ctx, tx, payload.OperationID)
	if err != nil {
		return err
	}
	var receipt PRCompletionReceipt
	if err = completionRecord(ctx, tx, "product_pr_completion", payload.OperationID, &receipt); err != nil {
		return err
	}
	if state != "completed" || intent.Source.Target != event.Target || event.EventID != intent.Source.ID+":settled" || receipt.SubscriptionID != intent.SubscriptionID {
		return errors.New("PR notification source conflict")
	}
	sub, err := k.ensureOperationSubscriptionTx(ctx, tx, intent.Source)
	if err != nil {
		return err
	}
	status := "deferred"
	b, err := sessionBindingTx(ctx, tx, intent.Source.SessionID)
	if err != nil {
		return err
	}
	w, err := loadWave(ctx, tx, intent.Source.Target.WaveID)
	if err != nil {
		return err
	}
	if b.Closed || w.Target() != event.Target || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || (b.Kind == "task" && b.TaskState.Terminal()) {
		status = "archived"
	} else {
		var awaited prOperationAwait
		err = completionRecord(ctx, tx, "product_pr_await", intent.Source.ID, &awaited)
		if err == nil {
			if awaited.SessionID != b.ID || awaited.SubscriptionID != sub.ID || awaited.Target != event.Target {
				return errors.New("PR await binding conflict")
			}
			status = "pending"
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	operationState := "completed"
	if receipt.Outcome == "canceled" {
		operationState = "canceled"
	}
	detail, err := json.Marshal(map[string]any{"outcome": receipt.Outcome, "resource_key": receipt.Resource.ResourceKey, "pr_number": receipt.Resource.Number, "head_sha": intent.Candidate.SHA, "work_item_id": intent.Candidate.WorkItemID})
	if err != nil {
		return err
	}
	n := Notification{ID: structureID("notification-", sub.ID, event.EventID), SubscriptionID: sub.ID, SessionID: sub.SessionID, Target: event.Target, SourceTarget: event.Target, EventID: event.EventID, SourceEventID: event.EventID, EventBoundary: event.Sequence, ObservedVia: "event", Kind: "operation", TaskID: intent.TaskID, OperationID: intent.Source.ID, OperationState: operationState, Detail: string(detail), State: status, CreatedAt: k.clock()}
	raw, err = json.Marshal(n)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,run_id,payload) VALUES(?,?,?,?,?,'',?) ON CONFLICT(subscription_id,event_id) DO NOTHING", n.ID, sub.ID, event.EventID, sub.SessionID, n.State, raw); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_subscriptions SET cursor=max(cursor,?) WHERE id=?", event.Sequence, sub.ID)
	return err
}
