package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func (k *KernelStore) ReconcilePRResourceNotifications(ctx context.Context, limit int) ([]string, error) {
	queued := []string{}
	if limit < 1 || limit > 128 {
		return queued, errors.New("bounded PR resource notification batch required")
	}
	ids, err := k.nextPRPendingRecordBatch(ctx, "product_pr_scan", "pr_resource_updates", limit)
	if err != nil {
		return queued, err
	}
	var failures []error
	for _, id := range ids {
		if err = k.queuePRResourceUpdate(ctx, id); err != nil {
			failures = append(failures, err)
		} else {
			queued = append(queued, id)
		}
	}
	return queued, errors.Join(failures...)
}
func (k *KernelStore) queuePRResourceUpdate(ctx context.Context, id string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var receipt PRResourceUpdateReceipt
	if err = completionRecord(ctx, tx, "product_pr_scan", id, &receipt); err != nil {
		return err
	}
	if receipt.NotificationState == "queued" || receipt.NotificationState == "unchanged" {
		return nil
	}
	if receipt.ScanID != id || receipt.NotificationState != "pending" || len(receipt.Changes) == 0 {
		return errors.New("PR resource outbox identity conflict")
	}
	var version PRResourceState
	if err = completionRecord(ctx, tx, "product_pr_state_version", structureID("pr-state-", receipt.ResourceKey, receipt.Digest), &version); err != nil {
		return err
	}
	digest, err := resourceFactDigest(version.HeadSHA, version.Sections)
	if err != nil {
		return err
	}
	if digest != receipt.Digest || version.Resource.ResourceKey != receipt.ResourceKey {
		return errors.New("PR resource outbox snapshot conflict")
	}
	w, err := loadWave(ctx, tx, version.Resource.WaveID)
	if err != nil {
		return err
	}
	eventID := structureID("pr-resource-event-", receipt.ResourceKey, id)
	if err = k.waveEventTx(ctx, tx, w, "product_resource_updated", "", eventID, map[string]string{"scan_id": id}); err != nil {
		return err
	}
	receipt.NotificationState = "queued"
	if err = kernelRecordTx(ctx, tx, "product_pr_scan", id, w.ID, receipt); err != nil {
		return err
	}
	return tx.Commit()
}

// Resource bodies remain in verified snapshots. This bounded notification gives
// provenance and change counts, without injecting external text as instructions.
func (k *KernelStore) fanoutPRResourceUpdateTx(ctx context.Context, tx *sql.Tx, event notificationIntent) error {
	if event.Kind != "product_resource_updated" {
		return nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT payload FROM kernel_events WHERE id=? AND kind='product_resource_updated'", event.EventID).Scan(&raw); err != nil {
		return err
	}
	var payload struct {
		ScanID string `json:"scan_id"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return err
	}
	var receipt PRResourceUpdateReceipt
	if err := completionRecord(ctx, tx, "product_pr_scan", payload.ScanID, &receipt); err != nil {
		return err
	}
	if receipt.ScanID != payload.ScanID || event.EventID != structureID("pr-resource-event-", receipt.ResourceKey, payload.ScanID) {
		return errors.New("PR resource notification source conflict")
	}
	rows, err := tx.QueryContext(ctx, "SELECT id FROM kernel_subscriptions WHERE wave_id=? AND resource=? ORDER BY id", event.Target.WaveID, "external:"+receipt.ResourceKey)
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	w, err := loadWave(ctx, tx, event.Target.WaveID)
	if err != nil {
		return err
	}
	counts := map[string]int{}
	for _, c := range receipt.Changes {
		counts[c.Kind+":"+c.Action]++
	}
	detail, err := json.Marshal(map[string]any{"scan_id": receipt.ScanID, "resource_key": receipt.ResourceKey, "snapshot_digest": receipt.Digest, "previous_digest": receipt.PreviousDigest, "changes": counts})
	if err != nil {
		return err
	}
	for _, id := range ids {
		var watch PRSubscriptionIntent
		if err = completionRecord(ctx, tx, "product_pr_subscription_intent", id, &watch); err != nil {
			return err
		}
		if watch.ID != id || watch.ResourceKey != receipt.ResourceKey || watch.Target.WaveID != event.Target.WaveID || watch.State != "bound" {
			return errors.New("PR resource subscriber binding conflict")
		}
		intent, _, err := prIntentTx(ctx, tx, watch.OperationID)
		if err != nil {
			return err
		}
		if intent.Source.SessionID != watch.SessionID || intent.Source.Target != watch.Target || intent.SubscriptionID != id {
			return errors.New("PR resource subscriber origin conflict")
		}
		b, err := sessionBindingTx(ctx, tx, watch.SessionID)
		if err != nil {
			return err
		}
		state := "deferred"
		if b.Closed || w.Target() != watch.Target || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || (b.Kind == "task" && b.TaskState.Terminal()) {
			state = "archived"
		} else {
			var awaited prOperationAwait
			err = completionRecord(ctx, tx, "product_pr_await", watch.OperationID, &awaited)
			if err == nil {
				if awaited.SessionID != watch.SessionID || awaited.SubscriptionID != id || awaited.Target != watch.Target {
					return errors.New("PR resource wait identity conflict")
				}
				state = "pending"
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		n := Notification{ID: structureID("notification-", id, event.EventID), SubscriptionID: id, SessionID: watch.SessionID, Target: watch.Target, SourceTarget: event.Target, EventID: event.EventID, SourceEventID: event.EventID, EventBoundary: event.Sequence, ObservedVia: "resource_scan", Kind: "external_update", TaskID: watch.TaskID, OperationID: watch.OperationID, Detail: string(detail), State: state, CreatedAt: k.clock()}
		raw, err = json.Marshal(n)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,run_id,payload) VALUES(?,?,?,?,?,'',?) ON CONFLICT(subscription_id,event_id) DO NOTHING", n.ID, id, event.EventID, n.SessionID, state, raw); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_subscriptions SET cursor=max(cursor,?) WHERE id=?", event.Sequence, id); err != nil {
			return err
		}
	}
	return nil
}
