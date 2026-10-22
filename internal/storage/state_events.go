package storage

import (
	"context"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// StateEvents is read-only. Notification delivery/reconciliation and models are
// deliberately absent. A stable sequence can be reused across reconnections.
func (k *KernelStore) StateEvents(ctx context.Context, after int64, limit int) (contract.StateEventPage, error) {
	page := contract.StateEventPage{Events: []contract.StateEvent{}, Cursor: after}
	if after < 0 || limit < 1 || limit > contract.MaxStatePage {
		return page, errors.New("state cursor/page limit invalid")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	var high int64
	if err = tx.QueryRowContext(ctx, "SELECT coalesce(max(sequence),0) FROM kernel_events").Scan(&high); err != nil {
		return page, err
	}
	if after > high {
		return page, errors.New("state cursor is ahead of this database")
	}
	rows, err := tx.QueryContext(ctx, `SELECT sequence,id,wave_id,task_id,kind,scope_revision,plan_revision,length(payload),CASE WHEN length(payload)<=? THEN payload ELSE NULL END FROM kernel_events WHERE sequence>? ORDER BY sequence LIMIT ?`, contract.MaxInline, after, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var event contract.StateEvent
		var payload []byte
		if err = rows.Scan(&event.Sequence, &event.ID, &event.Target.WaveID, &event.TaskID, &event.Kind, &event.Target.ScopeRevision, &event.Target.PlanRevision, &event.PayloadBytes, &payload); err != nil {
			return page, err
		}
		if len(page.Events) == limit {
			page.More = true
			break
		}
		event.Payload = payload
		event.PayloadOmitted = payload == nil
		page.Events = append(page.Events, event)
		page.Cursor = event.Sequence
	}
	if err = rows.Err(); err != nil {
		return page, err
	}
	if err = rows.Close(); err != nil {
		return page, err
	}
	if err = page.Validate(after, limit); err != nil {
		return page, err
	}
	return page, tx.Commit()
}
