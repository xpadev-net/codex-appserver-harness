package storage

import (
	"context"
	"errors"
)

type CloudCompletionTargetPage struct {
	OperationIDs []string `json:"operation_ids"`
	NextAfter    string   `json:"next_after"`
}

// CloudCompletionTargets discovers held requests independently of current
// business controls. It returns opaque IDs only: each reader authenticates its
// own round, so damaged evidence for one request cannot poison the whole page.
// Unknown sends lack a completed request and remain held for separate recovery.
func (k *KernelStore) CloudCompletionTargets(ctx context.Context, after string, limit int) (CloudCompletionTargetPage, error) {
	page := CloudCompletionTargetPage{OperationIDs: []string{}}
	if limit < 1 || limit > 128 {
		return page, errors.New("bounded Cloud completion page required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	ids, err := inputIDsTx(ctx, tx, `SELECT d.run_id FROM kernel_dispatch_intents d
 JOIN kernel_tool_operations o ON o.id=d.run_id
 WHERE d.state='held' AND o.state='completed' AND o.name='pr_review_request'
 AND d.run_id>? ORDER BY d.run_id LIMIT ?`, after, limit+1)
	if err != nil {
		return page, err
	}
	if len(ids) > limit {
		page.NextAfter = ids[limit-1]
		ids = ids[:limit]
	}
	page.OperationIDs = ids
	return page, tx.Commit()
}
