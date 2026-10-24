package storage

import (
	"context"
	"errors"
)

type MergeReconciliationTargetPage struct {
	OperationIDs []string `json:"operation_ids"`
	NextAfter    string   `json:"next_after"`
}

// Discovery returns only opaque IDs. The reader authenticates each journal
// independently; malformed JSON or an invalid state must not poison a page or
// hide later healthy entries. Known states without a saved unknown stop stay
// excluded. This is a work list, never physical-stop or merge evidence.
func (k *KernelStore) MergeReconciliationTargets(ctx context.Context, after string, limit int) (MergeReconciliationTargetPage, error) {
	page := MergeReconciliationTargetPage{OperationIDs: []string{}}
	if limit < 1 || limit > 128 {
		return page, errors.New("bounded merge reconciliation page required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	ids, err := inputIDsTx(ctx, tx, `SELECT id FROM records WHERE kind='product_merge' AND id>?
 AND CASE WHEN json_valid(payload) THEN COALESCE(json_extract(payload,'$.merge.state'),'') NOT IN
 ('prepared','launching','unknown','canceled','stopped_unsent','rejected','merged','aborted_http','stopped_before_http') ELSE 1 END
 ORDER BY id LIMIT ?`, after, limit+1)
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
