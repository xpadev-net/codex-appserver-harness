package storage

import (
	"context"
	"errors"
)

type PRDispatchOperation struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type PRDispatchOperationPage struct {
	Operations []PRDispatchOperation `json:"operations"`
	NextAfter  string                `json:"next_after"`
}

// Only accepted operations are eligible for execution. Unknown sends are never
// requeued by this reader; reconciliation needs an independent stop proof.
func (k *KernelStore) PRDispatchOperations(ctx context.Context, after string, limit int) (PRDispatchOperationPage, error) {
	page := PRDispatchOperationPage{Operations: []PRDispatchOperation{}}
	if limit < 1 || limit > 128 {
		return page, errors.New("bounded PR dispatch page required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM kernel_tool_operations WHERE state='accepted' AND name IN ('pr_publish','pr_review_request') AND id>? ORDER BY id LIMIT ?", after, limit+1)
	if err != nil {
		return page, err
	}
	for _, id := range ids {
		if len(page.Operations) == limit {
			page.NextAfter = page.Operations[len(page.Operations)-1].ID
			break
		}
		intent, state, err := prIntentTx(ctx, tx, id)
		if err != nil {
			return page, err
		}
		if state != "accepted" {
			return page, errors.New("PR operation snapshot changed")
		}
		page.Operations = append(page.Operations, PRDispatchOperation{ID: id, Name: intent.Source.Name})
	}
	return page, tx.Commit()
}
