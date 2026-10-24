package storage

import (
	"context"
	"errors"
)

type ProductExecutionPage struct {
	IDs       []string `json:"ids"`
	NextAfter string   `json:"next_after"`
}

// Current-plan Tasks and outstanding physical jobs have independent cursors.
// A stale Candidate must not hide a runner whose stopping facts still need
// recording. Queue entries are hints; every owner rechecks before an effect.
func (k *KernelStore) ProductExecutionTasks(ctx context.Context, after string, limit int) (ProductExecutionPage, error) {
	return k.productExecutionPage(ctx, after, limit, `SELECT t.id FROM kernel_tasks t JOIN kernel_waves w ON w.id=t.wave_id JOIN kernel_plan_bindings b ON b.wave_id=w.id AND b.revision=json_extract(w.payload,'$.plan_revision') AND b.task_id=t.id WHERE t.id>? AND json_extract(w.payload,'$.policy.stage')='P' AND json_array_length(json_extract(t.payload,'$.spec.work_item_ids'))=1 ORDER BY t.id LIMIT ?`)
}

func (k *KernelStore) ProductRunningJobs(ctx context.Context, after string, limit int) (ProductExecutionPage, error) {
	return k.productExecutionPage(ctx, after, limit, `SELECT id FROM records WHERE kind='product_runner_job' AND json_extract(payload,'$.job.state')='launching' AND id>? ORDER BY id LIMIT ?`)
}

func (k *KernelStore) productExecutionPage(ctx context.Context, after string, limit int, query string) (ProductExecutionPage, error) {
	out := ProductExecutionPage{IDs: []string{}}
	if limit < 1 || limit > 128 {
		return out, errors.New("product execution page limit must be 1..128")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	ids, err := inputIDsTx(ctx, tx, query, after, limit+1)
	if err != nil {
		return out, err
	}
	if len(ids) > limit {
		ids = ids[:limit]
		out.NextAfter = ids[len(ids)-1]
	}
	out.IDs = ids
	return out, nil
}
