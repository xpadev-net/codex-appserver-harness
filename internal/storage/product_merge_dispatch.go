package storage

import (
	"context"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type MergeDispatchTarget struct {
	Cursor  string `json:"cursor"`
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	MergeID string `json:"merge_id"`
}
type MergeDispatchTargetPage struct {
	Targets   []MergeDispatchTarget `json:"targets"`
	NextAfter string                `json:"next_after"`
}

// MergeDispatchTargets returns opaque discovery only. A request and the merge
// it creates share MergeID, so a worker cannot recover its own in-flight launch
// when a later scan discovers the newly persisted journal.
func (k *KernelStore) MergeDispatchTargets(ctx context.Context, after string, limit int) (MergeDispatchTargetPage, error) {
	page := MergeDispatchTargetPage{Targets: []MergeDispatchTarget{}}
	if limit < 1 || limit > 128 {
		return page, errors.New("bounded merge dispatch page required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT cursor,kind,id FROM (
 SELECT 'request:'||o.id cursor,'request' kind,o.id id FROM kernel_tool_operations o
 WHERE o.name='pr_review_request' AND o.state='completed'
 AND NOT EXISTS(SELECT 1 FROM records m WHERE m.kind='product_merge'
 AND CASE WHEN json_valid(m.payload) THEN json_extract(m.payload,'$.merge.value.request_id')=o.id ELSE 0 END)
 UNION ALL
 SELECT 'merge:'||r.id cursor,'merge' kind,r.id FROM records r WHERE r.kind='product_merge'
 AND CASE WHEN json_valid(r.payload) THEN
 COALESCE(json_extract(r.payload,'$.merge.state'),'') NOT IN
 ('canceled','stopped_unsent','stopped_unknown','rejected','merged','aborted_http','stopped_before_http')
 OR (json_extract(r.payload,'$.merge.state')='merged' AND NOT EXISTS
 (SELECT 1 FROM records receipt WHERE receipt.kind='product_merged_work_item' AND receipt.id=r.id)) ELSE 1 END
 ) WHERE cursor>? ORDER BY cursor LIMIT ?`, after, limit+1)
	if err != nil {
		return page, err
	}
	for rows.Next() {
		var target MergeDispatchTarget
		if err := rows.Scan(&target.Cursor, &target.Kind, &target.ID); err != nil {
			rows.Close()
			return page, err
		}
		target.MergeID = target.ID
		if target.Kind == "request" {
			target.MergeID = productMergeID(target.ID)
		}
		page.Targets = append(page.Targets, target)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return page, err
	}
	if len(page.Targets) > limit {
		page.Targets = page.Targets[:limit]
		page.NextAfter = page.Targets[limit-1].Cursor
	}
	return page, tx.Commit()
}

// CancelStalePreparedProductMerge observes a positive revision/candidate change
// in the same transaction that revokes the still-unclaimed intent. Missing or
// corrupt current metadata is uncertainty, never a stale-candidate proof.
func (k *KernelStore) CancelStalePreparedProductMerge(ctx context.Context, id string) (ProductMerge, bool, error) {
	var job ProductMerge
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, false, err
	}
	defer tx.Rollback()
	job, err = k.readProductMergeTx(ctx, tx, id)
	if err != nil {
		return job, false, err
	}
	if job.Value.HTTPProtocol != productMergeHTTPProtocol || job.State != "prepared" {
		return job, false, ErrProductMergeHeld
	}
	p := job.Value.Product.Value
	w, err := loadWave(ctx, tx, p.Target.WaveID)
	if err != nil {
		return job, false, err
	}
	stale := w.Target() != p.Target || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted
	if !stale {
		current, err := loadProductTargetTx(ctx, tx, p.Target, p.WorkItem.ID)
		if err != nil {
			return job, false, err
		}
		v := current.Value
		stale = v.Candidate != p.Candidate || v.WorkItem.Version != p.WorkItem.Version || v.PRNumber != p.PRNumber || v.Repository != p.Repository || v.BaseBranch != p.BaseBranch
	}
	if !stale {
		return job, false, tx.Commit()
	}
	job, err = k.cancelPreparedProductMergeTx(ctx, tx, job, "stale-prepared-"+job.Digest, "authenticated current candidate or Wave revision superseded prepared merge")
	if err != nil {
		return job, false, err
	}
	if err := tx.Commit(); err != nil {
		return job, false, err
	}
	return job, true, nil
}
