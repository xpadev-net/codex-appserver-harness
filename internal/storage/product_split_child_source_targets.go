package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type ProductSplitChildSourceTarget struct {
	Cursor      string          `json:"cursor"`
	WorkItemID  string          `json:"work_item_id"`
	Target      contract.Target `json:"target"`
	Unavailable bool            `json:"unavailable,omitempty"`
}
type ProductSplitChildSourcePage struct {
	Targets   []ProductSplitChildSourceTarget `json:"targets"`
	NextAfter string                          `json:"next_after"`
}

// Discovery is opaque and read-only. The observer must authenticate each local
// binding before GET; a malformed row never becomes permission to read a URL.
func (k *KernelStore) ProductSplitChildSourceTargets(ctx context.Context, repositoryID int64, after string, limit int) (ProductSplitChildSourcePage, error) {
	page := ProductSplitChildSourcePage{Targets: []ProductSplitChildSourceTarget{}}
	if repositoryID < 1 || limit < 1 || limit > 32 || len(after) > 64 {
		return page, ErrProductSplitHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT rowid,kind,id FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission')
 AND CASE WHEN kind IN ('product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission') THEN 1 WHEN json_valid(payload) THEN coalesce(json_extract(payload,'$.value.issue.repository_id')=?,1) ELSE 1 END
 AND 'child-source:'||printf('%020d',rowid)>? ORDER BY rowid LIMIT ?`, repositoryID, after, limit+1)
	if err != nil {
		return page, err
	}
	type entry struct {
		row      int64
		kind, id string
	}
	var found []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.row, &e.kind, &e.id); err != nil {
			rows.Close()
			return page, err
		}
		found = append(found, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return page, err
	}
	if len(found) > limit {
		page.NextAfter = fmt.Sprintf("child-source:%020d", found[limit-1].row)
		found = found[:limit]
	}
	for _, e := range found {
		target := ProductSplitChildSourceTarget{Cursor: fmt.Sprintf("child-source:%020d", e.row)}
		var binding ProductSplitChildScopeBinding
		var err error
		switch e.kind {
		case "product_nested_split_child_scope", "product_nested_split_child_scope_history", "product_nested_split_child_scope_admission":
			var envelope ProductNestedSplitChildScopes
			err = taskEvidenceRecord(ctx, tx, e.kind, e.id, &envelope)
			if err == nil {
				matched := false
				for _, child := range envelope.Value.Scopes.Value.Children {
					if child.Value.Child.Value.WorkItemID == e.id || child.ID == e.id {
						if matched {
							err = ErrTaskEvidence
							break
						}
						binding, matched = child, true
					}
				}
				if !matched {
					err = ErrTaskEvidence
				}
			}
			if err == nil && binding.Value.Issue.RepositoryID != repositoryID {
				continue
			}
		default:
			err = taskEvidenceRecord(ctx, tx, e.kind, e.id, &binding)
		}
		if err != nil && !errors.Is(err, ErrTaskEvidence) {
			return page, err
		}
		v := binding.Value
		if err != nil || !v.Request.Target.Valid() || v.Child.Value.WorkItemID == "" || v.Issue.RepositoryID != repositoryID {
			target.Unavailable = true
			page.Targets = append(page.Targets, target)
			continue
		}
		w, err := loadWave(ctx, tx, v.Request.Target.WaveID)
		if err != nil {
			var syntax *json.SyntaxError
			var shape *json.UnmarshalTypeError
			if err == sql.ErrNoRows || readinessHoldError(err) || errors.As(err, &syntax) || errors.As(err, &shape) {
				target.Unavailable = true
				page.Targets = append(page.Targets, target)
				continue
			}
			return page, err
		}
		if w.ID != v.Request.Target.WaveID || !w.Target().Valid() {
			target.Unavailable = true
			page.Targets = append(page.Targets, target)
			continue
		}
		if w.Policy.Stage != "P" || !w.Policy.Product || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || w.ScopeRevision != v.Request.Target.ScopeRevision || w.Policy.Revision != v.Request.Target.PolicyRevision {
			continue
		}
		target.WorkItemID, target.Target = v.Child.Value.WorkItemID, w.Target()
		page.Targets = append(page.Targets, target)
	}
	return page, tx.Commit()
}

// This classification denies the root-source shortcut. It grants no child
// scope, freshness, model, quality or mutation authority.
func (k *KernelStore) IsKnownProductSplitChildWorkItem(ctx context.Context, workID string) (bool, error) {
	if workID == "" {
		return false, nil
	}
	return productSplitChildExecutionHeld(ctx, k.DB, workID)
}
