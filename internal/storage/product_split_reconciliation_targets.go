package storage

import (
	"context"
	"errors"
	"strings"
)

type SplitCreationReconciliationTarget struct {
	Cursor      string `json:"cursor"`
	ID          string `json:"id"`
	Unavailable bool   `json:"unavailable,omitempty"`
}
type SplitCreationReconciliationPage struct {
	Targets   []SplitCreationReconciliationTarget `json:"targets"`
	NextAfter string                              `json:"next_after"`
}

// This work list grants no Issue/stop authority. Discovery uses opaque record
// keys so one malformed payload cannot hide later healthy jobs. Independent
// witnesses also retain entries whose authoritative creation head was lost.
func (k *KernelStore) SplitCreationReconciliationTargets(ctx context.Context, after string, limit int) (SplitCreationReconciliationPage, error) {
	page := SplitCreationReconciliationPage{Targets: []SplitCreationReconciliationTarget{}}
	if limit < 1 || limit > 128 || len(after) > 512 {
		return page, errors.New("bounded split reconciliation page required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT cursor,id,resource,witness_kind FROM (
 SELECT 'creation:'||id AS cursor,id,'' AS resource,'' AS witness_kind FROM records WHERE kind IN ('product_split_create','product_split_create_history','product_split_create_admission','product_split_create_gate','product_split_create_retry_budget','product_split_preparation_creation')
 UNION SELECT 'creation:'||work_item_id,work_item_id,'','' FROM records WHERE kind IN ('product_split_create_event','product_split_create_gate_history')
 UNION SELECT 'creation:'||run_id,run_id,'','' FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'
 UNION SELECT 'creation:'||operation_key,operation_key,'','' FROM kernel_budget_ledger WHERE kind='split_create_retry'
 UNION SELECT 'resource:'||id,'',id,'' FROM records WHERE kind='product_split_created_resource'
 UNION SELECT 'scope:'||printf('%020d',rowid),'',id,kind FROM records WHERE kind IN
 ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_child_scope_source','product_split_registrations','product_split_registrations_history','product_split_registrations_admission')
 UNION SELECT 'scope:'||printf('%020d',rowid),'',id,kind FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item')
 AND CASE WHEN json_valid(payload) THEN coalesce(json_extract(payload,'$.value.split_scope'),json_extract(payload,'$.split_scope')) IS NOT NULL ELSE 1 END
 ) WHERE cursor>? ORDER BY cursor LIMIT ?`, after, limit+1)
	if err != nil {
		return page, err
	}
	type entry struct{ cursor, id, resource, witnessKind string }
	var found []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.cursor, &e.id, &e.resource, &e.witnessKind); err != nil {
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
		page.NextAfter = found[limit-1].cursor
		found = found[:limit]
	}
	for _, e := range found {
		target := SplitCreationReconciliationTarget{Cursor: e.cursor, ID: e.id}
		if e.witnessKind != "" {
			id, err := splitScopeWitnessCreation(ctx, tx, e.witnessKind, e.resource)
			if err != nil && !errors.Is(err, ErrTaskEvidence) {
				return page, err
			}
			if err != nil {
				target.Unavailable = true
			} else {
				target.ID = id
			}
		} else if e.resource != "" {
			var binding splitCreatedResource
			err := taskEvidenceRecord(ctx, tx, "product_split_created_resource", e.resource, &binding)
			if err != nil && !errors.Is(err, ErrTaskEvidence) {
				return page, err
			}
			if err != nil || !strings.HasPrefix(binding.CreationID, "split-create-") || len(binding.CreationID) > 192 || binding.ProposalID == "" || binding.ParentHeadID == "" || binding.ChildID == "" || binding.IssueID < 1 {
				target.Unavailable = true
			} else {
				target.ID = binding.CreationID
			}
		}
		page.Targets = append(page.Targets, target)
	}
	return page, tx.Commit()
}
