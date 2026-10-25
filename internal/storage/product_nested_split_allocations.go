package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Every seat retains the complete ancestor group as well as its own allocation.
// The embedded plan is evidence, never permission to repair missing originals.
type ProductNestedSplitAllocationValue struct {
	ProductNestedSplitChildPlanValue
	ChildPlanID     string                 `json:"child_plan_id"`
	ChildPlanDigest string                 `json:"child_plan_digest"`
	Allocation      ProductSplitAllocation `json:"allocation"`
}
type ProductNestedSplitAllocation struct {
	ID     string                            `json:"id"`
	Value  ProductNestedSplitAllocationValue `json:"value"`
	Digest string                            `json:"digest"`
}

// Ledger snapshots participate only in negative witness queries. The physical
// records remain the sole authority for positive reads and new admissions.
func splitScopeWitnessSQL(query string) string {
	return `WITH split_ancestor_sets AS NOT MATERIALIZED (
 SELECT rowid,kind,id,work_item_id,CASE WHEN json_valid(payload) THEN CASE kind
 WHEN 'product_work_item_registration' THEN json_extract(payload,'$.value.split_scope.ancestor_set')
 WHEN 'product_work_item_registration_version' THEN json_extract(payload,'$.value.split_scope.ancestor_set')
 WHEN 'wave_work_item' THEN json_extract(payload,'$.split_scope.ancestor_set')
 ELSE json_extract(payload,'$.value.ancestor_set') END ELSE NULL END AS ancestor_set
 FROM main.records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item','product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission')
 ), split_scope_witnesses AS NOT MATERIALIZED (
 SELECT rowid,kind,id,work_item_id,payload FROM main.records
 UNION ALL SELECT rowid,'product_nested_split_allocation_ledger',operation_key,wave_id,payload
 FROM kernel_budget_ledger WHERE kind='split_children'
 AND json_type(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.allocation') IS NOT NULL
 UNION ALL SELECT rowid,'product_nested_split_creation_ancestor',kind||':'||id,
 coalesce(json_extract(CASE WHEN json_valid(ancestry) THEN ancestry ELSE '{}' END,'$.value.request.target.wave_id'),work_item_id),ancestry FROM (
 SELECT rowid,kind,id,work_item_id,CASE kind
 WHEN 'product_split_create' THEN json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.value.ancestry')
 WHEN 'product_split_create_event' THEN json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.value.ancestry')
 WHEN 'product_split_create_gate' THEN json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.ancestry')
 WHEN 'product_split_create_gate_history' THEN json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.ancestry')
 ELSE json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.ancestry') END AS ancestry
 FROM main.records WHERE kind IN ('product_split_create','product_split_create_event','product_split_create_history','product_split_create_admission','product_split_create_gate','product_split_create_gate_history','product_split_create_retry_budget','product_split_created_resource')
 ) WHERE ancestry IS NOT NULL
 UNION ALL SELECT rowid,'product_nested_split_retry_ancestor',operation_key,
 wave_id,json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.ancestry')
 FROM kernel_budget_ledger WHERE kind='split_create_retry'
 AND json_type(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.ancestry') IS NOT NULL
 UNION ALL SELECT rowid,'product_nested_split_scope_group',kind||':'||id,work_item_id,
 json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.scopes')
 FROM main.records WHERE kind IN ('product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source')
 UNION ALL SELECT r.rowid,'product_split_ancestor_group',json_array(r.kind,r.id,cast(a.key AS text)),r.work_item_id,
 json_object('value',json(CASE WHEN json_valid(a.value) THEN a.value ELSE '{}' END))
 FROM split_ancestor_sets r,json_each(CASE WHEN json_valid(r.ancestor_set) THEN
 CASE WHEN json_type(r.ancestor_set,'$.value.groups')='array' AND json_array_length(r.ancestor_set,'$.value.groups') BETWEEN 1 AND 128
 THEN json_extract(r.ancestor_set,'$.value.groups') ELSE '[]' END ELSE '[]' END) a
 UNION ALL SELECT rowid,'product_split_ancestor_group',json_array(kind,id,'unreadable'),work_item_id,'{}'
 FROM split_ancestor_sets WHERE CASE
 WHEN ancestor_set IS NULL THEN kind IN ('product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission')
 WHEN NOT json_valid(ancestor_set) THEN 1
 WHEN coalesce(json_type(ancestor_set)='object' AND json_type(ancestor_set,'$.value.groups')='array' AND json_array_length(ancestor_set,'$.value.groups') BETWEEN 1 AND 128,0)=0 THEN 1
 ELSE 0 END

 ) ` + query
}

func deriveNestedSplitAllocation(r ProductSplitChildPlanRequest, plan ProductSplitChildPlan, created time.Time) (ProductSplitAllocation, error) {
	var saved ProductSplitAllocation
	if created.IsZero() || created.Before(plan.Set.Value.CreatedAt) {
		return saved, ErrProductSplitHeld
	}
	saved.Value = ProductSplitAllocationValue{Request: r, PlanID: plan.Set.ID, PlanDigest: plan.Set.Digest, ParentHeadID: plan.Set.Value.ParentHeadID, WorkItemIDs: []string{}, CreatedAt: created}
	for _, child := range plan.Children {
		saved.Value.WorkItemIDs = append(saved.Value.WorkItemIDs, child.Value.WorkItemID)
	}
	var err error
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	saved.ID = structureID("split-allocation-", r.ProposalID, saved.Digest)
	return saved, nil
}

func loadNestedSplitAllocation(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductNestedSplitAllocation, error) {
	var saved ProductNestedSplitAllocation
	err := taskEvidenceRecord(ctx, tx, "product_nested_split_allocation", r.ProposalID, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.allocation.value.request.proposal_id')=?) OR EXISTS(SELECT 1 FROM kernel_budget_ledger WHERE kind='split_children' AND operation_key=?)`, r.ProposalID, r.ProposalID).Scan(&known); err != nil {
			return saved, err
		}
		if known {
			return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
		}
		return saved, err
	}
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.ID != structureID("nested-split-allocation-", r.ProposalID, digest) {
		return saved, ErrProductSplitHeld
	}
	plan, err := loadNestedSplitChildPlan(ctx, tx, r)
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	if saved.Value.ChildPlanID != plan.ID || saved.Value.ChildPlanDigest != plan.Digest || !reflect.DeepEqual(saved.Value.ProductNestedSplitChildPlanValue, plan.Value) {
		return saved, ErrProductSplitHeld
	}
	allocation, err := deriveNestedSplitAllocation(r, plan.Value.Plan, saved.Value.Allocation.Value.CreatedAt)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(allocation, saved.Value.Allocation) {
		return saved, ErrProductSplitHeld
	}
	records := []struct{ kind, id string }{{"product_nested_split_allocation_history", saved.ID}, {"product_nested_split_allocation_admission", r.ProposalID}}
	for _, id := range allocation.Value.WorkItemIDs {
		records = append(records, struct{ kind, id string }{"product_nested_split_work_item_seat", id})
	}
	for _, record := range records {
		var witness ProductNestedSplitAllocation
		if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &witness); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(witness, saved) {
			return saved, ErrProductSplitHeld
		}
	}
	var amount, unknown int
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT amount,unknown,payload FROM kernel_budget_ledger WHERE wave_id=? AND operation_key=? AND kind='split_children'", r.Target.WaveID, r.ProposalID).Scan(&amount, &unknown, &raw); err != nil {
		return saved, splitEvidenceErr(err)
	}
	var ledger ProductNestedSplitAllocation
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	if amount != len(allocation.Value.WorkItemIDs) || unknown != 0 || !reflect.DeepEqual(ledger, saved) {
		return saved, ErrProductSplitHeld
	}
	return saved, nil
}

// Called only after generic admission has checked the live Wave, current
// proposal, fresh private source policy and the absence of earlier allocation.
func (k *KernelStore) reserveNestedSplitChildrenTx(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest, w domain.Wave) (ProductSplitAllocation, error) {
	var empty ProductSplitAllocation
	plan, err := loadNestedSplitChildPlan(ctx, tx, r)
	if err != nil {
		return empty, splitEvidenceErr(err)
	}
	ids, err := productWorkItemLifetimeIDsTx(ctx, tx, w)
	if err != nil {
		return empty, err
	}
	if len(ids)+len(plan.Value.Plan.Children) > maxProductWorkItems {
		return empty, ErrOperationBudget
	}
	allocation, err := deriveNestedSplitAllocation(r, plan.Value.Plan, k.clock())
	if err != nil {
		return empty, err
	}
	for _, id := range allocation.Value.WorkItemIDs {
		if ids[id] {
			return empty, ErrProductSplitHeld
		}
		var collision bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_work_item_registration','wave_work_item','product_split_work_item_seat','product_nested_split_work_item_seat')) OR EXISTS(SELECT 1 FROM records WHERE kind='product_work_item_registration_version' AND json_extract(payload,'$.value.definition.id')=?)", id, id).Scan(&collision); err != nil {
			return empty, err
		}
		if collision {
			return empty, ErrProductSplitHeld
		}
	}
	saved := ProductNestedSplitAllocation{Value: ProductNestedSplitAllocationValue{ProductNestedSplitChildPlanValue: plan.Value, ChildPlanID: plan.ID, ChildPlanDigest: plan.Digest, Allocation: allocation}}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return empty, err
	}
	saved.ID = structureID("nested-split-allocation-", r.ProposalID, saved.Digest)
	raw, err := json.Marshal(saved)
	if err != nil {
		return empty, err
	}
	if len(raw) > 16<<20 {
		return empty, ErrProductSplitHeld
	}
	records := []struct{ kind, id string }{{"product_nested_split_allocation", r.ProposalID}, {"product_nested_split_allocation_history", saved.ID}, {"product_nested_split_allocation_admission", r.ProposalID}}
	for _, id := range allocation.Value.WorkItemIDs {
		records = append(records, struct{ kind, id string }{"product_nested_split_work_item_seat", id})
	}
	for _, record := range records {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, w.ID, saved); err != nil {
			return empty, err
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'split_children',?,0,?)", w.ID, r.ProposalID, len(allocation.Value.WorkItemIDs), raw); err != nil {
		return empty, err
	}
	return allocation, nil
}
