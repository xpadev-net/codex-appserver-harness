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

type ProductSplitAllocationValue struct {
	Request      ProductSplitChildPlanRequest `json:"request"`
	PlanID       string                       `json:"plan_id"`
	PlanDigest   string                       `json:"plan_digest"`
	ParentHeadID string                       `json:"parent_head_id"`
	WorkItemIDs  []string                     `json:"work_item_ids"`
	CreatedAt    time.Time                    `json:"created_at"`
}
type ProductSplitAllocation struct {
	ID     string                      `json:"id"`
	Value  ProductSplitAllocationValue `json:"value"`
	Digest string                      `json:"digest"`
}
type splitWorkItemSeat struct {
	ProposalID       string `json:"proposal_id"`
	AllocationID     string `json:"allocation_id"`
	AllocationDigest string `json:"allocation_digest"`
	ParentHeadID     string `json:"parent_head_id"`
	WorkItemID       string `json:"work_item_id"`
}

func loadProductSplitAllocation(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductSplitAllocation, error) {
	var saved, history ProductSplitAllocation
	err := taskEvidenceRecord(ctx, tx, "product_split_allocation", r.ProposalID, &saved)
	if errors.Is(err, sql.ErrNoRows) {
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_allocation_admission' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_allocation_history' AND work_item_id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_work_item_seat' AND json_extract(payload,'$.proposal_id')=?) OR EXISTS(SELECT 1 FROM kernel_budget_ledger WHERE operation_key=? AND kind='split_children')", r.ProposalID, r.ProposalID, r.ProposalID, r.ProposalID).Scan(&known); err != nil {
			return saved, err
		}
		if !known {
			var witnessErr error
			known, witnessErr = splitCreationWitnessTx(ctx, tx, r.Target.WaveID, splitProposalHead(ProductSplitProposalRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID}))
			if witnessErr != nil {
				return saved, witnessErr
			}
		}
		if known {
			return saved, ErrProductSplitHeld
		}
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return saved, err
		}
		return saved, splitEvidenceErr(err)
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if saved.Value.Request != r || saved.Digest != digest || saved.ID != structureID("split-allocation-", r.ProposalID, digest) || saved.Value.CreatedAt.IsZero() {
		return saved, ErrProductSplitHeld
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_allocation_history", saved.ID, &history); err != nil {
		return saved, splitEvidenceErr(err)
	}
	var admission splitChildAdmission
	if err := taskEvidenceRecord(ctx, tx, "product_split_allocation_admission", r.ProposalID, &admission); err != nil {
		return saved, splitEvidenceErr(err)
	}
	if !reflect.DeepEqual(history, saved) || admission != (splitChildAdmission{saved.Digest, saved.Value.ParentHeadID}) {
		return saved, ErrProductSplitHeld
	}
	plan, err := loadProductSplitChildPlan(ctx, tx, r)
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	if saved.Value.PlanID != plan.Set.ID || saved.Value.PlanDigest != plan.Set.Digest || saved.Value.ParentHeadID != plan.Set.Value.ParentHeadID || len(saved.Value.WorkItemIDs) != len(plan.Children) {
		return saved, ErrProductSplitHeld
	}
	for i, child := range plan.Children {
		var seat splitWorkItemSeat
		if saved.Value.WorkItemIDs[i] != child.Value.WorkItemID {
			return saved, ErrProductSplitHeld
		}
		if err := taskEvidenceRecord(ctx, tx, "product_split_work_item_seat", child.Value.WorkItemID, &seat); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if seat != (splitWorkItemSeat{r.ProposalID, saved.ID, saved.Digest, saved.Value.ParentHeadID, child.Value.WorkItemID}) {
			return saved, ErrProductSplitHeld
		}
	}
	var raw []byte
	var amount, unknown int
	if err := tx.QueryRowContext(ctx, "SELECT amount,unknown,payload FROM kernel_budget_ledger WHERE wave_id=? AND operation_key=? AND kind='split_children'", r.Target.WaveID, r.ProposalID).Scan(&amount, &unknown, &raw); err != nil {
		return saved, splitEvidenceErr(err)
	}
	var ledger ProductSplitAllocation
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	if amount != len(plan.Children) || unknown != 0 || !reflect.DeepEqual(ledger, saved) {
		return saved, ErrProductSplitHeld
	}
	return saved, nil
}

// Reads the union of independent witnesses. Loss of one budget/index record
// cannot free a seat or permit a replacement allocation.
func splitAllocationIDsTx(ctx context.Context, tx *sql.Tx, waveID string) ([]string, error) {
	var orphan bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM kernel_resource_leases l WHERE l.wave_id=? AND l.resource LIKE 'product-split-mutation:%' AND NOT EXISTS(SELECT 1 FROM records r WHERE r.kind='product_split_create' AND r.id=l.run_id))", waveID).Scan(&orphan); err != nil {
		return nil, err
	}
	if orphan {
		return nil, ErrProductSplitHeld
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM records WHERE work_item_id=? AND kind IN ('product_split_allocation','product_split_allocation_admission')
 UNION SELECT work_item_id FROM records WHERE kind='product_split_allocation_history' AND json_extract(payload,'$.value.request.target.wave_id')=?
 UNION SELECT json_extract(payload,'$.proposal_id') FROM records WHERE work_item_id=? AND kind='product_split_work_item_seat'
 UNION SELECT operation_key FROM kernel_budget_ledger WHERE wave_id=? AND kind='split_children'
 UNION SELECT json_extract(payload,'$.request.proposal_id') FROM records WHERE kind='product_split_create_history' AND json_extract(payload,'$.request.target.wave_id')=?
 UNION SELECT json_extract(payload,'$.proposal_id') FROM records WHERE kind IN ('product_split_create_admission','product_split_created_resource') AND work_item_id=?
 UNION SELECT json_extract(payload,'$.value.value.request.proposal_id') FROM records WHERE kind IN ('product_split_create','product_split_create_event') AND json_extract(payload,'$.value.value.request.target.wave_id')=?
 UNION SELECT json_extract(payload,'$.value.request.proposal_id') FROM records WHERE kind IN ('product_split_create_gate','product_split_create_gate_history') AND json_extract(payload,'$.value.request.target.wave_id')=?
 UNION SELECT json_extract(payload,'$.proposal_id') FROM records WHERE kind='product_split_create_retry_budget' AND work_item_id=?
 UNION SELECT json_extract(payload,'$.proposal_id') FROM kernel_budget_ledger WHERE kind='split_create_retry' AND wave_id=?
 UNION SELECT json_extract(payload,'$.value.proposal_id') FROM records WHERE kind IN ('product_split_preparation','product_split_preparation_history','product_split_preparation_admission') AND json_extract(payload,'$.value.wave_id')=?
 UNION SELECT json_extract(payload,'$.proposal_id') FROM records WHERE kind='product_split_preparation_creation' AND work_item_id=?
 UNION SELECT json_extract(payload,'$.preparation.value.proposal_id') FROM records WHERE kind='product_split_tool_adoption' AND json_extract(payload,'$.source.target.wave_id')=?
 UNION SELECT json_extract(payload,'$.value.request.proposal_id') FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission','product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission') AND json_extract(payload,'$.value.request.target.wave_id')=?
 UNION SELECT json_extract(payload,'$.request.proposal_id') FROM records WHERE kind='product_split_child_scope_source' AND work_item_id=? LIMIT ?`, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, maxProductWorkItems+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) > maxProductWorkItems {
		return nil, ErrProductSplitHeld
	}
	return ids, nil
}

// Existing immutable registration versions retain lifetime identity even when
// their mutable heads are removed. No new execution authority is inferred.
func productWorkItemLifetimeIDsTx(ctx context.Context, tx *sql.Tx, w domain.Wave) (map[string]bool, error) {
	ids := map[string]bool{}
	rows, err := tx.QueryContext(ctx, "SELECT payload FROM records WHERE work_item_id=? AND kind IN ('product_work_item_registration','product_work_item_registration_version')", w.ID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		var registration ProductWorkItemRegistrationSnapshot
		if err := json.Unmarshal(raw, &registration); err != nil {
			rows.Close()
			return nil, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
		}
		digest, err := contentDigest(registration.Value)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if digest != registration.Digest || registration.Value.WaveID != w.ID || registration.Value.Definition.ID == "" {
			rows.Close()
			return nil, ErrTaskEvidence
		}
		ids[registration.Value.Definition.ID] = true
		if len(ids) > maxProductWorkItems {
			rows.Close()
			return nil, ErrProductSplitHeld
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	// Initial root admission remains an independent seat witness if both
	// registration indexes are lost.
	var admission productAdmissionReceipt
	err = taskEvidenceRecord(ctx, tx, "product_wave_admission", structureID("product-admission-", w.Source, w.SourceID), &admission)
	if err == nil {
		digest, err := contentDigest(admission.Value)
		if err != nil {
			return nil, err
		}
		if admission.Digest != digest || admission.Value.WaveID != w.ID || admission.Value.Source != w.Source || admission.Value.SourceID != w.SourceID {
			return nil, ErrTaskEvidence
		}
		for id := range admission.Value.Registrations {
			ids[id] = true
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	allocations, err := splitAllocationIDsTx(ctx, tx, w.ID)
	if err != nil {
		return nil, err
	}
	for _, id := range allocations {
		var head ProductSplitAllocation
		if err := taskEvidenceRecord(ctx, tx, "product_split_allocation", id, &head); err != nil {
			return nil, splitEvidenceErr(err)
		}
		if head.Value.Request.Target.WaveID != w.ID || head.Value.Request.ProposalID != id {
			return nil, ErrProductSplitHeld
		}
		allocation, err := loadProductSplitAllocation(ctx, tx, head.Value.Request)
		if err != nil {
			return nil, err
		}
		for _, workID := range allocation.Value.WorkItemIDs {
			ids[workID] = true
		}
	}
	if len(ids) > maxProductWorkItems {
		return nil, ErrProductSplitHeld
	}
	return ids, nil
}

// ReserveProductSplitChildren reserves the complete set before external issue
// creation. It grants no HTTP/model execution or parent retirement. Seats and
// lifetime ledger entries are never released by pause, scope changes or retry.
func (k *KernelStore) ReserveProductSplitChildren(ctx context.Context, r ProductSplitChildPlanRequest) (ProductSplitAllocation, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitAllocation{}, err
	}
	defer tx.Rollback()
	saved, err := k.reserveProductSplitChildrenTx(ctx, tx, r)
	if err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

func (k *KernelStore) reserveProductSplitChildrenTx(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductSplitAllocation, error) {
	var saved ProductSplitAllocation
	if !r.Target.Valid() || r.ParentWorkItemID == "" || r.ProposalID == "" || !validSourceDigest(r.ProposalDigest) {
		return saved, ErrProductSplitHeld
	}
	prior, err := loadProductSplitAllocation(ctx, tx, r)
	if err == nil {
		return prior, nil
	}
	// A genuinely absent allocation has no evidence wrapper.
	if err != sql.ErrNoRows {
		return saved, err
	}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return saved, err
	}
	if w.Target() != r.Target || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return saved, ErrProductSplitHeld
	}
	proposal, err := k.readProductSplitProposalTx(ctx, tx, r.Target, r.ParentWorkItemID)
	if err != nil {
		return saved, err
	}
	if proposal.ID != r.ProposalID || proposal.Digest != r.ProposalDigest {
		return saved, ErrProductSplitHeld
	}
	plan, err := loadProductSplitChildPlan(ctx, tx, r)
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	ids, err := productWorkItemLifetimeIDsTx(ctx, tx, w)
	if err != nil {
		return saved, err
	}
	if len(ids)+len(plan.Children) > maxProductWorkItems {
		return saved, ErrOperationBudget
	}
	saved.Value = ProductSplitAllocationValue{Request: r, PlanID: plan.Set.ID, PlanDigest: plan.Set.Digest, ParentHeadID: plan.Set.Value.ParentHeadID, WorkItemIDs: []string{}, CreatedAt: k.clock()}
	for _, child := range plan.Children {
		if ids[child.Value.WorkItemID] {
			return saved, ErrProductSplitHeld
		}
		var collision bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_work_item_registration','wave_work_item','product_split_work_item_seat')) OR EXISTS(SELECT 1 FROM records WHERE kind='product_work_item_registration_version' AND json_extract(payload,'$.value.definition.id')=?)", child.Value.WorkItemID, child.Value.WorkItemID).Scan(&collision); err != nil {
			return saved, err
		}
		if collision {
			return saved, ErrProductSplitHeld
		}
		saved.Value.WorkItemIDs = append(saved.Value.WorkItemIDs, child.Value.WorkItemID)
	}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	saved.ID = structureID("split-allocation-", r.ProposalID, saved.Digest)
	for _, workID := range saved.Value.WorkItemIDs {
		seat := splitWorkItemSeat{r.ProposalID, saved.ID, saved.Digest, saved.Value.ParentHeadID, workID}
		if err := kernelRecordTx(ctx, tx, "product_split_work_item_seat", workID, w.ID, seat); err != nil {
			return saved, err
		}
	}
	for _, record := range []struct {
		kind, id, owner string
		value           any
	}{
		{"product_split_allocation", r.ProposalID, w.ID, saved},
		{"product_split_allocation_history", saved.ID, r.ProposalID, saved},
		{"product_split_allocation_admission", r.ProposalID, w.ID, splitChildAdmission{saved.Digest, saved.Value.ParentHeadID}},
	} {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, record.owner, record.value); err != nil {
			return saved, err
		}
	}
	raw, err := json.Marshal(saved)
	if err != nil {
		return saved, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'split_children',?,0,?)", w.ID, r.ProposalID, len(plan.Children), raw); err != nil {
		return saved, err
	}
	return saved, nil
}

func (k *KernelStore) ReadProductSplitAllocation(ctx context.Context, r ProductSplitChildPlanRequest) (ProductSplitAllocation, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitAllocation{}, err
	}
	defer tx.Rollback()
	return loadProductSplitAllocation(ctx, tx, r)
}
