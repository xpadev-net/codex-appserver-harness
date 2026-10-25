package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

// WorkItem pause/cancel also fence local preparation/sealing. MergeHold remains
// a merge-only control and must survive candidate adoption without stopping fix.
func productWorkItemExecutionHeld(ctx context.Context, q queryer, w domain.Wave, workID string) (bool, error) {
	held, err := productWorkItemProjectionHeld(ctx, q, w, workID)
	if err != nil || held {
		return held, err
	}
	held, err = productSplitChildExecutionHeld(ctx, q, workID)
	if err != nil || held {
		return held, err
	}
	return productSplitParentPreparationHeld(ctx, q, w, workID, "")
}

const productExecutionSourceMaxAge = 5 * time.Second

// A stack-local permit is checked again after the caller's remaining proofs.
// It is valid only in that transaction and is never persisted or memoized.
type productExecutionPermit struct {
	tx       *sql.Tx
	deadline time.Time
	readAt   []time.Time
}

func (p productExecutionPermit) current(k *KernelStore, tx *sql.Tx) bool {
	if p.tx == nil { // Preserve the existing root execution contract.
		return true
	}
	if p.tx != tx {
		return false
	}
	now := k.clock()
	if !now.Before(p.deadline) {
		return false
	}
	for _, at := range p.readAt {
		if now.Before(at) || now.Sub(at) > productExecutionSourceMaxAge {
			return false
		}
	}
	return true
}

// Production execution requires current evidence in the caller's transaction.
// The queryer-only metadata predicate above remains deliberately conservative.
func (k *KernelStore) productWorkItemExecutionHeld(ctx context.Context, tx *sql.Tx, w domain.Wave, workID string) (bool, error) {
	return k.productWorkItemExecutionPermitTx(ctx, tx, w, workID, nil)
}

func (k *KernelStore) productWorkItemExecutionPermitTx(ctx context.Context, tx *sql.Tx, w domain.Wave, workID string, permit *productExecutionPermit) (bool, error) {
	known, err := productSplitChildExecutionHeld(ctx, tx, workID)
	if err != nil || !known {
		if err != nil {
			return true, err
		}
		return productWorkItemExecutionHeld(ctx, tx, w, workID)
	}
	current, err := loadWave(ctx, tx, w.ID)
	if err != nil {
		return true, err
	}
	if current.Target() != w.Target() || current.Validate() != nil || current.Policy.Stage != "P" || !current.Policy.Product || current.DispatchPaused() || current.State == domain.WaveBlocked || current.State == domain.WaveCanceled || current.State == domain.WaveCompleted {
		return true, nil
	}
	w = current
	registration, err := loadProductWorkItemRegistration(ctx, tx, workID)
	if err != nil {
		return executionEvidenceHeld(err)
	}
	binding, err := productSplitChildScopeTx(ctx, tx, w, registration)
	if err != nil {
		return executionEvidenceHeld(err)
	}
	if binding == nil {
		return true, nil
	}
	if held, err := productWorkItemProjectionHeld(ctx, tx, w, workID); err != nil || held {
		return true, err
	}
	if held, err := productSplitParentPreparationHeld(ctx, tx, w, workID, ""); err != nil || held {
		return true, err
	}
	policy, err := loadSplitChildSourcePolicy(ctx, tx, *binding)
	if err != nil {
		return executionEvidenceHeld(err)
	}
	lineage, err := splitSourceLineageTx(ctx, tx, *binding)
	if err != nil {
		return executionEvidenceHeld(err)
	}
	sources, err := k.splitSourceLineageReadyTx(ctx, tx, *binding, policy.Value.PolicyDigest, productExecutionSourceMaxAge)
	if err != nil {
		return executionEvidenceHeld(err)
	}
	for _, ancestor := range lineage {
		projection, err := productWorkItemProjection(ctx, tx, w, ancestor.Value.Request.ParentWorkItemID)
		if err != nil {
			return executionEvidenceHeld(err)
		}
		// SPLIT and a merged parent's own partial scope are expected ancestry.
		// Pause/cancel and a changed scope/policy still stop descendants.
		if projection != nil && (projection.Paused || projection.Phase == domain.Canceled || projection.ScopeRevision != w.ScopeRevision || projection.PolicyRevision != w.Policy.Revision) {
			return true, nil
		}
	}
	var rootRead *ProductSourceRead
	switch w.Source {
	case "github":
		read, err := readProductSourceReadTx(ctx, tx, w)
		if err != nil {
			return executionEvidenceHeld(err)
		}
		snapshot, err := loadProductSourceSnapshot(ctx, tx, w.ID)
		if err != nil {
			return executionEvidenceHeld(err)
		}
		status, err := k.sourceStatusTx(ctx, tx, w, snapshot.Value.Facts)
		if err != nil {
			return executionEvidenceHeld(err)
		}
		if status != "ready" || snapshot.Value.Status != "ready" || read.Value.PolicyDigest != policy.Value.PolicyDigest {
			return true, nil
		}
		rootRead = &read
	case "direct": // Immutable admitted request; there is no external Root Issue.
	default:
		return true, nil
	}
	// Recheck all actual read ages after every canonical proof and control read.
	// These live observations are never stored in the immutable proof memo.
	now := k.clock()
	if !now.Before(w.Deadline) {
		return true, nil
	}
	for _, source := range sources {
		if now.Before(source.CheckedAt) || now.Sub(source.CheckedAt) > productExecutionSourceMaxAge {
			return true, nil
		}
	}
	if rootRead != nil && (now.Before(rootRead.Value.ReadAt) || now.Sub(rootRead.Value.ReadAt) > productExecutionSourceMaxAge) {
		return true, nil
	}
	if permit != nil {
		*permit = productExecutionPermit{tx: tx, deadline: w.Deadline}
		for _, source := range sources {
			permit.readAt = append(permit.readAt, source.CheckedAt)
		}
		if rootRead != nil {
			permit.readAt = append(permit.readAt, rootRead.Value.ReadAt)
		}
	}
	return false, nil
}

func executionEvidenceHeld(err error) (bool, error) {
	if errors.Is(err, ErrProductSplitHeld) || errors.Is(err, ErrTaskEvidence) || errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	return true, err
}

// allowedHead is supplied only by split source/metadata validation. It permits
// the original intent to finish, but forbids a new split of the same parent
// epoch after replanning with a different head/key.
func productSplitParentPreparationHeld(ctx context.Context, q queryer, w domain.Wave, workID, allowedHead string) (bool, error) {
	var prepared bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(
 SELECT 1 FROM records WHERE kind IN ('product_split_preparation','product_split_preparation_history','product_split_preparation_admission')
 AND json_extract(payload,'$.value.wave_id')=?
 AND (json_extract(payload,'$.value.target') IS NULL OR (
 json_extract(payload,'$.value.parent_work_item_id')=? AND json_extract(payload,'$.value.target.scope_revision')=? AND json_extract(payload,'$.value.target.policy_revision')=?))
 AND (?='' OR coalesce(json_extract(payload,'$.value.parent_head_id'),'')<>?)
 ) OR EXISTS(
 SELECT 1 FROM records WHERE kind='product_split_preparation_creation' AND work_item_id=?
 AND (json_extract(payload,'$.target') IS NULL OR (
 json_extract(payload,'$.parent_work_item_id')=? AND json_extract(payload,'$.target.scope_revision')=? AND json_extract(payload,'$.target.policy_revision')=?))
 AND (?='' OR coalesce(json_extract(payload,'$.parent_head_id'),'')<>?)
 ) OR EXISTS(
 SELECT 1 FROM records WHERE kind='product_split_tool_adoption' AND json_extract(payload,'$.source.target.wave_id')=?
 AND (json_extract(payload,'$.preparation.value.target') IS NULL OR (
 json_extract(payload,'$.preparation.value.parent_work_item_id')=? AND json_extract(payload,'$.preparation.value.target.scope_revision')=? AND json_extract(payload,'$.preparation.value.target.policy_revision')=?))
 AND (?='' OR coalesce(json_extract(payload,'$.parent_head_id'),'')<>?)
 )`,
		w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead,
		w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead,
		w.ID, workID, w.ScopeRevision, w.Policy.Revision, allowedHead, allowedHead).Scan(&prepared)
	if err != nil || prepared {
		return prepared, err
	}
	return splitChildScopeParentExecutionHeld(ctx, q, w, workID, allowedHead)
}

// Split source/preflight reads must authenticate the same parent projection
// without interpreting their own durable preparation as a send prohibition.
func productWorkItemProjectionHeld(ctx context.Context, q queryer, w domain.Wave, workID string) (bool, error) {
	work, err := productWorkItemProjection(ctx, q, w, workID)
	if err != nil || work == nil {
		return false, err
	}
	return work.Paused || work.Terminal() || work.ScopeRevision != w.ScopeRevision || work.PolicyRevision != w.Policy.Revision, nil
}

func productWorkItemProjection(ctx context.Context, q queryer, w domain.Wave, workID string) (*domain.WorkItem, error) {
	var membership struct {
		CandidateID string `json:"candidate_id"`
	}
	if err := taskEvidenceRecord(ctx, q, "wave_work_item", workID, &membership); err != nil {
		return nil, err
	}
	var saved ProductTargetSnapshot
	err := taskEvidenceRecord(ctx, q, "product_target", productTargetID(w.Target(), workID), &saved)
	if errors.Is(err, sql.ErrNoRows) {
		if membership.CandidateID != "" {
			return nil, ErrTaskEvidence
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if saved.Value.Target.WaveID != w.ID {
		return nil, ErrTaskEvidence
	}
	saved, err = loadProductTargetTx(ctx, q, saved.Value.Target, workID)
	if err != nil {
		return nil, err
	}
	if saved.Value.Candidate.ID != membership.CandidateID {
		return nil, ErrTaskEvidence
	}
	var candidate runner.Candidate
	if err := taskEvidenceRecord(ctx, q, "product_candidate", structureID("product-candidate-", workID, membership.CandidateID), &candidate); err != nil {
		return nil, err
	}
	if candidate != saved.Value.Candidate {
		return nil, ErrTaskEvidence
	}
	return &saved.Value.WorkItem, nil
}
