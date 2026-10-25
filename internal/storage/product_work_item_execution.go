package storage

import (
	"context"
	"database/sql"
	"errors"

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
	return productSplitParentPreparationHeld(ctx, q, w, workID, "")
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
	var membership struct {
		CandidateID string `json:"candidate_id"`
	}
	if err := taskEvidenceRecord(ctx, q, "wave_work_item", workID, &membership); err != nil {
		return false, err
	}
	var saved ProductTargetSnapshot
	err := taskEvidenceRecord(ctx, q, "product_target", productTargetID(w.Target(), workID), &saved)
	if errors.Is(err, sql.ErrNoRows) {
		if membership.CandidateID != "" {
			return false, ErrTaskEvidence
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if saved.Value.Target.WaveID != w.ID {
		return false, ErrTaskEvidence
	}
	saved, err = loadProductTargetTx(ctx, q, saved.Value.Target, workID)
	if err != nil {
		return false, err
	}
	if saved.Value.Candidate.ID != membership.CandidateID {
		return false, ErrTaskEvidence
	}
	var candidate runner.Candidate
	if err := taskEvidenceRecord(ctx, q, "product_candidate", structureID("product-candidate-", workID, membership.CandidateID), &candidate); err != nil {
		return false, err
	}
	if candidate != saved.Value.Candidate {
		return false, ErrTaskEvidence
	}
	work := saved.Value.WorkItem
	return work.Paused || work.Terminal() || work.ScopeRevision != w.ScopeRevision || work.PolicyRevision != w.Policy.Revision, nil
}
