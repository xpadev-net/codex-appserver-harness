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
