package storage

import (
	"context"
	"database/sql"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ProductContinuationInput is a bounded origin summary. It contains no private
// checkout/proof payload and never carries forward quality or formal approval.
type ProductContinuationInput struct {
	ID                  string          `json:"id"`
	Digest              string          `json:"digest"`
	WorkItemID          string          `json:"work_item_id"`
	OriginalTarget      contract.Target `json:"original_target"`
	ParentWorkItemID    string          `json:"parent_work_item_id"`
	ParentTarget        contract.Target `json:"parent_target"`
	ParentMergeID       string          `json:"parent_merge_id"`
	ParentReceiptDigest string          `json:"parent_receipt_digest"`
	ParentCandidateSHA  string          `json:"parent_candidate_sha"`
	ParentMergeSHA      string          `json:"parent_merge_sha"`
}

func productContinuationInput(saved ProductContinuation) ProductContinuationInput {
	v := saved.Value
	return ProductContinuationInput{ID: saved.ID, Digest: saved.Digest, WorkItemID: v.Request.Definition.ID, OriginalTarget: v.Request.Target, ParentWorkItemID: v.ParentWorkItemID, ParentTarget: v.ParentTarget, ParentMergeID: v.Request.ParentMergeID, ParentReceiptDigest: v.Request.ParentReceiptDigest, ParentCandidateSHA: v.ParentCandidateSHA, ParentMergeSHA: v.ParentMergeSHA}
}

// The active leaf uses current source registration. Older nodes are original
// audit facts and may have an older registration head. Every hop still needs
// its own immutable versions and original authenticated merged projection.
func (k *KernelStore) productContinuationInputsTx(ctx context.Context, q queryer, w domain.Wave, workID string) ([]ProductContinuation, error) {
	if w.Policy.Stage != "P" || !w.Policy.Product {
		return nil, nil
	}
	var out []ProductContinuation
	seen := map[string]bool{}
	for {
		saved, err := loadProductContinuation(ctx, q, workID)
		if errors.Is(err, sql.ErrNoRows) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if seen[workID] || len(out) >= maxProductWorkItems {
			return nil, ErrTaskEvidence
		}
		seen[workID] = true
		if err := k.checkProductContinuationInWaveTx(ctx, q, saved, w, len(out) == 0); err != nil {
			return nil, err
		}
		if len(out) > 0 {
			child := out[len(out)-1].Value
			parent := saved.Value.Registration.Value
			if parent.ScopeRevision > child.ParentTarget.ScopeRevision || parent.PolicyRevision > child.ParentTarget.PolicyRevision {
				return nil, ErrTaskEvidence
			}
		}
		out = append(out, saved)
		workID = saved.Value.ParentWorkItemID
	}
}
