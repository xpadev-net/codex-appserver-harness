package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Immediate-parent criterion IDs stay in Coverage. Only host bookkeeping is
// projected onto the original Wave IDs; local obligations are never flattened.
func nestedSplitCriterionRootsTx(ctx context.Context, tx *sql.Tx, w domain.Wave, registration ProductWorkItemRegistrationSnapshot, binding ProductSplitChildScopeBinding) (map[string]string, error) {
	proposal, err := loadProductSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: binding.Value.Request.Target, ParentWorkItemID: binding.Value.Request.ParentWorkItemID})
	if err != nil {
		return nil, splitEvidenceErr(err)
	}
	if binding.Value.Child.Value.Depth != proposal.Value.Depth+1 || binding.Value.Child.Value.Depth < 1 {
		return nil, ErrProductSplitHeld
	}
	roots, err := nestedSplitRootsFromBinding(registration, binding, proposal)
	if err != nil {
		return nil, err
	}
	rootSet := map[string]bool{}
	for _, root := range roots {
		rootSet[root] = true
	}
	for _, criterion := range w.Criteria {
		delete(rootSet, criterion.ID)
	}
	if len(rootSet) != 0 {
		return nil, ErrProductSplitHeld
	}
	return roots, nil
}

func nestedSplitRootsFromBinding(registration ProductWorkItemRegistrationSnapshot, binding ProductSplitChildScopeBinding, proposal ProductSplitProposal) (map[string]string, error) {
	roots := map[string]string{}
	rootSet := map[string]bool{}
	for _, mapping := range binding.Value.Child.Value.Child.Coverage {
		root := mapping.ParentCriterion
		if proposal.Value.ParentSplitScope != nil {
			var ok bool
			root, ok = proposal.Value.ParentCriterionRoots[root]
			if !ok {
				return nil, ErrProductSplitHeld
			}
		}
		if root == "" || !slices.Contains(registration.Value.Definition.Criteria, root) || roots[mapping.ChildCriterion] != "" || !slices.Contains(binding.Value.Scope.Included, mapping.ChildCriterion) {
			return nil, ErrProductSplitHeld
		}
		roots[mapping.ChildCriterion] = root
		rootSet[root] = true
	}
	if len(roots) != len(binding.Value.Scope.Included) || len(rootSet) != len(registration.Value.Definition.Criteria) {
		return nil, ErrProductSplitHeld
	}
	return roots, nil
}

// ProductSplitProposalContext is a read-only derived view. It has no journal ID,
// admission receipt, creation permission or model/quality grant.
type ProductSplitProposalContext struct {
	Value  ProductSplitProposalValue `json:"value"`
	Digest string                    `json:"digest"`
}

func (k *KernelStore) InspectProductSplitProposalContext(ctx context.Context, r ProductSplitProposalRequest) (ProductSplitProposalContext, error) {
	var out ProductSplitProposalContext
	if !r.Target.Valid() || r.Key == "" || len(r.Key) > 256 || !utf8.ValidString(r.Key) || r.ParentWorkItemID == "" || !validSourceDigest(r.RegistrationDigest) || len(r.Proposal.Children) > maxProductWorkItems {
		return out, ErrProductSplitHeld
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > 1<<20 {
		return out, ErrProductSplitHeld
	}
	var detached ProductSplitProposalRequest
	if err := json.Unmarshal(raw, &detached); err != nil {
		return out, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	out.Value, err = k.productSplitContextTx(ctx, tx, detached)
	if err != nil {
		return ProductSplitProposalContext{}, err
	}
	out.Digest, err = contentDigest(out.Value)
	return out, err
}
