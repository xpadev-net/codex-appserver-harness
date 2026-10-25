package storage

import (
	"context"
	"database/sql"
	"reflect"
	"strconv"
	"time"
)

func loadSplitCreationAncestry(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (*ProductNestedSplitAllocation, error) {
	proposal, err := loadProductSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID})
	if err != nil {
		return nil, splitEvidenceErr(err)
	}
	if proposal.ID != r.ProposalID || proposal.Digest != r.ProposalDigest {
		return nil, ErrProductSplitHeld
	}
	if proposal.Value.ParentSplitScope == nil {
		return nil, nil
	}
	saved, err := loadNestedSplitAllocation(ctx, tx, r)
	if err != nil {
		return nil, splitEvidenceErr(err)
	}
	return &saved, nil
}

func (k *KernelStore) nestedSplitCreationSourceTx(ctx context.Context, tx *sql.Tx, job ProductSplitCreate) (ProductSplitChildSource, error) {
	var empty ProductSplitChildSource
	if job.Value.Ancestry == nil {
		return empty, ErrProductSplitHeld
	}
	binding, err := splitChildSourceBindingTx(ctx, tx, job.Value.Request.Target, job.Value.Request.ParentWorkItemID)
	if err != nil {
		return empty, err
	}
	source, err := loadSplitChildSource(ctx, tx, binding)
	if err != nil {
		return empty, err
	}
	now := k.clock()
	if source.Value.Status != "ready" || source.Value.Facts.PolicyDigest != job.Value.Ancestry.Value.SourcePolicyDigest || now.Before(source.CheckedAt) || now.Sub(source.CheckedAt) > 5*time.Second {
		return empty, ErrProductSplitHeld
	}
	return source, nil
}

// Historical activation retains its original semantic source receipt even if
// the live Issue later changes. Freshness is checked only on new activation.
func validateNestedSplitCreationGateSource(ctx context.Context, tx *sql.Tx, job ProductSplitCreate, gate ProductSplitCreateGate) error {
	if job.Value.Ancestry == nil {
		return nil
	}
	var source ProductSplitChildSource
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_source_history", gate.ParentSourceID, &source); err != nil {
		return splitEvidenceErr(err)
	}
	digest, err := contentDigest(source.Value)
	if err != nil {
		return err
	}
	if source.ID != gate.ParentSourceID || source.Digest != gate.ParentSourceDigest || digest != source.Digest || source.Version < 1 || source.Version > maxSplitSourceVersions || source.ID != structureID("split-child-source-", job.Value.Request.ParentWorkItemID, strconv.Itoa(source.Version)+":"+digest) || source.ObservedAt.IsZero() || !source.CheckedAt.Equal(source.ObservedAt) || source.ObservedAt.After(gate.ObservedAt) {
		return ErrProductSplitHeld
	}
	for _, binding := range job.Value.Ancestry.Value.Children {
		if binding.Value.Child.Value.WorkItemID == job.Value.Request.ParentWorkItemID {
			reference, err := splitSourceReferenceTx(ctx, tx, binding)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(source.Value.splitChildSourceReference, reference) || source.Value.Facts.validate() != nil || source.Value.Status != "ready" || splitChildSourceStatus(binding, source.Value.Facts, job.Value.Ancestry.Value.SourcePolicyDigest) != "ready" {
				return ErrProductSplitHeld
			}
			return nil
		}
	}
	return ErrProductSplitHeld
}
