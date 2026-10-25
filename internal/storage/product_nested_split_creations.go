package storage

import (
	"context"
	"database/sql"
	"reflect"
	"strconv"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
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

// Both production callers have authenticated the complete creation ancestry and
// run splitCreatePreflightTx in this same transaction, with no intervening scope
// writes. Reuse that canonical binding instead of walking its full graph again;
// current registration copies and source/policy records are still required.
func splitCreationParentBindingTx(ctx context.Context, tx *sql.Tx, job ProductSplitCreate) (ProductSplitChildScopeBinding, error) {
	var empty ProductSplitChildScopeBinding
	if job.Value.Ancestry == nil {
		return empty, ErrProductSplitHeld
	}
	group := job.Value.Ancestry.Value.ProductSplitRegistrationsValue
	if len(group.Children) != len(group.Registrations) {
		return empty, ErrProductSplitHeld
	}
	parentID := job.Value.Request.ParentWorkItemID
	var binding ProductSplitChildScopeBinding
	var registration ProductWorkItemRegistrationSnapshot
	found := false
	for i, child := range group.Children {
		if child.Value.Child.Value.WorkItemID != parentID {
			continue
		}
		if found {
			return empty, ErrProductSplitHeld
		}
		binding, registration, found = child, group.Registrations[i], true
	}
	if !found {
		return empty, ErrProductSplitHeld
	}
	w, err := loadWave(ctx, tx, job.Value.Request.Target.WaveID)
	if err != nil {
		return empty, err
	}
	ref := registration.Value.SplitScope
	if w.Target() != job.Value.Request.Target || !productRegistrationScope(registration, w) || registration.Value.Definition.ID != parentID || ref == nil || ref.BindingID != binding.ID || ref.BindingDigest != binding.Digest || ref.Request != binding.Value.Request || ref.SetID != group.ScopeSetID || ref.SetDigest != group.ScopeSetDigest || ref.SourceID != binding.Value.SourceID {
		return empty, ErrProductSplitHeld
	}
	for _, record := range []struct{ kind, id string }{{"product_work_item_registration", parentID}, {"product_work_item_registration_version", structureID("product-work-", parentID, registration.Digest)}} {
		var saved ProductWorkItemRegistrationSnapshot
		if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &saved); err != nil {
			return empty, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(saved, registration) {
			return empty, ErrProductSplitHeld
		}
	}
	var member struct {
		Target     contract.Target                  `json:"target"`
		SplitScope *ProductSplitChildScopeReference `json:"split_scope"`
	}
	if err := taskEvidenceRecord(ctx, tx, "wave_work_item", parentID, &member); err != nil {
		return empty, splitEvidenceErr(err)
	}
	if member.Target.WaveID != w.ID || member.Target.ScopeRevision != w.ScopeRevision || member.Target.PolicyRevision != w.Policy.Revision || !reflect.DeepEqual(member.SplitScope, ref) {
		return empty, ErrProductSplitHeld
	}
	return binding, nil
}

func (k *KernelStore) nestedSplitCreationSourceTx(ctx context.Context, tx *sql.Tx, job ProductSplitCreate) (ProductSplitChildSource, error) {
	var empty ProductSplitChildSource
	if job.Value.Ancestry == nil {
		return empty, ErrProductSplitHeld
	}
	binding, err := splitCreationParentBindingTx(ctx, tx, job)
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
