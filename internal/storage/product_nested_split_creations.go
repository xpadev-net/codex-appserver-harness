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

type ProductSplitAncestorSourceReference struct {
	WorkItemID   string `json:"work_item_id"`
	SourceID     string `json:"source_id"`
	SourceDigest string `json:"source_digest"`
}

func (k *KernelStore) nestedSplitCreationSourceTx(ctx context.Context, tx *sql.Tx, job ProductSplitCreate) (ProductSplitChildSource, []ProductSplitAncestorSourceReference, error) {
	var empty ProductSplitChildSource
	binding, err := splitCreationParentBindingTx(ctx, tx, job)
	if err != nil {
		return empty, nil, err
	}
	sources, err := k.splitSourceLineageReadyTx(ctx, tx, binding, 5*time.Second)
	if err != nil {
		return empty, nil, err
	}
	if sources[0].Value.Facts.PolicyDigest != job.Value.Ancestry.Value.SourcePolicyDigest {
		return empty, nil, ErrProductSplitHeld
	}
	var ancestors []ProductSplitAncestorSourceReference
	for _, source := range sources[1:] {
		ancestors = append(ancestors, ProductSplitAncestorSourceReference{WorkItemID: source.Value.Child.Value.WorkItemID, SourceID: source.ID, SourceDigest: source.Digest})
	}
	return sources[0], ancestors, nil
}

// Historical activation retains its original semantic source receipt even if
// the live Issue later changes. Freshness is checked only on new activation.
func validateNestedSplitCreationGateSource(ctx context.Context, tx *sql.Tx, job ProductSplitCreate, gate ProductSplitCreateGate) error {
	if job.Value.Ancestry == nil {
		if len(gate.AncestorSources) != 0 {
			return ErrProductSplitHeld
		}
		return nil
	}
	for _, binding := range job.Value.Ancestry.Value.Children {
		if binding.Value.Child.Value.WorkItemID != job.Value.Request.ParentWorkItemID {
			continue
		}
		lineage, err := splitSourceLineageTx(ctx, tx, binding)
		if err != nil {
			return err
		}
		if len(gate.AncestorSources) != len(lineage)-1 {
			return ErrProductSplitHeld
		}
		for i, ancestor := range lineage {
			id, digest := gate.ParentSourceID, gate.ParentSourceDigest
			if i > 0 {
				ref := gate.AncestorSources[i-1]
				if ref.WorkItemID != ancestor.Value.Child.Value.WorkItemID {
					return ErrProductSplitHeld
				}
				id, digest = ref.SourceID, ref.SourceDigest
			}
			if err := validateSplitCreationHistoricalSource(ctx, tx, ancestor, id, digest, gate.ObservedAt); err != nil {
				return err
			}
		}
		return nil
	}
	return ErrProductSplitHeld
}

func validateSplitCreationHistoricalSource(ctx context.Context, tx *sql.Tx, binding ProductSplitChildScopeBinding, id, expectedDigest string, observedAt time.Time) error {
	var source ProductSplitChildSource
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_source_history", id, &source); err != nil {
		return splitEvidenceErr(err)
	}
	digest, err := contentDigest(source.Value)
	if err != nil {
		return err
	}
	if source.ID != id || source.Digest != expectedDigest || digest != source.Digest || source.Version < 1 || source.Version > maxSplitSourceVersions || source.ID != structureID("split-child-source-", binding.Value.Child.Value.WorkItemID, strconv.Itoa(source.Version)+":"+digest) || source.ObservedAt.IsZero() || !source.CheckedAt.Equal(source.ObservedAt) || source.ObservedAt.After(observedAt) {
		return ErrProductSplitHeld
	}
	policy, err := loadSplitChildSourcePolicy(ctx, tx, binding)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(source.Value.splitChildSourceReference, policy.Value.splitChildSourceReference) || source.Value.Facts.validate() != nil || source.Value.Status != "ready" || splitChildSourceStatus(binding, source.Value.Facts, policy.Value.PolicyDigest) != "ready" {
		return ErrProductSplitHeld
	}
	return nil
}
