package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

type ProductNestedSplitChildScopesValue struct {
	ProductNestedSplitAllocationValue
	AllocationEnvelopeID     string                  `json:"allocation_envelope_id"`
	AllocationEnvelopeDigest string                  `json:"allocation_envelope_digest"`
	Scopes                   ProductSplitChildScopes `json:"scopes"`
}
type ProductNestedSplitChildScopes struct {
	ID     string                             `json:"id"`
	Value  ProductNestedSplitChildScopesValue `json:"value"`
	Digest string                             `json:"digest"`
}

func nestedSplitScopeRecords(saved ProductNestedSplitChildScopes) []struct{ kind, id string } {
	records := []struct{ kind, id string }{{"product_nested_split_child_scopes", saved.Value.Scopes.Value.Request.ProposalID}, {"product_nested_split_child_scopes_history", saved.ID}, {"product_nested_split_child_scopes_admission", saved.Value.Scopes.Value.Request.ProposalID}}
	for _, binding := range saved.Value.Scopes.Value.Children {
		workID := binding.Value.Child.Value.WorkItemID
		records = append(records, struct{ kind, id string }{"product_nested_split_child_scope", workID}, struct{ kind, id string }{"product_nested_split_child_scope_history", binding.ID}, struct{ kind, id string }{"product_nested_split_child_scope_admission", workID}, struct{ kind, id string }{"product_nested_split_child_scope_source", fmt.Sprintf("github:%d:issue:%d", binding.Value.Issue.RepositoryID, binding.Value.Issue.Number)})
	}
	return records
}

func loadNestedSplitChildScopes(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductNestedSplitChildScopes, error) {
	return readSplitProof(ctx, tx, "loadNestedSplitChildScopes", r, func(read context.Context) (ProductNestedSplitChildScopes, error) {
		return loadNestedSplitChildScopesUncached(read, tx, r)
	})
}

func loadNestedSplitChildScopesUncached(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductNestedSplitChildScopes, error) {
	var saved ProductNestedSplitChildScopes
	malformed, err := splitScopeUnreadableForWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return saved, err
	}
	if malformed {
		return saved, ErrProductSplitHeld
	}
	err = taskEvidenceRecord(ctx, tx, "product_nested_split_child_scopes", r.ProposalID, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, splitScopeWitnessSQL(`SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.scopes.value.request.proposal_id')=?) OR EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind='product_split_ancestor_group' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.proposal_id')=?)`), r.ProposalID, r.ProposalID).Scan(&known); err != nil {
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
	if saved.Digest != digest || saved.ID != structureID("nested-split-scopes-", r.ProposalID, digest) || saved.Value.Scopes.Value.Request != r {
		return saved, ErrProductSplitHeld
	}
	if len(saved.Value.Scopes.Value.Children) < 2 || len(saved.Value.Scopes.Value.Children) > maxProductWorkItems {
		return saved, ErrProductSplitHeld
	}
	// Missing independent copies are never repairable; reject them before
	// walking the full upstream history. Matching copies grant no authority.
	for _, record := range nestedSplitScopeRecords(saved) {
		var witness ProductNestedSplitChildScopes
		if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &witness); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(witness, saved) {
			return saved, ErrProductSplitHeld
		}
	}
	allocation, err := loadNestedSplitAllocation(ctx, tx, r)
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	if saved.Value.AllocationEnvelopeID != allocation.ID || saved.Value.AllocationEnvelopeDigest != allocation.Digest || !reflect.DeepEqual(saved.Value.ProductNestedSplitAllocationValue, allocation.Value) {
		return saved, ErrProductSplitHeld
	}
	scopes, err := deriveSplitChildScopesWithAncestry(ctx, tx, r, saved.Value.Scopes.Value.CreatedAt, &allocation)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(saved.Value.Scopes, scopes) {
		return saved, ErrProductSplitHeld
	}

	return saved, nil
}

func (k *KernelStore) bindNestedSplitChildScopesTx(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductSplitChildScopes, error) {
	var empty ProductSplitChildScopes
	if err := k.splitCreatePreflightTx(ctx, tx, r); err != nil {
		return empty, err
	}
	allocation, err := loadNestedSplitAllocation(ctx, tx, r)
	if err != nil {
		return empty, splitEvidenceErr(err)
	}
	scopes, err := deriveSplitChildScopesWithAncestry(ctx, tx, r, k.clock(), &allocation)
	if err != nil {
		return empty, err
	}
	saved := ProductNestedSplitChildScopes{Value: ProductNestedSplitChildScopesValue{ProductNestedSplitAllocationValue: allocation.Value, AllocationEnvelopeID: allocation.ID, AllocationEnvelopeDigest: allocation.Digest, Scopes: scopes}}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return empty, err
	}
	saved.ID = structureID("nested-split-scopes-", r.ProposalID, saved.Digest)
	raw, err := json.Marshal(saved)
	if err != nil {
		return empty, err
	}
	if len(raw) > 16<<20 {
		return empty, ErrProductSplitHeld
	}
	for _, binding := range scopes.Value.Children {
		var claimed bool
		key := fmt.Sprintf("github:%d:issue:%d", binding.Value.Issue.RepositoryID, binding.Value.Issue.Number)
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM kernel_waves WHERE source='github' AND source_id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_work_item_registration' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_child_scope_source','product_nested_split_child_scope_source') AND id=?)`, binding.Value.SourceID, binding.Value.SourceID, key).Scan(&claimed); err != nil {
			return empty, err
		}
		if claimed {
			return empty, ErrProductSplitHeld
		}
	}
	for _, record := range nestedSplitScopeRecords(saved) {
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind=? AND id=?)", record.kind, record.id).Scan(&exists); err != nil {
			return empty, err
		}
		if exists {
			return empty, ErrProductSplitHeld
		}
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, r.Target.WaveID, saved); err != nil {
			return empty, err
		}
	}
	return scopes, nil
}
