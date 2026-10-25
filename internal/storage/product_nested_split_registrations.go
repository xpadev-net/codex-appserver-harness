package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type ProductNestedSplitRegistrationsValue struct {
	ProductNestedSplitChildScopesValue
	ScopeEnvelopeID     string                        `json:"scope_envelope_id"`
	ScopeEnvelopeDigest string                        `json:"scope_envelope_digest"`
	AncestorSet         ProductSplitAncestorWitnesses `json:"ancestor_set"`
	RegistrationSet     ProductSplitRegistrations     `json:"registration_set"`
}
type ProductNestedSplitRegistrations struct {
	ID     string                               `json:"id"`
	Value  ProductNestedSplitRegistrationsValue `json:"value"`
	Digest string                               `json:"digest"`
}

func loadNestedSplitRegistrations(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductNestedSplitRegistrations, error) {
	var saved ProductNestedSplitRegistrations
	malformed, err := splitScopeUnreadableForWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return saved, err
	}
	if malformed {
		return saved, ErrProductSplitHeld
	}
	err = taskEvidenceRecord(ctx, tx, "product_nested_split_registrations", r.ProposalID, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_nested_split_registrations_history','product_nested_split_registrations_admission') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.registration_set.value.request.proposal_id')=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.proposal_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.proposal_id'))=? AND coalesce(json_type(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.ancestor_set'),json_type(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.ancestor_set'))='object')`, r.ProposalID, r.ProposalID).Scan(&known); err != nil {
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
	if saved.Digest != digest || saved.ID != structureID("nested-split-registrations-", r.ProposalID, digest) || saved.Value.RegistrationSet.Value.Request != r {
		return saved, ErrProductSplitHeld
	}
	for _, record := range []struct{ kind, id string }{{"product_nested_split_registrations_history", saved.ID}, {"product_nested_split_registrations_admission", r.ProposalID}} {
		var witness ProductNestedSplitRegistrations
		if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &witness); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(saved, witness) {
			return saved, ErrProductSplitHeld
		}
	}
	if len(saved.Value.RegistrationSet.Value.Registrations) < 2 || len(saved.Value.RegistrationSet.Value.Registrations) > maxProductWorkItems {
		return saved, ErrProductSplitHeld
	}
	// Independent registration copies are required before the costly upstream
	// history walk; their agreement does not replace that authority check.
	for _, registration := range saved.Value.RegistrationSet.Value.Registrations {
		id := registration.Value.Definition.ID
		for _, record := range []struct{ kind, id string }{{"product_work_item_registration", id}, {"product_work_item_registration_version", structureID("product-work-", id, registration.Digest)}} {
			var witness ProductWorkItemRegistrationSnapshot
			if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &witness); err != nil {
				return saved, splitEvidenceErr(err)
			}
			if !reflect.DeepEqual(registration, witness) {
				return saved, ErrProductSplitHeld
			}
		}
		var member struct {
			Target      contract.Target                  `json:"target"`
			CandidateID string                           `json:"candidate_id"`
			SplitScope  *ProductSplitChildScopeReference `json:"split_scope,omitempty"`
		}
		if err := taskEvidenceRecord(ctx, tx, "wave_work_item", id, &member); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if member.Target.WaveID != r.Target.WaveID || member.Target.ScopeRevision != r.Target.ScopeRevision || member.Target.PolicyRevision != r.Target.PolicyRevision || !reflect.DeepEqual(member.SplitScope, registration.Value.SplitScope) {
			return saved, ErrProductSplitHeld
		}
	}
	scopes, err := loadNestedSplitChildScopes(ctx, tx, r)
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	if saved.Value.ScopeEnvelopeID != scopes.ID || saved.Value.ScopeEnvelopeDigest != scopes.Digest || !reflect.DeepEqual(saved.Value.ProductNestedSplitChildScopesValue, scopes.Value) {
		return saved, ErrProductSplitHeld
	}
	ancestors, err := deriveSplitAncestorWitnesses(scopes)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(ancestors, saved.Value.AncestorSet) {
		return saved, ErrProductSplitHeld
	}
	registrations, err := deriveSplitRegistrationsWithAncestors(ctx, tx, scopes.Value.Scopes, saved.Value.RegistrationSet.Value.RegisteredAt, &scopes, &ancestors)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(registrations, saved.Value.RegistrationSet) {
		return saved, ErrProductSplitHeld
	}

	return saved, nil
}

func (k *KernelStore) registerNestedSplitChildrenTx(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest, w domain.Wave) (ProductSplitRegistrations, error) {
	var empty ProductSplitRegistrations
	if err := k.splitCreatePreflightTx(ctx, tx, r); err != nil {
		return empty, err
	}
	scopes, err := loadNestedSplitChildScopes(ctx, tx, r)
	if err != nil {
		return empty, splitEvidenceErr(err)
	}
	ancestors, err := deriveSplitAncestorWitnesses(scopes)
	if err != nil {
		return empty, err
	}
	ids, err := productWorkItemLifetimeIDsTx(ctx, tx, w)
	if err != nil {
		return empty, err
	}
	registrations, err := deriveSplitRegistrationsWithAncestors(ctx, tx, scopes.Value.Scopes, k.clock(), &scopes, &ancestors)
	if err != nil {
		return empty, err
	}
	saved := ProductNestedSplitRegistrations{Value: ProductNestedSplitRegistrationsValue{ProductNestedSplitChildScopesValue: scopes.Value, ScopeEnvelopeID: scopes.ID, ScopeEnvelopeDigest: scopes.Digest, AncestorSet: ancestors, RegistrationSet: registrations}}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return empty, err
	}
	saved.ID = structureID("nested-split-registrations-", r.ProposalID, saved.Digest)
	raw, err := json.Marshal(saved)
	if err != nil {
		return empty, err
	}
	if len(raw) > 16<<20 {
		return empty, ErrProductSplitHeld
	}
	for _, registration := range registrations.Value.Registrations {
		id := registration.Value.Definition.ID
		if !ids[id] {
			return empty, ErrProductSplitHeld
		}
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_work_item_registration','wave_work_item')) OR EXISTS(SELECT 1 FROM records WHERE kind='product_work_item_registration_version' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.definition.id')=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_target' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.work_item.id')=?)`, id, id, id).Scan(&known); err != nil {
			return empty, err
		}
		if known {
			return empty, ErrProductSplitHeld
		}
		for _, record := range []struct{ kind, id string }{{"product_work_item_registration", id}, {"product_work_item_registration_version", structureID("product-work-", id, registration.Digest)}} {
			if err := kernelRecordTx(ctx, tx, record.kind, record.id, w.ID, registration); err != nil {
				return empty, err
			}
		}
		member := struct {
			Target      contract.Target                  `json:"target"`
			CandidateID string                           `json:"candidate_id"`
			SplitScope  *ProductSplitChildScopeReference `json:"split_scope,omitempty"`
		}{w.Target(), "", registration.Value.SplitScope}
		if err := kernelRecordTx(ctx, tx, "wave_work_item", id, w.ID, member); err != nil {
			return empty, err
		}
	}
	for _, record := range []struct{ kind, id string }{{"product_nested_split_registrations", r.ProposalID}, {"product_nested_split_registrations_history", saved.ID}, {"product_nested_split_registrations_admission", r.ProposalID}} {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, w.ID, saved); err != nil {
			return empty, err
		}
	}
	return registrations, nil
}
