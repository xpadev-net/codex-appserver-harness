package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// This immutable reference retains the child's own scope and numeric source.
// Definition.Criteria remains the parent criterion mapping for Wave planning;
// it is not evidence that one part satisfies an entire parent criterion.
type ProductSplitChildScopeReference struct {
	Request       ProductSplitChildPlanRequest         `json:"request"`
	SetID         string                               `json:"set_id"`
	SetDigest     string                               `json:"set_digest"`
	BindingID     string                               `json:"binding_id"`
	BindingDigest string                               `json:"binding_digest"`
	SourceID      string                               `json:"source_id"`
	ParentHeadID  string                               `json:"parent_head_id"`
	Creations     []ProductSplitChildCreationReference `json:"creations"`
}

// Embedding preserves the same full child/creation witnesses as scope bindings.
// An independent registration receipt cannot hide missing upstream metadata.
type ProductSplitRegistrationsValue struct {
	ProductSplitChildScopesValue
	ScopeSetID     string                                `json:"scope_set_id"`
	ScopeSetDigest string                                `json:"scope_set_digest"`
	Registrations  []ProductWorkItemRegistrationSnapshot `json:"registrations"`
	RegisteredAt   time.Time                             `json:"registered_at"`
}
type ProductSplitRegistrations struct {
	ID     string                         `json:"id"`
	Value  ProductSplitRegistrationsValue `json:"value"`
	Digest string                         `json:"digest"`
}

func deriveProductSplitRegistrations(ctx context.Context, tx *sql.Tx, scopes ProductSplitChildScopes, at time.Time) (ProductSplitRegistrations, error) {
	var out ProductSplitRegistrations
	r := scopes.Value.Request
	var parent ProductWorkItemRegistrationSnapshot
	if len(scopes.Value.Children) < 2 || at.IsZero() || at.Before(scopes.Value.CreatedAt) {
		return out, ErrProductSplitHeld
	}
	parentDigest := scopes.Value.Children[0].Value.ParentRegistrationDigest
	if err := taskEvidenceRecord(ctx, tx, "product_work_item_registration_version", structureID("product-work-", r.ParentWorkItemID, parentDigest), &parent); err != nil {
		return out, splitEvidenceErr(err)
	}
	digest, err := contentDigest(parent.Value)
	if err != nil || digest != parentDigest || parent.Digest != digest || parent.Value.Definition.ID != r.ParentWorkItemID || parent.Value.WaveID != r.Target.WaveID || parent.Value.ScopeRevision != r.Target.ScopeRevision || parent.Value.PolicyRevision != r.Target.PolicyRevision || parent.Value.SplitScope != nil {
		return out, ErrProductSplitHeld
	}
	out.Value = ProductSplitRegistrationsValue{ProductSplitChildScopesValue: scopes.Value, ScopeSetID: scopes.ID, ScopeSetDigest: scopes.Digest, Registrations: []ProductWorkItemRegistrationSnapshot{}, RegisteredAt: at}
	for _, binding := range scopes.Value.Children {
		v := binding.Value
		child := v.Child.Value
		if v.ParentRegistrationDigest != parentDigest || child.ParentWorkItemID != r.ParentWorkItemID || child.Repository != parent.Value.Definition.Repository || child.RepositoryID != parent.Value.Definition.RepositoryID {
			return ProductSplitRegistrations{}, ErrProductSplitHeld
		}
		registration := ProductWorkItemRegistrationSnapshot{Value: ProductWorkItemRegistration{
			WaveID: r.Target.WaveID, ScopeRevision: r.Target.ScopeRevision, PolicyRevision: r.Target.PolicyRevision, Source: parent.Value.Source, SourceID: parent.Value.SourceID,
			Definition: ProductWorkItemDefinition{ID: child.WorkItemID, Repository: child.Repository, RepositoryID: child.RepositoryID, Criteria: append([]string{}, child.ParentCriteria...)},
			SplitScope: &ProductSplitChildScopeReference{Request: r, SetID: scopes.ID, SetDigest: scopes.Digest, BindingID: binding.ID, BindingDigest: binding.Digest, SourceID: v.SourceID, ParentHeadID: v.ParentHeadID, Creations: append([]ProductSplitChildCreationReference{}, v.Creations...)},
		}}
		registration.Digest, err = contentDigest(registration.Value)
		if err != nil {
			return ProductSplitRegistrations{}, err
		}
		out.Value.Registrations = append(out.Value.Registrations, registration)
	}
	out.Digest, err = contentDigest(out.Value)
	if err != nil {
		return ProductSplitRegistrations{}, err
	}
	out.ID = structureID("split-registrations-", r.ProposalID, out.Digest)
	return out, nil
}

func loadProductSplitRegistrations(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductSplitRegistrations, error) {
	malformed, err := splitScopeUnreadableForWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return ProductSplitRegistrations{}, err
	}
	if malformed {
		return ProductSplitRegistrations{}, ErrProductSplitHeld
	}
	var saved ProductSplitRegistrations
	err = taskEvidenceRecord(ctx, tx, "product_split_registrations", r.ProposalID, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_registrations_admission' AND id=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_registrations_history' AND (work_item_id=? OR json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.request.proposal_id')=?))
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.request.proposal_id'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.request.proposal_id'))=?)`, r.ProposalID, r.ProposalID, r.ProposalID, r.ProposalID).Scan(&known); err != nil {
			return saved, err
		}
		if known {
			return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
		}
		return saved, sql.ErrNoRows
	}
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	if saved.Value.Request != r {
		return saved, ErrProductSplitHeld
	}
	scopes, err := loadProductSplitChildScopes(ctx, tx, r)
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	expected, err := deriveProductSplitRegistrations(ctx, tx, scopes, saved.Value.RegisteredAt)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(saved, expected) {
		return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	for _, record := range []struct{ kind, id string }{{"product_split_registrations_history", saved.ID}, {"product_split_registrations_admission", r.ProposalID}} {
		var witness ProductSplitRegistrations
		if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &witness); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(saved, witness) {
			return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
		}
	}
	for _, registration := range saved.Value.Registrations {
		id := registration.Value.Definition.ID
		for _, record := range []struct{ kind, id string }{{"product_work_item_registration", id}, {"product_work_item_registration_version", structureID("product-work-", id, registration.Digest)}} {
			var witness ProductWorkItemRegistrationSnapshot
			if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &witness); err != nil {
				return saved, splitEvidenceErr(err)
			}
			if !reflect.DeepEqual(registration, witness) {
				return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
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
	return saved, nil
}

// RegisterProductSplitChildren adopts every reserved child in one transaction.
// It creates no Tasks, recharges no seats and does not retire the parent. Child
// execution remains held until its own fresh source and local quality adapters
// are connected; the parent mapping must never stand in for local part quality.
func (k *KernelStore) RegisterProductSplitChildren(ctx context.Context, r ProductSplitChildPlanRequest) (ProductSplitRegistrations, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitRegistrations{}, err
	}
	defer tx.Rollback()
	saved, err := loadProductSplitRegistrations(ctx, tx, r)
	if err == nil {
		return saved, nil
	}
	if err != sql.ErrNoRows {
		return ProductSplitRegistrations{}, err
	}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return ProductSplitRegistrations{}, err
	}
	if w.Target() != r.Target || w.Policy.Stage != "P" || !w.Policy.Product || w.DispatchPaused() || w.State == domain.WaveBlocked || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) {
		return ProductSplitRegistrations{}, ErrProductSplitHeld
	}
	scopes, err := loadProductSplitChildScopes(ctx, tx, r)
	if err != nil {
		return ProductSplitRegistrations{}, splitEvidenceErr(err)
	}
	proposal, err := loadProductSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID})
	if err != nil {
		return ProductSplitRegistrations{}, splitEvidenceErr(err)
	}
	if _, err := k.productSplitContextTx(ctx, tx, proposal.Value.Request); err != nil {
		return ProductSplitRegistrations{}, err
	}
	ids, err := productWorkItemLifetimeIDsTx(ctx, tx, w)
	if err != nil {
		return ProductSplitRegistrations{}, err
	}
	saved, err = deriveProductSplitRegistrations(ctx, tx, scopes, k.clock())
	if err != nil {
		return ProductSplitRegistrations{}, err
	}
	raw, err := json.Marshal(saved)
	if err != nil {
		return ProductSplitRegistrations{}, err
	}
	if len(raw) > 16<<20 {
		return ProductSplitRegistrations{}, ErrProductSplitHeld
	}
	for _, registration := range saved.Value.Registrations {
		id := registration.Value.Definition.ID
		if !ids[id] {
			return ProductSplitRegistrations{}, ErrProductSplitHeld
		}
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_work_item_registration','wave_work_item'))
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_work_item_registration_version' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.definition.id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_target' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.work_item.id')=?)`, id, id, id).Scan(&known); err != nil {
			return ProductSplitRegistrations{}, err
		}
		if known {
			return ProductSplitRegistrations{}, ErrProductSplitHeld
		}
		for _, record := range []struct{ kind, id string }{{"product_work_item_registration", id}, {"product_work_item_registration_version", structureID("product-work-", id, registration.Digest)}} {
			if err := kernelRecordTx(ctx, tx, record.kind, record.id, w.ID, registration); err != nil {
				return ProductSplitRegistrations{}, err
			}
		}
		member := struct {
			Target      contract.Target                  `json:"target"`
			CandidateID string                           `json:"candidate_id"`
			SplitScope  *ProductSplitChildScopeReference `json:"split_scope,omitempty"`
		}{w.Target(), "", registration.Value.SplitScope}
		if err := kernelRecordTx(ctx, tx, "wave_work_item", id, w.ID, member); err != nil {
			return ProductSplitRegistrations{}, err
		}
	}
	for _, record := range []struct{ kind, id, owner string }{{"product_split_registrations", r.ProposalID, w.ID}, {"product_split_registrations_history", saved.ID, w.ID}, {"product_split_registrations_admission", r.ProposalID, w.ID}} {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, record.owner, saved); err != nil {
			return ProductSplitRegistrations{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ProductSplitRegistrations{}, err
	}
	return saved, nil
}

func (k *KernelStore) ReadProductSplitRegistrations(ctx context.Context, r ProductSplitChildPlanRequest) (ProductSplitRegistrations, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitRegistrations{}, err
	}
	defer tx.Rollback()
	return loadProductSplitRegistrations(ctx, tx, r)
}

func productSplitChildExecutionHeld(ctx context.Context, q queryer, workID string) (bool, error) {
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.definition.id'),CASE WHEN kind='wave_work_item' THEN id END)=? AND coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope')) IS NOT NULL)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.child.value.work_item_id')=?)
 OR EXISTS(SELECT 1 FROM records r,json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.children') c WHERE r.kind IN ('product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission') AND json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.value.child.value.work_item_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scope_source' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.work_item_id')=?)`, workID, workID, workID, workID).Scan(&known)
	return known, err
}
