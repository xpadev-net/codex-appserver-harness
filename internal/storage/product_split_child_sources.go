package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

const maxSplitSourceVersions = 1024

// ErrProductSplitSourceSuperseded means another observation won the source CAS.
// It grants no freshness: a caller may only retry by performing a new GET.
var ErrProductSplitSourceSuperseded = errors.New("split child source observation superseded")

// The compact reference preserves negative creation/budget/source witnesses;
// authoritative reads still require the complete registered local binding.
type splitChildSourceReference struct {
	AncestorSet       *ProductSplitAncestorWitnesses       `json:"ancestor_set,omitempty"`
	Request           ProductSplitChildPlanRequest         `json:"request"`
	ParentHeadID      string                               `json:"parent_head_id"`
	BindingID         string                               `json:"binding_id"`
	BindingDigest     string                               `json:"binding_digest"`
	ScopeDigest       string                               `json:"scope_digest"`
	SourceID          string                               `json:"source_id"`
	Child             splitChildSourceChild                `json:"child"`
	Issue             splitChildSourceIssue                `json:"issue"`
	InitialCreationID string                               `json:"initial_creation_id"`
	CreationID        string                               `json:"creation_id"`
	Creations         []ProductSplitChildCreationReference `json:"creations"`
}
type splitChildSourceChild struct {
	ID    string               `json:"id"`
	Value splitChildSourceWork `json:"value"`
}
type splitChildSourceWork struct {
	WorkItemID string `json:"work_item_id"`
}
type splitChildSourceIssue struct {
	RepositoryID int64 `json:"repository_id"`
	ID           int64 `json:"id"`
	Number       int   `json:"number"`
}

type ProductSplitChildSourceFacts struct {
	RepositoryID  int64     `json:"repository_id"`
	IssueID       int64     `json:"issue_id"`
	IssueNumber   int       `json:"issue_number"`
	PolicyDigest  string    `json:"policy_digest"`
	Available     bool      `json:"available"`
	IssueState    string    `json:"issue_state"`
	Hold          bool      `json:"hold"`
	RequestDigest string    `json:"request_digest"`
	UpdatedAt     time.Time `json:"updated_at"`
	Failure       string    `json:"failure"`
}
type ProductSplitChildSourceValue struct {
	splitChildSourceReference
	Status string                       `json:"status"`
	Facts  ProductSplitChildSourceFacts `json:"facts"`
}
type ProductSplitChildSource struct {
	ID         string                       `json:"id"`
	Version    int                          `json:"version"`
	ObservedAt time.Time                    `json:"observed_at"`
	CheckedAt  time.Time                    `json:"checked_at"`
	Value      ProductSplitChildSourceValue `json:"value"`
	Digest     string                       `json:"digest"`
}
type ProductSplitChildSourceUpdate struct {
	Snapshot ProductSplitChildSource `json:"snapshot"`
	Changed  bool                    `json:"changed"`
}
type splitChildSourcePolicyValue struct {
	splitChildSourceReference
	PolicyDigest string `json:"policy_digest"`
}
type splitChildSourcePolicy struct {
	Value  splitChildSourcePolicyValue `json:"value"`
	Digest string                      `json:"digest"`
}

func splitSourceReference(binding ProductSplitChildScopeBinding) splitChildSourceReference {
	v := binding.Value
	return splitChildSourceReference{Request: v.Request, ParentHeadID: v.ParentHeadID, BindingID: binding.ID, BindingDigest: binding.Digest, ScopeDigest: v.ScopeDigest, SourceID: v.SourceID, Child: splitChildSourceChild{ID: v.Child.ID, Value: splitChildSourceWork{WorkItemID: v.Child.Value.WorkItemID}}, Issue: splitChildSourceIssue{RepositoryID: v.Issue.RepositoryID, ID: v.Issue.ID, Number: v.Issue.Number}, InitialCreationID: v.InitialCreationID, CreationID: v.CreationID, Creations: append([]ProductSplitChildCreationReference{}, v.Creations...)}
}

// Callers authenticate the complete binding first. This inexpensive current
// registration comparison carries its exact negative witnesses into every source
// copy; the compact set never substitutes for canonical binding authority.
func splitSourceReferenceTx(ctx context.Context, tx *sql.Tx, binding ProductSplitChildScopeBinding) (splitChildSourceReference, error) {
	out := splitSourceReference(binding)
	if binding.Value.Child.Value.Depth < 2 {
		return out, nil
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, binding.Value.Child.Value.WorkItemID)
	if err != nil {
		return out, splitEvidenceErr(err)
	}
	ref := registration.Value.SplitScope
	if ref == nil || ref.AncestorSet == nil || ref.BindingID != binding.ID || ref.BindingDigest != binding.Digest || ref.Request != binding.Value.Request || ref.SourceID != binding.Value.SourceID || ref.ParentHeadID != binding.Value.ParentHeadID || !reflect.DeepEqual(ref.Creations, binding.Value.Creations) {
		return out, ErrProductSplitHeld
	}
	out.AncestorSet = ref.AncestorSet
	return out, nil
}

func (facts ProductSplitChildSourceFacts) validate() error {
	if facts.RepositoryID < 1 || facts.IssueID < 1 || facts.IssueNumber < 1 || !validSourceDigest(facts.PolicyDigest) {
		return ErrProductSplitHeld
	}
	if facts.Available {
		if facts.IssueState != "open" && facts.IssueState != "closed" || !validSourceDigest(facts.RequestDigest) || facts.UpdatedAt.IsZero() || facts.Failure != "" {
			return ErrProductSplitHeld
		}
	} else if facts.IssueState != "" || facts.Hold || facts.RequestDigest != "" || !facts.UpdatedAt.IsZero() || facts.Failure != "read_failed" && facts.Failure != "repository_mismatch" && facts.Failure != "invalid_source" {
		return ErrProductSplitHeld
	}
	return nil
}

func splitChildSourceStatus(binding ProductSplitChildScopeBinding, facts ProductSplitChildSourceFacts, pin string) string {
	if pin != "" && pin != facts.PolicyDigest {
		return "policy_changed"
	}
	if !facts.Available {
		return "unavailable"
	}
	v := binding.Value
	if facts.RepositoryID != v.Issue.RepositoryID || facts.IssueID != v.Issue.ID || facts.IssueNumber != v.Issue.Number {
		return "identity_changed"
	}
	if facts.RequestDigest != ProductSourceRequestDigest(v.Issue.Title+"\n\n"+v.Issue.Body) {
		return "request_changed"
	}
	if facts.IssueState != "open" {
		return "closed"
	}
	if facts.Hold {
		return "hold"
	}
	return "ready"
}

func splitChildSourceBindingTx(ctx context.Context, tx *sql.Tx, target contract.Target, workID string) (ProductSplitChildScopeBinding, error) {
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return ProductSplitChildScopeBinding{}, err
	}
	if !target.Valid() || w.ScopeRevision != target.ScopeRevision || w.Policy.Revision != target.PolicyRevision {
		return ProductSplitChildScopeBinding{}, ErrProductSplitHeld
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, workID)
	if err != nil {
		return ProductSplitChildScopeBinding{}, splitEvidenceErr(err)
	}
	binding, err := productSplitChildScopeTx(ctx, tx, w, registration)
	if err != nil {
		return ProductSplitChildScopeBinding{}, err
	}
	if binding == nil {
		return ProductSplitChildScopeBinding{}, ErrProductSplitHeld
	}
	return *binding, nil
}

func loadSplitChildSourcePolicy(ctx context.Context, tx *sql.Tx, binding ProductSplitChildScopeBinding) (splitChildSourcePolicy, error) {
	var saved, history splitChildSourcePolicy
	id := binding.Value.Child.Value.WorkItemID
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_source_policy", id, &saved); err != nil {
		return saved, splitEvidenceErr(err)
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	reference, err := splitSourceReferenceTx(ctx, tx, binding)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || !validSourceDigest(saved.Value.PolicyDigest) || !reflect.DeepEqual(saved.Value.splitChildSourceReference, reference) {
		return saved, ErrProductSplitHeld
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_source_policy_history", id, &history); err != nil {
		return saved, splitEvidenceErr(err)
	}
	if !reflect.DeepEqual(saved, history) {
		return saved, ErrProductSplitHeld
	}
	return saved, nil
}

func loadSplitChildSource(ctx context.Context, tx *sql.Tx, binding ProductSplitChildScopeBinding) (ProductSplitChildSource, error) {
	var saved, history ProductSplitChildSource
	id := binding.Value.Child.Value.WorkItemID
	err := taskEvidenceRecord(ctx, tx, "product_split_child_source", id, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, splitScopeWitnessSQL(`SELECT EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind IN ('product_split_child_source_policy','product_split_child_source_policy_history') AND id=?) OR EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind='product_split_child_source_history' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.child.value.work_item_id')=?) OR EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind IN ('product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission','product_split_parent_scope_tool','product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request','product_nested_split_children','product_nested_split_children_history','product_nested_split_children_admission','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission','product_nested_split_allocation','product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat','product_nested_split_allocation_ledger','product_nested_split_creation_ancestor','product_nested_split_retry_ancestor','product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source','product_nested_split_scope_group','product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission','product_split_ancestor_group') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.parent_work_item_id')=?)`), id, id, id).Scan(&known); err != nil {
			return saved, err
		}
		if known {
			return saved, ErrProductSplitHeld
		}
		return saved, sql.ErrNoRows
	}
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	policy, err := loadSplitChildSourcePolicy(ctx, tx, binding)
	if err != nil {
		return saved, err
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if digest != saved.Digest || saved.Version < 1 || saved.Version > maxSplitSourceVersions || saved.ID != structureID("split-child-source-", id, strconv.Itoa(saved.Version)+":"+digest) || saved.ObservedAt.IsZero() || saved.CheckedAt.Before(saved.ObservedAt) || !reflect.DeepEqual(saved.Value.splitChildSourceReference, policy.Value.splitChildSourceReference) || saved.Value.Facts.validate() != nil || saved.Value.Status != splitChildSourceStatus(binding, saved.Value.Facts, policy.Value.PolicyDigest) {
		return saved, ErrProductSplitHeld
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_source_history", saved.ID, &history); err != nil {
		return saved, splitEvidenceErr(err)
	}
	// CheckedAt is a replaceable freshness read, not a new semantic history.
	copy := saved
	copy.CheckedAt = history.CheckedAt
	if !history.CheckedAt.Equal(history.ObservedAt) || !reflect.DeepEqual(copy, history) {
		return saved, ErrProductSplitHeld
	}
	return saved, nil
}

func (k *KernelStore) ReadProductSplitChildSource(ctx context.Context, target contract.Target, workID string) (ProductSplitChildSource, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitChildSource{}, err
	}
	defer tx.Rollback()
	binding, err := splitChildSourceBindingTx(ctx, tx, target, workID)
	if err != nil {
		return ProductSplitChildSource{}, err
	}
	return loadSplitChildSource(ctx, tx, binding)
}

func (k *KernelStore) ReadProductSplitChildSourcePolicy(ctx context.Context, target contract.Target, workID string) (string, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	binding, err := splitChildSourceBindingTx(ctx, tx, target, workID)
	if err != nil {
		return "", err
	}
	pin, err := loadSplitChildSourcePolicy(ctx, tx, binding)
	if err != nil {
		return "", err
	}
	return pin.Value.PolicyDigest, nil
}

// ObserveProductSplitChildSource accepts host-verified facts only. The CAS is
// the prior snapshot ID (including its version), not its semantic digest.
// It changes no Wave hold, budgets, Tasks or execution authority.
func (k *KernelStore) ObserveProductSplitChildSource(ctx context.Context, target contract.Target, workID, expectedSnapshotID string, facts ProductSplitChildSourceFacts) (ProductSplitChildSourceUpdate, error) {
	return k.ObserveProductSplitChildSourceAt(ctx, target, workID, expectedSnapshotID, facts, k.clock())
}

// ObserveProductSplitChildSourceAt preserves the trusted host's GET completion
// time across potentially expensive canonical proof validation. Saving a receipt
// cannot renew the age of an earlier external observation.
func (k *KernelStore) ObserveProductSplitChildSourceAt(ctx context.Context, target contract.Target, workID, expectedSnapshotID string, facts ProductSplitChildSourceFacts, readAt time.Time) (ProductSplitChildSourceUpdate, error) {
	readAt = readAt.UTC().Round(0)
	if readAt.IsZero() || readAt.After(k.clock()) {
		return ProductSplitChildSourceUpdate{}, ErrProductSplitHeld
	}
	if err := facts.validate(); err != nil {
		return ProductSplitChildSourceUpdate{}, err
	}
	facts.UpdatedAt = facts.UpdatedAt.UTC().Round(0)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitChildSourceUpdate{}, err
	}
	defer tx.Rollback()
	binding, err := splitChildSourceBindingTx(ctx, tx, target, workID)
	if err != nil {
		return ProductSplitChildSourceUpdate{}, err
	}
	prior, priorErr := loadSplitChildSource(ctx, tx, binding)
	if priorErr != nil && priorErr != sql.ErrNoRows {
		return ProductSplitChildSourceUpdate{}, priorErr
	}
	if priorErr == nil && expectedSnapshotID != prior.ID || priorErr != nil && expectedSnapshotID != "" {
		return ProductSplitChildSourceUpdate{}, errors.Join(ErrProductSplitHeld, ErrProductSplitSourceSuperseded)
	}
	now := readAt
	if now.Before(binding.Value.CreatedAt) {
		return ProductSplitChildSourceUpdate{}, ErrProductSplitHeld
	}
	if priorErr == nil && now.Before(prior.CheckedAt) {
		return ProductSplitChildSourceUpdate{}, errors.Join(ErrProductSplitHeld, ErrProductSplitSourceSuperseded)
	}
	reference, err := splitSourceReferenceTx(ctx, tx, binding)
	if err != nil {
		return ProductSplitChildSourceUpdate{}, err
	}
	pin := facts.PolicyDigest
	if priorErr == nil {
		policy, err := loadSplitChildSourcePolicy(ctx, tx, binding)
		if err != nil {
			return ProductSplitChildSourceUpdate{}, err
		}
		pin = policy.Value.PolicyDigest
	} else {
		value := splitChildSourcePolicyValue{splitChildSourceReference: reference, PolicyDigest: pin}
		digest, err := contentDigest(value)
		if err != nil {
			return ProductSplitChildSourceUpdate{}, err
		}
		for _, kind := range []string{"product_split_child_source_policy", "product_split_child_source_policy_history"} {
			if err := kernelRecordTx(ctx, tx, kind, workID, target.WaveID, splitChildSourcePolicy{Value: value, Digest: digest}); err != nil {
				return ProductSplitChildSourceUpdate{}, err
			}
		}
	}
	value := ProductSplitChildSourceValue{splitChildSourceReference: reference, Facts: facts, Status: splitChildSourceStatus(binding, facts, pin)}
	digest, err := contentDigest(value)
	if err != nil {
		return ProductSplitChildSourceUpdate{}, err
	}
	saved := prior
	changed := priorErr != nil || digest != prior.Digest
	if changed {
		if prior.Version >= maxSplitSourceVersions {
			return ProductSplitChildSourceUpdate{}, ErrProductSplitHeld
		}
		saved = ProductSplitChildSource{Version: prior.Version + 1, ObservedAt: now, CheckedAt: now, Value: value, Digest: digest}
		saved.ID = structureID("split-child-source-", workID, strconv.Itoa(saved.Version)+":"+digest)
		if err := kernelRecordTx(ctx, tx, "product_split_child_source_history", saved.ID, target.WaveID, saved); err != nil {
			return ProductSplitChildSourceUpdate{}, err
		}
	} else {
		saved.CheckedAt = now
	}
	if err := kernelRecordTx(ctx, tx, "product_split_child_source", workID, target.WaveID, saved); err != nil {
		return ProductSplitChildSourceUpdate{}, err
	}
	if err := tx.Commit(); err != nil {
		return ProductSplitChildSourceUpdate{}, err
	}
	return ProductSplitChildSourceUpdate{Snapshot: saved, Changed: changed}, nil
}

// This is a source-only check. It never grants execution, quality or a send.
func (k *KernelStore) RequireProductSplitChildSource(ctx context.Context, target contract.Target, workID, policyDigest string, maxAge time.Duration) (ProductSplitChildSource, error) {
	if !validSourceDigest(policyDigest) || maxAge <= 0 || maxAge > 5*time.Minute {
		return ProductSplitChildSource{}, ErrProductSplitHeld
	}
	saved, err := k.ReadProductSplitChildSource(ctx, target, workID)
	if err != nil {
		return saved, err
	}
	now := k.clock()
	if saved.Value.Status != "ready" || saved.Value.Facts.PolicyDigest != policyDigest || now.Before(saved.CheckedAt) || now.Sub(saved.CheckedAt) > maxAge {
		return ProductSplitChildSource{}, ErrProductSplitHeld
	}
	return saved, nil
}
