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

// The complete original group retains every sibling source, creation chain and
// seat witness. A parent reference alone would forget those independent slots.
type ProductSplitParentScopeValue struct {
	ProductSplitRegistrationsValue
	ParentWorkItemID   string    `json:"parent_work_item_id"`
	BindingID          string    `json:"binding_id"`
	BindingDigest      string    `json:"binding_digest"`
	GroupID            string    `json:"group_id"`
	GroupDigest        string    `json:"group_digest"`
	SourcePolicyDigest string    `json:"source_policy_digest"`
	SavedAt            time.Time `json:"saved_at"`
}
type ProductSplitParentScope struct {
	ID     string                       `json:"id"`
	Value  ProductSplitParentScopeValue `json:"value"`
	Digest string                       `json:"digest"`
}

func deriveSplitParentScope(ctx context.Context, tx *sql.Tx, binding ProductSplitChildScopeBinding, at time.Time) (ProductSplitParentScope, error) {
	var out ProductSplitParentScope
	group, err := loadProductSplitRegistrations(ctx, tx, binding.Value.Request)
	if err != nil {
		return out, splitEvidenceErr(err)
	}
	policy, err := loadSplitChildSourcePolicy(ctx, tx, binding)
	if err != nil {
		return out, err
	}
	if at.IsZero() || at.Before(group.Value.RegisteredAt) {
		return out, ErrProductSplitHeld
	}
	out.Value = ProductSplitParentScopeValue{ProductSplitRegistrationsValue: group.Value, ParentWorkItemID: binding.Value.Child.Value.WorkItemID, BindingID: binding.ID, BindingDigest: binding.Digest, GroupID: group.ID, GroupDigest: group.Digest, SourcePolicyDigest: policy.Value.PolicyDigest, SavedAt: at}
	out.Digest, err = contentDigest(out.Value)
	if err != nil {
		return out, err
	}
	out.ID = structureID("split-parent-scope-", out.Value.ParentWorkItemID, out.Digest)
	raw, err := json.Marshal(out)
	if err != nil {
		return ProductSplitParentScope{}, err
	}
	if len(raw) > 16<<20 {
		return ProductSplitParentScope{}, ErrProductSplitHeld
	}
	return out, nil
}

func loadSplitParentScope(ctx context.Context, tx *sql.Tx, binding ProductSplitChildScopeBinding) (ProductSplitParentScope, error) {
	var saved ProductSplitParentScope
	id := binding.Value.Child.Value.WorkItemID
	err := taskEvidenceRecord(ctx, tx, "product_split_parent_scope", id, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_parent_scope_admission' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_parent_scope_history' AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.parent_work_item_id')=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request','product_nested_split_children','product_nested_split_children_history','product_nested_split_children_admission','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.parent_work_item_id')=?)`, id, id, id).Scan(&known); err != nil {
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
	expected, err := deriveSplitParentScope(ctx, tx, binding, saved.Value.SavedAt)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(saved, expected) {
		return saved, ErrProductSplitHeld
	}
	for _, record := range []struct{ kind, id string }{{"product_split_parent_scope_history", saved.ID}, {"product_split_parent_scope_admission", id}} {
		var witness ProductSplitParentScope
		if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &witness); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(saved, witness) {
			return saved, ErrProductSplitHeld
		}
	}
	return saved, nil
}

// Read authenticates current scope/policy/source ownership, including while
// paused. It is metadata only, not fresh source or nested admission authority.
func (k *KernelStore) ReadProductSplitParentScope(ctx context.Context, target contract.Target, parentID string) (ProductSplitParentScope, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitParentScope{}, err
	}
	defer tx.Rollback()
	binding, err := splitChildSourceBindingTx(ctx, tx, target, parentID)
	if err != nil {
		return ProductSplitParentScope{}, err
	}
	saved, err := loadSplitParentScope(ctx, tx, binding)
	if err != nil {
		return ProductSplitParentScope{}, err
	}
	if saved.Value.SavedAt.After(k.clock()) {
		return ProductSplitParentScope{}, ErrProductSplitHeld
	}
	return saved, nil
}

// Prepare atomically pins three complete witnesses. It creates no proposal,
// child, Task, source adoption or new seat. Exact replay never repairs loss.
func (k *KernelStore) PrepareProductSplitParentScope(ctx context.Context, target contract.Target, parentID string) (ProductSplitParentScope, error) {
	var empty ProductSplitParentScope
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	binding, err := splitChildSourceBindingTx(ctx, tx, target, parentID)
	if err != nil {
		return empty, err
	}
	prior, err := loadSplitParentScope(ctx, tx, binding)
	if err == nil {
		if prior.Value.SavedAt.After(k.clock()) {
			return empty, ErrProductSplitHeld
		}
		return prior, nil
	}
	if err != sql.ErrNoRows {
		return empty, err
	}
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return empty, err
	}
	now := k.clock()
	if w.DispatchPaused() || w.State == domain.WaveBlocked || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || !now.Before(w.Deadline) {
		return empty, ErrProductSplitHeld
	}
	source, err := loadSplitChildSource(ctx, tx, binding)
	if err != nil {
		return empty, splitEvidenceErr(err)
	}
	if source.Value.Status != "ready" || now.Before(source.CheckedAt) || now.Sub(source.CheckedAt) > time.Minute {
		return empty, ErrProductSplitHeld
	}
	saved, err := deriveSplitParentScope(ctx, tx, binding, now)
	if err != nil {
		return empty, err
	}
	for _, record := range []struct{ kind, id string }{{"product_split_parent_scope", parentID}, {"product_split_parent_scope_history", saved.ID}, {"product_split_parent_scope_admission", parentID}} {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, target.WaveID, saved); err != nil {
			return empty, err
		}
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	return saved, nil
}
