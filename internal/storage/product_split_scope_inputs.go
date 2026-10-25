package storage

import (
	"context"
	"database/sql"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

// ProductSplitScopeInput is public requirement data, never a source freshness,
// execution, quality or parent-completion grant. The original Wave requirement
// remains in the manifest alongside this child's complete local obligation.
type ProductSplitScopeInput struct {
	BindingID            string               `json:"binding_id"`
	BindingDigest        string               `json:"binding_digest"`
	OriginalTarget       contract.Target      `json:"original_target"`
	WorkItemID           string               `json:"work_item_id"`
	ParentWorkItemID     string               `json:"parent_work_item_id"`
	SourceID             string               `json:"source_id"`
	Depth                int                  `json:"depth"`
	Scope                verification.Scope   `json:"scope"`
	ScopeDigest          string               `json:"scope_digest"`
	Coverage             []splitting.Coverage `json:"coverage"`
	DependsOnWorkItemIDs []string             `json:"depends_on_work_item_ids"`
}

func productSplitScopeInput(binding ProductSplitChildScopeBinding) ProductSplitScopeInput {
	v := binding.Value
	return ProductSplitScopeInput{BindingID: binding.ID, BindingDigest: binding.Digest, OriginalTarget: v.Request.Target, WorkItemID: v.Child.Value.WorkItemID, ParentWorkItemID: v.Request.ParentWorkItemID, SourceID: v.SourceID, Depth: v.Child.Value.Depth, Scope: v.Scope, ScopeDigest: v.ScopeDigest, Coverage: append([]splitting.Coverage{}, v.Child.Value.Child.Coverage...), DependsOnWorkItemIDs: append([]string{}, v.Child.Value.DependsOnWorkItemIDs...)}
}

func productSplitChildScopeTx(ctx context.Context, tx *sql.Tx, w domain.Wave, registration ProductWorkItemRegistrationSnapshot) (*ProductSplitChildScopeBinding, error) {
	id := registration.Value.Definition.ID
	ref := registration.Value.SplitScope
	if ref == nil {
		known, err := productSplitChildExecutionHeld(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if known {
			return nil, ErrProductSplitHeld
		}
		return nil, nil
	}
	if w.Policy.Stage != "P" || !w.Policy.Product || !productRegistrationScope(registration, w) {
		return nil, ErrProductSplitHeld
	}
	saved, err := loadProductSplitRegistrations(ctx, tx, ref.Request)
	if err != nil {
		return nil, splitEvidenceErr(err)
	}
	for i, child := range saved.Value.Registrations {
		if child.Value.Definition.ID != id {
			continue
		}
		if !reflect.DeepEqual(registration, child) || i >= len(saved.Value.Children) || ref.SetID != saved.Value.ScopeSetID || ref.SetDigest != saved.Value.ScopeSetDigest {
			return nil, ErrProductSplitHeld
		}
		binding := saved.Value.Children[i]
		if ref.BindingID != binding.ID || ref.BindingDigest != binding.Digest || binding.Value.Child.Value.WorkItemID != id || binding.Value.Request.Target.ScopeRevision != w.ScopeRevision || binding.Value.Request.Target.PolicyRevision != w.Policy.Revision {
			return nil, ErrProductSplitHeld
		}
		return &binding, nil
	}
	return nil, ErrProductSplitHeld
}

// ReadProductSplitChildScope authenticates the registered local scope. It is a
// metadata read, including while paused, and performs no external observation.
func (k *KernelStore) ReadProductSplitChildScope(ctx context.Context, target contract.Target, workID string) (ProductSplitChildScopeBinding, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitChildScopeBinding{}, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return ProductSplitChildScopeBinding{}, err
	}
	if w.Target() != target {
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

func productVerificationScopeTx(ctx context.Context, tx *sql.Tx, w domain.Wave, registration ProductWorkItemRegistrationSnapshot) (verification.Scope, error) {
	binding, err := productSplitChildScopeTx(ctx, tx, w, registration)
	if err != nil {
		return verification.Scope{}, err
	}
	if binding != nil {
		return binding.Value.Scope, nil
	}
	return productRootVerificationScope(w, registration), nil
}
