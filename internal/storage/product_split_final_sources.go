package storage

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Final source pins are semantic identities. Repeating the same external GET
// can refresh CheckedAt without changing an admitted final verification plan.
type ProductSplitFinalSourceReference struct {
	WorkItemID         string `json:"work_item_id"`
	RegistrationDigest string `json:"registration_digest"`
	BindingID          string `json:"binding_id"`
	BindingDigest      string `json:"binding_digest"`
	ScopeDigest        string `json:"scope_digest"`
	SnapshotID         string `json:"snapshot_id"`
	SourceDigest       string `json:"source_digest"`
	Version            int    `json:"version"`
	PolicyDigest       string `json:"policy_digest"`
}
type ProductSplitFinalSourcesValue struct {
	Target  contract.Target                    `json:"target"`
	Sources []ProductSplitFinalSourceReference `json:"sources"`
}
type ProductSplitFinalSources struct {
	Value  ProductSplitFinalSourcesValue `json:"value"`
	Digest string                        `json:"digest"`
}

// This read authenticates the whole registered tree and checks actual GET ages.
// It supplies no merged coverage, Task permission, acceptance or Issue mutation.
func (k *KernelStore) ReadProductSplitFinalSources(ctx context.Context, target contract.Target) (*ProductSplitFinalSources, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return nil, err
	}
	if w.Target() != target {
		return nil, ErrProductFinalPlanHeld
	}
	// One synchronous read-only walk; runtime freshness results are never cached.
	read := context.WithValue(ctx, splitProofReadContextKey{}, &splitProofReadMemo{tx: tx, active: map[splitProofReadKey]bool{}, saved: map[splitProofReadKey][]byte{}})
	coverage, err := k.productCoverageTx(read, tx, w)
	if err != nil {
		return nil, err
	}
	return k.productSplitFinalSourcesTx(read, tx, w, coverage)
}

func (k *KernelStore) productSplitFinalSourcesTx(ctx context.Context, tx *sql.Tx, w domain.Wave, coverage ProductCoverageLedger) (*ProductSplitFinalSources, error) {
	memo, _ := ctx.Value(splitProofReadContextKey{}).(*splitProofReadMemo)
	if memo == nil || memo.tx != tx {
		ctx = context.WithValue(ctx, splitProofReadContextKey{}, &splitProofReadMemo{tx: tx, active: map[splitProofReadKey]bool{}, saved: map[splitProofReadKey][]byte{}})
	}
	if coverage.Value.Target != w.Target() {
		return nil, ErrProductFinalPlanHeld
	}
	for _, pending := range coverage.Value.Unavailable {
		if strings.HasPrefix(pending.Reason, "split_") {
			return nil, ErrProductFinalPlanHeld
		}
	}
	if len(coverage.Value.Splits) == 0 {
		return nil, nil
	}
	facts := map[string]ProductCoverageFact{}
	for _, fact := range coverage.Value.Merged {
		facts[fact.WorkItemID] = fact
	}
	delegated := map[string]bool{}
	children := map[string]ProductSplitCoverageChild{}
	for _, group := range coverage.Value.Splits {
		delegated[group.ParentWorkItemID] = true
		for _, child := range group.Children {
			if _, duplicate := children[child.WorkItemID]; duplicate {
				return nil, ErrProductFinalPlanHeld
			}
			children[child.WorkItemID] = child
		}
	}
	if len(children) == 0 || len(children) > maxProductWorkItems {
		return nil, ErrProductFinalPlanHeld
	}
	// An unavailable group must not disappear from a seemingly complete walk.
	for _, pending := range coverage.Value.Unavailable {
		if _, known := children[pending.WorkItemID]; known || delegated[pending.WorkItemID] {
			return nil, ErrProductFinalPlanHeld
		}
	}
	ids := make([]string, 0, len(children))
	for id := range children {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	proof := &ProductSplitFinalSources{Value: ProductSplitFinalSourcesValue{Target: w.Target(), Sources: []ProductSplitFinalSourceReference{}}}
	snapshots := make([]ProductSplitChildSource, 0, len(ids))
	for _, id := range ids {
		registration, err := loadProductWorkItemRegistration(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		binding, err := productSplitChildScopeTx(ctx, tx, w, registration)
		if err != nil {
			return nil, err
		}
		expected := children[id]
		if binding == nil || registration.Digest != expected.RegistrationDigest || binding.ID != expected.BindingID || binding.Digest != expected.BindingDigest || binding.Value.ScopeDigest != expected.ScopeDigest {
			return nil, ErrProductFinalPlanHeld
		}
		source, err := loadSplitChildSource(ctx, tx, *binding)
		if err != nil {
			return nil, err
		}
		if !splitFinalSourceStatusAllowed(source, expected, facts[id], delegated[id]) {
			return nil, ErrProductFinalPlanHeld
		}
		snapshots = append(snapshots, source)
		proof.Value.Sources = append(proof.Value.Sources, ProductSplitFinalSourceReference{WorkItemID: id, RegistrationDigest: registration.Digest, BindingID: binding.ID, BindingDigest: binding.Digest, ScopeDigest: binding.Value.ScopeDigest, SnapshotID: source.ID, SourceDigest: source.Digest, Version: source.Version, PolicyDigest: source.Value.Facts.PolicyDigest})
	}
	now := k.clock()
	for _, source := range snapshots {
		if now.Before(source.CheckedAt) || now.Sub(source.CheckedAt) > 5*time.Second {
			return nil, ErrProductFinalPlanHeld
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	digest, err := contentDigest(proof.Value)
	if err != nil {
		return nil, err
	}
	proof.Digest = digest
	return proof, nil
}

func splitFinalSourceStatusAllowed(source ProductSplitChildSource, child ProductSplitCoverageChild, fact ProductCoverageFact, delegated bool) bool {
	// Status classifies closed before hold. A final-purpose exception must still
	// reject an explicit hold rather than treating every closed Issue as harmless.
	if source.Value.Facts.Hold || !source.Value.Facts.Available {
		return false
	}
	if source.Value.Status == "ready" {
		return source.Value.Facts.IssueState == "open"
	}
	// Delegated parents require a managed close binding in addition to all leaf
	// merges. Until that operation is wired, their closed Issues remain held.
	return !delegated && source.Value.Status == "closed" && source.Value.Facts.IssueState == "closed" && fact.WorkItemID == child.WorkItemID && fact.RegistrationDigest == child.RegistrationDigest && fact.SplitScopeDigest == child.ScopeDigest && sameStringSet(fact.LocalCriteria, child.LocalCriteria) && len(fact.LocalCriteria) > 0 && fixedSHA(fact.MergeSHA) && fact.MergeID != "" && validSourceDigest(fact.ReceiptDigest)
}

func (k *KernelStore) requireProductSplitFinalSourcesTx(ctx context.Context, tx *sql.Tx, w domain.Wave, coverage ProductCoverageLedger, expected string) error {
	proof, err := k.productSplitFinalSourcesTx(ctx, tx, w, coverage)
	if err != nil {
		return err
	}
	actual := ""
	if proof != nil {
		actual = proof.Digest
	}
	if actual != expected {
		return ErrProductFinalPlanHeld
	}
	return nil
}
