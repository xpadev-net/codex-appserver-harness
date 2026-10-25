package storage

import (
	"context"
	"database/sql"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// ReadProductSplitSourceLineage selects canonical own/ancestor Issue sources
// for trusted observation. It grants neither freshness nor execution authority.
func (k *KernelStore) ReadProductSplitSourceLineage(ctx context.Context, target contract.Target, workID string) ([]string, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	binding, err := splitChildSourceBindingTx(ctx, tx, target, workID)
	if err != nil {
		return nil, err
	}
	lineage, err := splitSourceLineageTx(ctx, tx, binding)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(lineage))
	for _, ancestor := range lineage {
		ids = append(ids, ancestor.Value.Child.Value.WorkItemID)
	}
	return ids, nil
}

// The caller authenticates the starting binding. Each preceding binding comes
// from its canonical registered group, never from a source's embedded witness.
// This read-only proof walk does not cache live source observations.
func splitSourceLineageTx(ctx context.Context, tx *sql.Tx, binding ProductSplitChildScopeBinding) ([]ProductSplitChildScopeBinding, error) {
	return readSplitProof(ctx, tx, "splitSourceLineageTx", binding, func(read context.Context) ([]ProductSplitChildScopeBinding, error) {
		depth := binding.Value.Child.Value.Depth
		if depth < 1 || depth > maxProductWorkItems {
			return nil, ErrProductSplitHeld
		}
		w, err := loadWave(read, tx, binding.Value.Request.Target.WaveID)
		if err != nil {
			return nil, err
		}
		// Historical callers authenticate against the original registration scope.
		w.ScopeRevision = binding.Value.Request.Target.ScopeRevision
		w.Policy.Revision = binding.Value.Request.Target.PolicyRevision
		var out []ProductSplitChildScopeBinding
		seen := map[string]bool{}
		for {
			id := binding.Value.Child.Value.WorkItemID
			if seen[id] || binding.Value.Child.Value.Depth != depth {
				return nil, ErrProductSplitHeld
			}
			seen[id] = true
			out = append(out, binding)
			if depth == 1 {
				return out, nil
			}
			registration, err := loadProductWorkItemRegistration(read, tx, binding.Value.Request.ParentWorkItemID)
			if err != nil {
				return nil, splitEvidenceErr(err)
			}
			parent, err := productSplitChildScopeTx(read, tx, w, registration)
			if err != nil {
				return nil, splitEvidenceErr(err)
			}
			if parent == nil {
				return nil, ErrProductSplitHeld
			}
			binding = *parent
			depth--
		}
	})
}

func (k *KernelStore) splitSourceLineageReadyTx(ctx context.Context, tx *sql.Tx, binding ProductSplitChildScopeBinding, policyDigest string, maxAge time.Duration) ([]ProductSplitChildSource, error) {
	lineage, err := splitSourceLineageTx(ctx, tx, binding)
	if err != nil {
		return nil, err
	}
	sources := make([]ProductSplitChildSource, 0, len(lineage))
	for _, ancestor := range lineage {
		source, err := loadSplitChildSource(ctx, tx, ancestor)
		if err != nil {
			return nil, splitEvidenceErr(err)
		}
		// A split stays in one repository and its HTTP spec selects one trusted
		// IssueSourcePolicy for that repository (validateSplitHTTPPolicy). Ready
		// under a different historical configuration cannot authorize this
		// operation; adopting another policy requires explicit new authority.
		if source.Value.Status != "ready" || source.Value.Facts.PolicyDigest != policyDigest {
			return nil, ErrProductSplitHeld
		}
		sources = append(sources, source)
	}
	// Measure all observations after the complete proof walk, without renewing
	// their actual GET times by saving or reading a gate.
	now := k.clock()
	for _, source := range sources {
		if now.Before(source.CheckedAt) || now.Sub(source.CheckedAt) > maxAge {
			return nil, ErrProductSplitHeld
		}
	}
	return sources, nil
}
