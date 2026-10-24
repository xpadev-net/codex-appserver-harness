package storage

import (
	"context"
	"database/sql"
	"slices"
	"sort"
)

// ProductBaseInput is private host evidence, not inline model input or a grant.
// Dependencies retain the exact original merge and repository identities.
type ProductBaseDependency struct {
	Fact         ProductCoverageFact `json:"fact"`
	RepositoryID int64               `json:"repository_id"`
	Repository   string              `json:"repository"`
	BaseBranch   string              `json:"base_branch"`
}
type ProductBaseInputValue struct {
	ConflictBaseSHA string                            `json:"conflict_base_sha,omitempty"`
	Preparation     ProductTaskPreparation            `json:"preparation"`
	Policy          ProductVerificationPolicySnapshot `json:"policy"`
	Dependencies    []ProductBaseDependency           `json:"dependencies"`
}
type ProductBaseInput struct {
	Value  ProductBaseInputValue `json:"value"`
	Digest string                `json:"digest"`
}

func (k *KernelStore) ReadProductBaseInput(ctx context.Context, taskID string) (ProductBaseInput, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductBaseInput{}, err
	}
	defer tx.Rollback()
	return k.productBaseInputTx(ctx, tx, taskID)
}
func (k *KernelStore) productBaseInputTx(ctx context.Context, tx *sql.Tx, taskID string) (ProductBaseInput, error) {
	var out ProductBaseInput
	prep, err := k.productTaskPreparationTx(ctx, tx, taskID)
	if err != nil {
		return out, err
	}
	w, err := loadWave(ctx, tx, prep.Task.WaveID)
	if err != nil {
		return out, err
	}
	if !UsesProductChangeOutput(w, prep.Task) {
		return out, ErrProductWorkspaceHeld
	}
	// Workspace materialization adds the fixed checkout to Task input. Keep
	// the base plan anchored to its original preparation input, after proving
	// that the materialized current input is still exactly derivable.
	fixed, err := k.fixedProductWorkspaceInputTx(ctx, tx, w, prep.Task.Spec)
	if err != nil {
		return out, err
	}
	if fixed != nil && fixed.Value.Request.BasePlanID != "" {
		actual, err := k.taskInputDigestTx(ctx, tx, w, prep.Task.Spec)
		if err != nil {
			return out, err
		}
		if prep.Task.InputDigest != actual {
			return out, ErrTaskEvidence
		}
		prep.Task.InputDigest = fixed.Value.InputDigest
	}
	pinned, err := loadProductVerificationPolicy(ctx, tx, w.ID, w.Policy.Revision, prep.Registration.Value.Definition.RepositoryID)
	if err != nil {
		return out, err
	}
	out.Value = ProductBaseInputValue{Preparation: prep, Policy: pinned, Dependencies: []ProductBaseDependency{}}
	conflict, err := k.productBaseConflictInputTx(ctx, tx, w, prep.Task.Spec)
	if err != nil {
		return out, err
	}
	if conflict != nil {
		if _, err := k.baseConflictCurrentProductTx(ctx, tx, w, *conflict); err != nil {
			return out, err
		}
		out.Value.ConflictBaseSHA = conflict.Proof.BaseMerge.BaseSHA
	}
	required := map[string]bool{}
	for _, dep := range prep.Task.Spec.DependsOn {
		prior, err := loadTask(ctx, tx, structureID("task-", w.ID, dep.TaskID))
		if err != nil {
			return out, err
		}
		if !UsesProductChangeOutput(w, prior) && !usesProductSubject(w, prior.Spec) {
			continue
		}
		for _, id := range prior.Spec.WorkItemIDs {
			if !slices.Contains(prep.Task.Spec.WorkItemIDs, id) {
				required[id] = true
			}
		}
	}
	if len(required) > 0 {
		coverage, err := k.productCoverageTx(ctx, tx, w)
		if err != nil {
			return out, err
		}
		for _, fact := range coverage.Value.Merged {
			if !required[fact.WorkItemID] {
				continue
			}
			var receipt ProductMergedWorkItem
			if err := taskEvidenceRecord(ctx, tx, "product_merged_work_item", fact.MergeID, &receipt); err != nil {
				return out, err
			}
			digest, err := contentDigest(receipt.Value)
			if err != nil {
				return out, err
			}
			if digest != fact.ReceiptDigest || receipt.Digest != digest {
				return out, ErrTaskEvidence
			}
			p := receipt.Value.Product.Value
			out.Value.Dependencies = append(out.Value.Dependencies, ProductBaseDependency{Fact: fact, RepositoryID: p.WorkItem.RepositoryID, Repository: p.Repository, BaseBranch: p.BaseBranch})
		}
	}
	sort.Slice(out.Value.Dependencies, func(i, j int) bool {
		return out.Value.Dependencies[i].Fact.MergeID < out.Value.Dependencies[j].Fact.MergeID
	})
	out.Digest, err = contentDigest(out.Value)
	return out, err
}
func baseInputAncestors(input ProductBaseInput, initial string) ([]string, error) {
	prep := input.Value.Preparation
	seed := initial
	if prep.Product != nil {
		seed = prep.Product.Value.Candidate.BaseSHA
	}
	if !fixedSHA(initial) || !fixedSHA(seed) {
		return nil, ErrProductBasePlanHeld
	}
	ancestors := []string{seed}
	if ref := prep.Task.Spec.ProductBaseConflict; ref != nil {
		// The authenticated source digest is part of preparation input. The
		// source base is added below by the private input snapshot.
		if !fixedSHA(input.Value.ConflictBaseSHA) {
			return nil, ErrProductBasePlanHeld
		}
		ancestors = append(ancestors, input.Value.ConflictBaseSHA)
	} else if input.Value.ConflictBaseSHA != "" {
		return nil, ErrProductBasePlanHeld
	}
	for _, dep := range input.Value.Dependencies {
		if dep.RepositoryID == prep.Registration.Value.Definition.RepositoryID {
			ancestors = append(ancestors, dep.Fact.MergeSHA)
		}
	}
	slices.Sort(ancestors)
	return slices.Compact(ancestors), nil
}

// RequiredAncestors derives the exact repository-local ancestry set for the host reader.
func (input ProductBaseInput) RequiredAncestors(initial string) ([]string, error) {
	return baseInputAncestors(input, initial)
}
