package storage

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrProductMergeDependencyHeld = fmt.Errorf("%w: product successor requires authenticated merged predecessor work", ErrProductWorkspaceHeld)

// productMergeDependenciesTx interprets a writer's explicit dependency on a
// different product WorkItem as a merged-change dependency. Local verification,
// review and fix within the same WorkItem still consume the fixed candidate
// before merge. A Task's normal completion never substitutes for a merge.
// Split siblings additionally retain their authenticated intent dependencies
// for every product role, even when the Task DAG omits those dependencies.
func (k *KernelStore) productMergeDependenciesTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) error {
	if !UsesProductChangeOutput(w, task) && !usesProductSubject(w, task.Spec) {
		return nil
	}
	own := task.Spec.WorkItemIDs
	required := map[string]map[string]string{}
	splitDependencies, err := productSplitDependencyIDsTx(ctx, tx, w, own)
	if err != nil {
		return err
	}
	for _, id := range splitDependencies {
		required[id] = map[string]string{}
	}
	for _, dependency := range task.Spec.DependsOn {
		// Reviews and verification may consume a predecessor candidate before its
		// PR merges. Immutable dependencies between split siblings still apply.
		if !UsesProductChangeOutput(w, task) {
			break
		}
		prior, err := loadTask(ctx, tx, structureID("task-", w.ID, dependency.TaskID))
		if err != nil {
			return err
		}
		if !UsesProductChangeOutput(w, prior) && !usesProductSubject(w, prior.Spec) {
			continue
		}
		cross := slices.ContainsFunc(prior.Spec.WorkItemIDs, func(id string) bool { return !slices.Contains(own, id) })
		if !cross {
			continue
		}
		if err := currentObservedTaskTx(ctx, tx, w, prior); err != nil {
			return err
		}
		_, err = k.completedTaskResultTx(ctx, tx, w, prior)
		if err != nil {
			return err
		}
		input, err := k.taskResultInputTx(ctx, tx, w, prior.ResultID)
		if err != nil {
			return err
		}
		digest, err := contentDigest(input)
		if err != nil {
			return err
		}
		for _, id := range prior.Spec.WorkItemIDs {
			if slices.Contains(own, id) {
				continue
			}
			if required[id] == nil {
				required[id] = map[string]string{}
			}
			required[id][prior.ResultID] = digest
		}
	}
	if len(required) == 0 {
		return nil
	}
	coverage, err := k.productCoverageTx(ctx, tx, w)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(required))
	for id := range required {
		ids = append(ids, id)
	}
	leaves, origins, err := productMergeDependencyLeaves(coverage, ids)
	if err != nil {
		return err
	}
	byOrigin := map[string][]string{}
	for _, origin := range origins {
		byOrigin[origin.WorkItemID] = origin.LeafWorkItemIDs
	}
	expanded := map[string]map[string]string{}
	for _, id := range leaves {
		expanded[id] = map[string]string{}
	}
	for id, results := range required {
		contributors, delegated := byOrigin[id]
		if delegated && len(results) > 0 {
			// A completed parent Task's output is not a leaf quality result.
			// Keep this explicit requirement held rather than inventing provenance.
			return ErrProductMergeDependencyHeld
		}
		if !delegated {
			contributors = []string{id}
		}
		for _, leaf := range contributors {
			for result, digest := range results {
				if prior, exists := expanded[leaf][result]; exists && prior != digest {
					return ErrProductMergeDependencyHeld
				}
				expanded[leaf][result] = digest
			}
		}
	}
	required = expanded
	for _, fact := range coverage.Value.Merged {
		results, needed := required[fact.WorkItemID]
		if !needed {
			continue
		}
		// Coverage authenticated this exact receipt in the same transaction.
		// Match the actual dependency outputs to the original merge's quality
		// packet as well; another completed result from that WorkItem is not
		// evidence that this dependency's changes were merged.
		var receipt ProductMergedWorkItem
		if err := taskEvidenceRecord(ctx, tx, "product_merged_work_item", fact.MergeID, &receipt); err != nil {
			return err
		}
		digest, err := contentDigest(receipt.Value)
		if err != nil {
			return err
		}
		if digest != fact.ReceiptDigest || receipt.Digest != digest {
			return ErrTaskEvidence
		}
		var ready ProductPublicationReadiness
		if err := taskEvidenceRecord(ctx, tx, "product_publication_readiness_history", receipt.Value.ReadinessID, &ready); err != nil {
			return err
		}
		digest, err = contentDigest(ready.Value)
		if err != nil {
			return err
		}
		if ready.Digest != digest || digest != receipt.Value.ReadinessDigest {
			return ErrTaskEvidence
		}
		matched := true
		for id, want := range results {
			if ready.Value.Quality.Value.Results[id] != want {
				matched = false
				break
			}
		}
		if matched {
			delete(required, fact.WorkItemID)
		}
	}
	if len(required) != 0 {
		return ErrProductMergeDependencyHeld
	}
	return nil
}
