package storage

import (
	"context"
	"database/sql"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

func (k *KernelStore) productExpandedReviewTx(ctx context.Context, tx *sql.Tx, w domain.Wave, source domain.Task, expansion TemplateExpansion) (bool, error) {
	saved, err := k.productTemplateExpansionTx(ctx, tx, expansion.Key)
	if err != nil {
		return false, err
	}
	p := saved.Product
	if saved.Request.Target.WaveID != w.ID || saved.Request.Target.ScopeRevision != w.ScopeRevision || saved.Request.Target.PolicyRevision != w.Policy.Revision || p.SourceReviews[source.ResultID] == "" || len(p.Replacements[source.ID]) == 0 {
		return false, ErrTaskEvidence
	}
	specs := map[string]contract.TaskSpec{}
	for _, phase := range saved.Admission.Plan.Phases {
		for _, spec := range phase.Tasks {
			specs[spec.ID] = spec
		}
	}
	locals := append(append([]string{}, p.Repairs...), p.Verifier)
	for _, local := range locals {
		spec, ok := specs[local]
		id := structureID("task-", w.ID, local)
		if !ok || spec.Kind != "fix" && spec.Kind != "verification" || !slices.Equal(spec.WorkItemIDs, []string{p.WorkItemID}) || !slices.Contains(saved.Admission.CreatedTaskIDs, id) {
			return false, ErrTaskEvidence
		}
		if local == p.Verifier && spec.Kind != "verification" {
			return false, ErrTaskEvidence
		}
		bound, err := templateTaskBoundTx(ctx, tx, w, id, map[string]bool{})
		if err != nil {
			return false, err
		}
		if !bound {
			return false, nil
		}
	}
	for _, ids := range p.Replacements {
		for _, id := range ids {
			task, err := loadTask(ctx, tx, id)
			if err != nil {
				return false, err
			}
			if task.Spec.Kind != "review" || !slices.Equal(task.Spec.WorkItemIDs, []string{p.WorkItemID}) || !slices.Contains(saved.Admission.CreatedTaskIDs, id) {
				return false, ErrTaskEvidence
			}
			var lineage templateReviewLineage
			if err := taskEvidenceRecord(ctx, tx, "product_template_review_lineage", id, &lineage); err != nil {
				return false, err
			}
			if lineage.ExpansionKey != saved.Key || lineage.Round != saved.Round {
				return false, ErrTaskEvidence
			}
			bound, err := templateTaskBoundTx(ctx, tx, w, id, map[string]bool{})
			if err != nil {
				return false, err
			}
			if !bound {
				return false, nil
			}
		}
	}
	return true, nil
}

// Every saved role must reach a clean report for the exact replacement. This
// follows immutable product lineage without treating an old report as quality.
func (k *KernelStore) productReviewReplacedTx(ctx context.Context, tx *sql.Tx, w domain.Wave, taskID string, candidate runner.Candidate, seen map[string]bool) (bool, error) {
	if seen[taskID] || len(seen) >= w.Policy.Limits.Tasks {
		return false, ErrTaskEvidence
	}
	seen[taskID] = true
	defer delete(seen, taskID)
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		return false, err
	}
	if task.State != contract.TaskCompleted {
		return false, nil
	}
	input, err := k.taskResultInputTx(ctx, tx, w, task.ResultID)
	if err != nil {
		return false, err
	}
	if input.ProductReview == nil {
		return false, ErrTaskEvidence
	}
	if input.ProductReview.Value.Candidate == candidate && input.ProductReview.Value.Review.Verdict == "clean" && len(input.ProductReview.Value.Review.Findings) == 0 && len(input.Result.Remaining) == 0 && len(input.Result.Blockers) == 0 {
		return true, nil
	}
	keys, err := inputIDsTx(ctx, tx, `SELECT id FROM records WHERE kind='template_expansion' AND work_item_id=? AND json_extract(payload,'$.product') IS NOT NULL AND EXISTS(SELECT 1 FROM json_each(json_extract(payload,'$.source_result_ids')) WHERE value=?) ORDER BY id`, w.ID, task.ResultID)
	if err != nil {
		return false, err
	}
	for _, key := range keys {
		expansion, err := k.productTemplateExpansionTx(ctx, tx, key)
		if err != nil {
			return false, err
		}
		bound, err := k.productExpandedReviewTx(ctx, tx, w, task, expansion)
		if err != nil {
			return false, err
		}
		if !bound {
			continue
		}
		all := true
		for _, id := range expansion.Product.Replacements[taskID] {
			clean, err := k.productReviewReplacedTx(ctx, tx, w, id, candidate, seen)
			if err != nil {
				return false, err
			}
			all = all && clean
		}
		if all {
			return true, nil
		}
	}
	return false, nil
}
