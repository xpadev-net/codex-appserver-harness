package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Only an unstarted controller-generated repair or future writer clone can
// capture a completed predecessor's new Candidate. Existing workspace/run
// snapshots are immutable.
func (k *KernelStore) captureProductTemplateWriterInputTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task, value ProductWorkspaceValue) (bool, error) {
	if (task.Spec.Kind != "work" && task.Spec.Kind != "fix") || value.Target.Valid() || value.InputDigest != "" {
		return false, nil
	}
	keys, err := inputIDsTx(ctx, tx, `SELECT id FROM records WHERE kind='template_expansion' AND work_item_id=? AND json_extract(payload,'$.product') IS NOT NULL AND (EXISTS(SELECT 1 FROM json_each(json_extract(payload,'$.product.repairs')) WHERE value=?) OR EXISTS(SELECT 1 FROM json_each(json_extract(payload,'$.product.future_clones')) WHERE value=?)) ORDER BY id`, w.ID, task.LocalID, task.ID)
	if err != nil {
		return false, err
	}
	if len(keys) != 1 {
		return false, nil
	}
	expansion, err := k.productTemplateExpansionTx(ctx, tx, keys[0])
	if err != nil {
		return false, err
	}
	if !slices.Contains(expansion.Admission.CreatedTaskIDs, task.ID) || expansion.Admission.Target != task.Target() || len(task.Spec.WorkItemIDs) != 1 || task.Spec.WorkItemIDs[0] != expansion.Product.WorkItemID {
		return false, ErrTaskEvidence
	}
	if !slices.Contains(expansion.Product.Repairs, task.LocalID) {
		var clone templateTaskClone
		if err := taskEvidenceRecord(ctx, tx, "template_future_clone", task.ID, &clone); err != nil {
			return false, errors.Join(ErrTaskEvidence, err)
		}
		if clone.ExpansionKey != keys[0] || expansion.Product.FutureClones[clone.OriginalID] != task.ID {
			return false, ErrTaskEvidence
		}
	}
	matched := false
	for _, phase := range expansion.Admission.Plan.Phases {
		for _, spec := range phase.Tasks {
			if spec.ID == task.LocalID {
				matched = reflect.DeepEqual(spec, task.Spec)
			}
		}
	}
	if !matched {
		return false, ErrTaskEvidence
	}
	if err := k.productFutureTaskUnusedTx(ctx, tx, w, task); err != nil {
		return false, errors.Join(ErrProductWorkspaceHeld, err)
	}
	product, err := loadProductTargetTx(ctx, tx, w.Target(), expansion.Product.WorkItemID)
	if err != nil {
		return false, err
	}
	if value.Request.BaseSHA != product.Value.Candidate.SHA {
		return false, ErrProductWorkspaceHeld
	}
	candidates := 0
	for _, dep := range task.Spec.DependsOn {
		prior, err := loadTask(ctx, tx, structureID("task-", w.ID, dep.TaskID))
		if err != nil {
			return false, err
		}
		if err := currentObservedTaskTx(ctx, tx, w, prior); err != nil {
			return false, err
		}
		input, err := k.taskResultInputTx(ctx, tx, w, prior.ResultID)
		if err != nil {
			return false, err
		}
		if input.ProductCandidate != nil {
			candidates++
			if input.ProductCandidate.Value.Candidate != product.Value.Candidate {
				return false, ErrProductWorkspaceHeld
			}
		}
	}
	if candidates != 1 {
		return false, ErrProductWorkspaceHeld
	}
	if err := k.materializeTaskInputTx(ctx, tx, w, task); err != nil {
		return false, err
	}
	return true, nil
}
