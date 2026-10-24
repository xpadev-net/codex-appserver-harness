package storage

import (
	"context"
	"database/sql"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type ProductTaskPreparation struct {
	SourceRequest *ProductSourceRequestAdoption       `json:"source_request,omitempty"`
	Continuations []ProductContinuation               `json:"continuations,omitempty"`
	Task          domain.Task                         `json:"task"`
	Registration  ProductWorkItemRegistrationSnapshot `json:"registration"`
	Product       *ProductTargetSnapshot              `json:"product,omitempty"`
}

// ReadProductTaskPreparation derives local host work from the active Phase.
// It grants no model dispatch: that still requires the separate runtime gate.
func (k *KernelStore) ReadProductTaskPreparation(ctx context.Context, taskID string) (ProductTaskPreparation, error) {
	var out ProductTaskPreparation
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	return k.productTaskPreparationTx(ctx, tx, taskID)
}
func (k *KernelStore) productTaskPreparationTx(ctx context.Context, tx *sql.Tx, taskID string) (ProductTaskPreparation, error) {
	var out ProductTaskPreparation
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		return out, err
	}
	w, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return out, err
	}
	if !UsesProductChangeOutput(w, task) && !usesProductSubject(w, task.Spec) || task.Target() != w.Target() || task.Paused || w.State == domain.WaveBlocked {
		return out, ErrProductWorkspaceHeld
	}
	set, _, drain, err := k.activePhaseTx(ctx, tx, w.Target())
	if err != nil {
		return out, err
	}
	if set.State != "active" || set.PhaseID != task.PhaseID || drain {
		return out, ErrProductWorkspaceHeld
	}
	if unusedErr := k.productFutureTaskUnusedTx(ctx, tx, w, task); unusedErr != nil {
		readable, err := k.preparedBaseTaskReadableTx(ctx, tx, w, task)
		if err != nil {
			return out, err
		}
		if !readable {
			return out, unusedErr
		}
	}
	held, err := productWorkItemExecutionHeld(ctx, tx, w, task.Spec.WorkItemIDs[0])
	if err != nil {
		return out, err
	}
	if held {
		return out, ErrProductWorkspaceHeld
	}
	for _, dep := range task.Spec.DependsOn {
		prior, err := loadTask(ctx, tx, structureID("task-", w.ID, dep.TaskID))
		if err != nil {
			return out, err
		}
		if err := currentObservedTaskTx(ctx, tx, w, prior); err != nil {
			return out, err
		}
		if prior.State != contract.TaskCompleted {
			return out, ErrProductWorkspaceHeld
		}
		if _, err := k.completedTaskResultTx(ctx, tx, w, prior); err != nil {
			return out, err
		}
	}
	if err := k.productMergeDependenciesTx(ctx, tx, w, task); err != nil {
		return out, err
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, task.Spec.WorkItemIDs[0])
	if err != nil {
		return out, err
	}
	if !productRegistrationScope(registration, w) {
		return out, ErrTaskEvidence
	}
	out.SourceRequest, err = k.productSourceRequestTx(ctx, tx, w)
	if err != nil {
		return out, err
	}
	lineage, err := k.productContinuationInputsTx(ctx, tx, w, task.Spec.WorkItemIDs[0])
	if err != nil {
		return out, err
	}
	out.Continuations = lineage
	out.Task, out.Registration = task, registration
	product, err := loadProductTargetTx(ctx, tx, w.Target(), task.Spec.WorkItemIDs[0])
	if err == nil {
		out.Product = &product
	} else if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	if task.Spec.Kind == "review" {
		sources, err := k.productSubjectSourcesTx(ctx, tx, w, task)
		if err != nil {
			return out, err
		}
		if sources[0].Value.VerificationPlan == nil {
			return out, ErrProductRunnerHeld
		}
		evidence, err := k.productVerificationEvidenceTx(ctx, tx, sources[0].Value.VerificationPlan.ID)
		if err != nil {
			return out, err
		}
		if !evidence.Value.Passed {
			return out, ErrProductRunnerHeld
		}
	}
	return out, nil
}
