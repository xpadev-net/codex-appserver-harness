package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// A ready workspace fixes its origin before the first model run. Work/fix and
// read-only review input must not follow future Candidate projections. Other inputs,
// including dependencies and source results, are still checked on every read.
func (k *KernelStore) fixedProductWorkspaceInputTx(ctx context.Context, tx *sql.Tx, w domain.Wave, spec contract.TaskSpec) (*ProductWorkspace, error) {
	if !UsesProductChangeOutput(w, domain.Task{Spec: spec}) && !usesProductSubject(w, spec) {
		return nil, nil
	}
	taskID := structureID("task-", w.ID, spec.ID)
	id := productWorkspaceID(taskID)
	saved, err := k.productWorkspaceTx(ctx, tx, id)
	if errors.Is(err, sql.ErrNoRows) {
		var history bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_workspace_history' AND id=?)", id).Scan(&history); err != nil {
			return nil, err
		}
		if history {
			return nil, ErrTaskEvidence
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if saved.State != "ready" {
		return nil, nil
	}
	var history productWorkspaceRecord
	if err := taskEvidenceRecord(ctx, tx, "product_workspace_history", id, &history); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskEvidence
		}
		return nil, err
	}
	digest, err := contentDigest(saved)
	if err != nil {
		return nil, err
	}
	if history.Digest != digest || !reflect.DeepEqual(history.Workspace, saved) {
		return nil, ErrTaskEvidence
	}
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(task.Spec, spec) || saved.Value.Target != task.Target() || saved.Value.Target.WaveID != w.ID || saved.Value.Target.ScopeRevision != w.ScopeRevision || saved.Value.Target.PolicyRevision != w.Policy.Revision || saved.Value.ParentSessionID != w.OrchestratorSessionID || saved.Value.SessionID != task.SessionID || saved.Value.WorkItemID != spec.WorkItemIDs[0] {
		return nil, ErrTaskEvidence
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, saved.Value.WorkItemID)
	if err != nil {
		return nil, err
	}
	if !productRegistrationScope(registration, w) || registration.Digest != saved.Value.RegistrationDigest || registration.Value.Definition.RepositoryID != saved.Value.Request.RepositoryID {
		return nil, ErrTaskEvidence
	}
	if spec.Kind == "review" || spec.Kind == "verification" {
		subject, err := k.fixedProductSubjectInputTx(ctx, tx, w, spec)
		if err != nil {
			return nil, err
		}
		if subject == nil || saved.Value.SubjectID != subject.ID || saved.Value.SubjectDigest != subject.Digest || saved.Value.Request.BaseSHA != subject.Value.Candidate.SHA {
			return nil, ErrTaskEvidence
		}
	} else if saved.Value.SubjectID != "" || saved.Value.SubjectDigest != "" {
		return nil, ErrTaskEvidence
	}
	if _, err := k.productWorkspaceVerificationPolicyTx(ctx, tx, w, task, saved.Value); err != nil {
		return nil, err
	}
	return &saved, nil
}

func (k *KernelStore) materializeProductTaskInputTx(ctx context.Context, tx *sql.Tx, saved ProductWorkspace) error {
	task, err := loadTask(ctx, tx, saved.Value.Request.TaskID)
	if err != nil {
		return err
	}
	w, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return err
	}
	return k.materializeTaskInputTx(ctx, tx, w, task)
}

func (k *KernelStore) materializeTaskInputTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) error {
	input, err := k.taskInputDigestTx(ctx, tx, w, task.Spec)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE kernel_plan_bindings SET input_digest=? WHERE wave_id=? AND revision=? AND task_id=? AND input_digest=? AND source_result_id=''", input, w.ID, w.PlanRevision, task.ID, task.InputDigest)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrTaskEvidence
	}
	task.InputDigest = input
	return saveTask(ctx, tx, task)
}
