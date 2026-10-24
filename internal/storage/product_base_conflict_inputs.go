package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// The source remains historical after the parent retires the unused Task. Its
// stopped Git proof is input to a new fix, never a completed model Task result.
func (k *KernelStore) productBaseConflictInputTx(ctx context.Context, tx *sql.Tx, w domain.Wave, spec contract.TaskSpec) (*ProductWorkspace, error) {
	r := spec.ProductBaseConflict
	if r == nil {
		return nil, nil
	}
	if r.Validate() != nil || w.Policy.Stage != "P" || !w.Policy.Product || spec.Kind != "fix" || len(spec.WorkItemIDs) != 1 || r.TaskID == structureID("task-", w.ID, spec.ID) || r.WorkspaceID != productWorkspaceID(r.TaskID) {
		return nil, ErrProductWorkspaceHeld
	}
	saved, err := k.productWorkspaceTx(ctx, tx, r.WorkspaceID)
	if err != nil {
		return nil, errors.Join(ErrTaskEvidence, err)
	}
	v, proof := saved.Value, saved.Proof
	if saved.State != "failed" || saved.Digest != r.WorkspaceDigest || v.Request.TaskID != r.TaskID || v.Target.WaveID != w.ID || v.Target.ScopeRevision != w.ScopeRevision || v.Target.PolicyRevision != w.Policy.Revision || v.Target.PlanRevision > w.PlanRevision || v.WorkItemID != spec.WorkItemIDs[0] || proof == nil || proof.Outcome != "failed" || proof.BaseMerge == nil || proof.BaseMerge.Outcome != "conflict" {
		return nil, ErrProductWorkspaceHeld
	}
	digest, err := contentDigest(proof)
	if err != nil || digest != r.ProofDigest {
		return nil, errors.Join(ErrTaskEvidence, err)
	}
	plan, err := k.baseWorkspacePlanTx(ctx, tx, v)
	if err != nil || plan.Value.Input.Value.Preparation.Product == nil {
		return nil, errors.Join(ErrTaskEvidence, err)
	}
	task, err := loadTask(ctx, tx, r.TaskID)
	if err != nil {
		return nil, err
	}
	original := plan.Value.Input.Value.Preparation.Task
	if task.Target() != v.Target || task.SessionID != v.SessionID || task.ParentSessionID != v.ParentSessionID || task.InputDigest != v.InputDigest || !reflect.DeepEqual(task.Spec, original.Spec) || task.Attempts != 0 || task.Paused || task.State != contract.TaskReady && task.State != contract.TaskCanceled {
		return nil, ErrProductWorkspaceHeld
	}
	var active string
	var used, stopped, creating bool
	if err := tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating,EXISTS(SELECT 1 FROM kernel_run_bindings WHERE session_id=?) FROM kernel_sessions WHERE id=?", task.SessionID, task.SessionID).Scan(&active, &stopped, &creating, &used); err != nil {
		return nil, err
	}
	if active != "" || used || !stopped || creating {
		return nil, ErrProductWorkspaceHeld
	}
	if original.Spec.ProductBaseConflict != nil {
		if _, err := k.productBaseConflictInputTx(ctx, tx, w, original.Spec); err != nil {
			return nil, err
		}
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, v.WorkItemID)
	if err != nil {
		return nil, err
	}
	if !productRegistrationScope(registration, w) || registration.Digest != v.RegistrationDigest {
		return nil, ErrProductWorkspaceHeld
	}
	for _, criterion := range spec.Criteria {
		if !slices.Contains(registration.Value.Definition.Criteria, criterion) {
			return nil, ErrProductWorkspaceHeld
		}
	}
	reservation, err := loadOperationReservationTx(ctx, tx, saved.ID)
	if err != nil {
		return nil, err
	}
	expected := OperationReleaseProof{OperationID: saved.ID, ProofID: proof.ProofID, Disposition: "process_stopped", Reason: proof.Reason}
	if reservation.Kind != "git" || reservation.State != "released" || reservation.Request.Target != v.Target || reservation.Request.Worktree != v.CWD || reservation.Request.Repository != v.Request.Repository || reservation.Proof == nil || *reservation.Proof != expected {
		return nil, ErrProductWorkspaceHeld
	}
	return &saved, nil
}

func (k *KernelStore) baseConflictCurrentProductTx(ctx context.Context, tx *sql.Tx, w domain.Wave, source ProductWorkspace) (ProductTargetSnapshot, error) {
	var current ProductTargetSnapshot
	held, err := productWorkItemExecutionHeld(ctx, tx, w, source.Value.WorkItemID)
	if err != nil || held {
		return current, errors.Join(ErrProductWorkspaceHeld, err)
	}
	if err := taskEvidenceRecord(ctx, tx, "product_target", productTargetID(w.Target(), source.Value.WorkItemID), &current); err != nil {
		return current, errors.Join(ErrTaskEvidence, err)
	}
	current, err = loadProductTargetTx(ctx, tx, current.Value.Target, source.Value.WorkItemID)
	if err != nil {
		return current, err
	}
	plan, err := k.baseWorkspacePlanTx(ctx, tx, source.Value)
	if err != nil {
		return current, err
	}
	old := plan.Value.Input.Value.Preparation.Product
	if old == nil || current.Value.Target.WaveID != w.ID || current.Value.Target.ScopeRevision != w.ScopeRevision || current.Value.Target.PolicyRevision != w.Policy.Revision || current.Value.Target.PlanRevision > w.PlanRevision || current.Value.WorkItem.Terminal() || current.Value.WorkItem.Paused {
		return current, ErrProductWorkspaceHeld
	}
	normalized := current.Value
	normalized.Target = old.Value.Target
	// A merge-only hold must survive the carry without blocking local repair.
	normalized.WorkItem.MergeHold = old.Value.WorkItem.MergeHold
	if !reflect.DeepEqual(normalized, old.Value) {
		return current, ErrProductWorkspaceHeld
	}
	return current, nil
}

// Only explicit, authenticated conflict repair references carry their existing
// product to the new parent plan. All other parent plan behavior stays intact.
func (k *KernelStore) carryBaseConflictProductsTx(ctx context.Context, tx *sql.Tx, w domain.Wave, target contract.Target, plan contract.PlanProposal) error {
	carried := map[string]bool{}
	for _, phase := range plan.Phases {
		for _, spec := range phase.Tasks {
			if spec.ProductBaseConflict == nil {
				continue
			}
			source, err := k.productBaseConflictInputTx(ctx, tx, w, spec)
			if err != nil {
				return err
			}
			current, err := k.baseConflictCurrentProductTx(ctx, tx, w, *source)
			if err != nil {
				return err
			}
			if carried[source.Value.WorkItemID] {
				continue
			}
			projected := current.Value
			projected.Target = target
			if _, err := k.setProductTargetTx(ctx, tx, current.Digest, projected); err != nil {
				return err
			}
			carried[source.Value.WorkItemID] = true
		}
	}
	return nil
}
