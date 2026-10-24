package storage

import (
	"context"
	"database/sql"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// ProductPreparationSummary is safe parent input. It contains identities and
// outcome facts, never private paths, remotes, arguments, credentials or logs.
type ProductPreparationSummary struct {
	Conflict        *contract.ProductBaseConflictReference `json:"conflict,omitempty"`
	BaseMerge       *ProductBaseMergeSummary               `json:"base_merge,omitempty"`
	Outcome         string                                 `json:"outcome,omitempty"`
	WorkspaceID     string                                 `json:"workspace_id"`
	WorkspaceDigest string                                 `json:"workspace_digest,omitempty"`
	State           string                                 `json:"state"`
	BasePlan        ProductVerificationPlanReference       `json:"base_plan"`
	BaseSHA         string                                 `json:"base_sha,omitempty"`
	CurrentPlan     ProductVerificationPlanReference       `json:"current_plan"`
	CurrentHead     string                                 `json:"current_head,omitempty"`
	ProofDigest     string                                 `json:"proof_digest,omitempty"`
}

type ProductBaseMergeSummary struct {
	SourceSHA       string   `json:"source_sha"`
	PreviousBaseSHA string   `json:"previous_base_sha"`
	BaseSHA         string   `json:"base_sha"`
	HeadSHA         string   `json:"head_sha,omitempty"`
	Outcome         string   `json:"outcome,omitempty"`
	Conflicts       []string `json:"conflicts,omitempty"`
	ConflictCount   int      `json:"conflict_count,omitempty"`
}

func publicBaseMergeSummary(merge *workspace.BaseMerge) *ProductBaseMergeSummary {
	summary := &ProductBaseMergeSummary{SourceSHA: merge.Source.SHA, PreviousBaseSHA: merge.Source.BaseSHA, BaseSHA: merge.BaseSHA, HeadSHA: merge.HeadSHA, Outcome: merge.Outcome, ConflictCount: len(merge.Conflicts)}
	bytes := 0
	for _, path := range merge.Conflicts {
		if len(summary.Conflicts) == 4 || bytes+len(path) > 4096 {
			break
		}
		summary.Conflicts = append(summary.Conflicts, path)
		bytes += len(path)
	}
	return summary
}

func preparationAttentionReason(reason string) bool {
	return reason == "product_workspace_failed" || reason == "product_workspace_owner_unresolved" || reason == "product_workspace_evidence_unavailable" || reason == "product_base_changed" || reason == "product_base_merge_conflict"
}
func (k *KernelStore) productPreparationAttentionTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) (string, *ProductPreparationSummary, error) {
	if !UsesProductChangeOutput(w, task) && !usesProductSubject(w, task.Spec) || task.State != contract.TaskReady || task.Attempts != 0 {
		return "", nil, nil
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return "", nil, err
	}
	var active string
	var used bool
	if err := tx.QueryRowContext(ctx, "SELECT active_run_id,EXISTS(SELECT 1 FROM kernel_run_bindings WHERE session_id=?) FROM kernel_sessions WHERE id=?", task.SessionID, task.SessionID).Scan(&active, &used); err != nil {
		return "", nil, err
	}
	if active != "" || used {
		return "", nil, nil
	}
	saved, err := k.productWorkspaceTx(ctx, tx, productWorkspaceID(task.ID))
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil, nil
		}
		if readinessHoldError(err) || errors.Is(err, ErrProductWorkspaceHeld) {
			return "product_workspace_evidence_unavailable", &ProductPreparationSummary{WorkspaceID: productWorkspaceID(task.ID), State: "evidence_unavailable"}, nil
		}
		return "", nil, err
	}
	summary := &ProductPreparationSummary{WorkspaceID: saved.ID, WorkspaceDigest: saved.Digest, State: saved.State, BasePlan: ProductVerificationPlanReference{ID: saved.Value.Request.BasePlanID, Digest: saved.Value.Request.BasePlanDigest}, BaseSHA: saved.Value.Request.BaseSHA}
	if saved.Proof != nil {
		summary.Outcome = saved.Proof.Outcome
		summary.ProofDigest, err = contentDigest(saved.Proof)
		if err != nil {
			return "", nil, err
		}
		if merge := saved.Proof.BaseMerge; merge != nil {
			summary.BaseMerge = publicBaseMergeSummary(merge)
			if saved.State == "failed" && merge.Outcome == "conflict" {
				summary.Conflict = &contract.ProductBaseConflictReference{TaskID: task.ID, WorkspaceID: saved.ID, WorkspaceDigest: saved.Digest, ProofDigest: summary.ProofDigest}
			}
		}
	}
	switch saved.State {
	case "failed", "stale":
		if summary.Conflict != nil {
			return "product_base_merge_conflict", summary, nil
		}
		return "product_workspace_failed", summary, nil
	case "launching":
		return "product_workspace_owner_unresolved", summary, nil
	case "prepared", "ready":
		if saved.Value.Request.BasePlanID == "" {
			return "", nil, nil
		}
		selected, plan, err := k.readProductBaseSelectionTx(ctx, tx, task.ID)
		if err != nil {
			if readinessHoldError(err) || errors.Is(err, ErrProductWorkspaceHeld) {
				summary.State = "evidence_unavailable"
				return "product_workspace_evidence_unavailable", summary, nil
			}
			return "", nil, err
		}
		if selected.Plan.ID != saved.Value.Request.BasePlanID || selected.Plan.Digest != saved.Value.Request.BasePlanDigest {
			summary.CurrentPlan = selected.Plan
			summary.CurrentHead = plan.Value.Head.HeadSHA
			return "product_base_changed", summary, nil
		}
	}
	return "", nil, nil
}

// NotifyProductPreparationAttention persists one meaningful exception input;
// polling or delivery acknowledgment cannot turn it into another model turn.
func (k *KernelStore) NotifyProductPreparationAttention(ctx context.Context, taskID string) (bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		return false, err
	}
	w, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return false, err
	}
	if task.Target() != w.Target() || w.PendingDirectionID != "" {
		return false, nil
	}
	reason, summary, err := k.productPreparationAttentionTx(ctx, tx, w, task)
	if err != nil || reason == "" {
		return false, err
	}
	detail, err := contentDigest(summary)
	if err != nil {
		return false, err
	}
	changed, err := k.saveParentAttentionTx(ctx, tx, w, task, reason, detail, summary)
	if err != nil {
		return false, err
	}
	return changed, tx.Commit()
}

// FinishChangedUnstartedWorkspace closes only a positively unclaimed local
// profile. The ordinary stopping transaction rechecks prepared/no-claim state;
// a concurrent claim or any missing evidence cannot be treated as not started.
func (k *KernelStore) FinishChangedUnstartedWorkspace(ctx context.Context, taskID string) (bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return false, err
	}
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		tx.Rollback()
		return false, err
	}
	w, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		tx.Rollback()
		return false, err
	}
	if w.DispatchPaused() || task.Paused || task.Target() != w.Target() || w.PendingDirectionID != "" {
		tx.Rollback()
		return false, nil
	}
	saved, err := k.productWorkspaceTx(ctx, tx, productWorkspaceID(taskID))
	if err != nil {
		tx.Rollback()
		return false, err
	}
	if saved.State != "prepared" || !saved.ClaimedAt.IsZero() {
		tx.Rollback()
		return false, nil
	}
	reason, _, err := k.productPreparationAttentionTx(ctx, tx, w, task)
	tx.Rollback()
	if err != nil || reason != "product_base_changed" {
		return false, err
	}
	proof := ProductWorkspaceProof{ProofID: saved.ID + ":base_changed_not_started", Outcome: "not_started", Reason: "fixed base changed before the local Git owner claimed this profile"}
	stopped, err := k.FinishProductWorkspace(ctx, taskID, proof)
	return err == nil && stopped.State == "failed", err
}
