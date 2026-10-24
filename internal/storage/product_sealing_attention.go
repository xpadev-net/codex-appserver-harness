package storage

import (
	"context"
	"database/sql"
	"errors"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ProductSealingSummary contains safe outcome facts, never paths, arguments,
// remotes, credentials, raw errors/logs or model final text.
type ProductSealingSummary struct {
	RunID           string                           `json:"run_id"`
	OutputDigest    string                           `json:"output_digest,omitempty"`
	Workspace       ProductVerificationPlanReference `json:"workspace"`
	Seal            ProductVerificationPlanReference `json:"seal"`
	State           string                           `json:"state"`
	Outcome         string                           `json:"outcome,omitempty"`
	ProofDigest     string                           `json:"proof_digest,omitempty"`
	HeadSHA         string                           `json:"head_sha,omitempty"`
	MergeHeadSHA    string                           `json:"merge_head_sha,omitempty"`
	Attempted       bool                             `json:"attempted,omitempty"`
	UnresolvedCount int                              `json:"unresolved_count,omitempty"`
}

func sealingAttentionReason(reason string) bool {
	return reason == "product_seal_failed" || reason == "product_seal_owner_unresolved" || reason == "product_seal_evidence_unavailable" || reason == "product_seal_adoption_held"
}

func (k *KernelStore) productSealingAttentionTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) (string, *ProductSealingSummary, error) {
	if !UsesProductChangeOutput(w, task) && !UsesProductVerificationOutput(w, task) || task.State != contract.TaskRunning && task.State != contract.TaskBlocked || task.Attempts == 0 {
		return "", nil, nil
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return "", nil, err
	}
	b, err := sessionBindingTx(ctx, tx, task.SessionID)
	if err != nil {
		return "", nil, err
	}
	if b.Kind != "task" || b.TaskID != task.ID || b.Target != task.Target() || b.ParentSessionID != w.OrchestratorSessionID {
		return "", nil, ErrTaskEvidence
	}
	var active, owner, thread, state, snapshot string
	var stopped, creating, runStopped, applied bool
	err = tx.QueryRowContext(ctx, `SELECT s.active_run_id,s.process_stopped,s.creating,r.session_id,r.thread_id,r.state,r.snapshot_id,r.process_stopped,r.applied FROM kernel_sessions s JOIN kernel_run_bindings r ON r.run_id=s.active_run_id WHERE s.id=?`, b.ID).Scan(&active, &stopped, &creating, &owner, &thread, &state, &snapshot, &runStopped, &applied)
	if err == sql.ErrNoRows {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}
	if owner != b.ID || thread != b.ThreadID || state != "completed" || !stopped || creating || !runStopped || applied {
		return "", nil, nil
	}
	saved, readErr := loadProductSeal(ctx, tx, productSealID(task.ID, active))
	if readErr == sql.ErrNoRows {
		var known bool
		id := productSealID(task.ID, active)
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_seal_history' AND id=?) OR EXISTS(SELECT 1 FROM kernel_dispatch_intents WHERE run_id=?)", id, id).Scan(&known); err != nil {
			return "", nil, err
		}
		if !known {
			return "", nil, nil // No admitted sealing owner; ordinary capacity waits are silent.
		}
		readErr = ErrTaskEvidence
	}
	summary := &ProductSealingSummary{RunID: active, Seal: ProductVerificationPlanReference{ID: productSealID(task.ID, active)}, Workspace: ProductVerificationPlanReference{ID: productWorkspaceID(task.ID)}, State: "evidence_unavailable"}
	unavailable := func() (string, *ProductSealingSummary, error) {
		return "product_seal_evidence_unavailable", summary, nil
	}
	if readErr != nil {
		if readinessHoldError(readErr) || errors.Is(readErr, ErrProductSealHeld) {
			return unavailable()
		}
		return "", nil, readErr
	}
	summary.Seal.Digest, summary.Workspace, summary.OutputDigest = saved.Digest, ProductVerificationPlanReference{ID: saved.Value.Workspace.ID, Digest: saved.Value.Workspace.Digest}, saved.Value.OutputDigest
	if saved.State == "failed" || saved.State == "stale" || saved.State == "sealed" {
		var history productSealRecord
		if err := taskEvidenceRecord(ctx, tx, "product_seal_history", saved.ID, &history); err != nil {
			if readinessHoldError(err) {
				return unavailable()
			}
			return "", nil, err
		}
		digest, err := contentDigest(saved)
		if err != nil {
			return "", nil, err
		}
		if history.Digest != digest {
			return unavailable()
		}
		historyDigest, err := contentDigest(history.Seal)
		if err != nil {
			return "", nil, err
		}
		if historyDigest != digest {
			return unavailable()
		}
	}
	if saved.Value.TaskID != task.ID || saved.Value.RunID != active || saved.Value.Workspace.Value.SessionID != b.ID || saved.Value.Workspace.Value.Target != task.Target() {
		return unavailable()
	}
	var run contract.SessionRun
	if err := taskEvidenceRecord(ctx, tx, "agent_run", active, &run); err != nil {
		if readinessHoldError(err) {
			return unavailable()
		}
		return "", nil, err
	}
	if run.ID != active || run.ThreadID != thread || run.State != "completed" || run.TurnID == "" || !run.BaselineKnown || slices.Contains(run.BaselineTurnIDs, run.TurnID) || bytesDigest([]byte(run.Final)) != saved.Value.OutputDigest {
		return unavailable()
	}
	if interrupted, err := runInterruptedTx(ctx, tx, b.ID, active); err != nil {
		return "", nil, err
	} else if interrupted {
		return "", nil, nil
	}
	if _, _, err := k.productRunInputTx(ctx, tx, b, w, task, active, snapshot, thread); err != nil {
		if readinessHoldError(err) {
			return unavailable()
		}
		return "", nil, err
	}
	summary.State = saved.State
	if proof := saved.Proof; proof != nil {
		summary.Outcome = proof.Outcome
		summary.ProofDigest, err = contentDigest(proof)
		if err != nil {
			return "", nil, err
		}
		if r := proof.Resolution; r != nil {
			summary.Attempted = r.Attempted
			if f := r.Failure; f != nil {
				summary.HeadSHA, summary.MergeHeadSHA, summary.UnresolvedCount = f.HeadSHA, f.MergeHeadSHA, len(f.Unresolved)
			}
		}
		if proof.Frozen != nil {
			summary.HeadSHA = proof.Frozen.SHA
		}
	}
	switch saved.State {
	case "failed", "stale":
		return "product_seal_failed", summary, nil
	case "launching":
		return "product_seal_owner_unresolved", summary, nil
	case "sealed":
		if err := k.productWorkspaceOriginCurrentTx(ctx, tx, saved.Value.Workspace); err != nil {
			if readinessHoldError(err) || errors.Is(err, ErrProductWorkspaceHeld) {
				return "product_seal_adoption_held", summary, nil
			}
			return "", nil, err
		}
	}
	return "", nil, nil
}

// NotifyProductSealingAttention observes an admitted local owner after a stopped
// model run. It never retries Git, restarts a model or applies its final result.
// The serial executor calls it after its synchronous owner returns or on restart;
// a launching state is not proof that the Git owner has stopped.
func (k *KernelStore) NotifyProductSealingAttention(ctx context.Context, taskID string) (bool, error) {
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
	reason, summary, err := k.productSealingAttentionTx(ctx, tx, w, task)
	if err != nil || reason == "" {
		return false, err
	}
	detail, err := contentDigest(summary)
	if err != nil {
		return false, err
	}
	changed, err := k.saveParentAttentionTx(ctx, tx, w, task, reason, detail, parentAttentionInput{Sealing: summary})
	if err != nil {
		return false, err
	}
	return changed, tx.Commit()
}
