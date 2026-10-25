package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"reflect"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func bytesDigest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

// This is host admission for local sealing, independent of model declarations.
// It does not apply the Task result or give the candidate a quality verdict.
func (k *KernelStore) productSealEvidenceTx(ctx context.Context, tx *sql.Tx, taskID, runID string) (ProductSealValue, error) {
	var value ProductSealValue
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		return value, err
	}
	w, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return value, err
	}
	if (!UsesProductChangeOutput(w, task) && !UsesProductVerificationOutput(w, task)) || w.Target() != task.Target() || w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) || task.Paused || (task.State != contract.TaskRunning && task.State != contract.TaskBlocked) {
		return value, ErrProductSealHeld
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return value, err
	}
	var sourcePermit productExecutionPermit
	held, err := k.productWorkItemExecutionPermitTx(ctx, tx, w, task.Spec.WorkItemIDs[0], &sourcePermit)
	if err != nil {
		return value, err
	}
	if held {
		return value, ErrProductSealHeld
	}
	b, err := sessionBindingTx(ctx, tx, task.SessionID)
	if err != nil {
		return value, err
	}
	if b.Kind != "task" || b.TaskID != task.ID || b.Target != task.Target() || b.ParentSessionID != w.OrchestratorSessionID || b.ThreadID == "" || b.Closed {
		return value, fmt.Errorf("%w: Task/session ownership conflict", ErrTaskEvidence)
	}
	saved, err := k.fixedProductWorkspaceInputTx(ctx, tx, w, task.Spec)
	if err != nil {
		return value, err
	}
	if saved == nil || b.CWD != saved.Value.CWD || b.Sandbox != "workspace-write" {
		return value, ErrProductSealHeld
	}
	inputs, err := k.taskInputWithWorkspaceTx(ctx, tx, w, task.Spec, saved)
	if err != nil {
		return value, err
	}
	origin, err := k.taskInputWithWorkspaceTx(ctx, tx, w, task.Spec, nil)
	if err != nil {
		return value, err
	}
	if inputs != task.InputDigest || origin != saved.Value.InputDigest {
		return value, ErrProductSealHeld
	}
	var owner, thread, state, snapshotID, active string
	var stopped, applied, sessionStopped, creating bool
	if err := tx.QueryRowContext(ctx, "SELECT session_id,thread_id,state,snapshot_id,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", runID).Scan(&owner, &thread, &state, &snapshotID, &stopped, &applied); err != nil {
		return value, err
	}
	if err := tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &sessionStopped, &creating); err != nil {
		return value, err
	}
	if owner != b.ID || thread != b.ThreadID || state != "completed" || !stopped || applied || active != runID || !sessionStopped || creating {
		return value, ErrProductSealHeld
	}
	var run contract.SessionRun
	if err := taskEvidenceRecord(ctx, tx, "agent_run", runID, &run); err != nil {
		return value, err
	}
	if run.ID != runID || run.ThreadID != thread || run.State != "completed" || run.TurnID == "" || !run.BaselineKnown || slices.Contains(run.BaselineTurnIDs, run.TurnID) {
		return value, fmt.Errorf("%w: run is not a normal nonbaseline completion", ErrTaskEvidence)
	}
	interrupted, err := runInterruptedTx(ctx, tx, b.ID, runID)
	if err != nil {
		return value, err
	}
	if interrupted {
		return value, ErrProductSealHeld
	}
	var result contract.TaskResult
	if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
		return value, fmt.Errorf("%w: invalid structured final", ErrTaskEvidence)
	}
	if err := result.ValidateAssignment(task.Target(), task.ID, task.PhaseID, task.Spec.Criteria); err != nil {
		return value, fmt.Errorf("%w: invalid product output contract", ErrTaskEvidence)
	}
	if (UsesProductChangeOutput(w, task) && result.SchemaVersion != 2 || UsesProductVerificationOutput(w, task) && result.SchemaVersion != 3) || result.Outcome != "result" {
		return value, fmt.Errorf("%w: output assignment mismatch", ErrTaskEvidence)
	}
	ready, err := k.productChangeReferenceTx(ctx, tx, b, w, task, result.ProductChanges[0])
	if err != nil {
		return value, err
	}
	if !ready {
		return value, ErrProductSealHeld
	}
	snapshot, input, err := k.productRunInputTx(ctx, tx, b, w, task, runID, snapshotID, thread)
	if err != nil {
		return value, err
	}
	if input.ProductWorkspace == nil || input.ProductWorkspace.Reference != result.ProductChanges[0] {
		return value, fmt.Errorf("%w: workspace manifest assignment mismatch", ErrTaskEvidence)
	}
	value = ProductSealValue{TaskID: taskID, RunID: runID, Workspace: *saved, InputDigest: inputs, OutputDigest: bytesDigest([]byte(run.Final)), SnapshotID: snapshotID, SnapshotDigest: snapshot.Digest}
	verificationInput, err := k.productVerificationSealInputTx(ctx, tx, w, task, *saved, result)
	if err != nil {
		return value, err
	}
	if verificationInput != nil {
		expectedPolicy := publicProductVerificationPolicy(verificationInput.Policy)
		if input.ProductSubject == nil || !reflect.DeepEqual(*input.ProductSubject, verificationInput.Subject) || input.ProductVerificationPolicy == nil || !reflect.DeepEqual(*input.ProductVerificationPolicy, expectedPolicy) {
			return value, ErrTaskEvidence
		}
		value.Verification = verificationInput
	}
	if !sourcePermit.current(k, tx) {
		return value, ErrProductSealHeld
	}
	return value, nil
}
