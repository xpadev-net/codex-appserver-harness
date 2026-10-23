package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
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
	if !UsesProductChangeOutput(w, task) || w.Target() != task.Target() || w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) || task.Paused || (task.State != contract.TaskRunning && task.State != contract.TaskBlocked) {
		return value, ErrProductSealHeld
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return value, err
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
	if err := result.Validate(); err != nil {
		return value, fmt.Errorf("%w: invalid product output contract", ErrTaskEvidence)
	}
	if result.SchemaVersion != 2 || result.Target != task.Target() || result.TaskID != task.ID || result.PhaseID != task.PhaseID || result.Outcome != "result" {
		return value, fmt.Errorf("%w: output assignment mismatch", ErrTaskEvidence)
	}
	ready, err := k.productChangeReferenceTx(ctx, tx, b, w, task, result.ProductChanges[0])
	if err != nil {
		return value, err
	}
	if !ready {
		return value, ErrProductSealHeld
	}
	snapshot := contract.InputSnapshot{ID: snapshotID}
	if err := tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", snapshotID).Scan(&snapshot.Digest, &snapshot.Input); err != nil {
		return value, err
	}
	if err := contract.ValidateSessionInput(snapshot.Input); err != nil {
		return value, fmt.Errorf("%w: invalid input snapshot", ErrTaskEvidence)
	}
	if bytesDigest(snapshot.Input) != snapshot.Digest {
		return value, fmt.Errorf("%w: input snapshot checksum mismatch", ErrTaskEvidence)
	}
	var budget RunBudgetBinding
	if err := taskEvidenceRecord(ctx, tx, "run_budget", runID, &budget); err != nil {
		return value, err
	}
	if budget.RunID != runID || budget.SessionID != b.ID || budget.TaskID != task.ID || budget.ThreadID != thread || budget.SnapshotID != snapshotID || budget.SnapshotDigest != snapshot.Digest || !slices.Equal(budget.Targets, []contract.Target{w.Target()}) {
		return value, fmt.Errorf("%w: run budget binding mismatch", ErrTaskEvidence)
	}
	var envelope struct {
		SchemaVersion int               `json:"schema_version"`
		SessionID     string            `json:"session_id"`
		Targets       []contract.Target `json:"targets"`
		Context       SessionContext    `json:"context"`
	}
	if err := json.Unmarshal(snapshot.Input, &envelope); err != nil {
		return value, fmt.Errorf("%w: invalid input envelope", ErrTaskEvidence)
	}
	if envelope.SchemaVersion != 1 || envelope.SessionID != b.ID || !slices.Equal(envelope.Targets, []contract.Target{w.Target()}) || len(envelope.Context.Waves) != 1 || envelope.Context.Waves[0].Target != w.Target() {
		return value, fmt.Errorf("%w: snapshot session or target mismatch", ErrTaskEvidence)
	}
	ref := envelope.Context.Waves[0].Manifest
	var grant inputReferenceGrant
	if err := taskEvidenceRecord(ctx, tx, "session_input_references", snapshotID, &grant); err != nil {
		return value, err
	}
	if grant.SessionID != b.ID || !slices.Contains(grant.References, ref) || ref.Target != w.Target() || ref.Section != "manifest" {
		return value, fmt.Errorf("%w: manifest grant mismatch", ErrTaskEvidence)
	}
	var manifest savedInputManifest
	if err := taskEvidenceRecord(ctx, tx, "session_input_manifest", ref.ID, &manifest); err != nil {
		return value, err
	}
	if manifest.SessionID != b.ID || manifest.Reference != ref || int64(len(manifest.Data)) != ref.Bytes || bytesDigest(manifest.Data) != ref.Digest {
		return value, fmt.Errorf("%w: manifest checksum mismatch", ErrTaskEvidence)
	}
	var input sessionManifest
	if err := json.Unmarshal(manifest.Data, &input); err != nil {
		return value, fmt.Errorf("%w: invalid saved manifest", ErrTaskEvidence)
	}
	if input.SessionID != b.ID || input.Kind != "task" || input.Wave.Target() != w.Target() || input.ProductWorkspace == nil || input.ProductWorkspace.Reference != result.ProductChanges[0] {
		return value, fmt.Errorf("%w: workspace manifest assignment mismatch", ErrTaskEvidence)
	}
	owners := 0
	for _, assigned := range input.Tasks {
		if assigned.ID != task.ID {
			continue
		}
		owners++
		if assigned.Target() != task.Target() || assigned.InputDigest != task.InputDigest || !reflect.DeepEqual(assigned.Spec, task.Spec) {
			return value, fmt.Errorf("%w: saved Task assignment mismatch", ErrTaskEvidence)
		}
	}
	if owners != 1 {
		return value, fmt.Errorf("%w: saved Task owner is not unique", ErrTaskEvidence)
	}
	value = ProductSealValue{TaskID: taskID, RunID: runID, Workspace: *saved, InputDigest: inputs, OutputDigest: bytesDigest([]byte(run.Final)), SnapshotID: snapshotID, SnapshotDigest: snapshot.Digest}
	return value, nil
}
