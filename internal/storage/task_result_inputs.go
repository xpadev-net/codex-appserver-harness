package storage

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// TaskResultInput transfers an already applied structured result as data. It
// neither reopens its source session nor grants reads of referenced artifacts.
// SourceSpec/Target are immutable; later cancellation of an omitted source Task
// must not change the new Task's saved input digest.
type TaskResultInput struct {
	ProductCandidate *ProductTaskCandidateSnapshot `json:"product_candidate,omitempty"`
	ResultID         string                        `json:"result_id"`
	RunID            string                        `json:"run_id"`
	TaskID           string                        `json:"task_id"`
	PhaseID          string                        `json:"phase_id"`
	Target           contract.Target               `json:"target"`
	SourceSpec       contract.TaskSpec             `json:"source_spec"`
	Result           contract.TaskResult           `json:"result"`
	Digest           string                        `json:"digest"`
}

func (k *KernelStore) taskResultInputTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, id string) (TaskResultInput, error) {
	var out TaskResultInput
	var owner string
	if err := tx.QueryRowContext(ctx, "SELECT work_item_id FROM records WHERE kind='task_result' AND id=?", id).Scan(&owner); err != nil {
		return out, err
	}
	source, err := loadTask(ctx, tx, owner)
	if err != nil {
		return out, err
	}
	if source.WaveID != wave.ID || source.ParentSessionID != wave.OrchestratorSessionID || source.ScopeRevision != wave.ScopeRevision || source.PolicyRevision != wave.Policy.Revision {
		return out, fmt.Errorf("%w: source result is outside current Wave/scope/policy", ErrTaskEvidence)
	}
	var result contract.TaskResult
	if err = taskEvidenceRecord(ctx, tx, "task_result", id, &result); err != nil {
		return out, err
	}
	if result.Validate() != nil || result.TaskID != source.ID || result.PhaseID != source.PhaseID || result.Target != source.Target() {
		return out, fmt.Errorf("%w: source result identity conflict", ErrTaskEvidence)
	}
	var raw []byte
	var runID, sessionID, state, threadID string
	var applied, stopped bool
	err = tx.QueryRowContext(ctx, `SELECT a.id,a.payload,r.session_id,r.state,r.thread_id,r.applied,r.process_stopped FROM records a JOIN kernel_run_bindings r ON r.run_id=a.id WHERE a.kind='task_result_application' AND a.work_item_id=? AND json_extract(a.payload,'$.result_id')=? ORDER BY a.rowid DESC LIMIT 1`, source.ID, id).Scan(&runID, &raw, &sessionID, &state, &threadID, &applied, &stopped)
	if err != nil {
		return out, err
	}
	var application ResultApplication
	if err = contract.DecodeStrict(raw, &application); err != nil {
		return out, fmt.Errorf("%w: invalid source application", ErrTaskEvidence)
	}
	if application.RunID != runID || application.TaskID != source.ID || application.Target != source.Target() || application.ResultID != id || application.Archived || !applied || !stopped || state != "completed" || sessionID != source.SessionID {
		return out, fmt.Errorf("%w: source result must be applied and stopped", ErrTaskEvidence)
	}
	expected := map[string]contract.TaskState{"result": contract.TaskCompleted, "waiting": contract.TaskWaiting, "blocked": contract.TaskBlocked}[result.Outcome]
	if application.State != expected {
		return out, fmt.Errorf("%w: source application outcome conflict", ErrTaskEvidence)
	}
	var active string
	var processStopped bool
	var creating bool
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating FROM kernel_sessions WHERE id=?", source.SessionID).Scan(&active, &processStopped, &creating); err != nil {
		return out, err
	}
	if active != "" || !processStopped || creating {
		return out, fmt.Errorf("%w: source session still requires reconciliation", ErrTaskEvidence)
	}
	binding, err := sessionBindingTx(ctx, tx, source.SessionID)
	if err != nil {
		return out, err
	}
	if binding.Kind != "task" || binding.TaskID != source.ID || binding.Target != source.Target() || binding.ThreadID != threadID || threadID == "" {
		return out, fmt.Errorf("%w: source session/thread conflict", ErrTaskEvidence)
	}
	var run contract.SessionRun
	if err = completionRecord(ctx, tx, "agent_run", runID, &run); err != nil {
		return out, err
	}
	if run.ID != runID || run.ThreadID != threadID || run.State != "completed" || run.TurnID == "" || !run.BaselineKnown || slices.Contains(run.BaselineTurnIDs, run.TurnID) {
		return out, fmt.Errorf("%w: source has no normal nonbaseline completion", ErrTaskEvidence)
	}
	var final contract.TaskResult
	if err = contract.DecodeStrict([]byte(run.Final), &final); err != nil {
		return out, fmt.Errorf("%w: source final output is unavailable", ErrTaskEvidence)
	}
	digest, err := contentDigest(result)
	if err != nil {
		return out, err
	}
	finalDigest, err := contentDigest(final)
	if err != nil {
		return out, err
	}
	if digest != finalDigest || id != structureID("result-", source.ID, runID) {
		return out, fmt.Errorf("%w: source result differs from applied run output", ErrTaskEvidence)
	}
	out = TaskResultInput{ResultID: id, RunID: runID, TaskID: source.ID, PhaseID: source.PhaseID, Target: source.Target(), SourceSpec: source.Spec, Result: result, Digest: digest}
	if (result.SchemaVersion == 2 || result.SchemaVersion == 3) && result.Outcome == "result" {
		candidate, err := loadProductTaskCandidate(ctx, tx, id)
		if err != nil {
			return out, err
		}
		out.ProductCandidate = &candidate
	}
	return out, nil
}

func (k *KernelStore) sourceResultDigestsTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, spec contract.TaskSpec) (map[string]string, error) {
	out := map[string]string{}
	for _, id := range spec.SourceResultIDs {
		input, err := k.taskResultInputTx(ctx, tx, wave, id)
		if err != nil {
			return nil, err
		}
		digest, err := contentDigest(input)
		if err != nil {
			return nil, err
		}
		out["source_result:"+id] = digest
	}
	return out, nil
}
