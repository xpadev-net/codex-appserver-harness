package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ResultApplication records logical application separately from the physical
// agent_run. A blocked reference may be reconciled without another model turn.
type ResultApplication struct {
	RunID    string             `json:"run_id"`
	TaskID   string             `json:"task_id"`
	Target   contract.Target    `json:"target"`
	ResultID string             `json:"result_id"`
	State    contract.TaskState `json:"state"`
	Reason   string             `json:"reason"`
	Archived bool               `json:"archived"`
}

var ErrTaskResultReferences = errors.New("Task result references require reconciliation")

func (k *KernelStore) ApplyTaskResult(ctx context.Context, sessionID, runID string) (ResultApplication, error) {
	receipt := ResultApplication{RunID: runID}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return receipt, err
	}
	if b.Kind != "task" {
		return receipt, errors.New("Task result requires a Task session")
	}
	task, err := loadTask(ctx, tx, b.TaskID)
	if err != nil {
		return receipt, err
	}
	receipt.TaskID, receipt.Target = task.ID, task.Target()
	var owner, thread, state, active string
	var stopped, applied int
	if err = tx.QueryRowContext(ctx, "SELECT session_id,thread_id,state,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", runID).Scan(&owner, &thread, &state, &stopped, &applied); err != nil {
		return receipt, err
	}
	if owner != b.ID || thread != b.ThreadID {
		return receipt, errors.New("Task result run binding conflict")
	}
	if applied == 1 {
		var prior ResultApplication
		if err = completionRecord(ctx, tx, "task_result_application", runID, &prior); err != nil {
			return receipt, err
		}
		if prior.TaskID != task.ID || prior.Target != task.Target() {
			return receipt, errors.New("Task result replay conflict")
		}
		return prior, nil
	}
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&active); err != nil {
		return receipt, err
	}
	var run contract.SessionRun
	if err = completionRecord(ctx, tx, "agent_run", runID, &run); err != nil {
		return receipt, err
	}
	if active != runID || state != "completed" || run.ID != runID || run.State != "completed" || run.TurnID == "" || run.ThreadID != b.ThreadID || stopped != 1 {
		return receipt, errors.New("normal completed and stopped active run required")
	}
	wave, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return receipt, err
	}
	if task.State.Terminal() || wave.Target() != task.Target() || wave.State == domain.WaveCanceled || wave.State == domain.WaveCompleted {
		receipt.Archived, receipt.Reason = true, "result retained for a terminal or superseded Task/Wave"
		if !task.State.Terminal() {
			task.State, task.Reason, task.CompletedAt = contract.TaskCanceled, receipt.Reason, k.clock()
		}
		receipt.State = task.State
		return receipt, k.finishTaskResultTx(ctx, tx, b, task, receipt, nil)
	}
	var result contract.TaskResult
	validationErr := contract.DecodeStrict([]byte(run.Final), &result)
	if validationErr == nil {
		validationErr = result.Validate()
	}
	if validationErr == nil && (result.TaskID != task.ID || result.PhaseID != task.PhaseID || result.Target != task.Target()) {
		validationErr = errors.New("Task result identity/scope mismatch")
	}
	if validationErr == nil {
		known := map[string]bool{}
		for _, id := range task.Spec.Criteria {
			known[id] = true
		}
		for _, c := range result.CriterionResults {
			if !known[c.CriterionID] {
				validationErr = errors.New("Task result invents or repeats criterion coverage")
				break
			}
			delete(known, c.CriterionID)
		}
		if validationErr == nil && result.Outcome == "result" && len(known) > 0 {
			validationErr = errors.New("Task result omits assigned criterion coverage")
		}
	}
	if validationErr == nil && wave.Policy.Stage == "K" && len(result.WorkItemRefs) > 0 {
		validationErr = errors.New("K Task cannot claim product Candidate results")
	}
	if validationErr == nil && task.Spec.Kind == "review" && result.Outcome == "result" && result.Review == nil {
		validationErr = errors.New("review Task requires a structured review verdict")
	}
	if validationErr != nil {
		task.State, task.Reason, task.CompletedAt = contract.TaskFailed, "invalid Task result: "+validationErr.Error(), k.clock()
		receipt.State, receipt.Reason = task.State, task.Reason
		return receipt, k.finishTaskResultTx(ctx, tx, b, task, receipt, nil)
	}
	// P Candidate/evidence authorization is supplied by Task_8. Until connected,
	// fail closed rather than turning generic artifacts into product acceptance.
	if len(result.WorkItemRefs) > 0 || (result.Outcome == "result" && len(task.Spec.WorkItemIDs) > 0) {
		return k.pendingTaskResultTx(ctx, tx, task, receipt, "product result validation is not connected")
	}
	pendingReason := ""
	for _, id := range resultArtifactReferences(result) {
		a, err := loadArtifact(ctx, tx, id)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return k.invalidTaskResultTx(ctx, tx, b, task, receipt, "referenced Artifact ID was never issued")
			}
			return receipt, err
		}
		if a.State == "expired" || a.State == "deleting" {
			return k.invalidTaskResultTx(ctx, tx, b, task, receipt, "referenced Artifact is expired")
		}
		grant := a
		grant.State = "committed" // Permission is independent from a staged write.
		if err = artifactReadableTx(ctx, tx, b, wave, grant); err != nil {
			if errors.Is(err, ErrArtifactGrant) || errors.Is(err, ErrArtifactScope) {
				return k.invalidTaskResultTx(ctx, tx, b, task, receipt, "referenced Artifact is not granted in the current scope")
			}
			pendingReason = "Artifact dependency binding requires reconciliation"
			continue
		}
		if a.State != "committed" {
			pendingReason = "referenced Artifact is not committed"
			continue
		}
		if _, err = k.readArtifactObject(a); err != nil {
			pendingReason = "referenced Artifact content requires reconciliation"
		}
	}
	if result.Review != nil {
		matched := false
		unknownReviewInput := false
		for _, id := range task.Spec.InputArtifactIDs {
			a, err := k.currentArtifactTx(ctx, tx, wave, id)
			if err != nil {
				pendingReason = "review input is unavailable"
				unknownReviewInput = true
				continue
			}
			matched = matched || a.Digest == result.Review.TargetDigest
		}
		for _, dep := range task.Spec.DependsOn {
			prior, err := loadTask(ctx, tx, structureID("task-", wave.ID, dep.TaskID))
			if err != nil {
				return receipt, err
			}
			priorResult, err := k.completedTaskResultTx(ctx, tx, wave, prior)
			if err != nil {
				pendingReason = "review dependency result requires reconciliation"
				unknownReviewInput = true
				continue
			}
			for _, id := range resultArtifactReferences(priorResult) {
				if dep.ArtifactID != "" && id != dep.ArtifactID {
					continue
				}
				a, err := k.currentArtifactTx(ctx, tx, wave, id)
				if err != nil {
					pendingReason = "review dependency Artifact is unavailable"
					unknownReviewInput = true
					continue
				}
				if err = artifactReadableTx(ctx, tx, b, wave, a); err != nil {
					continue
				}
				matched = matched || a.Digest == result.Review.TargetDigest
			}
		}
		if !matched && !unknownReviewInput {
			task.State, task.Reason, task.CompletedAt = contract.TaskFailed, "review target is not a bound immutable input", k.clock()
			receipt.State, receipt.Reason = task.State, task.Reason
			return receipt, k.finishTaskResultTx(ctx, tx, b, task, receipt, nil)
		}
	}
	if result.Outcome == "waiting" {
		for _, id := range result.SubscriptionIDs {
			var owner, waveID string
			var scope, plan int
			var raw []byte
			err = tx.QueryRowContext(ctx, "SELECT session_id,wave_id,scope_revision,plan_revision,payload FROM kernel_subscriptions WHERE id=?", id).Scan(&owner, &waveID, &scope, &plan, &raw)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return receipt, err
			}
			if errors.Is(err, sql.ErrNoRows) || owner != b.ID || waveID != wave.ID || scope != wave.ScopeRevision || plan != wave.PlanRevision {
				return k.invalidTaskResultTx(ctx, tx, b, task, receipt, "waiting requires this Task's durable current subscriptions")
			}
			var subscription struct {
				Target contract.Target `json:"target"`
			}
			if err = json.Unmarshal(raw, &subscription); err != nil {
				return receipt, err
			}
			if subscription.Target != task.Target() {
				return k.invalidTaskResultTx(ctx, tx, b, task, receipt, "waiting subscription policy/target is stale")
			}
		}
	}
	if pendingReason != "" {
		return k.pendingTaskResultTx(ctx, tx, task, receipt, pendingReason)
	}
	if result.Outcome == "waiting" {
		task.State, task.Reason = contract.TaskWaiting, "waiting on durable subscriptions"
	} else if result.Outcome == "blocked" {
		task.State, task.Reason = contract.TaskBlocked, strings.Join(result.Blockers, "; ")
	} else {
		task.State, task.Reason, task.CompletedAt = contract.TaskCompleted, "", k.clock()
	}
	receipt.ResultID = structureID("result-", task.ID, runID)
	if task.State == contract.TaskCompleted {
		task.ResultID = receipt.ResultID
	}
	receipt.State, receipt.Reason = task.State, task.Reason
	return receipt, k.finishTaskResultTx(ctx, tx, b, task, receipt, &result)
}

func (k *KernelStore) invalidTaskResultTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, task domain.Task, receipt ResultApplication, reason string) (ResultApplication, error) {
	task.State, task.Reason, task.CompletedAt = contract.TaskFailed, reason, k.clock()
	receipt.State, receipt.Reason = task.State, reason
	return receipt, k.finishTaskResultTx(ctx, tx, b, task, receipt, nil)
}
func (k *KernelStore) pendingTaskResultTx(ctx context.Context, tx *sql.Tx, task domain.Task, receipt ResultApplication, reason string) (ResultApplication, error) {
	task.State, task.Reason = contract.TaskBlocked, reason
	receipt.State, receipt.Reason = task.State, reason
	if err := saveTask(ctx, tx, task); err != nil {
		return receipt, err
	}
	if err := kernelRecordTx(ctx, tx, "task_result_pending", receipt.RunID, task.ID, receipt); err != nil {
		return receipt, err
	}
	wave, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return receipt, err
	}
	identity, err := contentDigest([]string{receipt.RunID, reason})
	if err != nil {
		return receipt, err
	}
	eventID := receipt.RunID + ":result_pending:" + identity
	var exists int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_events WHERE id=?", eventID).Scan(&exists); err != nil {
		return receipt, err
	}
	if exists == 0 {
		if err = k.waveEventTx(ctx, tx, wave, "task_result_pending", task.ID, eventID, receipt); err != nil {
			return receipt, err
		}
	}
	if err := tx.Commit(); err != nil {
		return receipt, err
	}
	return receipt, ErrTaskResultReferences
}
func (k *KernelStore) finishTaskResultTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, task domain.Task, receipt ResultApplication, result *contract.TaskResult) error {
	if result != nil {
		if err := kernelRecordTx(ctx, tx, "task_result", receipt.ResultID, task.ID, result); err != nil {
			return err
		}
		for _, id := range resultArtifactReferences(*result) {
			if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO kernel_artifact_refs(artifact_id,task_id,kind) VALUES(?,?,'result')", id, task.ID); err != nil {
				return err
			}
		}
	}
	if err := saveTask(ctx, tx, task); err != nil {
		return err
	}
	if err := kernelRecordTx(ctx, tx, "task_result_application", receipt.RunID, task.ID, receipt); err != nil {
		return err
	}
	if err := kernelRecordTx(ctx, tx, "session_run_disposition", receipt.RunID, b.ID, map[string]string{"reason": "Task result application"}); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE kernel_run_bindings SET applied=1 WHERE run_id=?", receipt.RunID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE kernel_sessions SET active_run_id='' WHERE id=? AND active_run_id=? AND process_stopped=1", b.ID, receipt.RunID); err != nil {
		return err
	}
	wave, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return err
	}
	wave.ScopeRevision, wave.PlanRevision, wave.Policy.Revision = task.ScopeRevision, task.PlanRevision, task.PolicyRevision
	if err = k.waveEventTx(ctx, tx, wave, "task_result_applied", task.ID, receipt.RunID+":result_applied", receipt); err != nil {
		return err
	}
	return tx.Commit()
}
func (k *KernelStore) completedTaskResultTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, task domain.Task) (contract.TaskResult, error) {
	var result contract.TaskResult
	if task.State != contract.TaskCompleted || task.ResultID == "" || task.ScopeRevision != wave.ScopeRevision || task.PolicyRevision != wave.Policy.Revision {
		return result, errors.New("dependency result is not current and complete")
	}
	if err := completionRecord(ctx, tx, "task_result", task.ResultID, &result); err != nil {
		return result, err
	}
	if err := result.Validate(); err != nil {
		return result, err
	}
	if result.TaskID != task.ID || result.PhaseID != task.PhaseID || result.Target != task.Target() || result.Outcome != "result" {
		return result, errors.New("dependency result binding conflict")
	}
	for _, id := range resultArtifactReferences(result) {
		if _, err := k.currentArtifactTx(ctx, tx, wave, id); err != nil {
			return result, fmt.Errorf("dependency Artifact is unavailable: %w", err)
		}
	}
	return result, nil
}
