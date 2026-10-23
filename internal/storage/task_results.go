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
	return k.applyTaskResult(ctx, sessionID, runID, nil)
}

// AdoptProductTaskResult is a host-only port. baseBranch comes from trusted
// repository configuration, never model output, Issue prose or a dynamic tool.
// Candidate adoption and normal Task result application commit together.
func (k *KernelStore) AdoptProductTaskResult(ctx context.Context, sessionID, runID, baseBranch string) (ResultApplication, error) {
	if strings.TrimSpace(baseBranch) == "" || len(baseBranch) > contract.MaxInline || strings.ContainsAny(baseBranch, "\x00\r\n\t ") {
		return ResultApplication{}, errors.New("trusted repository base branch required")
	}
	return k.applyTaskResult(ctx, sessionID, runID, &baseBranch)
}

func (k *KernelStore) applyTaskResult(ctx context.Context, sessionID, runID string, productBaseBranch *string) (ResultApplication, error) {
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
		if productBaseBranch != nil && prior.ResultID != "" && !prior.Archived {
			adopted, err := loadProductTaskCandidate(ctx, tx, prior.ResultID)
			if err != nil {
				return receipt, err
			}
			if adopted.Value.BaseBranch != *productBaseBranch {
				return receipt, errors.New("product adoption base branch replay conflict")
			}
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
	interrupted, err := runInterruptedTx(ctx, tx, b.ID, runID)
	if err != nil {
		return receipt, err
	}
	if interrupted {
		task.State, task.Reason = contract.TaskBlocked, "operator_interrupt: completed history retained without accepting success"
		receipt.State, receipt.Reason, receipt.Archived = task.State, task.Reason, true
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
	if validationErr == nil && result.SchemaVersion == 2 && !UsesProductChangeOutput(wave, task) {
		validationErr = errors.New("TaskResult schema 2 requires a trusted P work/fix assignment")
	}
	if validationErr == nil && task.Spec.Kind == "review" && result.Outcome == "result" && result.Review == nil {
		validationErr = errors.New("review Task requires a structured review verdict")
	}
	if validationErr != nil {
		task.State, task.Reason, task.CompletedAt = contract.TaskFailed, "invalid Task result: "+validationErr.Error(), k.clock()
		receipt.State, receipt.Reason = task.State, task.Reason
		return receipt, k.finishTaskResultTx(ctx, tx, b, task, receipt, nil)
	}
	var productSeal *ProductSeal
	if productBaseBranch != nil && (!UsesProductChangeOutput(wave, task) || result.SchemaVersion != 2 || result.Outcome != "result") {
		return receipt, errors.New("product adoption requires successful P work/fix output")
	}
	if result.SchemaVersion == 2 && result.Outcome == "result" {
		ready, err := k.productChangeReferenceTx(ctx, tx, b, wave, task, result.ProductChanges[0])
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrTaskEvidence) {
			return k.pendingTaskResultTx(ctx, tx, task, receipt, "product change workspace evidence is unavailable")
		}
		if errors.Is(err, ErrProductChangeReference) {
			return k.invalidTaskResultTx(ctx, tx, b, task, receipt, err.Error())
		}
		if err != nil {
			return receipt, err
		}
		if !ready {
			return k.pendingTaskResultTx(ctx, tx, task, receipt, "product change workspace is not ready")
		}
		if productBaseBranch == nil {
			return k.pendingTaskResultTx(ctx, tx, task, receipt, "product candidate adoption requires the trusted host")
		}
		sealed, err := k.productSealForAdoptionTx(ctx, tx, task.ID, runID)
		if readinessHoldError(err) || errors.Is(err, ErrProductSealHeld) || errors.Is(err, ErrProductChangeReference) {
			return k.pendingTaskResultTx(ctx, tx, task, receipt, "product candidate sealing evidence is unavailable or stale")
		}
		if err != nil {
			return receipt, err
		}
		productSeal = &sealed
	}
	// P Candidate/evidence authorization is supplied by Task_8. Until connected,
	// fail closed rather than turning generic artifacts into product acceptance.
	if len(result.WorkItemRefs) > 0 || (result.Outcome == "result" && len(task.Spec.WorkItemIDs) > 0 && productSeal == nil) {
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
		if task.Spec.Kind == "review" {
			var expected templateReviewTarget
			err := completionRecord(ctx, tx, "template_review_target", task.ID, &expected)
			if err == nil && (expected.RunID != runID || expected.Target != task.Target() || expected.Subject.Digest != result.Review.TargetDigest) {
				return k.invalidTaskResultTx(ctx, tx, b, task, receipt, "review target does not match the bound replacement subject")
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return receipt, err
			}
			if errors.Is(err, sql.ErrNoRows) {
				var input templateTaskInput
				contextErr := completionRecord(ctx, tx, "template_task_context", task.ID, &input)
				if contextErr == nil {
					pendingReason = "generated review target binding requires reconciliation"
				} else if !errors.Is(contextErr, sql.ErrNoRows) {
					return receipt, contextErr
				}
			}
		}
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
	if err = k.replacePRWaitsTx(ctx, tx, b.ID, task.Target()); err != nil {
		return receipt, err
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
		if err = k.awaitPRSubscriptionsTx(ctx, tx, b.ID, runID, task.Target(), result.SubscriptionIDs); err != nil {
			return receipt, err
		}
		task.State, task.Reason = contract.TaskWaiting, "waiting on durable subscriptions"
	} else if result.Outcome == "blocked" {
		task.State, task.Reason = contract.TaskBlocked, strings.Join(result.Blockers, "; ")
	} else {
		task.State, task.Reason, task.CompletedAt = contract.TaskCompleted, "", k.clock()
	}
	receipt.ResultID = structureID("result-", task.ID, runID)
	if productSeal != nil {
		if err := k.adoptProductCandidateTx(ctx, tx, task, receipt.ResultID, *productSeal, *productBaseBranch); err != nil {
			return receipt, err
		}
	}
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
		if task.State == contract.TaskCompleted {
			if err := k.recordTemplateSubjectTx(ctx, tx, task, *result); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "UPDATE kernel_plan_bindings SET source_result_id=?,input_digest=? WHERE wave_id=? AND revision=? AND task_id=?", task.ResultID, task.InputDigest, task.WaveID, task.PlanRevision, task.ID); err != nil {
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
	if err := clearSessionRunHoldTx(ctx, tx, b.ID, receipt.RunID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE kernel_run_bindings SET applied=1 WHERE run_id=?", receipt.RunID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE kernel_sessions SET active_run_id='' WHERE id=? AND active_run_id=? AND process_stopped=1", b.ID, receipt.RunID); err != nil {
		return err
	}
	wakeOutcome := "archived"
	if receipt.Archived && receipt.State == contract.TaskBlocked {
		// Interrupted work did not consume its inbox successfully. Keep inputs
		// pending; a blocked Task still requires explicit recovery authorization.
		wakeOutcome = "failed"
	}
	if result != nil && !receipt.Archived {
		wakeOutcome = "applied"
	}
	if err := k.finishWakeTx(ctx, tx, b.ID, receipt.RunID, wakeOutcome); err != nil {
		return err
	}
	if err := k.finishDirectOperationInputsTx(ctx, tx, b.ID, receipt.RunID, wakeOutcome); err != nil {
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
		return result, fmt.Errorf("%w: dependency result is not current and complete", ErrTaskEvidence)
	}
	if err := taskEvidenceRecord(ctx, tx, "task_result", task.ResultID, &result); err != nil {
		return result, err
	}
	if err := result.Validate(); err != nil {
		return result, fmt.Errorf("%w: %v", ErrTaskEvidence, err)
	}
	if result.TaskID != task.ID || result.PhaseID != task.PhaseID || result.Target != task.Target() || result.Outcome != "result" {
		return result, fmt.Errorf("%w: dependency result binding conflict", ErrTaskEvidence)
	}
	if result.SchemaVersion == 2 {
		if _, err := loadProductTaskCandidate(ctx, tx, task.ResultID); err != nil {
			return result, err
		}
	}
	for _, id := range resultArtifactReferences(result) {
		if _, err := k.currentArtifactTx(ctx, tx, wave, id); err != nil {
			return result, fmt.Errorf("dependency Artifact is unavailable: %w", err)
		}
	}
	return result, nil
}
