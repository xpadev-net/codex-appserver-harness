package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ReadyRequest is a trusted controller request. Recovery is an explicit decision
// after classifying a failure; a blocked Task is never retried just by polling.
// Resume selects waiting Tasks after the controller has reserved their wake.
type ReadyRequest struct {
	Target          contract.Target
	RecoveryTaskIDs []string
	ResumeTaskIDs   []string
}

type ReadyTask struct {
	Task        domain.Task `json:"task"`
	InputDigest string      `json:"input_digest"`
}
type TaskHold struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}
type ReadySet struct {
	Target  contract.Target `json:"target"`
	PhaseID string          `json:"phase_id"`
	State   string          `json:"state"`
	Reason  string          `json:"reason"`
	Tasks   []ReadyTask     `json:"tasks"`
	Held    []TaskHold      `json:"held"`
}

func readinessHoldError(err error) bool {
	return errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrTaskEvidence) || errors.Is(err, ErrTaskBinding) || errors.Is(err, ErrArtifactUnavailable) || errors.Is(err, ErrArtifactScope) || errors.Is(err, ErrArtifactIntegrity) || errors.Is(err, ErrTemplateNeedsMain) || errors.Is(err, ErrProductContinuationHeld) || errors.Is(err, ErrProductSplitHeld)
}

// ReadyTasks evaluates only saved facts. Selection reserves no process or run;
// dispatch must check the same conditions again in its write transaction.
func (k *KernelStore) ReadyTasks(ctx context.Context, request ReadyRequest) (ReadySet, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ReadySet{}, err
	}
	defer tx.Rollback()
	set, tasks, drain, err := k.activePhaseTx(ctx, tx, request.Target)
	if err != nil {
		return set, err
	}
	allowed := func(ids []string, state contract.TaskState) (map[string]bool, error) {
		out := map[string]bool{}
		for _, id := range ids {
			found := false
			for _, task := range tasks {
				found = found || (task.ID == id && task.State == state)
			}
			if !found || out[id] {
				return nil, errors.New("recovery/resume must name distinct Tasks in the active Phase with the expected state")
			}
			out[id] = true
		}
		return out, nil
	}
	recoveries, err := allowed(request.RecoveryTaskIDs, contract.TaskBlocked)
	if err != nil {
		return set, err
	}
	resumes, err := allowed(request.ResumeTaskIDs, contract.TaskWaiting)
	if err != nil {
		return set, err
	}
	if set.State != "active" {
		return set, tx.Commit()
	}
	wave, err := loadWave(ctx, tx, request.Target.WaveID)
	if err != nil {
		return set, err
	}
	for _, task := range tasks {
		if task.State.Terminal() {
			continue
		}
		reason := ""
		switch task.State {
		case contract.TaskBlocked:
			if !recoveries[task.ID] {
				reason = "recovery_required"
			}
		case contract.TaskWaiting:
			if !resumes[task.ID] {
				reason = "wake_required"
			}
		case contract.TaskReady:
		case contract.TaskRunning:
			reason = "run_in_progress"
		default:
			return set, errors.New("unknown persisted Task state")
		}
		if reason == "" {
			reason, err = k.taskReadyTx(ctx, tx, wave, task, drain, false)
		}
		if err != nil {
			return set, err
		}
		if reason != "" {
			set.Held = append(set.Held, TaskHold{TaskID: task.ID, Reason: reason})
			continue
		}
		if task.State == contract.TaskWaiting {
			var count int
			if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_wake_intents WHERE session_id=? AND state='reserved'", task.SessionID).Scan(&count); err != nil {
				return set, err
			}
			if count != 1 {
				set.Held = append(set.Held, TaskHold{task.ID, "wake_required"})
				continue
			}
		}
		input, err := k.taskInputDigestTx(ctx, tx, wave, task.Spec)
		if err != nil {
			if !readinessHoldError(err) {
				return set, err
			}
			set.Held = append(set.Held, TaskHold{task.ID, "input_unavailable"})
			continue
		}
		set.Tasks = append(set.Tasks, ReadyTask{Task: task, InputDigest: input})
	}
	return set, tx.Commit()
}

// activePhaseTx derives progress, never copies WorkItem.phase into a Phase.
// A completed report (including a review reporting defects) satisfies execution
// completion; product quality/merge gates remain independent.
func (k *KernelStore) activePhaseTx(ctx context.Context, tx *sql.Tx, target contract.Target) (ReadySet, []domain.Task, bool, error) {
	set := ReadySet{Target: target, State: "held", Tasks: []ReadyTask{}, Held: []TaskHold{}}
	if !target.Valid() {
		return set, nil, false, errors.New("versioned scheduler target required")
	}
	wave, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return set, nil, false, err
	}
	if wave.Target() != target {
		return set, nil, false, errors.New("stale scheduler target")
	}
	switch {
	case wave.DispatchPaused():
		set.Reason = "paused"
	case wave.State == domain.WaveCanceled || wave.State == domain.WaveCompleted:
		set.Reason = "terminal"
	case !k.clock().Before(wave.Deadline):
		set.Reason = "deadline_exceeded"
	case wave.State == domain.WavePlanning && wave.DirectionID != "":
		set.Reason = "plan_required"
	}
	if set.Reason != "" {
		return set, nil, false, nil
	}
	var raw []byte
	var digest string
	err = tx.QueryRowContext(ctx, "SELECT digest,payload FROM kernel_plans WHERE wave_id=? AND revision=?", wave.ID, wave.PlanRevision).Scan(&digest, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		set.Reason = "plan_required"
		return set, nil, false, nil
	}
	if err != nil {
		return set, nil, false, err
	}
	var admission PlanAdmission
	if err = json.Unmarshal(raw, &admission); err != nil {
		return set, nil, false, err
	}
	actual, err := contentDigest(admission.Plan)
	if err != nil {
		return set, nil, false, err
	}
	if admission.Target != target || admission.Digest != digest || actual != digest {
		return set, nil, false, errors.New("saved scheduler plan binding conflict")
	}
	if err = domain.ValidateWavePlan(wave, admission.Plan); err != nil {
		return set, nil, false, err
	}
	for position, spec := range admission.Plan.Phases {
		phaseID := structureID("phase-", wave.ID, spec.ID)
		var phase domain.WavePhase
		if err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_phases WHERE id=? AND wave_id=?", phaseID, wave.ID).Scan(&raw); err != nil {
			return set, nil, false, err
		}
		if err = json.Unmarshal(raw, &phase); err != nil {
			return set, nil, false, err
		}
		if phase.ID != phaseID || phase.WaveID != wave.ID || phase.Position != position || !reflect.DeepEqual(phase.Spec, spec) {
			return set, nil, false, errors.New("saved scheduler Phase conflict")
		}
		tasks := make([]domain.Task, 0, len(spec.Tasks))
		complete, drain := true, false
		for _, expected := range spec.Tasks {
			task, err := loadTask(ctx, tx, structureID("task-", wave.ID, expected.ID))
			if err != nil {
				return set, nil, false, err
			}
			if task.PhaseID != phaseID || task.LocalID != expected.ID || !reflect.DeepEqual(task.Spec, expected) {
				return set, nil, false, errors.New("saved scheduler Task conflict")
			}
			if err = currentObservedTaskTx(ctx, tx, wave, task); err != nil {
				return set, nil, false, err
			}
			tasks = append(tasks, task)
			if task.State == contract.TaskCompleted {
				if _, err = k.completedTaskResultTx(ctx, tx, wave, task); err != nil {
					if !readinessHoldError(err) {
						return set, nil, false, err
					}
					complete, drain = false, true
				}
			} else {
				complete = false
			}
			if task.State == contract.TaskBlocked || task.State == contract.TaskFailed || task.State == contract.TaskCanceled {
				drain = true
			}
			var state string
			err = tx.QueryRowContext(ctx, "SELECT r.state FROM kernel_sessions s JOIN kernel_run_bindings r ON r.run_id=s.active_run_id WHERE s.id=?", task.SessionID).Scan(&state)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return set, nil, false, err
			}
			if state == "unknown" || state == "blocked" || state == "failed" || state == "interrupted" {
				drain = true
			}
		}
		if complete {
			phaseReason := ""
			for _, task := range tasks {
				if task.Spec.Kind != "review" {
					continue
				}
				result, err := k.completedTaskResultTx(ctx, tx, wave, task)
				if err != nil {
					return set, nil, false, err
				}
				reason := ""
				if result.Review == nil || result.Review.Verdict == "inconclusive" {
					reason = "review_inconclusive"
				} else if result.Review.Verdict == "findings" {
					expanded, err := k.expandedReviewResultTx(ctx, tx, wave, task)
					if err != nil {
						return set, nil, false, err
					}
					if !expanded {
						reason = "review_findings"
					}
				}
				if reason != "" {
					phaseReason = reason
					if reason == "review_inconclusive" {
						break
					}
				}
			}
			if phaseReason != "" {
				set.PhaseID, set.Reason = phaseID, phaseReason
				return set, tasks, false, nil
			}
			continue
		}
		set.PhaseID, set.State = phaseID, "active"
		if drain {
			set.Reason = "drain_and_block"
		}
		return set, tasks, drain, nil
	}
	set.State, set.Reason = "execution_completed", "all_success"
	return set, nil, false, nil
}

func (k *KernelStore) taskReadyTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, task domain.Task, drain, prepared bool) (string, error) {
	if task.Target() != wave.Target() || task.State.Terminal() {
		return "stale_or_terminal", nil
	}
	if task.Paused {
		return "paused", nil
	}
	if drain && task.Attempts == 0 && task.State != contract.TaskBlocked {
		return "drain_and_block", nil
	}
	if task.State != contract.TaskReady && task.State != contract.TaskBlocked && task.State != contract.TaskWaiting {
		return "run_in_progress", nil
	}
	if task.Attempts >= wave.Policy.Limits.TaskAttempts {
		return "attempts_exhausted", nil
	}
	used, _, saturated, err := waveTokensTx(ctx, tx, wave.ID)
	if err != nil {
		return "", err
	}
	if saturated || used >= wave.Policy.Limits.Tokens {
		return "token_budget_exhausted", nil
	}
	var active string
	var creating, stopped int
	if err := tx.QueryRowContext(ctx, "SELECT creating,process_stopped,active_run_id FROM kernel_sessions WHERE id=?", task.SessionID).Scan(&creating, &stopped, &active); err != nil {
		return "", err
	}
	if active != "" || creating != 0 || (!prepared && stopped != 1) {
		return "process_reconciliation_required", nil
	}
	// Product execution requires this live host runtime plus authenticated local
	// preparation. Publication/merge retain their separate canonical gates.
	if len(task.Spec.WorkItemIDs) > 0 {
		if reason, err := k.productTaskReadinessTx(ctx, tx, wave, task); err != nil || reason != "" {
			return reason, err
		}
	}
	for _, dependency := range task.Spec.DependsOn {
		prior, err := loadTask(ctx, tx, structureID("task-", wave.ID, dependency.TaskID))
		if err != nil {
			return "", err
		}
		if err = currentObservedTaskTx(ctx, tx, wave, prior); err != nil {
			if !readinessHoldError(err) {
				return "", err
			}
			return "dependency_unavailable", nil
		}
		if _, err = k.completedTaskResultTx(ctx, tx, wave, prior); err != nil {
			if !readinessHoldError(err) {
				return "", err
			}
			return "dependency_unavailable", nil
		}
	}
	if _, err := k.templateReviewSubjectTx(ctx, tx, task); err != nil {
		if !readinessHoldError(err) {
			return "", err
		}
		return "template_subject_unavailable", nil
	}
	if _, err := k.taskInputDigestTx(ctx, tx, wave, task.Spec); err != nil {
		if !readinessHoldError(err) {
			return "", err
		}
		return "input_unavailable", nil
	}
	return "", nil
}

func (k *KernelStore) checkTaskDispatchTx(ctx context.Context, tx *sql.Tx, task domain.Task) error {
	set, tasks, drain, err := k.activePhaseTx(ctx, tx, task.Target())
	if err != nil {
		return err
	}
	if set.State != "active" || task.PhaseID != set.PhaseID {
		return fmt.Errorf("Task Phase not ready: %s", set.Reason)
	}
	for _, current := range tasks {
		if current.ID != task.ID {
			continue
		}
		wave, err := loadWave(ctx, tx, task.WaveID)
		if err != nil {
			return err
		}
		reason, err := k.taskReadyTx(ctx, tx, wave, current, drain, true)
		if err != nil {
			return err
		}
		if reason != "" {
			if strings.HasPrefix(reason, "product_") {
				return fmt.Errorf("%w: Task not ready: %s", ErrProductAdmissionChanged, reason)
			}
			return fmt.Errorf("Task not ready: %s", reason)
		}
		return nil
	}
	return errors.New("Task not in current Phase")
}
