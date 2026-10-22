package storage

import (
	"context"
	"database/sql"
	"errors"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// A findings report completes execution, but later phases need its saved
// fix/review expansion. Require the generated fix/review Tasks to remain bound
// to this plan, so a parent cannot drop the repair while carrying the report.
func expandedReviewResultTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, task domain.Task) (bool, error) {
	ids, err := inputIDsTx(ctx, tx, `SELECT id FROM records WHERE kind='template_expansion' AND work_item_id=? AND EXISTS(SELECT 1 FROM json_each(json_extract(payload,'$.source_result_ids')) WHERE value=?) ORDER BY id`, wave.ID, task.ResultID)
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		var expansion TemplateExpansion
		if err = completionRecord(ctx, tx, "template_expansion", id, &expansion); err != nil {
			return false, err
		}
		if expansion.Key != id || expansion.Request.Target.WaveID != wave.ID || expansion.Request.Target.ScopeRevision != wave.ScopeRevision || expansion.Request.Target.PolicyRevision != wave.Policy.Revision || !slices.Contains(expansion.SourceResultIDs, task.ResultID) || expansion.Admission.Mode != "template" {
			return false, errors.New("template progression receipt conflict")
		}
		valid, reviews, fixes := true, 0, 0
		for _, phase := range expansion.Admission.Plan.Phases {
			for _, spec := range phase.Tasks {
				taskID := structureID("task-", wave.ID, spec.ID)
				if !slices.Contains(expansion.Admission.CreatedTaskIDs, taskID) || spec.Kind != "fix" && spec.Kind != "review" {
					continue
				}
				bound, err := templateTaskBoundTx(ctx, tx, wave, taskID, map[string]bool{})
				if err != nil {
					return false, err
				}
				valid = valid && bound
				if spec.Kind == "review" {
					reviews++
				} else {
					fixes++
				}
			}
		}
		if valid && reviews > 0 && fixes > 0 {
			return true, nil
		}
	}
	return false, nil
}

type templateTaskClone struct {
	OriginalID   string `json:"source_task_id"`
	ExpansionKey string `json:"expansion_key"`
}

func templateTaskBoundTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, id string, seen map[string]bool) (bool, error) {
	if seen[id] || len(seen) >= wave.Policy.Limits.Tasks {
		return false, errors.New("template clone lineage exceeds Task bound")
	}
	seen[id] = true
	task, err := loadTask(ctx, tx, id)
	if err != nil {
		return false, err
	}
	if task.State != contract.TaskCanceled && task.State != contract.TaskFailed {
		err = currentObservedTaskTx(ctx, tx, wave, task)
		if err == nil {
			return true, nil
		}
		if !readinessHoldError(err) {
			return false, err
		}
	}
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM records WHERE kind='template_future_clone' AND work_item_id=? AND json_extract(payload,'$.source_task_id')=? ORDER BY rowid DESC", wave.ID, id)
	if err != nil {
		return false, err
	}
	for _, replacement := range ids {
		var clone templateTaskClone
		if err = completionRecord(ctx, tx, "template_future_clone", replacement, &clone); err != nil {
			return false, err
		}
		var expansion TemplateExpansion
		if err = completionRecord(ctx, tx, "template_expansion", clone.ExpansionKey, &expansion); err != nil {
			return false, err
		}
		if clone.OriginalID != id || expansion.Key != clone.ExpansionKey || expansion.Admission.Mode != "template" || expansion.Request.Target.WaveID != wave.ID || expansion.Request.Target.ScopeRevision != wave.ScopeRevision || expansion.Request.Target.PolicyRevision != wave.Policy.Revision || !slices.Contains(expansion.Admission.CreatedTaskIDs, replacement) {
			return false, errors.New("template clone receipt conflict")
		}
		bound, err := templateTaskBoundTx(ctx, tx, wave, replacement, seen)
		if err != nil || bound {
			return bound, err
		}
	}
	return false, nil
}

// ReconcileSessionTemplates applies at most one saved expansion per current
// Wave, without model/process/run reservations. Parent ownership is bounded by
// MaxMainWaves; Task scans also recover progression after a dispatch page boundary.
func (k *KernelStore) ReconcileSessionTemplates(ctx context.Context, sessionID string) (bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return false, err
	}
	ids := []string{}
	if b.Kind == "task" {
		ids = append(ids, b.Target.WaveID)
	} else if b.Kind == "orchestrator" {
		ids, err = inputIDsTx(ctx, tx, `SELECT id FROM kernel_waves WHERE json_extract(payload,'$.orchestrator_session_id')=? AND json_extract(payload,'$.state') NOT IN ('completed','canceled') ORDER BY id LIMIT ?`, b.ID, contract.MaxMainWaves+1)
		if err != nil {
			return false, err
		}
		if len(ids) > contract.MaxMainWaves {
			return false, errors.New("main Wave bound exceeded")
		}
	} else {
		return false, errors.New("unknown template progression session kind")
	}
	if err = tx.Rollback(); err != nil {
		return false, err
	}
	changed := false
	var failures []error
	for _, id := range ids {
		expanded, err := k.reconcileWaveTemplate(ctx, id)
		changed = changed || expanded
		if err != nil {
			failures = append(failures, err)
		}
	}
	return changed, errors.Join(failures...)
}
func (k *KernelStore) reconcileWaveTemplate(ctx context.Context, waveID string) (bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	wave, err := loadWave(ctx, tx, waveID)
	if err != nil {
		return false, err
	}
	set, tasks, _, err := k.activePhaseTx(ctx, tx, wave.Target())
	if err != nil {
		return false, err
	}
	if set.State == "active" && set.Reason == "" {
		changed, err := k.saveReadyInputAttentionTx(ctx, tx, wave, tasks)
		if err != nil {
			return false, err
		}
		if err = tx.Commit(); err != nil {
			return false, err
		}
		return changed, nil
	}
	if set.Reason == "review_inconclusive" || set.Reason == "drain_and_block" {
		changed, err := k.savePhaseAttentionTx(ctx, tx, wave, tasks)
		if err != nil {
			return false, err
		}
		if err = tx.Commit(); err != nil {
			return false, err
		}
		return changed, nil
	}
	if set.Reason != "review_findings" {
		return false, nil
	}
	var request TemplateExpansionRequest
	for _, task := range tasks {
		if task.Spec.Kind != "review" {
			continue
		}
		result, err := k.completedTaskResultTx(ctx, tx, wave, task)
		if err != nil {
			return false, err
		}
		if result.Review != nil && result.Review.Verdict == "findings" {
			expanded, err := expandedReviewResultTx(ctx, tx, wave, task)
			if err != nil {
				return false, err
			}
			if !expanded {
				request = TemplateExpansionRequest{Target: wave.Target(), ReviewTaskID: task.ID}
				break
			}
		}
	}
	if request.ReviewTaskID == "" {
		return false, nil
	}
	if err = tx.Rollback(); err != nil {
		return false, err
	}
	receipt, err := k.expandReviewTemplates(ctx, request, true)
	if errors.Is(err, ErrTemplateNeedsMain) {
		return k.saveTemplateAttention(ctx, request, err.Error())
	}
	if errors.Is(err, ErrPlanAdmissionHeld) {
		return false, nil
	}
	return err == nil && receipt.Admission.Target.PlanRevision > request.Target.PlanRevision, err
}

// Used inside automatic expansion's write transaction. A selected parent
// snapshot/run or unstarted wake must not be invalidated by a controller replan.
func idleTemplateParentTx(ctx context.Context, tx *sql.Tx, wave domain.Wave) error {
	var idle bool
	err := tx.QueryRowContext(ctx, `SELECT creating=0 AND active_run_id='' AND process_stopped=1 AND NOT EXISTS(SELECT 1 FROM kernel_wake_intents WHERE session_id=kernel_sessions.id AND state IN ('reserved','bound')) AND NOT EXISTS(SELECT 1 FROM kernel_run_bindings WHERE session_id=kernel_sessions.id AND (applied=0 OR process_stopped=0)) FROM kernel_sessions WHERE id=?`, wave.OrchestratorSessionID).Scan(&idle)
	if err != nil {
		return err
	}
	if !idle {
		return ErrPlanAdmissionHeld
	}
	return nil
}

// A controller-owned plan revision continues an accepted background Wave.
// Preserve each outstanding terminal wait with a versioned successor, retaining
// the original subscription/request as history rather than rewriting its scope.
func (k *KernelStore) followTemplateTerminalWaitsTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, expansion TemplateExpansion) error {
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM kernel_subscriptions WHERE session_id=? AND wave_id=? AND scope_revision=? AND plan_revision=? AND resource=? ORDER BY id", wave.OrchestratorSessionID, wave.ID, wave.ScopeRevision, wave.PlanRevision, "kernel:wave-terminal:"+wave.ID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		prior, err := subscriptionTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if prior.Target != wave.Target() || prior.Request.Kind != "wave_terminal" {
			return errors.New("template terminal subscription conflict")
		}
		next, err := k.subscribeRequestTx(ctx, tx, SubscriptionRequest{SessionID: wave.OrchestratorSessionID, Target: expansion.Admission.Target, Key: structureID("template-terminal-", id, expansion.Key), Kind: "wave_terminal"})
		if err != nil {
			return err
		}
		if err = kernelRecordTx(ctx, tx, "template_terminal_follow", next.ID, id, map[string]any{"previous_subscription_id": id, "subscription_id": next.ID, "expansion_key": expansion.Key, "from": prior.Target, "to": next.Target}); err != nil {
			return err
		}
	}
	return nil
}
