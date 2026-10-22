package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrMainResultInvalid = errors.New("main result is outside its saved contract or scope")
var ErrMainResultHeld = errors.New("main result is held by policy or unapplied Task work")

type MainResultApplication struct {
	SessionID  string               `json:"session_id"`
	RunID      string               `json:"run_id"`
	ThreadID   string               `json:"thread_id"`
	SnapshotID string               `json:"snapshot_id"`
	Targets    []contract.Target    `json:"targets"`
	State      string               `json:"state"` // applied or archived
	Reason     string               `json:"reason"`
	Result     *contract.MainResult `json:"result"`
	Plans      []PlanAdmission      `json:"plans"`
}

// ApplyMainResult applies one known normal main completion after stop proof.
// Errors leave the stored physical outcome/input untouched for classification
// or reference reconciliation, never another implicit model turn. All selected
// Waves and all input bindings commit together; unknown starts cannot enter.
func (k *KernelStore) ApplyMainResult(ctx context.Context, sessionID, runID string) (MainResultApplication, error) {
	receipt := MainResultApplication{SessionID: sessionID, RunID: runID, Plans: []PlanAdmission{}}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return receipt, err
	}
	if b.Kind != "orchestrator" {
		return receipt, errors.New("main result requires an Orchestrator session")
	}
	var owner, thread, state, snapshotID string
	var stopped, applied int
	if err = tx.QueryRowContext(ctx, "SELECT session_id,thread_id,state,snapshot_id,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", runID).Scan(&owner, &thread, &state, &snapshotID, &stopped, &applied); err != nil {
		return receipt, err
	}
	if owner != b.ID {
		return receipt, errors.New("main run owner conflict")
	}
	if applied == 1 {
		var prior MainResultApplication
		if err = completionRecord(ctx, tx, "main_result_application", runID, &prior); err != nil {
			return receipt, err
		}
		if prior.SessionID != b.ID || prior.RunID != runID || prior.ThreadID != thread || prior.SnapshotID != snapshotID {
			return receipt, errors.New("main result replay conflict")
		}
		return prior, nil // Logical identity survives a later audited thread rotation.
	}
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&active); err != nil {
		return receipt, err
	}
	var run contract.SessionRun
	if err = completionRecord(ctx, tx, "agent_run", runID, &run); err != nil {
		return receipt, err
	}
	if active != runID || thread != b.ThreadID || state != "completed" || stopped != 1 || run.ID != runID || run.ThreadID != thread || run.State != "completed" || run.TurnID == "" || !run.BaselineKnown || slices.Contains(run.BaselineTurnIDs, run.TurnID) {
		return receipt, errors.New("normal completed and stopped active main run required")
	}
	snapshot := contract.InputSnapshot{ID: snapshotID}
	if err = tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", snapshotID).Scan(&snapshot.Digest, &snapshot.Input); err != nil {
		return receipt, err
	}
	if snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) {
		return receipt, errors.New("main snapshot digest conflict")
	}
	targets, err := mainBudgetTargets(b, snapshot)
	if err != nil {
		return receipt, err
	}
	var budget RunBudgetBinding
	if err = completionRecord(ctx, tx, "run_budget", runID, &budget); err != nil {
		return receipt, err
	}
	if budget.RunID != runID || budget.SessionID != b.ID || budget.ThreadID != thread || budget.SnapshotID != snapshotID || budget.SnapshotDigest != snapshot.Digest || !slices.Equal(budget.Targets, targets) {
		return receipt, errors.New("main scope budget binding conflict")
	}
	targets, err = effectiveRunTargetsTx(ctx, tx, b, runID, snapshotID, targets)
	if err != nil {
		return receipt, err
	}
	receipt.ThreadID, receipt.SnapshotID = thread, snapshotID
	receipt.Targets = targets
	interrupted, err := runInterruptedTx(ctx, tx, b.ID, runID)
	if err != nil {
		return receipt, err
	}
	obsolete := b.Closed
	allRetired := true
	waves := map[string]domain.Wave{}
	for _, target := range targets {
		wave, err := loadWave(ctx, tx, target.WaveID)
		if err != nil {
			return receipt, err
		}
		if wave.OrchestratorSessionID != b.ID {
			return receipt, errors.New("main snapshot belongs to another Orchestrator")
		}
		waves[wave.ID] = wave
		retired := b.Closed || wave.Target() != target || wave.PendingDirectionID != "" || wave.State == domain.WaveCanceled || wave.State == domain.WaveCompleted
		allRetired = allRetired && retired
		old := wave.Target() != target || wave.PendingDirectionID != ""
		if wave.State == domain.WaveCanceled {
			var terminal int
			if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_wake_intents wi JOIN kernel_notifications n ON n.run_id=wi.run_id WHERE wi.session_id=? AND wi.run_id=? AND wi.state='bound' AND n.session_id=? AND json_extract(n.payload,'$.target.wave_id')=? AND n.state='bound_to_run' AND json_extract(n.payload,'$.kind')='wave_terminal' AND json_extract(n.payload,'$.wave_state')='canceled'", b.ID, runID, b.ID, wave.ID).Scan(&terminal); err != nil {
				return receipt, err
			}
			old = old || terminal == 0
		}
		obsolete = obsolete || old
	}
	if interrupted && !allRetired {
		return receipt, ErrInterruptionHeld
	}
	if interrupted || obsolete {
		receipt.State, receipt.Reason = "archived", "superseded or canceled main input"
		if interrupted {
			receipt.Reason = "operator_interrupt: late main output retained without actions"
		}
		outcome := "superseded"
		if interrupted {
			outcome = "archived"
		}
		return receipt, k.finishMainResultTx(ctx, tx, b, receipt, outcome)
	}
	var result contract.MainResult
	if contract.DecodeStrict([]byte(run.Final), &result) != nil || result.Validate() != nil || result.SessionID != b.ID || len(result.Decisions) != len(targets) {
		return receipt, ErrMainResultInvalid
	}
	selected := map[string]contract.Target{}
	for _, target := range targets {
		selected[target.WaveID] = target
	}
	acceptedPlans := map[string]PlanAdmission{}
	for _, decision := range result.Decisions {
		if selected[decision.Target.WaveID] != decision.Target {
			return receipt, ErrMainResultInvalid
		}
		wave := waves[decision.Target.WaveID]
		if decision.Action == "plan" {
			accepted, err := acceptedRunPlanTx(ctx, tx, b, runID, decision.Target, *decision.Plan)
			if err != nil {
				return receipt, err
			}
			if accepted != nil {
				acceptedPlans[wave.ID] = *accepted
			}
			if wave.Paused && accepted == nil {
				return receipt, ErrMainResultHeld
			}
			if (wave.State == domain.WaveCompleted || !k.clock().Before(wave.Deadline)) && accepted == nil {
				return receipt, ErrMainResultInvalid
			}
			if err = domain.ValidateWavePlan(wave, *decision.Plan); err != nil {
				return receipt, errors.Join(ErrMainResultInvalid, err)
			}
			var active int
			if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_sessions s JOIN kernel_tasks t ON t.id=s.task_id WHERE t.wave_id=? AND (s.active_run_id<>'' OR s.process_stopped=0 OR s.creating<>0)", wave.ID).Scan(&active); err != nil {
				return receipt, err
			}
			if active != 0 && accepted == nil {
				return receipt, ErrMainResultHeld
			}
		}
		for _, id := range decision.SubscriptionIDs {
			var session, waveID string
			var scope, plan int
			var raw []byte
			if err = tx.QueryRowContext(ctx, "SELECT session_id,wave_id,scope_revision,plan_revision,payload FROM kernel_subscriptions WHERE id=?", id).Scan(&session, &waveID, &scope, &plan, &raw); errors.Is(err, sql.ErrNoRows) {
				return receipt, ErrMainResultInvalid
			} else if err != nil {
				return receipt, err
			}
			var sub struct {
				Target contract.Target `json:"target"`
			}
			if json.Unmarshal(raw, &sub) != nil || session != b.ID || waveID != wave.ID || scope != wave.ScopeRevision || plan != wave.PlanRevision || sub.Target != wave.Target() {
				return receipt, ErrMainResultInvalid
			}
		}
	}
	// Validate the whole batch before adopting any plan. The transaction also
	// rolls back budgets, sessions and notification fanout if a later write fails.
	for _, decision := range result.Decisions {
		if decision.Action == "plan" {
			if accepted, ok := acceptedPlans[decision.Target.WaveID]; ok {
				receipt.Plans = append(receipt.Plans, accepted)
				continue
			}
			plan, err := k.adoptPlanTx(ctx, tx, decision.Target, "main-result:"+runID, *decision.Plan)
			if err != nil {
				if errors.Is(err, ErrPlanAdmission) {
					return receipt, errors.Join(ErrMainResultInvalid, err)
				}
				if errors.Is(err, ErrPlanAdmissionHeld) {
					return receipt, errors.Join(ErrMainResultHeld, err)
				}
				return receipt, err
			}
			receipt.Plans = append(receipt.Plans, plan)
		}
	}
	receipt.Result, receipt.State = &result, "applied"
	return receipt, k.finishMainResultTx(ctx, tx, b, receipt, "applied")
}
func (k *KernelStore) finishMainResultTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, receipt MainResultApplication, outcome string) error {
	if err := k.finishMainResultEffectsTx(ctx, tx, b, receipt, outcome); err != nil {
		return err
	}
	return tx.Commit()
}

func (k *KernelStore) finishMainResultEffectsTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, receipt MainResultApplication, outcome string) error {
	if err := kernelRecordTx(ctx, tx, "main_result_application", receipt.RunID, b.ID, receipt); err != nil {
		return err
	}
	if err := kernelRecordTx(ctx, tx, "session_run_disposition", receipt.RunID, b.ID, map[string]string{"reason": "main result " + receipt.State}); err != nil {
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
	for _, target := range receipt.Targets {
		wave, err := loadWave(ctx, tx, target.WaveID)
		if err != nil {
			return err
		}
		wave.ScopeRevision, wave.PlanRevision, wave.Policy.Revision = target.ScopeRevision, target.PlanRevision, target.PolicyRevision
		event := map[string]string{"session_id": b.ID, "run_id": receipt.RunID, "state": receipt.State, "reason": receipt.Reason}
		if err = k.waveEventTx(ctx, tx, wave, "main_result_"+receipt.State, "", receipt.RunID+":main_result:"+wave.ID, event); err != nil {
			return err
		}
	}
	if outcome == "superseded" {
		if err := k.requeueCurrentMainInputsTx(ctx, tx, b, receipt.RunID); err != nil {
			return err
		}
		return nil
	}
	if err := k.finishWakeTx(ctx, tx, b.ID, receipt.RunID, outcome); err != nil {
		return err
	}
	if err := k.finishDirectOperationInputsTx(ctx, tx, b.ID, receipt.RunID, outcome); err != nil {
		return err
	}
	return nil
}

// A stale Wave cannot consume another Wave's current undecided input. Retire
// the shared physical batch, archive obsolete notifications, and return other
// notifications/mail to pending without replaying a model judgment.
func (k *KernelStore) requeueCurrentMainInputsTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, runID string) error {
	rows, err := tx.QueryContext(ctx, "SELECT id FROM kernel_notifications WHERE session_id=? AND run_id=? AND state IN ('bound_to_run','bound_to_tool') ORDER BY id", b.ID, runID)
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, id := range ids {
		n, err := notificationTx(ctx, tx, id)
		if err != nil {
			return err
		}
		wave, err := loadWave(ctx, tx, n.Target.WaveID)
		if err != nil {
			return err
		}
		next, nextRun := "pending", ""
		obsolete, err := k.obsoleteWakeNotificationTx(ctx, tx, b, wave, n)
		if err != nil {
			return err
		}
		if obsolete {
			next, nextRun = "archived", runID
		}
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_notifications SET state=?,run_id=? WHERE id=?", next, nextRun, id); err != nil {
			return err
		}
		if n.MessageID != "" && !seen[n.MessageID] {
			seen[n.MessageID] = true
			result, err := tx.ExecContext(ctx, "UPDATE kernel_mailbox SET state=?,run_id=? WHERE session_id=? AND message_id=? AND run_id=? AND state='bound_to_run'", next, nextRun, b.ID, n.MessageID, runID)
			if err != nil {
				return err
			}
			if count, err := result.RowsAffected(); err != nil || count != 1 {
				return errors.New("superseded main mailbox binding conflict")
			}
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_wake_intents SET state='superseded' WHERE session_id=? AND run_id=? AND state='bound'", b.ID, runID)
	return err
}
