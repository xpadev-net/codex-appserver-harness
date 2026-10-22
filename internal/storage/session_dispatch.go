package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// SessionDispatchState is a consistent view of durable execution facts. A
// stopped process alone is not permission to repeat an unapplied/unknown run.
// Selection does not reserve a process, charge a turn, or change input states.
type SessionDispatchState struct {
	Binding        contract.SessionBinding `json:"binding"`
	Creating       bool                    `json:"creating"`
	ProcessStopped bool                    `json:"process_stopped"`
	Run            *contract.SessionRun    `json:"run"`
	RunStopped     bool                    `json:"run_stopped"`
	Wake           *WakeReservation        `json:"wake"`
}
type SessionDispatchPage struct {
	SessionIDs []string `json:"session_ids"`
	NextAfter  string   `json:"next_after"`
}

func (k *KernelStore) SessionDispatchState(ctx context.Context, sessionID string) (SessionDispatchState, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return SessionDispatchState{}, err
	}
	defer tx.Rollback()
	return sessionDispatchStateTx(ctx, tx, sessionID)
}
func sessionDispatchStateTx(ctx context.Context, tx *sql.Tx, sessionID string) (SessionDispatchState, error) {
	var out SessionDispatchState
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return out, err
	}
	out.Binding = b
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT creating,process_stopped,active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&out.Creating, &out.ProcessStopped, &active); err != nil {
		return out, err
	}
	if active != "" {
		var owner, thread, state string
		var applied bool
		if err = tx.QueryRowContext(ctx, "SELECT session_id,thread_id,state,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", active).Scan(&owner, &thread, &state, &out.RunStopped, &applied); err != nil {
			return out, err
		}
		var run contract.SessionRun
		if err = completionRecord(ctx, tx, "agent_run", active, &run); err != nil {
			return out, err
		}
		if applied || owner != b.ID || thread != b.ThreadID || run.ID != active || run.ThreadID != thread || run.State != state {
			return out, errors.New("inconsistent active session run")
		}
		out.Run = &run
	}
	var wakeID string
	err = tx.QueryRowContext(ctx, "SELECT id FROM kernel_wake_intents WHERE session_id=? AND state IN ('reserved','bound') ORDER BY id LIMIT 1", b.ID).Scan(&wakeID)
	if err == nil {
		wake, err := wakeTx(ctx, tx, wakeID)
		if err != nil {
			return out, err
		}
		if wake.Intent.State == "bound" && active != wake.Intent.RunID || active != "" && active != wake.Intent.RunID {
			return out, errors.New("session wake/run identity conflict")
		}
		out.Wake = &wake
	} else if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	return out, nil
}
func (k *KernelStore) DispatchSessions(ctx context.Context, after string, limit int) (SessionDispatchPage, error) {
	out := SessionDispatchPage{SessionIDs: []string{}}
	if limit < 1 || limit > 128 {
		return out, errors.New("invalid session dispatch page size")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM kernel_sessions WHERE id>? ORDER BY id LIMIT ?", after, limit+1)
	if err != nil {
		return out, err
	}
	if len(ids) > limit {
		ids = ids[:limit]
		out.NextAfter = ids[len(ids)-1]
	}
	out.SessionIDs = ids
	return out, nil
}

type PlanningInputPage struct {
	Targets   []contract.Target `json:"targets"`
	NextAfter string            `json:"next_after"`
}

// PlanningInputs selects only new, currently eligible planning reasons. A saved
// run budget already addressing this exact target suppresses automatic replay,
// including a previous report, failure or unknown outcome. A safely archived
// shared main judgment leaves its still-current initial Wave inputs undecided;
// those targets may be selected again, as with requeued wake notifications.
// Messages/notifications
// use their separate durable wake path. New scope can supply a new initial reason.
func (k *KernelStore) PlanningInputs(ctx context.Context, sessionID, after string, limit int) (PlanningInputPage, error) {
	out := PlanningInputPage{Targets: []contract.Target{}}
	if limit < 1 || limit > 128 {
		return out, errors.New("invalid planning input page size")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	state, err := sessionDispatchStateTx(ctx, tx, sessionID)
	if err != nil {
		return out, err
	}
	b := state.Binding
	if b.Kind != "orchestrator" {
		return out, errors.New("planning input requires main session")
	}
	if b.Closed || state.Creating || !state.ProcessStopped || state.Run != nil || state.Wake != nil {
		return out, nil
	}
	ids, err := inputIDsTx(ctx, tx, `SELECT w.id FROM kernel_waves w LEFT JOIN records s ON s.kind='wave_tool_scope' AND s.id=w.id
 WHERE w.id>? AND coalesce(json_extract(s.payload,'$.orchestrator_session_id'),json_extract(w.payload,'$.orchestrator_session_id'))=?
 AND coalesce(json_extract(s.payload,'$.state'),json_extract(w.payload,'$.state')) NOT IN ('completed','canceled') ORDER BY w.id LIMIT ?`, after, b.ID, limit+1)
	if err != nil {
		return out, err
	}
	if len(ids) > limit {
		ids = ids[:limit]
		out.NextAfter = ids[len(ids)-1]
	}
	for _, id := range ids {
		w, err := loadWave(ctx, tx, id)
		if err != nil {
			return out, err
		}
		if w.OrchestratorSessionID != b.ID || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || !k.clock().Before(w.Deadline) {
			continue
		}
		held, err := wakeBudgetHeldTx(ctx, tx, b, w)
		if err != nil {
			return out, err
		}
		if held {
			continue
		}
		var planRaw []byte
		err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_plans WHERE wave_id=? AND revision=?", w.ID, w.PlanRevision).Scan(&planRaw)
		if err == nil {
			var admission PlanAdmission
			if err = json.Unmarshal(planRaw, &admission); err != nil {
				return out, err
			}
			if admission.Target == w.Target() {
				continue
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
		var addressed bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records r,json_each(r.payload,'$.targets') t
 WHERE r.kind='run_budget' AND json_extract(r.payload,'$.session_id')=?
 AND json_extract(t.value,'$.wave_id')=? AND json_extract(t.value,'$.scope_revision')=?
 AND json_extract(t.value,'$.plan_revision')=? AND json_extract(t.value,'$.policy_revision')=?
 AND NOT EXISTS(SELECT 1 FROM records a JOIN kernel_run_bindings b ON b.run_id=a.id
 WHERE a.kind='main_result_application' AND a.id=r.id AND b.session_id=json_extract(r.payload,'$.session_id')
 AND b.applied=1 AND b.process_stopped=1 AND json_extract(a.payload,'$.state')='archived'))`, b.ID, w.ID, w.ScopeRevision, w.PlanRevision, w.Policy.Revision).Scan(&addressed); err != nil {
			return out, err
		}
		if !addressed {
			out.Targets = append(out.Targets, w.Target())
		}
	}
	return out, nil
}

// RunnableWake checks a saved unstarted intent without changing its ID, input
// batch or snapshot. Runtime admission must check these conditions again.
func (k *KernelStore) RunnableWake(ctx context.Context, id string) (WakeReservation, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return WakeReservation{}, err
	}
	defer tx.Rollback()
	wake, err := wakeTx(ctx, tx, id)
	if err != nil {
		return wake, err
	}
	if wake.Intent.State != "reserved" {
		return wake, ErrWakeHeld
	}
	state, err := sessionDispatchStateTx(ctx, tx, wake.Intent.SessionID)
	if err != nil {
		return wake, err
	}
	if state.Run != nil || state.Creating || !state.ProcessStopped || state.Wake == nil || state.Wake.Intent.ID != id {
		return wake, ErrWakeHeld
	}
	if err = k.checkWakeTargetsTx(ctx, tx, state.Binding, wake.Intent); err != nil {
		return wake, err
	}
	return wake, nil
}

// CheckSessionInputGates distinguishes a changed admission gate from a permanent
// launcher failure. It is a pure, current-DB check after a known non-start.
func (k *KernelStore) CheckSessionInputGates(ctx context.Context, sessionID, snapshotID string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state, err := sessionDispatchStateTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	b := state.Binding
	if b.Closed || state.Creating || !state.ProcessStopped || state.Run != nil {
		return ErrWakeHeld
	}
	if b.Kind == "task" {
		set, tasks, drain, err := k.activePhaseTx(ctx, tx, b.Target)
		if err != nil {
			return err
		}
		if set.State != "active" {
			return ErrWakeHeld
		}
		for _, task := range tasks {
			if task.ID == b.TaskID {
				if task.State != contract.TaskReady && task.State != contract.TaskWaiting {
					return ErrWakeHeld
				}
				if task.State == contract.TaskWaiting && state.Wake == nil {
					return ErrWakeHeld
				}
				w, err := loadWave(ctx, tx, task.WaveID)
				if err != nil {
					return err
				}
				reason, err := k.taskReadyTx(ctx, tx, w, task, drain, false)
				if err != nil {
					return err
				}
				if reason != "" {
					return ErrWakeHeld
				}
				return nil
			}
		}
		return ErrWakeHeld
	}
	if state.Wake != nil && state.Wake.Intent.SnapshotID == snapshotID {
		return k.checkWakeTargetsTx(ctx, tx, b, state.Wake.Intent)
	}
	snapshot := contract.InputSnapshot{ID: snapshotID}
	if err = tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", snapshotID).Scan(&snapshot.Digest, &snapshot.Input); err != nil {
		return err
	}
	if snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) {
		return errors.New("dispatch snapshot digest conflict")
	}
	targets, err := mainBudgetTargets(b, snapshot)
	if err != nil {
		return err
	}
	for _, target := range targets {
		w, err := loadWave(ctx, tx, target.WaveID)
		if err != nil {
			return err
		}
		if w.OrchestratorSessionID != b.ID || w.Target() != target || w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) {
			return ErrWakeHeld
		}
		held, err := wakeBudgetHeldTx(ctx, tx, b, w)
		if err != nil {
			return err
		}
		if held {
			return ErrWakeHeld
		}
	}
	return nil
}

// ReleaseUnstartedWake returns inputs to pending only when the reserved model
// run was never journaled and its preparation is proven stopped. Retain the
// immutable old intent/snapshot; a future reservation receives a fresh run ID.
func (k *KernelStore) ReleaseUnstartedWake(ctx context.Context, id, reason string) error {
	if strings.TrimSpace(reason) == "" || len(reason) > contract.MaxInline || !utf8.ValidString(reason) {
		return errors.New("bounded non-start disposition reason required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	wake, err := wakeTx(ctx, tx, id)
	if err != nil {
		return err
	}
	if wake.Intent.State == "not_started" {
		var prior map[string]string
		if err = completionRecord(ctx, tx, "wake_not_started", id, &prior); err != nil {
			return err
		}
		if prior["reason"] != reason {
			return errors.New("non-start disposition replay conflict")
		}
		return nil
	}
	if wake.Intent.State != "reserved" {
		return ErrWakeHeld
	}
	state, err := sessionDispatchStateTx(ctx, tx, wake.Intent.SessionID)
	if err != nil {
		return err
	}
	var runs int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings WHERE run_id=?", wake.Intent.RunID).Scan(&runs); err != nil {
		return err
	}
	if state.Run != nil || !state.ProcessStopped || state.Creating || runs != 0 {
		return ErrWakeHeld
	}
	seen := map[string]bool{}
	for _, nid := range wake.Intent.NotificationIDs {
		n, err := notificationTx(ctx, tx, nid)
		if err != nil {
			return err
		}
		if n.SessionID != state.Binding.ID || n.State != "bound_to_run" || n.RunID != wake.Intent.RunID {
			return errors.New("non-start wake input binding conflict")
		}
		w, err := loadWave(ctx, tx, n.Target.WaveID)
		if err != nil {
			return err
		}
		next, nextRun := "pending", ""
		obsolete, err := k.obsoleteWakeNotificationTx(ctx, tx, state.Binding, w, n)
		if err != nil {
			return err
		}
		if obsolete {
			next, nextRun = "archived", wake.Intent.RunID
		}
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_notifications SET state=?,run_id=? WHERE id=?", next, nextRun, n.ID); err != nil {
			return err
		}
		if n.MessageID != "" && !seen[n.MessageID] {
			seen[n.MessageID] = true
			result, err := tx.ExecContext(ctx, "UPDATE kernel_mailbox SET state=?,run_id=? WHERE message_id=? AND session_id=? AND state='bound_to_run' AND run_id=?", next, nextRun, n.MessageID, state.Binding.ID, wake.Intent.RunID)
			if err != nil {
				return err
			}
			if count, err := result.RowsAffected(); err != nil || count != 1 {
				return errors.New("non-start mailbox binding conflict")
			}
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_wake_intents SET state='not_started' WHERE id=?", id); err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "wake_not_started", id, state.Binding.ID, map[string]string{"reason": reason, "run_id": wake.Intent.RunID, "preparation_id": state.Binding.PreparationID}); err != nil {
		return err
	}
	return tx.Commit()
}

func clearSessionRunHoldTx(ctx context.Context, tx *sql.Tx, sessionID, runID string) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM records WHERE kind='kernel_dispatch_hold' AND id=? AND json_extract(payload,'$.run_id')=?", sessionID, runID)
	return err
}
