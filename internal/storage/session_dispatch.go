package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

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
// including a previous report, failure or unknown outcome. Messages/notifications
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
 AND json_extract(t.value,'$.plan_revision')=? AND json_extract(t.value,'$.policy_revision')=?)`, b.ID, w.ID, w.ScopeRevision, w.PlanRevision, w.Policy.Revision).Scan(&addressed); err != nil {
			return out, err
		}
		if !addressed {
			out.Targets = append(out.Targets, w.Target())
		}
	}
	return out, nil
}
