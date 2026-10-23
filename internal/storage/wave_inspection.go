package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type WaveSummary struct {
	SourceHold           *domain.WaveSourceHold `json:"source_hold,omitempty"`
	ID                   string                 `json:"id"`
	Source               string                 `json:"source"`
	SourceID             string                 `json:"source_id"`
	Target               contract.Target        `json:"target"`
	State                domain.WaveState       `json:"state"`
	Paused               bool                   `json:"paused"`
	Reason               string                 `json:"reason"`
	OriginalRequestBytes int                    `json:"original_request_bytes"`
	Criteria             int                    `json:"criteria"`
	TasksCreated         int                    `json:"tasks_created"`
	PhasesCreated        int                    `json:"phases_created"`
	MainTurns            int                    `json:"main_turns"`
	Replans              int                    `json:"replans"`
	FixRounds            int                    `json:"fix_rounds"`
	CreatedAt            time.Time              `json:"created_at"`
	Deadline             time.Time              `json:"deadline"`
}
type WaveSummaryPage struct {
	Waves     []WaveSummary `json:"waves"`
	NextAfter string        `json:"next_after"`
}

func (k *KernelStore) WavePage(ctx context.Context, after string, limit int) (WaveSummaryPage, error) {
	page := WaveSummaryPage{Waves: []WaveSummary{}}
	if limit < 1 || limit > 128 {
		return page, errors.New("invalid Wave page limit")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM kernel_waves WHERE id>? ORDER BY id LIMIT ?", after, limit+1)
	if err != nil {
		return page, err
	}
	if len(ids) > limit {
		ids = ids[:limit]
		page.NextAfter = ids[len(ids)-1]
	}
	for _, id := range ids {
		w, err := loadWave(ctx, tx, id)
		if err != nil {
			return page, err
		}
		page.Waves = append(page.Waves, WaveSummary{SourceHold: w.SourceHold, ID: w.ID, Source: w.Source, SourceID: w.SourceID, Target: w.Target(), State: w.State, Paused: w.Paused, Reason: w.Reason, OriginalRequestBytes: len(w.OriginalRequest), Criteria: len(w.Criteria), TasksCreated: w.TasksCreated, PhasesCreated: w.PhasesCreated, MainTurns: w.MainTurns, Replans: w.Replans, FixRounds: w.FixRounds, CreatedAt: w.CreatedAt, Deadline: w.Deadline})
	}
	return page, nil
}

type RunInspection struct {
	ID         string            `json:"id"`
	ThreadID   string            `json:"thread_id"`
	TurnID     string            `json:"turn_id"`
	State      string            `json:"state"`
	Started    string            `json:"started"`
	FinalBytes int64             `json:"final_bytes"`
	Budget     *RunBudgetBinding `json:"budget,omitempty"`
}
type DispatchHoldInspection struct {
	SessionID     string `json:"session_id"`
	RunID         string `json:"run_id"`
	PreparationID string `json:"preparation_id"`
	Reason        string `json:"reason"`
}
type SessionInspection struct {
	DispatchHold   *DispatchHoldInspection `json:"dispatch_hold,omitempty"`
	Binding        contract.SessionBinding `json:"binding"`
	Creating       bool                    `json:"creating"`
	ProcessStopped bool                    `json:"process_stopped"`
	RunStopped     bool                    `json:"run_stopped"`
	ActiveRunID    string                  `json:"active_run_id"`
	WakeID         string                  `json:"wake_id"`
	LastRun        *RunInspection          `json:"last_run,omitempty"`
}
type TaskInspection struct {
	Task              domain.Task       `json:"task"`
	Current           bool              `json:"current_plan"`
	Session           SessionInspection `json:"session"`
	ResultOutcome     string            `json:"result_outcome"`
	Artifacts         []string          `json:"artifact_ids"`
	ResultUnavailable bool              `json:"result_unavailable"`
}
type PhaseInspection struct {
	ID            string                       `json:"id"`
	LocalID       string                       `json:"local_id"`
	Position      int                          `json:"position"`
	Current       bool                         `json:"current_plan"`
	Completion    contract.CompletionCondition `json:"completion"`
	FailurePolicy string                       `json:"failure_policy"`
}
type WaveInspection struct {
	Wave            domain.Wave       `json:"wave"`
	Main            SessionInspection `json:"main"`
	Budget          WaveRunBudget     `json:"budget"`
	Phases          []PhaseInspection `json:"phases"`
	PhasesTruncated bool              `json:"phases_truncated"`
	NextPhaseAfter  string            `json:"next_phase_after"`
	Tasks           []TaskInspection  `json:"tasks"`
	NextTaskAfter   string            `json:"next_task_after"`
}

func inspectSessionTx(ctx context.Context, tx *sql.Tx, id string) (SessionInspection, error) {
	var view SessionInspection
	b, err := sessionBindingTx(ctx, tx, id)
	if err != nil {
		return view, err
	}
	view.Binding = b
	if err = tx.QueryRowContext(ctx, "SELECT creating,process_stopped,active_run_id FROM kernel_sessions WHERE id=?", id).Scan(&view.Creating, &view.ProcessStopped, &view.ActiveRunID); err != nil {
		return view, err
	}
	if view.ActiveRunID != "" {
		if err = tx.QueryRowContext(ctx, "SELECT process_stopped FROM kernel_run_bindings WHERE run_id=?", view.ActiveRunID).Scan(&view.RunStopped); err != nil {
			return view, err
		}
	}
	err = tx.QueryRowContext(ctx, "SELECT id FROM kernel_wake_intents WHERE session_id=? AND state IN ('reserved','bound') ORDER BY id LIMIT 1", id).Scan(&view.WakeID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return view, err
	}
	var hold DispatchHoldInspection
	err = completionRecord(ctx, tx, "kernel_dispatch_hold", id, &hold)
	if err == nil {
		if hold.SessionID != id {
			return view, errors.New("dispatch hold owner mismatch")
		}
		// Historical holds remain audit evidence. A replacement preparation/run
		// no longer belongs to that hold and must not be displayed as currently held.
		if hold.PreparationID == b.PreparationID && (view.ActiveRunID == "" || view.ActiveRunID == hold.RunID) {
			view.DispatchHold = &hold
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return view, err
	}
	var run RunInspection
	err = tx.QueryRowContext(ctx, `SELECT id,coalesce(json_extract(payload,'$.thread_id'),''),coalesce(json_extract(payload,'$.turn_id'),''),json_extract(payload,'$.state'),json_extract(payload,'$.started'),coalesce(length(CAST(json_extract(payload,'$.final') AS BLOB)),0) FROM records WHERE kind='agent_run' AND work_item_id=? ORDER BY rowid DESC LIMIT 1`, id).Scan(&run.ID, &run.ThreadID, &run.TurnID, &run.State, &run.Started, &run.FinalBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return view, nil
	}
	if err != nil {
		return view, err
	}
	var budget RunBudgetBinding
	if err = completionRecord(ctx, tx, "run_budget", run.ID, &budget); err != nil {
		return view, err
	}
	run.Budget = &budget
	view.LastRun = &run
	return view, nil
}

// InspectWave is a bounded, consistent read snapshot. It neither reconciles
// notifications nor opens a process. Run bodies are represented by ID/byte count.
func (k *KernelStore) InspectWave(ctx context.Context, id, after, afterPhase string, limit int) (WaveInspection, error) {
	view := WaveInspection{Phases: []PhaseInspection{}, Tasks: []TaskInspection{}}
	if limit < 1 || limit > 128 {
		return view, errors.New("invalid Task page limit")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return view, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, id)
	if err != nil {
		return view, err
	}
	view.Wave = w
	view.Main, err = inspectSessionTx(ctx, tx, w.OrchestratorSessionID)
	if err != nil {
		return view, err
	}
	tokens, unknown, saturated, err := waveTokensTx(ctx, tx, w.ID)
	if err != nil {
		return view, err
	}
	view.Budget = WaveRunBudget{WaveID: w.ID, ObservedTokens: tokens, UnobservedRuns: unknown, Saturated: saturated, TokenLimit: w.Policy.Limits.Tokens, MainTurns: w.MainTurns, MainTurnLimit: w.Policy.Limits.MainTurns, Deadline: w.Deadline, DeadlineExceeded: !k.clock().Before(w.Deadline)}
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM kernel_phases WHERE wave_id=? AND id>? ORDER BY id LIMIT ?", w.ID, afterPhase, limit+1)
	if err != nil {
		return view, err
	}
	if len(ids) > limit {
		ids = ids[:limit]
		view.PhasesTruncated = true
		view.NextPhaseAfter = ids[len(ids)-1]
	}
	for _, id := range ids {
		var raw []byte
		var phase domain.WavePhase
		if err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_phases WHERE id=?", id).Scan(&raw); err != nil {
			return view, err
		}
		if err = json.Unmarshal(raw, &phase); err != nil {
			return view, err
		}
		var current bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM kernel_plan_bindings b JOIN kernel_tasks t ON t.id=b.task_id WHERE b.wave_id=? AND b.revision=? AND t.phase_id=?)", w.ID, w.PlanRevision, id).Scan(&current); err != nil {
			return view, err
		}
		view.Phases = append(view.Phases, PhaseInspection{ID: id, LocalID: phase.LocalID, Position: phase.Position, Current: current, Completion: phase.Spec.Completion, FailurePolicy: phase.Spec.FailurePolicy})
	}
	ids, err = inputIDsTx(ctx, tx, "SELECT id FROM kernel_tasks WHERE wave_id=? AND id>? ORDER BY id LIMIT ?", w.ID, after, limit+1)
	if err != nil {
		return view, err
	}
	if len(ids) > limit {
		ids = ids[:limit]
		view.NextTaskAfter = ids[len(ids)-1]
	}
	for _, id := range ids {
		task, err := loadTask(ctx, tx, id)
		if err != nil {
			return view, err
		}
		item := TaskInspection{Task: task, Artifacts: []string{}}
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM kernel_plan_bindings WHERE wave_id=? AND revision=? AND task_id=?)", w.ID, w.PlanRevision, id).Scan(&item.Current); err != nil {
			return view, err
		}
		item.Session, err = inspectSessionTx(ctx, tx, task.SessionID)
		if err != nil {
			return view, err
		}
		if task.ResultID != "" {
			var result contract.TaskResult
			err = taskEvidenceRecord(ctx, tx, "task_result", task.ResultID, &result)
			if err != nil {
				if !readinessHoldError(err) {
					return view, err
				}
				item.ResultUnavailable = true
			} else if result.Validate() != nil || result.TaskID != task.ID || result.Target != task.Target() {
				item.ResultUnavailable = true
			} else {
				item.ResultOutcome = result.Outcome
				item.Artifacts = resultArtifactReferences(result)
			}
		}
		view.Tasks = append(view.Tasks, item)
	}
	return view, nil
}
