package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrWaveRunBudget = errors.New("Wave run budget is exhausted")
var ErrMainThreadTurns = errors.New("main thread turn limit requires an audited rebind")

type RunBudgetBinding struct {
	RunID          string            `json:"run_id"`
	SessionID      string            `json:"session_id"`
	TaskID         string            `json:"task_id"`
	ThreadID       string            `json:"thread_id"`
	SnapshotID     string            `json:"snapshot_id"`
	SnapshotDigest string            `json:"snapshot_digest"`
	Targets        []contract.Target `json:"targets"`
	ObservedTokens int64             `json:"observed_tokens"`
	HasObservation bool              `json:"has_observation"`
	FinalUsage     bool              `json:"final_usage"`
}
type RunTokenObservation struct {
	SessionID   string
	RunID       string
	ThreadID    string
	TurnID      string
	TotalForRun *int64
	Final       bool
}
type WaveRunBudget struct {
	WaveID           string    `json:"wave_id"`
	ObservedTokens   int64     `json:"observed_tokens"`
	UnobservedRuns   int       `json:"unobserved_runs"`
	Saturated        bool      `json:"saturated"`
	TokenLimit       int64     `json:"token_limit"`
	MainTurns        int       `json:"main_turns"`
	MainTurnLimit    int       `json:"main_turn_limit"`
	Deadline         time.Time `json:"deadline"`
	DeadlineExceeded bool      `json:"deadline_exceeded"`
}

func waveTokensTx(ctx context.Context, q *sql.Tx, waveID string) (int64, int, bool, error) {
	rows, err := q.QueryContext(ctx, "SELECT amount,unknown FROM kernel_budget_ledger WHERE wave_id=? AND kind='tokens'", waveID)
	if err != nil {
		return 0, 0, false, err
	}
	defer rows.Close()
	var total int64
	unknown := 0
	saturated := false
	for rows.Next() {
		var amount int64
		var missing int
		if err = rows.Scan(&amount, &missing); err != nil {
			return 0, 0, false, err
		}
		if amount < 0 || (missing != 0 && missing != 1) {
			return 0, 0, false, errors.New("invalid token ledger")
		}
		if amount > math.MaxInt64-total {
			total = math.MaxInt64
			saturated = true
		} else {
			total += amount
		}
		unknown += missing
	}
	return total, unknown, saturated, rows.Err()
}
func (k *KernelStore) WaveRunBudget(ctx context.Context, waveID string) (WaveRunBudget, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return WaveRunBudget{}, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, waveID)
	if err != nil {
		return WaveRunBudget{}, err
	}
	tokens, unknown, saturated, err := waveTokensTx(ctx, tx, waveID)
	if err != nil {
		return WaveRunBudget{}, err
	}
	r := WaveRunBudget{WaveID: waveID, ObservedTokens: tokens, UnobservedRuns: unknown, Saturated: saturated, TokenLimit: w.Policy.Limits.Tokens, MainTurns: w.MainTurns, MainTurnLimit: w.Policy.Limits.MainTurns, Deadline: w.Deadline, DeadlineExceeded: !k.clock().Before(w.Deadline)}
	return r, tx.Commit()
}

// Budget holds preserve pending inputs while allowing another Wave in a shared
// wake to progress. Thread limits require rotation, so do not retire their input.
func wakeBudgetHeldTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, w domain.Wave) (bool, error) {
	used, _, saturated, err := waveTokensTx(ctx, tx, w.ID)
	if err != nil {
		return false, err
	}
	if saturated || used >= w.Policy.Limits.Tokens {
		return true, nil
	}
	if b.Kind == "orchestrator" {
		return w.MainTurns >= w.Policy.Limits.MainTurns, nil
	}
	if b.Kind == "task" {
		task, err := loadTask(ctx, tx, b.TaskID)
		if err != nil {
			return false, err
		}
		return task.Attempts >= w.Policy.Limits.TaskAttempts, nil
	}
	return false, nil
}

// Existing schema-v2 runs predate durable budget bindings. Refuse them before
// any write rather than silently treating historic consumption as zero.
func validateKernelRunBudgetFormat(db *sql.DB) error {
	var missing int
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM kernel_run_bindings b
 LEFT JOIN records r ON r.kind='run_budget' AND r.id=b.run_id WHERE r.id IS NULL)`).Scan(&missing); err != nil {
		return err
	}
	if missing != 0 {
		return errors.New("kernel run budget bindings missing; explicit offline reconciliation/migration required")
	}
	return nil
}

func mainBudgetTargets(b contract.SessionBinding, snapshot contract.InputSnapshot) ([]contract.Target, error) {
	var input struct {
		SchemaVersion int               `json:"schema_version"`
		SessionID     string            `json:"session_id"`
		Targets       []contract.Target `json:"targets"`
	}
	if err := json.Unmarshal(snapshot.Input, &input); err != nil {
		return nil, err
	}
	if input.SchemaVersion != 1 || input.SessionID != b.ID || len(input.Targets) == 0 || len(input.Targets) > contract.MaxMainWaves {
		return nil, errors.New("main run requires a scoped Wave input snapshot")
	}
	targets := append([]contract.Target{}, input.Targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].WaveID < targets[j].WaveID })
	for i, t := range targets {
		if !t.Valid() || (i > 0 && targets[i-1].WaveID == t.WaveID) {
			return nil, errors.New("main snapshot requires distinct versioned Wave targets")
		}
	}
	return targets, nil
}
func terminalMainBudgetTargetTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, runID string, w domain.Wave) error {
	var id string
	if err := tx.QueryRowContext(ctx, "SELECT id FROM kernel_wake_intents WHERE session_id=? AND run_id=? AND state='bound'", b.ID, runID).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("terminal Wave run requires its bound terminal observation")
		}
		return err
	}
	wake, err := wakeTx(ctx, tx, id)
	if err != nil {
		return err
	}
	for _, id := range wake.Intent.NotificationIDs {
		n, err := notificationTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if n.Target == w.Target() && n.Kind == "wave_terminal" && n.WaveState == w.State {
			return nil
		}
	}
	return errors.New("terminal Wave run has no matching terminal observation")
}
func (k *KernelStore) reserveRunBudgetTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, snapshot contract.InputSnapshot, run contract.SessionRun) error {
	targets := []contract.Target{b.Target}
	var err error
	if b.Kind == "orchestrator" {
		targets, err = mainBudgetTargets(b, snapshot)
		if err != nil {
			return err
		}
	}
	binding := RunBudgetBinding{RunID: run.ID, SessionID: b.ID, TaskID: b.TaskID, ThreadID: b.ThreadID, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest, Targets: targets}
	threadLimit := math.MaxInt
	for _, target := range targets {
		w, err := loadWave(ctx, tx, target.WaveID)
		if err != nil {
			return err
		}
		if err = w.Validate(); err != nil {
			return err
		}
		if w.Target() != target || w.DispatchPaused() || !k.clock().Before(w.Deadline) {
			return fmt.Errorf("%w: %s is stale, paused or expired", ErrWaveRunBudget, w.ID)
		}
		if b.Kind == "orchestrator" {
			if w.OrchestratorSessionID != b.ID {
				return errors.New("main budget target is not owned")
			}
			if w.State == domain.WaveCompleted || w.State == domain.WaveCanceled {
				if err = terminalMainBudgetTargetTx(ctx, tx, b, run.ID, w); err != nil {
					return err
				}
			}
			if w.MainTurns >= w.Policy.Limits.MainTurns {
				return fmt.Errorf("%w: %s main turns", ErrWaveRunBudget, w.ID)
			}
			threadLimit = min(threadLimit, w.Policy.Limits.ThreadTurns)
		}
		used, _, saturated, err := waveTokensTx(ctx, tx, w.ID)
		if err != nil {
			return err
		}
		if saturated || used >= w.Policy.Limits.Tokens {
			return fmt.Errorf("%w: %s tokens", ErrWaveRunBudget, w.ID)
		}
		if b.Kind == "orchestrator" {
			w.MainTurns++
			if err = saveWave(ctx, tx, w); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'main_turn',1,0,?)", w.ID, run.ID, []byte(`{}`)); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'tokens',0,1,?)", w.ID, run.ID, []byte(`{}`)); err != nil {
			return err
		}
	}
	if b.Kind == "orchestrator" {
		var turns int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings WHERE session_id=? AND thread_id=?", b.ID, b.ThreadID).Scan(&turns); err != nil {
			return err
		}
		if turns >= threadLimit {
			return ErrMainThreadTurns
		}
	}
	return kernelRecordTx(ctx, tx, "run_budget", run.ID, b.ID, binding)
}

// ObserveRunTokens records trusted current-turn increments, including delayed
// facts for stopped/applied runs. Thread cumulative totals are never accepted.
func (k *KernelStore) ObserveRunTokens(ctx context.Context, observation RunTokenObservation) error {
	if observation.TotalForRun != nil && *observation.TotalForRun < 0 || observation.Final && observation.TotalForRun == nil {
		return errors.New("invalid run token observation")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var owner, thread string
	if err = tx.QueryRowContext(ctx, "SELECT session_id,thread_id FROM kernel_run_bindings WHERE run_id=?", observation.RunID).Scan(&owner, &thread); err != nil {
		return err
	}
	var run contract.SessionRun
	if err = completionRecord(ctx, tx, "agent_run", observation.RunID, &run); err != nil {
		return err
	}
	if owner != observation.SessionID || thread != observation.ThreadID || run.ID != observation.RunID || run.ThreadID != thread || run.TurnID == "" || run.TurnID != observation.TurnID || !run.BaselineKnown || slices.Contains(run.BaselineTurnIDs, run.TurnID) {
		return errors.New("token observation does not match the saved run/turn")
	}
	if err = applyRunTokensTx(ctx, tx, observation.RunID, observation.TotalForRun, observation.Final); err != nil {
		return err
	}
	return tx.Commit()
}
func applyRunTokensTx(ctx context.Context, tx *sql.Tx, runID string, total *int64, final bool) error {
	var r RunBudgetBinding
	if err := completionRecord(ctx, tx, "run_budget", runID, &r); err != nil {
		return err
	}
	if r.RunID != runID || len(r.Targets) == 0 {
		return errors.New("invalid run budget binding")
	}
	if total == nil {
		return nil
	} // Unknown evidence never erases prior observations.
	if *total < 0 {
		return errors.New("negative run tokens")
	}
	before := r
	if *total > r.ObservedTokens {
		r.ObservedTokens = *total
		r.HasObservation = true
		r.FinalUsage = final
	} else if *total == r.ObservedTokens {
		r.HasObservation = true
		r.FinalUsage = r.FinalUsage || final
	}
	if before.ObservedTokens == r.ObservedTokens && before.HasObservation == r.HasObservation && before.FinalUsage == r.FinalUsage {
		return nil
	}
	n := int64(len(r.Targets))
	share, remainder := r.ObservedTokens/n, r.ObservedTokens%n
	for i, target := range r.Targets {
		amount := share
		if int64(i) < remainder {
			amount++
		}
		unknown := 1
		if r.FinalUsage {
			unknown = 0
		}
		result, err := tx.ExecContext(ctx, "UPDATE kernel_budget_ledger SET amount=?,unknown=? WHERE wave_id=? AND operation_key=? AND kind='tokens'", amount, unknown, target.WaveID, runID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.New("run token allocation disappeared")
		}
	}
	return kernelRecordTx(ctx, tx, "run_budget", runID, r.SessionID, r)
}
func observeRunUsageTx(ctx context.Context, tx *sql.Tx, run contract.SessionRun) error {
	if run.TurnID == "" || !run.BaselineKnown || slices.Contains(run.BaselineTurnIDs, run.TurnID) || len(run.Usage) == 0 {
		return nil
	}
	var usage struct {
		Last struct {
			TotalTokens *int64 `json:"totalTokens"`
		} `json:"last"`
	}
	if err := json.Unmarshal(run.Usage, &usage); err != nil || usage.Last.TotalTokens == nil || *usage.Last.TotalTokens < 0 {
		return nil
	}
	return applyRunTokensTx(ctx, tx, run.ID, usage.Last.TotalTokens, false)
}
