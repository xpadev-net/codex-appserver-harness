package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrDirectionHeld = errors.New("direction awaits settlement of the previous revision")

type WaveDirectionRequest struct {
	Key         string                 `json:"key"`
	Target      contract.Target        `json:"target"`
	Instruction string                 `json:"instruction"`
	Criteria    []domain.WaveCriterion `json:"criteria,omitempty"`
}
type WaveDirection struct {
	ID            string               `json:"id"`
	Request       WaveDirectionRequest `json:"request"`
	Digest        string               `json:"digest"`
	State         string               `json:"state"` // queued, applied, canceled
	CreatedAt     time.Time            `json:"created_at"`
	CompletedAt   time.Time            `json:"completed_at"`
	AppliedTarget contract.Target      `json:"applied_target"`
}

// RequestWaveDirection records trusted operator input. It does not steer or
// interrupt an active model turn, change scope, discard results, or reset usage.
func (k *KernelStore) RequestWaveDirection(ctx context.Context, r WaveDirectionRequest) (WaveDirection, error) {
	var d WaveDirection
	if !r.Target.Valid() || strings.TrimSpace(r.Key) == "" || len(r.Key) > 256 || !utf8.ValidString(r.Key) || strings.TrimSpace(r.Instruction) == "" || len(r.Instruction) > contract.MaxInline || !utf8.ValidString(r.Instruction) || r.Criteria != nil && len(r.Criteria) == 0 {
		return d, errors.New("versioned direction key and bounded instruction required")
	}
	digest, err := contentDigest(r)
	if err != nil {
		return d, err
	}
	id := structureID("direction-", r.Target.WaveID, r.Key)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return d, err
	}
	defer tx.Rollback()
	err = completionRecord(ctx, tx, "wave_direction", id, &d)
	if err == nil {
		if d.ID != id || d.Digest != digest {
			return d, errors.New("direction replay conflict")
		}
		return d, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return d, err
	}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return d, err
	}
	if w.Target() != r.Target || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) || w.PendingDirectionID != "" {
		return d, ErrDirectionHeld
	}
	prospective := w
	if r.Criteria != nil {
		prospective.Criteria = r.Criteria
	}
	if err = prospective.Validate(); err != nil {
		return d, err
	}
	d = WaveDirection{ID: id, Request: r, Digest: digest, State: "queued", CreatedAt: k.clock()}
	w.PendingDirectionID = id
	if err = saveWave(ctx, tx, w); err != nil {
		return d, err
	}
	if err = kernelRecordTx(ctx, tx, "wave_direction", id, w.ID, d); err != nil {
		return d, err
	}
	if err = k.waveEventTx(ctx, tx, w, "direction_requested", "", id+":requested", d); err != nil {
		return d, err
	}
	return d, tx.Commit()
}

// ApplyWaveDirection is a controller safe-boundary operation. Sent/unknown runs,
// unbound creation and unapplied results must first be explicitly reconciled.
// A fresh plan must allocate new Task identities for the new scope; the original
// request, old results and cumulative budgets remain available for audit.
func (k *KernelStore) ApplyWaveDirection(ctx context.Context, id string) (WaveDirection, error) {
	var d WaveDirection
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return d, err
	}
	defer tx.Rollback()
	if err = completionRecord(ctx, tx, "wave_direction", id, &d); err != nil {
		return d, err
	}
	if d.ID != id {
		return d, errors.New("direction identity conflict")
	}
	if digest, err := contentDigest(d.Request); err != nil || digest != d.Digest {
		return d, errors.New("direction request digest conflict")
	}
	if d.State == "applied" || d.State == "canceled" {
		return d, nil
	}
	w, err := loadWave(ctx, tx, d.Request.Target.WaveID)
	if err != nil {
		return d, err
	}
	if d.State != "queued" || w.PendingDirectionID != id || w.Target() != d.Request.Target || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) || w.ScopeRevision == math.MaxInt {
		return d, ErrDirectionHeld
	}
	held, err := unsettledWaveSessionsTx(ctx, tx, w)
	if err != nil {
		return d, err
	}
	if held {
		return d, ErrDirectionHeld
	}
	rows, err := tx.QueryContext(ctx, "SELECT id FROM kernel_tasks WHERE wave_id=? ORDER BY id", w.ID)
	if err != nil {
		return d, err
	}
	ids := []string{}
	for rows.Next() {
		var taskID string
		if err = rows.Scan(&taskID); err != nil {
			rows.Close()
			return d, err
		}
		ids = append(ids, taskID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return d, err
	}
	for _, taskID := range ids {
		task, err := loadTask(ctx, tx, taskID)
		if err != nil {
			return d, err
		}
		if !task.State.Terminal() {
			receipt := OperatorControlReceipt{ID: id, Request: OperatorControl{Action: "cancel", Reason: "superseded by direction " + id}, CreatedAt: k.clock()}
			if err = k.applyTaskControlTx(ctx, tx, w, task, receipt); err != nil {
				return d, err
			}
		}
	}
	w.ScopeRevision++
	w.State, w.Reason, w.PendingDirectionID = domain.WavePlanning, "direction requires a new plan", ""
	w.DirectionID = id
	if d.Request.Criteria != nil {
		w.Criteria = d.Request.Criteria
	}
	if err = w.Validate(); err != nil {
		return d, err
	}
	if err = saveWave(ctx, tx, w); err != nil {
		return d, err
	}
	d.State, d.AppliedTarget, d.CompletedAt = "applied", w.Target(), k.clock()
	if err = kernelRecordTx(ctx, tx, "wave_direction", id, w.ID, d); err != nil {
		return d, err
	}
	if err = k.waveEventTx(ctx, tx, w, "direction_applied", "", id+":applied", d); err != nil {
		return d, err
	}
	return d, tx.Commit()
}

func unsettledWaveSessionsTx(ctx context.Context, tx *sql.Tx, w domain.Wave) (bool, error) {
	var held int
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM kernel_sessions s JOIN kernel_tasks t ON t.id=s.task_id WHERE t.wave_id=? AND
 (s.creating<>0 OR s.process_stopped=0 OR s.active_run_id<>'' OR EXISTS(SELECT 1 FROM kernel_run_bindings r WHERE r.session_id=s.id AND r.applied=0)))`, w.ID).Scan(&held)
	if err != nil || held != 0 {
		return held != 0, err
	}
	var creating, stopped int
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT creating,process_stopped,active_run_id FROM kernel_sessions WHERE id=?", w.OrchestratorSessionID).Scan(&creating, &stopped, &active); err != nil {
		return false, err
	}
	if creating != 0 || stopped == 0 && active == "" {
		return true, nil // A preparation with no scoped run may belong to this Wave.
	}
	rows, err := tx.QueryContext(ctx, "SELECT run_id FROM kernel_run_bindings WHERE session_id=? AND applied=0", w.OrchestratorSessionID)
	if err != nil {
		return false, err
	}
	var runs []string
	for rows.Next() {
		var run string
		if err = rows.Scan(&run); err != nil {
			rows.Close()
			return false, err
		}
		runs = append(runs, run)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, run := range runs {
		var budget RunBudgetBinding
		err = completionRecord(ctx, tx, "run_budget", run, &budget)
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		for _, target := range budget.Targets {
			if target.WaveID == w.ID {
				return true, nil
			}
		}
	}
	return false, nil
}

func cancelWaveDirectionTx(ctx context.Context, tx *sql.Tx, w *domain.Wave, now time.Time) error {
	if w.PendingDirectionID == "" {
		return nil
	}
	var d WaveDirection
	if err := completionRecord(ctx, tx, "wave_direction", w.PendingDirectionID, &d); err != nil {
		return err
	}
	if d.ID != w.PendingDirectionID || d.Request.Target != w.Target() || d.State != "queued" {
		return errors.New("pending direction source conflict")
	}
	d.State, d.CompletedAt = "canceled", now
	w.PendingDirectionID = ""
	return kernelRecordTx(ctx, tx, "wave_direction", d.ID, w.ID, d)
}

type WaveDirectionPage struct {
	Directions []WaveDirection `json:"directions"`
	NextAfter  string          `json:"next_after"`
}

func (k *KernelStore) PendingWaveDirections(ctx context.Context, after string, limit int) (WaveDirectionPage, error) {
	p := WaveDirectionPage{Directions: []WaveDirection{}}
	if limit < 1 || limit > 128 {
		return p, errors.New("direction page limit must be 1..128")
	}
	rows, err := k.DB.QueryContext(ctx, "SELECT id,payload FROM records WHERE kind='wave_direction' AND id>? AND json_extract(payload,'$.state')='queued' ORDER BY id LIMIT ?", after, limit+1)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return p, err
		}
		var d WaveDirection
		if err = json.Unmarshal(raw, &d); err != nil {
			return p, err
		}
		if d.ID != id || d.State != "queued" {
			return p, errors.New("pending direction identity conflict")
		}
		if len(p.Directions) == limit {
			p.NextAfter = p.Directions[limit-1].ID
			break
		}
		p.Directions = append(p.Directions, d)
	}
	return p, rows.Err()
}
