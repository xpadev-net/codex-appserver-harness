package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

var ErrInterruptionHeld = errors.New("interruption requires the exact active process generation or stopping proof")

type SessionInterruptRequest struct {
	Key           string          `json:"key"`
	Target        contract.Target `json:"target"`
	SessionID     string          `json:"session_id"`
	RunID         string          `json:"run_id"`
	PreparationID string          `json:"preparation_id"`
	Reason        string          `json:"reason"`
}
type SessionInterruption struct {
	ID          string                  `json:"id"`
	Request     SessionInterruptRequest `json:"request"`
	Digest      string                  `json:"digest"`
	OwnerID     string                  `json:"owner_id"`
	State       string                  `json:"state"` // pending, stopped
	CreatedAt   time.Time               `json:"created_at"`
	CompletedAt time.Time               `json:"completed_at"`
}

func validateInterruption(i SessionInterruption, id string) error {
	digest, err := contentDigest(i.Request)
	if err != nil || i.ID != id || id != structureID("interrupt-", i.Request.SessionID, i.Request.Key) || i.Digest != digest || !i.Request.Target.Valid() || i.OwnerID == "" || i.Request.RunID == "" || i.Request.PreparationID == "" || i.CreatedAt.IsZero() || i.State != "pending" && i.State != "stopped" || i.State == "stopped" && i.CompletedAt.IsZero() {
		return errors.New("interruption source conflict")
	}
	return nil
}

// RequestSessionInterrupt is trusted operator input, not a model/IPC command.
// It records intent and blocks the Task; it neither signals nor releases a
// process, fabricates a run outcome, or authorizes automatic recovery.
func (k *KernelStore) RequestSessionInterrupt(ctx context.Context, r SessionInterruptRequest) (SessionInterruption, error) {
	var result SessionInterruption
	if !r.Target.Valid() {
		return result, ErrInterruptionHeld
	}
	for _, value := range []string{r.Key, r.SessionID, r.RunID, r.PreparationID, r.Reason} {
		if strings.TrimSpace(value) == "" || len(value) > contract.MaxInline || !utf8.ValidString(value) {
			return result, errors.New("bounded interruption identity and reason required")
		}
	}
	if len(r.Key) > 256 {
		return result, errors.New("interruption key exceeds 256 bytes")
	}
	digest, err := contentDigest(r)
	if err != nil {
		return result, err
	}
	id := structureID("interrupt-", r.SessionID, r.Key)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	err = completionRecord(ctx, tx, "session_interrupt", id, &result)
	if err == nil {
		if result.ID != id || result.Request != r || result.Digest != digest {
			return result, errors.New("interruption replay conflict")
		}
		return result, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	b, err := sessionBindingTx(ctx, tx, r.SessionID)
	if err != nil {
		return result, err
	}
	var active string
	var stopped, applied int
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &stopped); err != nil {
		return result, err
	}
	if active != r.RunID || stopped != 0 || b.PreparationID != r.PreparationID {
		return result, ErrInterruptionHeld
	}
	var session string
	if err = tx.QueryRowContext(ctx, "SELECT session_id,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", r.RunID).Scan(&session, &stopped, &applied); err != nil {
		return result, err
	}
	if session != b.ID || stopped != 0 || applied != 0 {
		return result, ErrInterruptionHeld
	}
	if b.Kind == "task" {
		if b.Target != r.Target {
			return result, ErrInterruptionHeld
		}
	} else if b.Kind == "orchestrator" {
		var budget RunBudgetBinding
		if err = completionRecord(ctx, tx, "run_budget", r.RunID, &budget); err != nil {
			return result, err
		}
		var snapshotID, threadID string
		if err = tx.QueryRowContext(ctx, "SELECT snapshot_id,thread_id FROM kernel_run_bindings WHERE run_id=?", r.RunID).Scan(&snapshotID, &threadID); err != nil {
			return result, err
		}
		if budget.RunID != r.RunID || budget.SessionID != b.ID || budget.SnapshotID != snapshotID || budget.ThreadID != threadID || threadID != b.ThreadID {
			return result, ErrInterruptionHeld
		}
		targets, err := effectiveRunTargetsTx(ctx, tx, b, r.RunID, snapshotID, budget.Targets)
		if err != nil {
			return result, err
		}
		if !slices.Contains(targets, r.Target) {
			return result, ErrInterruptionHeld
		}
	} else {
		return result, ErrInterruptionHeld
	}
	var owner string
	if err = completionRecord(ctx, tx, "managed_preparation", r.PreparationID, &owner); err != nil {
		return result, err
	}
	p, err := managedProcessTx(ctx, tx, owner)
	if err != nil {
		return result, err
	}
	if p.PreparationID != r.PreparationID || p.Request.Binding.ID != b.ID || p.Request.RunID != r.RunID || p.State == "stopped" {
		return result, ErrInterruptionHeld
	}
	result = SessionInterruption{ID: id, Request: r, Digest: digest, OwnerID: owner, State: "pending", CreatedAt: k.clock()}
	if err = kernelRecordTx(ctx, tx, "session_interrupt", id, b.ID, result); err != nil {
		return result, err
	}
	// Logical interruption belongs to the run across read-only recovery launches.
	// Each physical stop request retains its own exact preparation and owner.
	var previous string
	err = completionRecord(ctx, tx, "run_interrupt", r.RunID, &previous)
	if errors.Is(err, sql.ErrNoRows) {
		err = kernelRecordTx(ctx, tx, "run_interrupt", r.RunID, b.ID, id)
	}
	if err != nil {
		return result, err
	}
	if err = kernelRecordTx(ctx, tx, "interrupt_preparation_stop", r.PreparationID, b.ID, id); err != nil {
		return result, err
	}
	if b.Kind == "task" {
		task, err := loadTask(ctx, tx, b.TaskID)
		if err != nil {
			return result, err
		}
		if !task.State.Terminal() {
			task.State, task.Reason = contract.TaskBlocked, "operator_interrupt: "+r.Reason
			if err = saveTask(ctx, tx, task); err != nil {
				return result, err
			}
		}
	}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return result, err
	}
	if err = k.waveEventTx(ctx, tx, w, "run_interrupt_requested", b.TaskID, id+":requested", result); err != nil {
		return result, err
	}
	return result, tx.Commit()
}

func runInterruptedTx(ctx context.Context, q queryer, sessionID, runID string) (bool, error) {
	var id string
	err := completionRecord(ctx, q, "run_interrupt", runID, &id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var i SessionInterruption
	if err = completionRecord(ctx, q, "session_interrupt", id, &i); err != nil {
		return false, err
	}
	if err = validateInterruption(i, id); err != nil || i.Request.SessionID != sessionID || i.Request.RunID != runID {
		return false, errors.New("interrupted run source conflict")
	}
	return true, nil
}

// RunWasInterrupted lets the Orchestrator guard main result application.
// A normal thread history result cannot revoke an operator interruption.
func (k *KernelStore) RunWasInterrupted(ctx context.Context, sessionID, runID string) (bool, error) {
	return runInterruptedTx(ctx, k.DB, sessionID, runID)
}

func (k *KernelStore) CompleteSessionInterrupt(ctx context.Context, id string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var i SessionInterruption
	if err = completionRecord(ctx, tx, "session_interrupt", id, &i); err != nil {
		return err
	}
	if err = validateInterruption(i, id); err != nil {
		return err
	}
	if i.State == "stopped" {
		return nil
	}
	p, err := managedProcessTx(ctx, tx, i.OwnerID)
	if err != nil {
		return err
	}
	if p.State != "stopped" || p.PreparationID != i.Request.PreparationID || p.Request.RunID != i.Request.RunID || p.Request.Binding.ID != i.Request.SessionID {
		return ErrInterruptionHeld
	}
	var proof ManagedStopProof
	if err = completionRecord(ctx, tx, "managed_stop_proof", i.OwnerID, &proof); err != nil {
		return err
	}
	if proof.OwnerID != i.OwnerID || proof.Disposition != p.StopDisposition || proof.Disposition == "group_stopped" && proof.Identity != p.Identity {
		return ErrInterruptionHeld
	}
	i.State, i.CompletedAt = "stopped", k.clock()
	if err = kernelRecordTx(ctx, tx, "session_interrupt", id, i.Request.SessionID, i); err != nil {
		return err
	}
	w, err := loadWave(ctx, tx, i.Request.Target.WaveID)
	if err != nil {
		return err
	}
	if err = k.waveEventTx(ctx, tx, w, "run_interrupt_stopped", p.Request.Binding.TaskID, id+":stopped", i); err != nil {
		return err
	}
	return tx.Commit()
}

type SessionInterruptPage struct {
	Interruptions []SessionInterruption `json:"interruptions"`
	NextAfter     string                `json:"next_after"`
}

func (k *KernelStore) PendingSessionInterrupts(ctx context.Context, after string, limit int) (SessionInterruptPage, error) {
	p := SessionInterruptPage{Interruptions: []SessionInterruption{}}
	if limit < 1 || limit > 128 {
		return p, errors.New("interruption page limit must be 1..128")
	}
	rows, err := k.DB.QueryContext(ctx, "SELECT id,payload FROM records WHERE kind='session_interrupt' AND id>? AND json_extract(payload,'$.state')='pending' ORDER BY id LIMIT ?", after, limit+1)
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
		var i SessionInterruption
		if err = json.Unmarshal(raw, &i); err != nil {
			return p, err
		}
		if err = validateInterruption(i, id); err != nil || i.State != "pending" {
			return p, errors.New("pending interruption identity conflict")
		}
		if len(p.Interruptions) == limit {
			p.NextAfter = p.Interruptions[limit-1].ID
			break
		}
		p.Interruptions = append(p.Interruptions, i)
	}
	return p, rows.Err()
}
