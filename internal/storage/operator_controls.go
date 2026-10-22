package storage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// OperatorControl is a trusted operator request, never a model tool or IPC
// command. Pause/resume only gate new dispatch; cancel keeps sent work tracked.
// Explicit run stopping and versioned direction changes have separate APIs.
type OperatorControl struct {
	Key    string          `json:"key"`
	Target contract.Target `json:"target"`
	TaskID string          `json:"task_id,omitempty"`
	Action string          `json:"action"` // pause, resume, cancel
	Reason string          `json:"reason"`
}

type OperatorControlReceipt struct {
	ID        string          `json:"id"`
	Request   OperatorControl `json:"request"`
	Digest    string          `json:"digest"`
	CreatedAt time.Time       `json:"created_at"`
}

func (k *KernelStore) ApplyOperatorControl(ctx context.Context, r OperatorControl) (OperatorControlReceipt, error) {
	var receipt OperatorControlReceipt
	if !r.Target.Valid() || strings.TrimSpace(r.Key) == "" || len(r.Key) > 256 || !utf8.ValidString(r.Key) || len(r.TaskID) > contract.MaxInline || !utf8.ValidString(r.TaskID) || strings.TrimSpace(r.Reason) == "" || len(r.Reason) > contract.MaxInline || !utf8.ValidString(r.Reason) {
		return receipt, errors.New("versioned operator action, key and reason required")
	}
	if r.Action != "pause" && r.Action != "resume" && r.Action != "cancel" {
		return receipt, errors.New("unsupported operator action")
	}
	digest, err := contentDigest(r)
	if err != nil {
		return receipt, err
	}
	id := structureID("operator-", r.Target.WaveID, r.Key)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback()
	err = completionRecord(ctx, tx, "operator_control", id, &receipt)
	if err == nil {
		if receipt.ID != id || receipt.Digest != digest || receipt.Request != r {
			return receipt, errors.New("operator action replay conflict")
		}
		return receipt, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return receipt, err
	}
	if w.Target() != r.Target || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted {
		return receipt, errors.New("operator target is stale or terminal")
	}
	receipt = OperatorControlReceipt{ID: id, Request: r, Digest: digest, CreatedAt: k.clock()}
	if r.TaskID != "" {
		task, err := loadTask(ctx, tx, r.TaskID)
		if err != nil {
			return receipt, err
		}
		if task.Target() != r.Target || task.State.Terminal() {
			return receipt, errors.New("operator Task is stale or terminal")
		}
		if err = k.applyTaskControlTx(ctx, tx, w, task, receipt); err != nil {
			return receipt, err
		}
	} else {
		if r.Action == "cancel" {
			w.State, w.Reason, w.CompletedAt = domain.WaveCanceled, r.Reason, receipt.CreatedAt
			// Cancellation supersedes pause. The parent's terminal observation
			// must remain deliverable; terminal state still forbids Task dispatch.
			w.Paused = false
			rows, err := tx.QueryContext(ctx, "SELECT id FROM kernel_tasks WHERE wave_id=? ORDER BY id", w.ID)
			if err != nil {
				return receipt, err
			}
			ids := []string{}
			for rows.Next() {
				var taskID string
				if err = rows.Scan(&taskID); err != nil {
					rows.Close()
					return receipt, err
				}
				ids = append(ids, taskID)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return receipt, err
			}
			for _, taskID := range ids {
				task, err := loadTask(ctx, tx, taskID)
				if err != nil {
					return receipt, err
				}
				if !task.State.Terminal() {
					if err = k.applyTaskControlTx(ctx, tx, w, task, receipt); err != nil {
						return receipt, err
					}
				}
			}
		} else {
			w.Paused = r.Action == "pause"
		}
		if err = saveWave(ctx, tx, w); err != nil {
			return receipt, err
		}
		if r.Action == "cancel" {
			if err = k.waveEventTx(ctx, tx, w, "wave_canceled", "", id+":wave_canceled", receipt); err != nil {
				return receipt, err
			}
		}
	}
	if err = kernelRecordTx(ctx, tx, "operator_control", id, w.ID, receipt); err != nil {
		return receipt, err
	}
	if err = k.waveEventTx(ctx, tx, w, "operator_control", r.TaskID, id, receipt); err != nil {
		return receipt, err
	}
	return receipt, tx.Commit()
}

func (k *KernelStore) applyTaskControlTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task, receipt OperatorControlReceipt) error {
	if receipt.Request.Action == "cancel" {
		task.State, task.Reason, task.CompletedAt = contract.TaskCanceled, receipt.Request.Reason, receipt.CreatedAt
	} else {
		task.Paused = receipt.Request.Action == "pause"
	}
	if err := saveTask(ctx, tx, task); err != nil {
		return err
	}
	if receipt.Request.Action == "cancel" {
		result := ResultApplication{TaskID: task.ID, Target: task.Target(), State: task.State, Reason: task.Reason}
		return k.waveEventTx(ctx, tx, w, "task_canceled", task.ID, receipt.ID+":task_canceled:"+task.ID, result)
	}
	return nil
}
