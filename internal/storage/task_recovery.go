package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrTaskRecoveryRequired = errors.New("blocked Task requires an explicit recovery authorization")

type taskRecoveryPermit struct {
	SessionID      string `json:"session_id"`
	TaskDigest     string `json:"task_digest"`
	RunID          string `json:"run_id"`
	SnapshotID     string `json:"snapshot_id"`
	SnapshotDigest string `json:"snapshot_digest"`
	Reason         string `json:"reason"`
	Consumed       bool   `json:"consumed"`
}

// AuthorizeTaskRecovery is a trusted controller/operator decision after failure
// classification, never a model tool. Bind it to the selected Task state, exact
// future run and input; polling readiness alone grants no retry authority.
func (k *KernelStore) AuthorizeTaskRecovery(ctx context.Context, expected domain.Task, runID string, snapshot contract.InputSnapshot, reason string) error {
	if expected.State != contract.TaskBlocked || runID == "" || snapshot.ID == "" || snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) || !utf8.ValidString(reason) || strings.TrimSpace(reason) == "" || len(reason) > contract.MaxInline {
		return ErrTaskRecoveryRequired
	}
	if err := contract.ValidateSessionInput(snapshot.Input); err != nil {
		return err
	}
	digest, err := contentDigest(expected)
	if err != nil {
		return err
	}
	permit := taskRecoveryPermit{SessionID: expected.SessionID, TaskDigest: digest, RunID: runID, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest, Reason: reason}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var prior taskRecoveryPermit
	err = completionRecord(ctx, tx, "task_recovery_permit", runID, &prior)
	if err == nil {
		prior.Consumed = false
		if prior != permit {
			return errors.New("recovery authorization identity conflict")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	current, err := loadTask(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	actual, err := contentDigest(current)
	if err != nil {
		return err
	}
	if actual != digest {
		return errors.New("Task changed before recovery authorization")
	}
	var active string
	var stopped int
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped FROM kernel_sessions WHERE id=?", expected.SessionID).Scan(&active, &stopped); err != nil {
		return err
	}
	if active != "" || stopped != 1 {
		return errors.New("reconcile and stop previous run before authorizing recovery")
	}
	if err = k.checkTaskDispatchTx(ctx, tx, current); err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "task_recovery_permit", runID, expected.ID, permit); err != nil {
		return err
	}
	return tx.Commit()
}

func consumeTaskRecoveryTx(ctx context.Context, tx *sql.Tx, task domain.Task, snapshot contract.InputSnapshot, run contract.SessionRun) error {
	if task.State != contract.TaskBlocked {
		return nil
	}
	var permit taskRecoveryPermit
	err := completionRecord(ctx, tx, "task_recovery_permit", run.ID, &permit)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrTaskRecoveryRequired
	}
	if err != nil {
		return err
	}
	digest, err := contentDigest(task)
	if err != nil {
		return err
	}
	if permit.Consumed || permit.SessionID != task.SessionID || permit.TaskDigest != digest || permit.RunID != run.ID || permit.SnapshotID != snapshot.ID || permit.SnapshotDigest != snapshot.Digest {
		return ErrTaskRecoveryRequired
	}
	permit.Consumed = true
	return kernelRecordTx(ctx, tx, "task_recovery_permit", run.ID, task.ID, permit)
}
