package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

var ErrMainRotationHeld = errors.New("main thread replacement awaits a safe input boundary")

type MainThreadRotation struct {
	ID               string    `json:"id"`
	SessionID        string    `json:"session_id"`
	Generation       int       `json:"generation"`
	OldThreadID      string    `json:"old_thread_id"`
	NewThreadID      string    `json:"new_thread_id"`
	OldPreparationID string    `json:"old_preparation_id"`
	SnapshotID       string    `json:"snapshot_id"`
	SnapshotDigest   string    `json:"snapshot_digest"`
	Reason           string    `json:"reason"`
	State            string    `json:"state"` // pending, bound
	StartedAt        time.Time `json:"started_at"`
	CompletedAt      time.Time `json:"completed_at"`
}

// EnsureMainThreadCapacity runs only for an actual selected next input. Thread
// replacement itself starts no process/model and spends no Wave/turn budget.
// The subsequent ordinary Prepare creates the new thread and completes audit.
func (k *KernelStore) EnsureMainThreadCapacity(ctx context.Context, sessionID string, snapshot contract.InputSnapshot) (bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return false, err
	}
	if b.Kind != "orchestrator" {
		return false, errors.New("main maintenance requires an orchestrator")
	}
	if b.ThreadID == "" {
		return false, nil
	}
	targets, err := k.rotationSnapshotTx(ctx, tx, b, snapshot)
	if err != nil {
		return false, err
	}
	limit := math.MaxInt
	for _, target := range targets {
		w, err := loadWave(ctx, tx, target.WaveID)
		if err != nil {
			return false, err
		}
		limit = min(limit, w.Policy.Limits.ThreadTurns)
	}
	var turns int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings WHERE session_id=? AND thread_id=?", b.ID, b.ThreadID).Scan(&turns); err != nil {
		return false, err
	}
	if turns < limit {
		return false, nil
	}
	if _, err = k.beginMainRotationTx(ctx, tx, b, snapshot, "thread_turn_limit"); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// BeginMainThreadReplacement is a trusted explicit recovery decision for a
// quiescent known history. It cannot discard unknown/unapplied runs, stop a
// process by assertion, or reuse a terminal Task's session.
func (k *KernelStore) BeginMainThreadReplacement(ctx context.Context, expected contract.SessionBinding, snapshot contract.InputSnapshot, reason string) (MainThreadRotation, error) {
	var out MainThreadRotation
	if strings.TrimSpace(reason) == "" || len(reason) > contract.MaxInline || !utf8.ValidString(reason) {
		return out, errors.New("bounded main replacement reason required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return out, err
	}
	if !reflect.DeepEqual(b, expected) {
		return out, ErrMainRotationHeld
	}
	if _, err = k.rotationSnapshotTx(ctx, tx, b, snapshot); err != nil {
		return out, err
	}
	out, err = k.beginMainRotationTx(ctx, tx, b, snapshot, reason)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}
func (k *KernelStore) rotationSnapshotTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, snapshot contract.InputSnapshot) ([]contract.Target, error) {
	if b.Kind != "orchestrator" || b.Closed || snapshot.ID == "" || snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) {
		return nil, ErrMainRotationHeld
	}
	if err := contract.ValidateSessionInput(snapshot.Input); err != nil {
		return nil, err
	}
	var digest string
	var data []byte
	if err := tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", snapshot.ID).Scan(&digest, &data); err != nil {
		return nil, err
	}
	if digest != snapshot.Digest || string(data) != string(snapshot.Input) {
		return nil, errors.New("replacement snapshot is not the saved input")
	}
	targets, err := mainBudgetTargets(b, snapshot)
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		w, err := loadWave(ctx, tx, target.WaveID)
		if err != nil {
			return nil, err
		}
		if w.OrchestratorSessionID != b.ID || w.Target() != target || w.DispatchPaused() || !k.clock().Before(w.Deadline) {
			return nil, ErrMainRotationHeld
		}
	}
	return targets, nil
}

func (k *KernelStore) beginMainRotationTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, snapshot contract.InputSnapshot, reason string) (MainThreadRotation, error) {
	var out MainThreadRotation
	if b.Kind != "orchestrator" || b.Closed || b.ThreadID == "" || b.Generation == math.MaxInt {
		return out, ErrMainRotationHeld
	}
	state, err := sessionDispatchStateTx(ctx, tx, b.ID)
	if err != nil {
		return out, err
	}
	if state.Run != nil || state.Creating || !state.ProcessStopped {
		return out, ErrMainRotationHeld
	}
	var unresolved int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings WHERE session_id=? AND (applied=0 OR process_stopped=0)", b.ID).Scan(&unresolved); err != nil {
		return out, err
	}
	if unresolved != 0 {
		return out, ErrMainRotationHeld
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='managed_process' AND json_extract(payload,'$.request.binding.id')=? AND json_extract(payload,'$.state')<>'stopped'", b.ID).Scan(&unresolved); err != nil {
		return out, err
	}
	if unresolved != 0 {
		return out, ErrMainRotationHeld
	}
	if state.Wake != nil {
		var owner string
		err = completionRecord(ctx, tx, "managed_run_claim", state.Wake.Intent.RunID, &owner)
		if err == nil {
			return out, ErrMainRotationHeld
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
	}
	var pending string
	if err = completionRecord(ctx, tx, "main_rotation_pending", b.ID, &pending); err == nil {
		return out, ErrMainRotationHeld
	} else if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	nextGeneration := b.Generation + 1
	out = MainThreadRotation{ID: structureID("main-rotation-", b.ID, strconv.Itoa(nextGeneration)), SessionID: b.ID, Generation: nextGeneration, OldThreadID: b.ThreadID, OldPreparationID: b.PreparationID, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest, Reason: reason, State: "pending", StartedAt: k.clock()}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_rebindings(session_id,generation,old_thread,new_thread,snapshot_id,reason,created_at) VALUES(?,?,?,'',?,?,?)", b.ID, nextGeneration, b.ThreadID, snapshot.ID, reason, k.clock()); err != nil {
		return out, err
	}
	if err = kernelRecordTx(ctx, tx, "main_thread_rotation", out.ID, b.ID, out); err != nil {
		return out, err
	}
	if err = kernelRecordTx(ctx, tx, "main_rotation_pending", b.ID, b.ID, out.ID); err != nil {
		return out, err
	}
	oldPreparation := b.PreparationID
	b.ThreadID = ""
	b.PreparationID = ""
	b.Generation = nextGeneration
	raw, err := json.Marshal(b)
	if err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET thread_id=NULL,payload=? WHERE id=?", raw, b.ID); err != nil {
		return out, err
	}
	// The known non-start was explicitly reconciled by this trusted replacement.
	if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='kernel_dispatch_hold' AND id=? AND json_extract(payload,'$.preparation_id')=?", b.ID, oldPreparation); err != nil {
		return out, err
	}
	return out, nil
}
func (k *KernelStore) finishMainRotationTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, thread string) error {
	if b.Kind != "orchestrator" {
		return nil
	}
	var id string
	err := completionRecord(ctx, tx, "main_rotation_pending", b.ID, &id)
	if errors.Is(err, sql.ErrNoRows) {
		// A lost INITIAL creation has no previous known thread. Its recovery audit
		// still receives the successfully observed next identity atomically.
		_, e := tx.ExecContext(ctx, "UPDATE kernel_rebindings SET new_thread=? WHERE session_id=? AND generation=? AND old_thread='' AND new_thread=''", thread, b.ID, b.Generation)
		return e
	}
	if err != nil {
		return err
	}
	var rotation MainThreadRotation
	if err = completionRecord(ctx, tx, "main_thread_rotation", id, &rotation); err != nil {
		return err
	}
	if rotation.ID != id || rotation.SessionID != b.ID || rotation.Generation != b.Generation || rotation.State != "pending" || thread == rotation.OldThreadID || thread == "" {
		return ErrMainRotationHeld
	}
	var reused bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM kernel_run_bindings WHERE session_id=? AND thread_id=?) OR EXISTS(SELECT 1 FROM kernel_rebindings WHERE session_id=? AND (old_thread=? OR new_thread=?))", b.ID, thread, b.ID, thread, thread).Scan(&reused); err != nil {
		return err
	}
	if reused {
		return errors.New("replacement must bind a fresh main thread")
	}
	rotation.NewThreadID = thread
	rotation.State = "bound"
	rotation.CompletedAt = k.clock()
	if err = kernelRecordTx(ctx, tx, "main_thread_rotation", id, b.ID, rotation); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE kernel_rebindings SET new_thread=? WHERE session_id=? AND generation=? AND new_thread=''", thread, b.ID, b.Generation)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return errors.New("main rotation audit binding conflict")
	}
	_, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='main_rotation_pending' AND id=?", b.ID)
	return err
}
