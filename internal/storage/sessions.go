package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func sessionBindingTx(ctx context.Context, q queryer, id string) (contract.SessionBinding, error) {
	var b contract.SessionBinding
	var raw []byte
	var kind, parent string
	var task, thread sql.NullString
	err := q.QueryRowContext(ctx, "SELECT kind,task_id,parent_id,thread_id,payload FROM kernel_sessions WHERE id=?", id).Scan(&kind, &task, &parent, &thread, &raw)
	if err != nil {
		return b, err
	}
	if err = json.Unmarshal(raw, &b); err != nil {
		return b, err
	}
	if b.ID != id || b.Kind != kind || b.ParentSessionID != parent || b.TaskID != task.String || b.ThreadID != thread.String {
		return b, errors.New("inconsistent session identity")
	}
	if b.Kind == "task" {
		t, err := loadTask(ctx, q, b.TaskID)
		if err != nil {
			return b, err
		}
		if t.SessionID != b.ID || t.ParentSessionID != b.ParentSessionID || t.PhaseID != b.PhaseID || t.Target() != b.Target {
			return b, errors.New("Task/session binding conflict")
		}
		b.TaskState = t.State
		b.Closed = b.Closed || t.State.Terminal()
	}
	return b, nil
}
func (k *KernelStore) SessionBinding(ctx context.Context, id string) (contract.SessionBinding, error) {
	return sessionBindingTx(ctx, k.DB, id)
}
func sessionDispatchableTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding) error {
	if b.Closed {
		return errors.New("closed session")
	}
	if b.Kind == "orchestrator" {
		return nil
	}
	if b.Kind != "task" {
		return errors.New("unknown session kind")
	}
	task, err := loadTask(ctx, tx, b.TaskID)
	if err != nil {
		return err
	}
	wave, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return err
	}
	if task.State.Terminal() || task.Paused || wave.Paused || wave.State == domain.WaveCanceled || wave.State == domain.WaveCompleted || wave.Target() != task.Target() {
		return errors.New("Task/Wave is stale, paused or terminal")
	}
	return nil
}
func (k *KernelStore) ReserveSessionThread(ctx context.Context, expected contract.SessionBinding) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b, expected) {
		return errors.New("session changed before thread creation")
	}
	if err = sessionDispatchableTx(ctx, tx, b); err != nil {
		return err
	}
	if b.Kind == "task" {
		wave, err := loadWave(ctx, tx, b.Target.WaveID)
		if err != nil {
			return err
		}
		if !k.clock().Before(wave.Deadline) {
			return errors.New("Wave deadline exceeded")
		}
	}
	creating := 0
	if b.ThreadID == "" {
		creating = 1
	}
	result, err := tx.ExecContext(ctx, "UPDATE kernel_sessions SET creating=?,process_stopped=0 WHERE id=? AND creating=0 AND active_run_id='' AND process_stopped=1", creating, b.ID)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("unknown thread creation or active run requires reconciliation")
	}
	b.PreparationID, err = opaqueID("prepare-")
	if err != nil {
		return err
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET payload=? WHERE id=?", raw, b.ID); err != nil {
		return err
	}
	if b.ThreadID == "" {
		if err = kernelRecordTx(ctx, tx, "thread_creation_intent", fmt.Sprintf("%s:g%d", b.ID, b.Generation), b.ID, b); err != nil {
			return err
		}
	}
	if err = kernelRecordTx(ctx, tx, "session_preparation", b.ID, b.ID, b); err != nil {
		return err
	}
	return tx.Commit()
}
func (k *KernelStore) BindSessionThread(ctx context.Context, expected contract.SessionBinding, thread string) error {
	if thread == "" {
		return errors.New("empty thread identity")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b, expected) || (b.ThreadID != "" && b.ThreadID != thread) {
		return errors.New("thread/session binding changed")
	}
	if err = sessionDispatchableTx(ctx, tx, b); err != nil {
		return err
	}
	var creating, stopped int
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT creating,process_stopped,active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&creating, &stopped, &active); err != nil {
		return err
	}
	if stopped != 0 || active != "" || (b.ThreadID == "" && creating != 1) {
		return errors.New("thread creation was not reserved")
	}
	b.ThreadID = thread
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET thread_id=?,creating=0,payload=? WHERE id=?", thread, raw, b.ID); err != nil {
		return err
	}
	return tx.Commit()
}

// ReserveSessionRun reserves a bounded Task attempt and an immutable snapshot
// before turn/start. Global/Wave scheduling reservations are added by Task_4.
func (k *KernelStore) ReserveSessionRun(ctx context.Context, expected contract.SessionBinding, snapshot contract.InputSnapshot, run contract.SessionRun) error {
	if snapshot.ID == "" || snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) {
		return errors.New("invalid input snapshot identity")
	}
	if err := contract.ValidateSessionInput(snapshot.Input); err != nil {
		return err
	}
	if run.ID == "" || run.ThreadID != expected.ThreadID || run.TurnID != "" || run.State != "starting" || !run.BaselineKnown || run.Started.IsZero() || run.Final != "" {
		return errors.New("invalid run start intent")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b, expected) || b.ThreadID == "" || b.PreparationID == "" {
		return errors.New("session changed before turn start")
	}
	if err = sessionDispatchableTx(ctx, tx, b); err != nil {
		return err
	}
	var active string
	var stopped, creating int
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &stopped, &creating); err != nil {
		return err
	}
	if active != "" || stopped != 0 || creating != 0 {
		return errors.New("exclusive prepared process required before turn start")
	}
	if b.Kind == "task" {
		task, err := loadTask(ctx, tx, b.TaskID)
		if err != nil {
			return err
		}
		wave, err := loadWave(ctx, tx, task.WaveID)
		if err != nil {
			return err
		}
		if !k.clock().Before(wave.Deadline) || task.Attempts >= wave.Policy.Limits.TaskAttempts {
			return errors.New("Wave deadline/Task attempt budget exhausted")
		}
		for _, dependency := range task.Spec.DependsOn {
			prior, err := loadTask(ctx, tx, structureID("task-", wave.ID, dependency.TaskID))
			if err != nil {
				return err
			}
			if prior.State != contract.TaskCompleted || prior.ResultID == "" || prior.ScopeRevision != wave.ScopeRevision || prior.PolicyRevision != wave.Policy.Revision {
				return errors.New("required Task result is not ready in the current scope")
			}
		}
		inputs, err := k.taskInputDigestTx(ctx, tx, wave, task.Spec)
		if err != nil {
			return err
		}
		task.InputDigest = inputs
		task.Attempts++
		task.State = contract.TaskRunning
		if err = saveTask(ctx, tx, task); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'task_attempt',1,0,?)", wave.ID, run.ID, []byte(`{}`)); err != nil {
			return err
		}
	}
	var savedDigest string
	var savedData []byte
	err = tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", snapshot.ID).Scan(&savedDigest, &savedData)
	if err == nil {
		if savedDigest != snapshot.Digest || string(savedData) != string(snapshot.Input) {
			return errors.New("snapshot identity conflict")
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_snapshots(id,digest,data) VALUES(?,?,?)", snapshot.ID, snapshot.Digest, snapshot.Input); err != nil {
			return err
		}
	} else {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_run_bindings(run_id,session_id,snapshot_id,thread_id,state) VALUES(?,?,?,?,'starting')", run.ID, b.ID, snapshot.ID, b.ThreadID); err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "agent_run", run.ID, b.ID, run); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET active_run_id=?,process_stopped=0 WHERE id=?", run.ID, b.ID); err != nil {
		return err
	}
	return tx.Commit()
}
func (k *KernelStore) Put(ctx context.Context, kind, id, owner string, value any) error {
	if kind != "agent_run" {
		return k.Store.Put(ctx, kind, id, owner, value)
	}
	run, ok := value.(contract.SessionRun)
	if !ok || run.ID != id {
		return errors.New("invalid session run fact")
	}
	switch run.State {
	case "running", "unknown", "blocked", "completed", "failed", "interrupted":
	default:
		return errors.New("run must be reserved before recording facts")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var session, thread string
	var applied int
	if err = tx.QueryRowContext(ctx, "SELECT session_id,thread_id,applied FROM kernel_run_bindings WHERE run_id=?", id).Scan(&session, &thread, &applied); err != nil {
		return err
	}
	if session != owner || thread != run.ThreadID {
		return errors.New("run fact binding conflict")
	}
	var previous contract.SessionRun
	if err = completionRecord(ctx, tx, "agent_run", id, &previous); err != nil {
		return err
	}
	if previous.TurnID != "" && run.TurnID != previous.TurnID {
		return errors.New("run turn association changed")
	}
	priorDigest, _ := contentDigest(previous)
	nextDigest, _ := contentDigest(run)
	if (previous.State == "completed" || applied == 1) && priorDigest != nextDigest {
		return errors.New("confirmed run completion cannot be overwritten")
	}
	if !previous.Started.Equal(run.Started) || previous.BaselineKnown != run.BaselineKnown || !reflect.DeepEqual(previous.BaselineTurnIDs, run.BaselineTurnIDs) {
		return errors.New("run start evidence cannot be overwritten")
	}
	if err = kernelRecordTx(ctx, tx, "agent_run", id, owner, run); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_run_bindings SET state=? WHERE run_id=?", run.State, id); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkSessionStopped is called only after the trusted process owner confirms all
// app-server children stopped. Normal turn completion alone never frees a slot.
func (k *KernelStore) MarkSessionStopped(ctx context.Context, sessionID, proofID string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", sessionID).Scan(&active); err != nil {
		return err
	}
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if proofID == "" || (active != "" && active != proofID) || (active == "" && b.PreparationID != proofID) {
		return errors.New("session stopping proof belongs to another run")
	}
	if active != "" {
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_run_bindings SET process_stopped=1 WHERE run_id=? AND session_id=?", active, sessionID); err != nil {
			return err
		}
	}
	// Keep the run binding until its outcome is applied or explicitly archived.
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET process_stopped=1 WHERE id=?", sessionID); err != nil {
		return err
	}
	return tx.Commit()
}
func (k *KernelStore) RebindMainThread(ctx context.Context, expected contract.SessionBinding, next, snapshotID, reason string) error {
	if expected.Kind != "orchestrator" || expected.ThreadID == "" || next == "" || next == expected.ThreadID || snapshotID == "" || reason == "" {
		return errors.New("audited main rebinding required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b, expected) {
		return errors.New("main binding changed")
	}
	var active string
	var stopped, creating int
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &stopped, &creating); err != nil {
		return err
	}
	if active != "" || stopped != 1 || creating != 0 {
		return errors.New("main process/run reconciliation incomplete")
	}
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings WHERE session_id=? AND (applied=0 OR process_stopped=0)", b.ID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errors.New("main has unapplied/unknown runs")
	}
	var digest string
	if err = tx.QueryRowContext(ctx, "SELECT digest FROM kernel_snapshots WHERE id=?", snapshotID).Scan(&digest); err != nil {
		return err
	}
	if digest == "" {
		return errors.New("saved snapshot required")
	}
	b.ThreadID = next
	b.Generation++
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_rebindings(session_id,generation,old_thread,new_thread,snapshot_id,reason,created_at) VALUES(?,?,?,?,?,?,?)", b.ID, b.Generation, expected.ThreadID, next, snapshotID, reason, k.clock()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET thread_id=?,payload=? WHERE id=?", next, raw, b.ID); err != nil {
		return err
	}
	return tx.Commit()
}

// ArchiveSessionRun records a known, stopped outcome after the controller has
// applied it (or classified it as canceled/invalid). Unknown starts require
// history reconciliation instead and cannot be discarded through this method.
func (k *KernelStore) ArchiveSessionRun(ctx context.Context, sessionID, runID, reason string) error {
	if reason == "" {
		return errors.New("run disposition reason required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var owner, state string
	var stopped, applied int
	if err = tx.QueryRowContext(ctx, "SELECT session_id,state,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", runID).Scan(&owner, &state, &stopped, &applied); err != nil {
		return err
	}
	if owner != sessionID || stopped != 1 || (state != "completed" && state != "failed" && state != "interrupted" && state != "blocked") {
		return errors.New("run must have a known stopped outcome before disposition")
	}
	if applied == 1 {
		var prior map[string]string
		if err = completionRecord(ctx, tx, "session_run_disposition", runID, &prior); err != nil {
			return err
		}
		if prior["reason"] != reason {
			return errors.New("run disposition identity conflict")
		}
		return nil
	}
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", sessionID).Scan(&active); err != nil {
		return err
	}
	if active != runID {
		return errors.New("run disposition belongs to another active run")
	}
	binding, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if binding.Kind == "task" {
		task, err := loadTask(ctx, tx, binding.TaskID)
		if err != nil {
			return err
		}
		if !task.State.Terminal() {
			task.State = contract.TaskBlocked
			task.Reason = reason
			if err = saveTask(ctx, tx, task); err != nil {
				return err
			}
		}
	}
	if err = kernelRecordTx(ctx, tx, "session_run_disposition", runID, sessionID, map[string]string{"reason": reason}); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_run_bindings SET applied=1 WHERE run_id=?", runID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET active_run_id='' WHERE id=? AND active_run_id=? AND process_stopped=1", sessionID, runID); err != nil {
		return err
	}
	return tx.Commit()
}

// RetireUnboundTaskSession abandons a lost thread/start response only after its
// process is proven stopped. The old Task/session is terminal; a replacement
// plan must allocate a fresh identity, never repeat this creation intent.
func (k *KernelStore) RetireUnboundTaskSession(ctx context.Context, sessionID, reason string) error {
	if reason == "" {
		return errors.New("thread loss reason required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if b.Kind != "task" || b.ThreadID != "" || b.Closed {
		return errors.New("only an unfinished unbound Task may be retired")
	}
	var creating, stopped int
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT creating,process_stopped,active_run_id FROM kernel_sessions WHERE id=?", sessionID).Scan(&creating, &stopped, &active); err != nil {
		return err
	}
	if creating != 1 || stopped != 1 || active != "" {
		return errors.New("unknown thread creation must be stopped before retirement")
	}
	task, err := loadTask(ctx, tx, b.TaskID)
	if err != nil {
		return err
	}
	task.State, task.Reason, task.CompletedAt = contract.TaskFailed, reason, k.clock()
	if err = saveTask(ctx, tx, task); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET creating=0 WHERE id=?", sessionID); err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "thread_creation_disposition", sessionID, sessionID, map[string]string{"reason": reason, "state": "retired"}); err != nil {
		return err
	}
	wave, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return err
	}
	if err = k.waveEventTx(ctx, tx, wave, "task_failed", task.ID, task.ID+":thread_lost", map[string]string{"reason": reason}); err != nil {
		return err
	}
	return tx.Commit()
}
