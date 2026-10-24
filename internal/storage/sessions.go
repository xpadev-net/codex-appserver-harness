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
	if task.State.Terminal() || task.Paused || wave.DispatchPaused() || wave.State == domain.WaveCanceled || wave.State == domain.WaveCompleted || wave.Target() != task.Target() {
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
	if err = k.reserveSessionThreadTx(ctx, tx, expected); err != nil {
		return err
	}
	return tx.Commit()
}
func (k *KernelStore) reserveSessionThreadTx(ctx context.Context, tx *sql.Tx, expected contract.SessionBinding) error {
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
	if err = k.reserveSessionResourcesTx(ctx, tx, b); err != nil {
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
	return nil
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
	var managedOwner string
	err = completionRecord(ctx, tx, "managed_preparation", b.PreparationID, &managedOwner)
	if err == nil {
		p, err := managedProcessTx(ctx, tx, managedOwner)
		if err != nil {
			return err
		}
		var method string
		if err := completionRecord(ctx, tx, "managed_thread_rpc", b.PreparationID, &method); err != nil {
			return err
		}
		want := "thread/start"
		if b.ThreadID != "" {
			want = "thread/resume"
		}
		if p.State != "started" || p.Request.Mode != "execute" || method != want {
			return ErrManagedProcessHeld
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	b.ThreadID = thread
	if err = bindSessionResourcesTx(ctx, tx, b, thread, ""); err != nil {
		return err
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET thread_id=?,creating=0,payload=? WHERE id=?", thread, raw, b.ID); err != nil {
		return err
	}
	if expected.ThreadID == "" {
		if err = kernelRecordTx(ctx, tx, "session_new_thread", thread, b.ID, b); err != nil {
			return err
		}
	}
	if err = k.finishMainRotationTx(ctx, tx, b, thread); err != nil {
		return err
	}
	if managedOwner != "" {
		if err = kernelRecordTx(ctx, tx, "managed_thread_ready", b.PreparationID, b.ID, ManagedThreadReady{OwnerID: managedOwner, ThreadID: thread}); err != nil {
			return err
		}
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
	var managedOwner string
	err = completionRecord(ctx, tx, "managed_preparation", b.PreparationID, &managedOwner)
	if err == nil {
		p, err := managedProcessTx(ctx, tx, managedOwner)
		if err != nil {
			return err
		}
		if p.State != "started" || p.Request.Mode != "execute" || p.Request.RunID != run.ID {
			return ErrManagedProcessHeld
		}
		var ready ManagedThreadReady
		if err = completionRecord(ctx, tx, "managed_thread_ready", b.PreparationID, &ready); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrManagedProcessHeld
			}
			return err
		}
		if ready.OwnerID != managedOwner || ready.ThreadID != b.ThreadID {
			return ErrManagedProcessHeld
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var runClaimOwner string
	err = completionRecord(ctx, tx, "managed_run_claim", run.ID, &runClaimOwner)
	if err == nil {
		if managedOwner == "" || runClaimOwner == "" || runClaimOwner != managedOwner {
			return ErrManagedProcessHeld
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		if managedOwner != "" {
			return ErrManagedProcessHeld
		}
	} else {
		return err
	}
	var deadlineStop int
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='deadline_preparation_stop' AND id=?)", b.PreparationID).Scan(&deadlineStop); err != nil {
		return err
	}
	if deadlineStop != 0 {
		return ErrDeadlineHeld
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
	if err = k.bindWakeRunTx(ctx, tx, b, snapshot, run); err != nil {
		return err
	}
	if err = k.reserveRunBudgetTx(ctx, tx, b, snapshot, run); err != nil {
		return err
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
		if err = k.checkTaskDispatchTx(ctx, tx, task); err != nil {
			return err
		}
		if err = consumeTaskRecoveryTx(ctx, tx, task, snapshot, run); err != nil {
			return err
		}
		subject, err := k.templateReviewSubjectTx(ctx, tx, task)
		if err != nil {
			return err
		}
		if subject != nil && task.Spec.Kind == "review" {
			if err = kernelRecordTx(ctx, tx, "template_review_target", task.ID, task.WaveID, templateReviewTarget{Subject: *subject, Target: task.Target(), RunID: run.ID}); err != nil {
				return err
			}
		}
		for _, dependency := range task.Spec.DependsOn {
			prior, err := loadTask(ctx, tx, structureID("task-", wave.ID, dependency.TaskID))
			if err != nil {
				return err
			}
			if prior.State != contract.TaskCompleted || prior.ResultID == "" || prior.ScopeRevision != wave.ScopeRevision || prior.PolicyRevision != wave.Policy.Revision {
				return errors.New("required Task result is not ready in the current scope")
			}
			if _, err = k.completedTaskResultTx(ctx, tx, wave, prior); err != nil {
				return err
			}
		}
		inputs, err := k.taskInputDigestTx(ctx, tx, wave, task.Spec)
		if err != nil {
			return err
		}
		task.InputDigest = inputs
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_plan_bindings SET input_digest=? WHERE wave_id=? AND revision=? AND task_id=?", inputs, task.WaveID, task.PlanRevision, task.ID); err != nil {
			return err
		}
		if len(task.Spec.WorkItemIDs) > 0 {
			if err := k.recordProductModelAdmissionTx(ctx, tx, wave, task, snapshot, run); err != nil {
				return err
			}
		}
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
	if err = bindSessionResourcesTx(ctx, tx, b, b.ThreadID, run.ID); err != nil {
		return err
	}
	return tx.Commit()
}
func (k *KernelStore) Put(ctx context.Context, kind, id, owner string, value any) error {
	if kind != "agent_run" {
		if kind == "session_input_manifest" {
			k.inputCache.clear()
		}
		return k.Store.Put(ctx, kind, id, owner, value)
	}
	run, ok := value.(contract.SessionRun)
	if !ok || run.ID != id {
		return errors.New("invalid session run fact")
	}
	switch run.State {
	case "running", "inProgress", "unknown", "blocked", "completed", "failed", "interrupted":
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
	// Tool handlers can observe a turn while turn/start is failing. Merge its
	// durable association under this same write transaction, then preserve the
	// incoming failure state so no later tool is authorized as still running.
	if run.TurnID == "" && previous.TurnID != "" && (run.State == "unknown" || run.State == "blocked") {
		run.TurnID = previous.TurnID
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
	if err = observeRunUsageTx(ctx, tx, run); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_run_bindings SET state=? WHERE run_id=?", run.State, id); err != nil {
		return err
	}
	return tx.Commit()
}

type SessionStopProof struct {
	SessionID     string    `json:"session_id"`
	PreparationID string    `json:"preparation_id"`
	RunID         string    `json:"run_id"`
	ProofID       string    `json:"proof_id"`
	Generation    int       `json:"generation"`
	ConfirmedAt   time.Time `json:"confirmed_at"`
}

func sessionStopProofID(sessionID, preparationID string) string {
	return structureID("session-stop-", sessionID, preparationID)
}

// MarkSessionStopped is called only after the trusted process owner confirms all
// app-server children stopped. Normal turn completion alone never frees a slot.
func (k *KernelStore) MarkSessionStopped(ctx context.Context, sessionID, proofID string) error {
	return k.markSessionStopped(ctx, sessionID, "", proofID)
}
func (k *KernelStore) MarkSessionPreparationStopped(ctx context.Context, sessionID, preparationID, proofID string) error {
	if preparationID == "" {
		return errors.New("preparation stopping identity required")
	}
	return k.markSessionStopped(ctx, sessionID, preparationID, proofID)
}
func (k *KernelStore) markSessionStopped(ctx context.Context, sessionID, preparationID, proofID string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = k.markSessionStoppedTx(ctx, tx, sessionID, preparationID, proofID); err != nil {
		return err
	}
	return tx.Commit()
}
func (k *KernelStore) markSessionStoppedTx(ctx context.Context, tx *sql.Tx, sessionID, preparationID, proofID string) (err error) {
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", sessionID).Scan(&active); err != nil {
		return err
	}
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if preparationID != "" && b.PreparationID != preparationID {
		return errors.New("stopping proof belongs to another preparation generation")
	}
	if preparationID == "" && active != "" {
		var recovered int
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('run_recovery','managed_run_claim') AND id=?)", active).Scan(&recovered); err != nil {
			return err
		}
		if recovered != 0 {
			return errors.New("managed or recovered run requires an explicit preparation stopping proof")
		}
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
	if err = k.releaseSessionResourcesTx(ctx, tx, b, proofID); err != nil {
		return err
	}
	// Retain the trusted stopping fact even for pre-feature processes whose
	// preparation never had a dispatch reservation. Do not infer it from a later
	// session latch, which may belong to another process generation.
	stop := SessionStopProof{SessionID: b.ID, PreparationID: b.PreparationID, RunID: active, ProofID: proofID, Generation: b.Generation, ConfirmedAt: k.clock()}
	stopID := sessionStopProofID(b.ID, b.PreparationID)
	var previous SessionStopProof
	err = completionRecord(ctx, tx, "session_stop_proof", stopID, &previous)
	if err == nil {
		if previous.SessionID != stop.SessionID || previous.PreparationID != stop.PreparationID || previous.RunID != stop.RunID || previous.ProofID != stop.ProofID || previous.Generation != stop.Generation {
			return errors.New("session stopping fact identity conflict")
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		if err = kernelRecordTx(ctx, tx, "session_stop_proof", stopID, b.ID, stop); err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
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
	if err = clearSessionRunHoldTx(ctx, tx, sessionID, runID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_run_bindings SET applied=1 WHERE run_id=?", runID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET active_run_id='' WHERE id=? AND active_run_id=? AND process_stopped=1", sessionID, runID); err != nil {
		return err
	}
	wakeOutcome := "archived"
	if !binding.Closed && (state == "failed" || state == "interrupted") {
		wakeOutcome = "failed"
	}
	if err = k.finishWakeTx(ctx, tx, sessionID, runID, wakeOutcome); err != nil {
		return err
	}
	if err = k.finishDirectOperationInputsTx(ctx, tx, sessionID, runID, wakeOutcome); err != nil {
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
	if err = k.retireUnboundTaskSessionTx(ctx, tx, sessionID, nil, reason); err != nil {
		return err
	}
	return tx.Commit()
}

func (k *KernelStore) retireUnboundTaskSessionTx(ctx context.Context, tx *sql.Tx, sessionID string, expected *contract.SessionBinding, reason string) error {
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if expected != nil && !reflect.DeepEqual(b, *expected) {
		return errors.New("Task thread retirement binding conflict")
	}
	if b.Kind != "task" || b.ThreadID != "" || b.Closed && b.TaskState != contract.TaskCanceled {
		return errors.New("only an unfinished or canceled unbound Task may be retired")
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
	alreadyCanceled := task.State == contract.TaskCanceled
	if !alreadyCanceled {
		task.State, task.Reason, task.CompletedAt = contract.TaskFailed, reason, k.clock()
		if err = saveTask(ctx, tx, task); err != nil {
			return err
		}
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
	kind := "task_failed"
	if alreadyCanceled {
		kind = "thread_creation_retired"
	}
	if err = k.waveEventTx(ctx, tx, wave, kind, task.ID, task.ID+":thread_lost", map[string]string{"reason": reason}); err != nil {
		return err
	}
	return nil
}

// RecoverUnboundMainThread audits a lost initial main thread/start after stopping
// proof. There can be no sent turn without a bound thread. Preserve its unknown
// creation intent and recovery snapshot before permitting a fresh generation.
func (k *KernelStore) RecoverUnboundMainThread(ctx context.Context, expected contract.SessionBinding, snapshot contract.InputSnapshot, reason string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = k.recoverUnboundMainThreadTx(ctx, tx, expected, snapshot, reason); err != nil {
		return err
	}
	return tx.Commit()
}

func (k *KernelStore) recoverUnboundMainThreadTx(ctx context.Context, tx *sql.Tx, expected contract.SessionBinding, snapshot contract.InputSnapshot, reason string) error {
	if strings.TrimSpace(reason) == "" || len(reason) > contract.MaxInline || !utf8.ValidString(reason) || expected.Generation == math.MaxInt || snapshot.ID == "" || snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) {
		return errors.New("audited recovery snapshot required")
	}
	if err := contract.ValidateSessionInput(snapshot.Input); err != nil {
		return err
	}
	b, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b, expected) || b.Kind != "orchestrator" || b.ThreadID != "" || b.Closed {
		return errors.New("unbound main recovery binding conflict")
	}
	var creating, stopped int
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT creating,process_stopped,active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&creating, &stopped, &active); err != nil {
		return err
	}
	if creating != 1 || stopped != 1 || active != "" {
		return errors.New("unknown main creation must be stopped before recovery")
	}
	var rotationID string
	var rotation *MainThreadRotation
	err = completionRecord(ctx, tx, "main_rotation_pending", b.ID, &rotationID)
	if err == nil {
		rotation = &MainThreadRotation{}
		if err = completionRecord(ctx, tx, "main_thread_rotation", rotationID, rotation); err != nil {
			return err
		}
		if rotation.ID != rotationID || rotation.SessionID != b.ID || rotation.Generation != b.Generation || rotation.State != "pending" {
			return ErrMainRotationHeld
		}
		if _, err = k.rotationSnapshotTx(ctx, tx, b, snapshot); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings WHERE session_id=?", b.ID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		if rotation == nil {
			return errors.New("unbound main unexpectedly has sent runs")
		}
		var unresolved int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings WHERE session_id=? AND (applied=0 OR process_stopped=0)", b.ID).Scan(&unresolved); err != nil {
			return err
		}
		if unresolved != 0 {
			return ErrMainRotationHeld
		}
	}
	var digest string
	var data []byte
	err = tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", snapshot.ID).Scan(&digest, &data)
	if err == nil {
		if digest != snapshot.Digest || string(data) != string(snapshot.Input) {
			return errors.New("snapshot identity conflict")
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_snapshots(id,digest,data) VALUES(?,?,?)", snapshot.ID, snapshot.Digest, snapshot.Input); err != nil {
			return err
		}
	} else {
		return err
	}
	if rotation != nil {
		recoveryID := structureID("main-rotation-recovery-", rotation.ID, strconv.Itoa(b.Generation+1))
		recovery := map[string]any{"rotation_id": rotation.ID, "from_generation": b.Generation, "to_generation": b.Generation + 1, "unknown_preparation_id": b.PreparationID, "from_snapshot_id": rotation.SnapshotID, "snapshot_id": snapshot.ID, "snapshot_digest": snapshot.Digest, "reason": reason, "created_at": k.clock()}
		if err = kernelRecordTx(ctx, tx, "main_rotation_recovery", recoveryID, b.ID, recovery); err != nil {
			return err
		}
		rotation.Generation = b.Generation + 1
		rotation.SnapshotID = snapshot.ID
		rotation.SnapshotDigest = snapshot.Digest
		if err = kernelRecordTx(ctx, tx, "main_thread_rotation", rotation.ID, b.ID, *rotation); err != nil {
			return err
		}
	}
	b.Generation++
	b.PreparationID = ""
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_rebindings(session_id,generation,old_thread,new_thread,snapshot_id,reason,created_at) VALUES(?,?,'','',?,?,?)", b.ID, b.Generation, snapshot.ID, reason, k.clock()); err != nil {
		return err
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET creating=0,payload=? WHERE id=?", raw, b.ID); err != nil {
		return err
	}
	return nil
}
