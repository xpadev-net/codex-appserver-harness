package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

var ErrManagedProcessHeld = errors.New("managed preparation needs owner reconciliation")

type ManagedProcessRequest struct {
	OwnerID string                  `json:"owner_id"`
	RunID   string                  `json:"run_id"`
	Mode    string                  `json:"mode"` // execute or recovery; recovery admits no model turn
	Binding contract.SessionBinding `json:"binding"`
}
type ManagedProcess struct {
	Request          ManagedProcessRequest `json:"request"`
	PreparationID    string                `json:"preparation_id"`
	RequestDigest    string                `json:"request_digest"`
	State            string                `json:"state"` // admitted, launching, started, stopped
	Identity         process.Identity      `json:"identity"`
	CreatedAt        time.Time             `json:"created_at"`
	LaunchObservedAt time.Time             `json:"launch_observed_at"`
	StoppedAt        time.Time             `json:"stopped_at"`
	StopDisposition  string                `json:"stop_disposition"`
}
type ManagedReservation struct {
	Binding contract.SessionBinding
	Process ManagedProcess
	Created bool
}

func managedProcessTx(ctx context.Context, q queryer, ownerID string) (ManagedProcess, error) {
	var p ManagedProcess
	if err := completionRecord(ctx, q, "managed_process", ownerID, &p); err != nil {
		return p, err
	}
	if err := validateManagedProcess(p, ownerID); err != nil {
		return p, err
	}
	return p, nil
}
func validateManagedProcess(p ManagedProcess, ownerID string) error {
	digest, err := contentDigest(p.Request)
	if err != nil {
		return err
	}
	if p.Request.OwnerID != ownerID || p.Request.RunID == "" || p.Request.Binding.ID == "" || p.PreparationID == "" || p.RequestDigest != digest || p.CreatedAt.IsZero() {
		return errors.New("managed process identity conflict")
	}
	if p.Request.Mode != "execute" && p.Request.Mode != "recovery" {
		return errors.New("invalid managed process mode")
	}
	switch p.State {
	case "admitted", "launching", "started", "stopped":
	default:
		return errors.New("invalid managed process state")
	}
	return nil
}
func (k *KernelStore) ManagedProcess(ctx context.Context, ownerID string) (ManagedProcess, error) {
	return managedProcessTx(ctx, k.DB, ownerID)
}
func saveManagedProcessTx(ctx context.Context, tx *sql.Tx, p ManagedProcess) error {
	return kernelRecordTx(ctx, tx, "managed_process", p.Request.OwnerID, p.Request.Binding.ID, p)
}

// ReserveManagedProcess attaches a unique runtime owner to capacity BEFORE any
// factory call. Replay exposes the existing fact and never permits another start.
func (k *KernelStore) ReserveManagedProcess(ctx context.Context, r ManagedProcessRequest) (ManagedReservation, error) {
	result := ManagedReservation{}
	for _, value := range []string{r.OwnerID, r.RunID} {
		if strings.TrimSpace(value) == "" || len(value) > contract.MaxInline || !utf8.ValidString(value) {
			return result, errors.New("bounded runtime owner/run identity required")
		}
	}
	if r.Mode != "execute" && r.Mode != "recovery" {
		return result, errors.New("managed process mode required")
	}
	digest, err := contentDigest(r)
	if err != nil {
		return result, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	prior, err := managedProcessTx(ctx, tx, r.OwnerID)
	if err == nil {
		if prior.RequestDigest != digest {
			return result, errors.New("runtime owner identity reused")
		}
		// Return the original identity, not a newer session's preparation.
		b := r.Binding
		b.PreparationID = prior.PreparationID
		return ManagedReservation{Binding: b, Process: prior, Created: false}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	if err = checkManagedRunClaimTx(ctx, tx, r); err != nil {
		return result, err
	}
	if r.Mode == "execute" {
		var exists int
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM kernel_run_bindings WHERE run_id=?)", r.RunID).Scan(&exists); err != nil {
			return result, err
		}
		if exists != 0 {
			return result, errors.New("existing runs require read-only recovery")
		}
		if err = k.reserveSessionThreadTx(ctx, tx, r.Binding); err != nil {
			return result, err
		}
	} else if err = k.reserveSessionRecoveryTx(ctx, tx, r.Binding, r.RunID); err != nil {
		return result, err
	}
	b, err := sessionBindingTx(ctx, tx, r.Binding.ID)
	if err != nil {
		return result, err
	}
	p := ManagedProcess{Request: r, PreparationID: b.PreparationID, RequestDigest: digest, State: "admitted", CreatedAt: k.clock()}
	if err = saveManagedProcessTx(ctx, tx, p); err != nil {
		return result, err
	}
	if err = kernelRecordTx(ctx, tx, "managed_preparation", b.PreparationID, b.ID, r.OwnerID); err != nil {
		return result, err
	}
	if err = kernelRecordTx(ctx, tx, "managed_run_claim", r.RunID, b.ID, r.OwnerID); err != nil {
		return result, err
	}
	result = ManagedReservation{Binding: b, Process: p, Created: true}
	return result, tx.Commit()
}

// Planned runs are globally owned even before kernel_run_bindings exists.
// A stopped predecessor can yield only to the same logical session; recovery
// may reuse that run without ever granting a second model start.
func checkManagedRunClaimTx(ctx context.Context, tx *sql.Tx, r ManagedProcessRequest) error {
	var owner string
	err := completionRecord(ctx, tx, "managed_run_claim", r.RunID, &owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	previous, err := managedProcessTx(ctx, tx, owner)
	if err != nil {
		return err
	}
	if previous.Request.Binding.ID != r.Binding.ID {
		return ErrManagedProcessHeld
	}
	reservation, err := loadProcessReservationTx(ctx, tx, previous.PreparationID)
	if err != nil {
		return err
	}
	if reservation.State != "released" || reservation.SessionID != r.Binding.ID {
		return ErrManagedProcessHeld
	}
	var proof SessionStopProof
	if err := completionRecord(ctx, tx, "session_stop_proof", sessionStopProofID(r.Binding.ID, previous.PreparationID), &proof); err != nil {
		return err
	}
	if proof.SessionID != r.Binding.ID || proof.PreparationID != previous.PreparationID || proof.RunID != reservation.RunID || proof.ConfirmedAt.IsZero() {
		return ErrManagedProcessHeld
	}
	return nil
}
func managedExecuteDispatchableTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, now time.Time) error {
	if err := sessionDispatchableTx(ctx, tx, b); err != nil {
		return err
	}
	if b.Kind == "task" {
		w, err := loadWave(ctx, tx, b.Target.WaveID)
		if err != nil {
			return err
		}
		if !now.Before(w.Deadline) {
			return ErrManagedProcessHeld
		}
	}
	return nil
}

func (k *KernelStore) reserveSessionRecoveryTx(ctx context.Context, tx *sql.Tx, expected contract.SessionBinding, runID string) error {
	b, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b, expected) || b.ThreadID == "" {
		return errors.New("recovery session binding conflict")
	}
	var active, owner, thread string
	var stopped, creating, runStopped, applied int
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &stopped, &creating); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, "SELECT session_id,thread_id,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", runID).Scan(&owner, &thread, &runStopped, &applied); err != nil {
		return err
	}
	if active != runID || owner != b.ID || thread != b.ThreadID || stopped != 1 || runStopped != 1 || creating != 0 || applied != 0 {
		return ErrManagedProcessHeld
	}
	// Reading history remains legal after pause/cancel/scope changes. This path
	// reserves a physical slot, never a turn, token row, Attempt or wake.
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET process_stopped=0 WHERE id=?", b.ID); err != nil {
		return err
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
	if err = bindSessionResourcesTx(ctx, tx, b, b.ThreadID, runID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_run_bindings SET process_stopped=0 WHERE run_id=?", runID); err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "run_recovery", runID, b.ID, map[string]string{"preparation_id": b.PreparationID, "session_id": b.ID}); err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, "session_preparation", b.ID, b.ID, b)
}
func validateManagedPreparationTx(ctx context.Context, tx *sql.Tx, p ManagedProcess) (contract.SessionBinding, error) {
	b, err := sessionBindingTx(ctx, tx, p.Request.Binding.ID)
	if err != nil {
		return b, err
	}
	expected := p.Request.Binding
	expected.PreparationID = p.PreparationID
	expected.TaskState = b.TaskState
	expected.Closed = b.Closed
	if !reflect.DeepEqual(b, expected) {
		return b, ErrManagedProcessHeld
	}
	var stopped int
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT process_stopped,active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&stopped, &active); err != nil {
		return b, err
	}
	if stopped != 0 || p.Request.Mode == "execute" && active != "" || p.Request.Mode == "recovery" && active != p.Request.RunID {
		return b, ErrManagedProcessHeld
	}
	r, err := loadProcessReservationTx(ctx, tx, p.PreparationID)
	if err != nil {
		return b, err
	}
	if r.State != "held" || r.SessionID != b.ID {
		return b, ErrManagedProcessHeld
	}
	return b, nil
}

// BeginManagedProcessLaunch is the durable send boundary. An admitted record
// is known not to have invoked its factory; launching with no PID is unknown.
func (k *KernelStore) BeginManagedProcessLaunch(ctx context.Context, ownerID string) (ManagedProcess, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ManagedProcess{}, err
	}
	defer tx.Rollback()
	p, err := managedProcessTx(ctx, tx, ownerID)
	if err != nil {
		return p, err
	}
	if p.State != "admitted" {
		return p, ErrManagedProcessHeld
	}
	if err = managedPreparationStopFenceTx(ctx, tx, p.PreparationID); err != nil {
		return p, err
	}
	b, err := validateManagedPreparationTx(ctx, tx, p)
	if err != nil {
		return p, err
	}
	if p.Request.Mode == "execute" {
		if err = managedExecuteDispatchableTx(ctx, tx, b, k.clock()); err != nil {
			return p, err
		}
	}

	p.State = "launching"
	if err = saveManagedProcessTx(ctx, tx, p); err != nil {
		return p, err
	}
	return p, tx.Commit()
}
func (k *KernelStore) RecordManagedProcessStarted(ctx context.Context, ownerID string, identity process.Identity) error {
	if identity.PID < 1 {
		return errors.New("observed process PID required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	p, err := managedProcessTx(ctx, tx, ownerID)
	if err != nil {
		return err
	}
	if p.State == "started" {
		if p.Identity != identity {
			return errors.New("process identity changed")
		}
		return nil
	}
	if p.State != "launching" {
		return ErrManagedProcessHeld
	}
	if _, err = validateManagedPreparationTx(ctx, tx, p); err != nil {
		return err
	}
	p.State, p.Identity, p.LaunchObservedAt = "started", identity, k.clock()
	if err = saveManagedProcessTx(ctx, tx, p); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkManagedThreadRPC fences even a lost thread/start response. It executes no
// RPC; caller must never infer non-start from a failed response after this point.
func (k *KernelStore) MarkManagedThreadRPC(ctx context.Context, ownerID, method string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	p, err := managedProcessTx(ctx, tx, ownerID)
	if err != nil {
		return err
	}
	if p.State != "started" || p.Request.Mode != "execute" {
		return ErrManagedProcessHeld
	}
	if err = managedPreparationStopFenceTx(ctx, tx, p.PreparationID); err != nil {
		return err
	}
	b, err := validateManagedPreparationTx(ctx, tx, p)
	if err != nil {
		return err
	}
	want := "thread/start"
	if b.ThreadID != "" {
		want = "thread/resume"
	}
	if method != want {
		return errors.New("thread RPC does not match the preparation")
	}
	if err = managedExecuteDispatchableTx(ctx, tx, b, k.clock()); err != nil {
		return err
	}
	var existing string
	err = completionRecord(ctx, tx, "managed_thread_rpc", p.PreparationID, &existing)
	if err == nil {
		return ErrManagedProcessHeld
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "managed_thread_rpc", p.PreparationID, b.ID, method); err != nil {
		return err
	}
	return tx.Commit()
}

type ManagedThreadReady struct {
	OwnerID  string `json:"owner_id"`
	ThreadID string `json:"thread_id"`
}

func managedPreparationStopFenceTx(ctx context.Context, tx *sql.Tx, id string) error {
	var held int
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('deadline_preparation_stop','interrupt_preparation_stop') AND id=?)", id).Scan(&held); err != nil {
		return err
	}
	if held != 0 {
		return ErrManagedProcessHeld
	}
	return nil
}

// CheckManagedRunSend rechecks exact physical ownership and durable stop intent
// immediately before turn/start. The reserved run remains sent/unknown until its
// outcome is reconciled; rejecting this check never permits a blind retry.
func (k *KernelStore) CheckManagedRunSend(ctx context.Context, ownerID string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	p, err := managedProcessTx(ctx, tx, ownerID)
	if err != nil {
		return err
	}
	if p.State != "started" || p.Request.Mode != "execute" {
		return ErrManagedProcessHeld
	}
	if err = managedPreparationStopFenceTx(ctx, tx, p.PreparationID); err != nil {
		return err
	}
	b, err := sessionBindingTx(ctx, tx, p.Request.Binding.ID)
	if err != nil {
		return err
	}
	var ready ManagedThreadReady
	if err = completionRecord(ctx, tx, "managed_thread_ready", p.PreparationID, &ready); err != nil {
		return err
	}
	expected := p.Request.Binding
	expected.PreparationID, expected.ThreadID, expected.TaskState, expected.Closed = p.PreparationID, ready.ThreadID, b.TaskState, b.Closed
	if ready.OwnerID != ownerID || !reflect.DeepEqual(b, expected) {
		return ErrManagedProcessHeld
	}
	var active, thread string
	var stopped, runStopped, applied int
	if err = tx.QueryRowContext(ctx, "SELECT s.active_run_id,s.process_stopped,r.process_stopped,r.applied,r.thread_id FROM kernel_sessions s JOIN kernel_run_bindings r ON r.run_id=s.active_run_id WHERE s.id=?", b.ID).Scan(&active, &stopped, &runStopped, &applied, &thread); err != nil {
		return err
	}
	if active != p.Request.RunID || stopped != 0 || runStopped != 0 || applied != 0 || thread != b.ThreadID {
		return ErrManagedProcessHeld
	}
	reservation, err := loadProcessReservationTx(ctx, tx, p.PreparationID)
	if err != nil {
		return err
	}
	if reservation.State != "held" || reservation.SessionID != b.ID || reservation.RunID != active {
		return ErrManagedProcessHeld
	}
	interrupted, err := runInterruptedTx(ctx, tx, b.ID, active)
	if err != nil {
		return err
	}
	if interrupted {
		return ErrInterruptionHeld
	}
	if err = sessionDispatchableTx(ctx, tx, b); err != nil {
		return err
	}
	var budget RunBudgetBinding
	if err = completionRecord(ctx, tx, "run_budget", active, &budget); err != nil {
		return err
	}
	if budget.RunID != active || budget.SessionID != b.ID || len(budget.Targets) == 0 || b.Kind == "task" && (len(budget.Targets) != 1 || budget.Targets[0] != b.Target) {
		return ErrManagedProcessHeld
	}
	for _, target := range budget.Targets {
		w, err := loadWave(ctx, tx, target.WaveID)
		if err != nil {
			return err
		}
		if w.Target() != target || w.DispatchPaused() || !k.clock().Before(w.Deadline) || b.Kind == "orchestrator" && w.OrchestratorSessionID != b.ID {
			return ErrManagedProcessHeld
		}
		if w.State == domain.WaveCanceled || w.State == domain.WaveCompleted {
			if b.Kind != "orchestrator" {
				return ErrManagedProcessHeld
			}
			if err = terminalMainBudgetTargetTx(ctx, tx, b, active, w); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

type ManagedStopProof struct {
	OwnerID     string           `json:"owner_id"`
	Disposition string           `json:"disposition"` // trusted not_started or group_stopped
	Identity    process.Identity `json:"identity"`
}

// ConfirmManagedProcessStopped accepts only trusted factory/process-owner facts.
// Metadata absence, cancellation and elapsed time never authorize this call.
func (k *KernelStore) ConfirmManagedProcessStopped(ctx context.Context, proof ManagedStopProof) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	p, err := managedProcessTx(ctx, tx, proof.OwnerID)
	if err != nil {
		return err
	}
	if p.State == "stopped" {
		var old ManagedStopProof
		if err = completionRecord(ctx, tx, "managed_stop_proof", proof.OwnerID, &old); err != nil {
			return err
		}
		if old != proof {
			return errors.New("managed stopping proof changed")
		}
		return nil
	}
	var rpc int
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='managed_thread_rpc' AND id=?)", p.PreparationID).Scan(&rpc); err != nil {
		return err
	}
	switch proof.Disposition {
	case "not_started":
		if p.Identity != (process.Identity{}) || proof.Identity != (process.Identity{}) || p.State == "started" || rpc != 0 {
			return ErrManagedProcessHeld
		}
	case "group_stopped":
		if !proof.Identity.Valid() || p.Identity.PID != 0 && p.Identity != proof.Identity {
			return ErrManagedProcessHeld
		}
	default:
		return errors.New("verified managed stopping disposition required")
	}
	b, err := sessionBindingTx(ctx, tx, p.Request.Binding.ID)
	if err != nil {
		return err
	}
	reservation, err := loadProcessReservationTx(ctx, tx, p.PreparationID)
	if err != nil {
		return err
	}
	if reservation.SessionID != p.Request.Binding.ID {
		return ErrManagedProcessHeld
	}
	var active string
	var creating int
	current := b.PreparationID == p.PreparationID
	if current {
		if err = tx.QueryRowContext(ctx, "SELECT active_run_id,creating FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &creating); err != nil {
			return err
		}
		if active != "" && active != p.Request.RunID {
			return ErrManagedProcessHeld
		}
	}
	if reservation.State == "released" {
		var previous SessionStopProof
		if err = completionRecord(ctx, tx, "session_stop_proof", sessionStopProofID(b.ID, p.PreparationID), &previous); err != nil {
			return err
		}
		if previous.SessionID != b.ID || previous.PreparationID != p.PreparationID || previous.RunID != reservation.RunID || previous.ConfirmedAt.IsZero() {
			return ErrManagedProcessHeld
		}
	} else {
		if !current {
			return ErrManagedProcessHeld
		}
		stopID := p.PreparationID
		if active != "" {
			stopID = active
		}
		if err = k.markSessionStoppedTx(ctx, tx, b.ID, p.PreparationID, stopID); err != nil {
			return err
		}
	}
	// No thread RPC was admitted: a known non-start/initialize failure can clear
	// the creation latch. Lost thread/start keeps its intent for audited recovery.
	if current && rpc == 0 && b.ThreadID == "" && active == "" && creating == 1 {
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_sessions SET creating=0 WHERE id=?", b.ID); err != nil {
			return err
		}
		if err = kernelRecordTx(ctx, tx, "thread_creation_disposition", p.PreparationID, b.ID, map[string]string{"state": "unsent", "owner_id": proof.OwnerID}); err != nil {
			return err
		}
	}
	if proof.Disposition == "group_stopped" && p.Identity.PID == 0 {
		p.Identity = proof.Identity
	}
	p.State, p.StoppedAt, p.StopDisposition = "stopped", k.clock(), proof.Disposition
	if err = saveManagedProcessTx(ctx, tx, p); err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "managed_stop_proof", proof.OwnerID, b.ID, proof); err != nil {
		return err
	}
	return tx.Commit()
}

type ManagedProcessPage struct {
	Processes []ManagedProcess `json:"processes"`
	NextAfter string           `json:"next_after"`
}

// The cursor bounds scanned records, including stopped history. Advance and
// wrap it across reconciliation cycles; replay never grants a new factory call.
func (k *KernelStore) PendingManagedProcesses(ctx context.Context, after string, limit int) (ManagedProcessPage, error) {
	page := ManagedProcessPage{Processes: []ManagedProcess{}}
	if limit < 1 || limit > 1000 {
		return page, errors.New("managed process page limit must be 1..1000")
	}
	rows, err := k.DB.QueryContext(ctx, "SELECT id,payload FROM records WHERE kind='managed_process' AND id>? ORDER BY id LIMIT ?", after, limit)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	scanned := 0
	last := ""
	for rows.Next() {
		var raw []byte
		var id string
		if err = rows.Scan(&id, &raw); err != nil {
			return page, err
		}
		scanned++
		last = id
		var p ManagedProcess
		if err = json.Unmarshal(raw, &p); err != nil {
			return page, err
		}
		if err = validateManagedProcess(p, id); err != nil {
			return page, err
		}
		if p.State != "stopped" {
			page.Processes = append(page.Processes, p)
		}
	}
	if scanned == limit {
		page.NextAfter = last
	}
	return page, rows.Err()
}
