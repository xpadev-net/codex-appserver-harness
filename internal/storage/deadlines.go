package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrDeadlineHeld = errors.New("deadline reconciliation is not eligible")

type DeadlineDisposition struct {
	ID             string           `json:"id"`
	Target         contract.Target  `json:"target"`
	Deadline       time.Time        `json:"deadline"`
	PreviousState  domain.WaveState `json:"previous_state"`
	PreviousReason string           `json:"previous_reason"`
	CreatedAt      time.Time        `json:"created_at"`
	ActionIDs      []string         `json:"action_ids"`
}
type DeadlineAction struct {
	ID            string          `json:"id"`
	DispositionID string          `json:"disposition_id"`
	Target        contract.Target `json:"target"`
	Kind          string          `json:"kind"` // stop_process or track_external
	SessionID     string          `json:"session_id"`
	PreparationID string          `json:"preparation_id"`
	RunID         string          `json:"run_id"`
	OperationID   string          `json:"operation_id"`
	State         string          `json:"state"`
	CreatedAt     time.Time       `json:"created_at"`
	CompletedAt   time.Time       `json:"completed_at"`
}

func deadlineDispositionID(w domain.Wave) string {
	return structureID("deadline-", w.ID, w.Deadline.UTC().Format(time.RFC3339Nano))
}

// ExpireWave persists a hold and stopping/tracking intents only. It does not
// signal a process, cancel an external operation or release any capacity.
func (k *KernelStore) ExpireWave(ctx context.Context, waveID string) (DeadlineDisposition, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return DeadlineDisposition{}, err
	}
	defer tx.Rollback()
	d, err := k.expireWaveTx(ctx, tx, waveID)
	if err != nil {
		return d, err
	}
	return d, tx.Commit()
}
func (k *KernelStore) expireWaveTx(ctx context.Context, tx *sql.Tx, waveID string) (DeadlineDisposition, error) {
	w, err := loadWave(ctx, tx, waveID)
	if err != nil {
		return DeadlineDisposition{}, err
	}
	if err := w.Validate(); err != nil {
		return DeadlineDisposition{}, err
	}
	if w.ID != waveID {
		return DeadlineDisposition{}, errors.New("deadline Wave identity conflict")
	}
	d := DeadlineDisposition{ID: deadlineDispositionID(w)}
	err = completionRecord(ctx, tx, "deadline_disposition", d.ID, &d)
	if err == nil {
		return d, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return d, err
	}
	if k.clock().Before(w.Deadline) || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted {
		return d, ErrDeadlineHeld
	}
	d = DeadlineDisposition{ID: d.ID, Target: w.Target(), Deadline: w.Deadline, PreviousState: w.State, PreviousReason: w.Reason, CreatedAt: k.clock(), ActionIDs: []string{}}
	actions := []DeadlineAction{}
	rows, err := tx.QueryContext(ctx, "SELECT id,active_run_id FROM kernel_sessions WHERE process_stopped=0")
	if err != nil {
		return d, err
	}
	type liveSession struct{ id, run string }
	sessions := []liveSession{}
	for rows.Next() {
		var s liveSession
		if err = rows.Scan(&s.id, &s.run); err != nil {
			rows.Close()
			return d, err
		}
		sessions = append(sessions, s)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return d, err
	}
	for _, s := range sessions {
		b, err := sessionBindingTx(ctx, tx, s.id)
		if err != nil {
			return d, err
		}
		owns := b.Kind == "task" && b.Target.WaveID == w.ID
		if b.Kind == "orchestrator" && b.ID == w.OrchestratorSessionID {
			// A prepared main process with no reserved run has no fixed target set
			// yet. Fence it once at expiry; later batches for other Waves remain free.
			owns = s.run == ""
			if s.run != "" {
				var budget RunBudgetBinding
				if err := completionRecord(ctx, tx, "run_budget", s.run, &budget); err != nil {
					return d, err
				}
				for _, target := range budget.Targets {
					owns = owns || target.WaveID == w.ID
				}
			}
		}
		if owns {
			actions = append(actions, DeadlineAction{SessionID: b.ID, PreparationID: b.PreparationID, RunID: s.run, Kind: "stop_process"})
		}
	}
	rows, err = tx.QueryContext(ctx, "SELECT run_id,payload FROM kernel_dispatch_intents WHERE wave_id=? AND state='held'", w.ID)
	if err != nil {
		return d, err
	}
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return d, err
		}
		var header struct {
			Kind string `json:"kind"`
		}
		if err = json.Unmarshal(raw, &header); err != nil {
			rows.Close()
			return d, err
		}
		if header.Kind == "app_server" {
			continue
		}
		var r OperationReservation
		if err = json.Unmarshal(raw, &r); err != nil {
			rows.Close()
			return d, err
		}
		if !validOperationCapacityKind(r.Kind) || r.ID != id || r.WaveID != w.ID {
			rows.Close()
			return d, errors.New("invalid deadline operation binding")
		}
		kind := "track_external"
		if r.Kind == "runner" || r.Kind == "git" {
			kind = "stop_process"
		}
		actions = append(actions, DeadlineAction{SessionID: r.SessionID, OperationID: id, Kind: kind})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return d, err
	}
	for _, a := range actions {
		a.ID = structureID("deadline-action-", d.ID, a.SessionID+":"+a.PreparationID+":"+a.RunID+":"+a.OperationID)
		a.DispositionID, a.Target, a.State, a.CreatedAt = d.ID, w.Target(), "pending", k.clock()
		if a.PreparationID != "" {
			if err = kernelRecordTx(ctx, tx, "deadline_preparation_stop", a.PreparationID, a.SessionID, a); err != nil {
				return d, err
			}
		}
		if err = kernelRecordTx(ctx, tx, "deadline_action", a.ID, w.ID, a); err != nil {
			return d, err
		}
		d.ActionIDs = append(d.ActionIDs, a.ID)
	}
	w.State, w.Reason = domain.WaveBlocked, "deadline_exceeded"
	if err = saveWave(ctx, tx, w); err != nil {
		return d, err
	}
	if err = kernelRecordTx(ctx, tx, "deadline_disposition", d.ID, w.ID, d); err != nil {
		return d, err
	}
	if err = k.waveEventTx(ctx, tx, w, "wave_deadline_exceeded", "", d.ID+":event", d); err != nil {
		return d, err
	}
	return d, nil
}

type DeadlineSweepPage struct {
	Dispositions []DeadlineDisposition `json:"dispositions"`
	NextAfter    string                `json:"next_after"`
}

// SweepWaveDeadlines scans at most limit Wave identities. Keep and wrap its
// cursor so nonexpired or terminal history cannot starve another Wave.
func (k *KernelStore) SweepWaveDeadlines(ctx context.Context, after string, limit int) (DeadlineSweepPage, error) {
	page := DeadlineSweepPage{Dispositions: []DeadlineDisposition{}}
	if limit < 1 || limit > 1000 {
		return page, errors.New("deadline page limit must be 1..1000")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT id FROM kernel_waves WHERE id>? ORDER BY id LIMIT ?", after, limit)
	if err != nil {
		return page, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return page, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return page, err
	}
	for _, id := range ids {
		d, err := k.expireWaveTx(ctx, tx, id)
		if errors.Is(err, ErrDeadlineHeld) {
			continue
		}
		if err != nil {
			return page, err
		}
		page.Dispositions = append(page.Dispositions, d)
	}
	if len(ids) == limit {
		page.NextAfter = ids[len(ids)-1]
	}
	return page, tx.Commit()
}

type DeadlineActionPage struct {
	Actions   []DeadlineAction `json:"actions"`
	NextAfter string           `json:"next_after"`
}

func (k *KernelStore) PendingDeadlineActions(ctx context.Context, after string, limit int) (DeadlineActionPage, error) {
	page := DeadlineActionPage{Actions: []DeadlineAction{}}
	if limit < 1 || limit > 1000 {
		return page, errors.New("deadline action page limit must be 1..1000")
	}
	rows, err := k.DB.QueryContext(ctx, "SELECT id,payload FROM records WHERE kind='deadline_action' AND id>? ORDER BY id LIMIT ?", after, limit)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	scanned := 0
	last := ""
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return page, err
		}
		scanned++
		last = id
		var a DeadlineAction
		if err = json.Unmarshal(raw, &a); err != nil {
			return page, err
		}
		if a.ID != id {
			return page, errors.New("deadline action identity conflict")
		}
		if a.State == "pending" {
			page.Actions = append(page.Actions, a)
		}
	}
	if scanned == limit {
		page.NextAfter = last
	}
	return page, rows.Err()
}

// CompleteDeadlineAction only acknowledges an already verified release. It
// cannot manufacture stopping evidence or free a newer process generation.
func (k *KernelStore) CompleteDeadlineAction(ctx context.Context, id string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var a DeadlineAction
	if err = completionRecord(ctx, tx, "deadline_action", id, &a); err != nil {
		return err
	}
	if a.ID != id {
		return errors.New("deadline action identity conflict")
	}
	if a.State == "completed" {
		return nil
	}
	if a.OperationID != "" {
		r, err := loadOperationReservationTx(ctx, tx, a.OperationID)
		if err != nil {
			return err
		}
		if r.SessionID != a.SessionID || r.WaveID != a.Target.WaveID || r.State != "released" || r.Proof == nil {
			return ErrDeadlineHeld
		}
	} else {
		r, err := loadProcessReservationTx(ctx, tx, a.PreparationID)
		if errors.Is(err, sql.ErrNoRows) {
			var proof SessionStopProof
			err = completionRecord(ctx, tx, "session_stop_proof", sessionStopProofID(a.SessionID, a.PreparationID), &proof)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrDeadlineHeld
			}
			if err != nil {
				return err
			}
			expected := a.PreparationID
			if a.RunID != "" {
				expected = a.RunID
			}
			if proof.SessionID != a.SessionID || proof.PreparationID != a.PreparationID || proof.RunID != a.RunID || proof.ProofID != expected || proof.ConfirmedAt.IsZero() {
				return ErrDeadlineHeld
			}
		} else if err != nil {
			return err
		} else if r.SessionID != a.SessionID || (a.RunID != "" && r.RunID != a.RunID) || r.State != "released" {
			return ErrDeadlineHeld
		}
	}
	a.State, a.CompletedAt = "completed", k.clock()
	if err = kernelRecordTx(ctx, tx, "deadline_action", id, a.Target.WaveID, a); err != nil {
		return err
	}
	return tx.Commit()
}

type DeadlineChangeRequest struct {
	Key              string          `json:"key"`
	Target           contract.Target `json:"target"`
	ExpectedDeadline time.Time       `json:"expected_deadline"`
	Deadline         time.Time       `json:"deadline"`
	Reason           string          `json:"reason"`
}
type DeadlineChangeAudit struct {
	Request   DeadlineChangeRequest `json:"request"`
	Digest    string                `json:"digest"`
	CreatedAt time.Time             `json:"created_at"`
}

// Trusted operator only. Existing stop/track intents remain until reconciled;
// changing a deadline never resets usage, scope, plan, or process ownership.
func (k *KernelStore) ChangeWaveDeadline(ctx context.Context, r DeadlineChangeRequest) error {
	if !r.Target.Valid() || strings.TrimSpace(r.Key) == "" || len(r.Key) > contract.MaxInline || !utf8.ValidString(r.Key) || strings.TrimSpace(r.Reason) == "" || len(r.Reason) > contract.MaxInline || !utf8.ValidString(r.Reason) {
		return errors.New("audited deadline change required")
	}
	digest, err := contentDigest(r)
	if err != nil {
		return err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	id := structureID("deadline-change-", r.Target.WaveID, r.Key)
	var audit DeadlineChangeAudit
	err = completionRecord(ctx, tx, "deadline_change", id, &audit)
	if err == nil {
		if audit.Digest != digest {
			return errors.New("deadline change identity conflict")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return err
	}
	if w.Target() != r.Target || !w.Deadline.Equal(r.ExpectedDeadline) || !r.Deadline.After(w.CreatedAt) || !r.Deadline.After(k.clock()) || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted {
		return ErrDeadlineHeld
	}
	if w.State == domain.WaveBlocked && w.Reason == "deadline_exceeded" {
		var d DeadlineDisposition
		if err = completionRecord(ctx, tx, "deadline_disposition", deadlineDispositionID(w), &d); err != nil {
			return err
		}
		w.State, w.Reason = d.PreviousState, d.PreviousReason
	}
	w.Deadline = r.Deadline
	if err = saveWave(ctx, tx, w); err != nil {
		return err
	}
	audit = DeadlineChangeAudit{Request: r, Digest: digest, CreatedAt: k.clock()}
	if err = kernelRecordTx(ctx, tx, "deadline_change", id, w.ID, audit); err != nil {
		return err
	}
	if err = k.waveEventTx(ctx, tx, w, "wave_deadline_changed", "", id+":event", audit); err != nil {
		return err
	}
	return tx.Commit()
}
