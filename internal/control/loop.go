package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
)

// SessionWork is controller-owned, reconstructed from durable ready/wake/run
// facts. Exactly one of Execute and Recovery is supplied. A hint cannot supply
// work, a model prompt, a scope change, or a stopping instruction.
type SessionWork struct {
	Execute  *ExecuteRequest
	Recovery *RecoveryRequest
}
type RecoveryRequest struct {
	SessionID string
	Run       contract.SessionRun
	Validate  worker.OutputValidator
}

func (w SessionWork) identity() (string, string, error) {
	if (w.Execute == nil) == (w.Recovery == nil) {
		return "", "", errors.New("exactly one session operation required")
	}
	if w.Execute != nil {
		return w.Execute.SessionID, w.Execute.RunID, nil
	}
	return w.Recovery.SessionID, w.Recovery.Run.ID, nil
}

type WorkPage struct {
	Recheck bool // durable completion applied during startup scan; restart cursor once

	Work      []SessionWork
	NextAfter string
}
type Completion struct {
	SessionID string
	RunID     string
	Outcome   Outcome
	Err       error
}

// WorkSource does bounded database reconciliation, never waits for a model or
// external operation. Complete applies/archives saved results idempotently;
// errors retain the completion for retry without executing the model again.
// Ready uses a stable key cursor and returns at most limit entries. Restart
// recovery must reconstruct unapplied runs independently of in-memory results.
type WorkSource interface {
	Ready(context.Context, string, int) (WorkPage, error)
	Complete(context.Context, Completion) error
}

type flight struct {
	work       SessionWork
	cancel     context.CancelFunc
	completion *Completion
	physical   atomic.Pointer[flightGeneration]
}

type flightGeneration struct{ owner, preparation string }

// Capture this flight's own launch generation before invoking its factory.
// Queued/rejected recovery work has no physical generation and cannot inherit
// an old owner's stop solely from a shared persistent run ID.
type flightFactory struct {
	Factory
	flight *flight
}

func (f flightFactory) Open(ctx context.Context, launch Launch) (Process, error) {
	f.flight.physical.Store(&flightGeneration{owner: launch.Process.Request.OwnerID, preparation: launch.Process.PreparationID})
	return f.Factory.Open(ctx, launch)
}

// Loop owns one controller's session dispatch. Durable storage remains the
// authority for global capacity, quiescence, source generations and admission.
// The surrounding daemon must hold its exclusive controller lock before Run.
type Loop struct {
	Runtime        Runtime
	Source         WorkSource
	ReconcileEvery time.Duration
	StopWait       time.Duration
	Report         func(error) // short, nonblocking diagnostic callback
	wake           chan struct{}
	running        atomic.Bool
}

func NewLoop(runtime Runtime, source WorkSource) *Loop {
	l := &Loop{Runtime: runtime, Source: source, wake: make(chan struct{}, 1)}
	previous := runtime.OnMutation
	l.Runtime.OnMutation = func() {
		if previous != nil {
			previous()
		}
		l.Wake()
	}
	return l
}

// Wake is deliberately payload-free and coalesced; all useful facts must have
// committed before callers send this best-effort hint.
func (l *Loop) Wake() {
	if l.wake != nil {
		select {
		case l.wake <- struct{}{}:
		default:
		}
	}
}
func (l *Loop) report(err error) {
	if err != nil && l.Report != nil {
		l.Report(err)
	}
}
func (l *Loop) Run(ctx context.Context) (runErr error) {
	if l.Runtime.Store == nil || l.Runtime.Factory == nil || l.Source == nil || l.wake == nil {
		return errors.New("configured loop required")
	}
	if !l.running.CompareAndSwap(false, true) {
		return errors.New("controller loop already running")
	}
	defer l.running.Store(false)
	every := l.ReconcileEvery
	if every == 0 {
		every = 60 * time.Second
	}
	wait := l.StopWait
	if wait == 0 {
		wait = 15 * time.Second
	}
	if every < 0 || wait < 0 {
		return errors.New("positive loop durations required")
	}
	if _, err := l.Runtime.Store.ExecutionCapacity(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	results := make(chan Completion, 1)
	abandon := make(chan struct{})
	flights := map[string]*flight{}
	var workCursor, processCursor, waveCursor, actionCursor, directionCursor, interruptCursor string
	// Always scan on startup. Loss of every hint merely defers a scan to ticker.
	scan := true
	defer func() {
		runErr = errors.Join(runErr, l.shutdown(ctx, wait, flights, results))
		close(abandon)
	}()
	finish := func() {
		ids := make([]string, 0, len(flights))
		for id := range flights {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			f := flights[id]
			if f.completion == nil {
				continue
			}
			if err := l.Source.Complete(ctx, *f.completion); err != nil {
				l.report(err)
				continue
			}
			f.cancel()
			workCursor = "" // application can make an earlier stable key ready.
			delete(flights, id)
		}
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if scan {
			scan = false
			// Deadline actions are persisted before any signal. Late actions are
			// matched against the exact preparation before canceling a local owner.
			page, e := l.Runtime.Store.SweepWaveDeadlines(ctx, waveCursor, 128)
			if e != nil {
				l.report(e)
			} else {
				waveCursor = page.NextAfter
			}
			e = l.maintain(ctx, flights, &processCursor, &actionCursor)
			if e != nil {
				l.report(e)
			}
			if e = l.maintainInterrupts(ctx, flights, &interruptCursor); e != nil {
				l.report(e)
			}
			finish()
			changed, e := l.maintainDirections(ctx, &directionCursor)
			if e != nil {
				l.report(e)
			}
			if changed {
				workCursor = ""
			}
			if ctx.Err() != nil {
				continue
			}
			// Failed completion application holds its bounded local admission until
			// reconciled; it never causes another turn on that session.
			current, capacityErr := l.Runtime.Store.ExecutionCapacity(ctx)
			available := 0
			if capacityErr != nil {
				l.report(capacityErr)
			} else {
				limit := current.Policy.Limits.Turns
				available = min(limit-len(flights), limit-current.HeldProcessSlots)
			}
			if available > 0 {
				page, e := l.Source.Ready(ctx, workCursor, min(128, available))
				if e != nil {
					l.report(e)
				} else {
					if len(page.Work) > min(128, available) {
						return fmt.Errorf("work source exceeded bounded page")
					}
					previousCursor := workCursor
					if page.NextAfter != "" && page.NextAfter <= previousCursor {
						return errors.New("work cursor must advance")
					}
					workCursor = page.NextAfter
					for _, work := range page.Work {
						sid, run, e := work.identity()
						if e != nil || sid == "" || run == "" {
							l.report(errors.Join(e, errors.New("work needs session/run identity")))
							continue
						}
						if flights[sid] != nil {
							continue
						}
						child, cancel := context.WithCancel(ctx)
						f := &flight{work: work, cancel: cancel}
						flights[sid] = f
						go func(w SessionWork, sid, run string, flight *flight) {
							runtime := l.Runtime
							runtime.Factory = flightFactory{Factory: runtime.Factory, flight: flight}
							var out Outcome
							var e error
							if w.Execute != nil {
								out, e = runtime.Execute(child, *w.Execute)
							} else {
								out, e = runtime.Reconcile(child, w.Recovery.SessionID, w.Recovery.Run, w.Recovery.Validate)
							}
							completion := Completion{SessionID: sid, RunID: run, Outcome: out, Err: e}
							// Shutdown drains this channel until each process owner has returned.
							select {
							case results <- completion:
							case <-abandon:
							}
						}(work, sid, run, f)
					}
					// Continue a bounded page immediately, rather than waiting 60 seconds
					// when a source page has only busy or rejected entries.
					if page.NextAfter != "" && page.NextAfter != previousCursor {
						scan = true
					}
					if page.Recheck {
						workCursor = ""
						scan = true
					}
				}
			}
		}
		if scan {
			continue
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
			workCursor = ""
			scan = true
		case <-l.wake:
			workCursor = ""
			scan = true
		case completion := <-results:
			f := flights[completion.SessionID]
			if f == nil {
				return errors.New("completion has no live owner")
			}
			_, run, _ := f.work.identity()
			if run != completion.RunID {
				return errors.New("completion source conflict")
			}
			f.completion = &completion
			workCursor = ""
			scan = true
		}
	}
}
func (l *Loop) shutdown(ctx context.Context, wait time.Duration, flights map[string]*flight, results <-chan Completion) error {
	for _, f := range flights {
		f.cancel()
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), wait)
	defer cancel()
	initialStopErr := l.requestShutdownStops(cleanup)
	l.report(initialStopErr)
	pending := 0
	for _, f := range flights {
		if f.completion == nil {
			pending++
		}
	}
	for pending > 0 {
		select {
		case c := <-results:
			f := flights[c.SessionID]
			if f == nil || f.completion != nil {
				return errors.Join(ctx.Err(), errors.New("shutdown completion conflict"))
			}
			f.completion = &c
			pending--
		case <-cleanup.Done():
			return errors.Join(ctx.Err(), process.ErrUnconfirmed, initialStopErr)
		}
	}
	var errs []error
	for _, f := range flights {
		if f.completion != nil {
			errs = append(errs, l.Source.Complete(cleanup, *f.completion))
			if errors.Is(f.completion.Err, process.ErrUnconfirmed) || (f.completion.Outcome.PreparationID != "" && !f.completion.Outcome.Stopped) {
				errs = append(errs, errors.Join(process.ErrUnconfirmed, f.completion.Err))
			}
		}
	}
	errs = append(errs, l.shutdownManaged(cleanup))
	return errors.Join(append(errs, ctx.Err())...)
}

// One unreadable process must not suppress another owner's deadline action.
func (l *Loop) maintain(ctx context.Context, flights map[string]*flight, processCursor, actionCursor *string) error {
	k := l.Runtime.Store
	var errs []error
	page, err := k.PendingManagedProcesses(ctx, *processCursor, 128)
	if err != nil {
		errs = append(errs, err)
	} else {
		*processCursor = page.NextAfter
		for _, p := range page.Processes {
			errs = append(errs, l.maintainProcess(ctx, p, flights))
		}
	}
	actions, err := k.PendingDeadlineActions(ctx, *actionCursor, 128)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	*actionCursor = actions.NextAfter
	for _, a := range actions.Actions {
		errs = append(errs, l.maintainAction(ctx, a, flights))
	}
	return errors.Join(errs...)
}

// Apply directions after Complete has disposed prior results, before selecting
// more work in the same event iteration. Failed application preserves the gate.
func (l *Loop) maintainDirections(ctx context.Context, cursor *string) (bool, error) {
	k := l.Runtime.Store
	directions, err := k.PendingWaveDirections(ctx, *cursor, 128)
	if err != nil {
		return false, err
	}
	*cursor = directions.NextAfter
	changed := false
	var errs []error
	for _, d := range directions.Directions {
		result, err := k.ApplyWaveDirection(ctx, d.ID)
		if err != nil && !errors.Is(err, storage.ErrDirectionHeld) {
			errs = append(errs, err)
		}
		changed = changed || err == nil && result.State == "applied"
	}
	return changed, errors.Join(errs...)
}
func (l *Loop) maintainProcess(ctx context.Context, p storage.ManagedProcess, flights map[string]*flight) error {
	if f := flights[p.Request.Binding.ID]; f != nil {
		_, run, _ := f.work.identity()
		if run == p.Request.RunID && f.completion == nil {
			return nil
		}
	}
	proof := storage.ManagedStopProof{OwnerID: p.Request.OwnerID}
	if p.State == "admitted" {
		proof.Disposition = "not_started"
	} else {
		if !p.Identity.Valid() {
			return nil
		}
		alive, e := process.GroupAlive(p.Identity)
		if e != nil {
			return e
		}
		if alive {
			return nil
		}
		proof.Disposition, proof.Identity = "group_stopped", p.Identity
	}
	return l.Runtime.Store.ConfirmManagedProcessStopped(ctx, proof)
}
func (l *Loop) maintainAction(ctx context.Context, a storage.DeadlineAction, flights map[string]*flight) error {
	if a.Kind != "stop_process" || a.OperationID != "" {
		return nil
	} // runner/Git use a separate operation adapter.
	if err := l.requestPreparationStop(ctx, a, flights); err != nil {
		return err
	}
	if err := l.Runtime.Store.CompleteDeadlineAction(ctx, a.ID); err != nil && !errors.Is(err, storage.ErrDeadlineHeld) {
		return err
	}
	return nil
}

// Deadline and explicit operator stops share the physical generation fence.
func (l *Loop) requestPreparationStop(ctx context.Context, a storage.DeadlineAction, flights map[string]*flight) error {
	k := l.Runtime.Store
	var ownerID string
	err := k.Record(ctx, "managed_preparation", a.PreparationID, &ownerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} // Legacy stop proof is reconciled separately.
	if err != nil {
		return err
	}
	p, err := k.ManagedProcess(ctx, ownerID)
	if err != nil {
		return err
	}
	if p.PreparationID != a.PreparationID || p.Request.Binding.ID != a.SessionID || a.RunID != "" && p.Request.RunID != a.RunID {
		return errors.New("process stop source conflict")
	}
	if p.State == "stopped" {
		return nil
	}
	if f := flights[a.SessionID]; f != nil && f.completion == nil {
		_, run, _ := f.work.identity()
		// Read-only recovery reuses a run ID with a NEW preparation. A historical
		// intent must never cancel that newer local owner merely by matching run ID.
		generation := f.physical.Load()
		if generation != nil && generation.owner == ownerID && generation.preparation == a.PreparationID && run == p.Request.RunID && (a.RunID == "" || run == a.RunID) {
			f.cancel()
			return nil
		}
	}
	if p.State == "admitted" {
		return k.ConfirmManagedProcessStopped(ctx, storage.ManagedStopProof{OwnerID: ownerID, Disposition: "not_started"})
	}
	if !p.Identity.Valid() {
		return process.ErrUnconfirmed
	}
	if err = process.RequestGroupStop(p.Identity); err != nil {
		return err
	}
	alive, err := process.GroupAlive(p.Identity)
	if err != nil {
		return err
	}
	if alive {
		return nil
	}
	return k.ConfirmManagedProcessStopped(ctx, storage.ManagedStopProof{OwnerID: ownerID, Disposition: "group_stopped", Identity: p.Identity})
}

func (l *Loop) maintainInterrupts(ctx context.Context, flights map[string]*flight, cursor *string) error {
	page, err := l.Runtime.Store.PendingSessionInterrupts(ctx, *cursor, 128)
	if err != nil {
		return err
	}
	*cursor = page.NextAfter
	var errs []error
	for _, i := range page.Interruptions {
		a := storage.DeadlineAction{Kind: "stop_process", SessionID: i.Request.SessionID, PreparationID: i.Request.PreparationID, RunID: i.Request.RunID}
		if err := l.requestPreparationStop(ctx, a, flights); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := l.Runtime.Store.CompleteSessionInterrupt(ctx, i.ID); err != nil && !errors.Is(err, storage.ErrInterruptionHeld) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Archived completion callbacks do not own physical quiescence. Scan the durable
// owners again on shutdown, including owners no longer present in flights.
func (l *Loop) shutdownManaged(ctx context.Context) error {
	k := l.Runtime.Store
	cursor := ""
	var errs []error
	for {
		page, err := k.PendingManagedProcesses(ctx, cursor, 128)
		if err != nil {
			return errors.Join(append(errs, process.ErrUnconfirmed, err)...)
		}
		for _, p := range page.Processes {
			proof := storage.ManagedStopProof{OwnerID: p.Request.OwnerID, Disposition: "not_started"}
			if p.State != "admitted" {
				if !p.Identity.Valid() {
					errs = append(errs, process.ErrUnconfirmed)
					continue
				}
				if err = process.StopGroup(ctx, p.Identity); err != nil {
					errs = append(errs, err)
					continue
				}
				proof.Disposition, proof.Identity = "group_stopped", p.Identity
			}
			if err = k.ConfirmManagedProcessStopped(ctx, proof); err != nil {
				errs = append(errs, errors.Join(process.ErrUnconfirmed, err))
			}
		}
		if page.NextAfter == "" {
			return errors.Join(errs...)
		}
		cursor = page.NextAfter
	}
}

// Request every known durable owner's stop before a slow local factory can
// consume the shutdown wait. Release still requires the later proof pass.
func (l *Loop) requestShutdownStops(ctx context.Context) error {
	cursor := ""
	var errs []error
	for {
		page, err := l.Runtime.Store.PendingManagedProcesses(ctx, cursor, 128)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		for _, p := range page.Processes {
			if p.Identity.Valid() {
				errs = append(errs, process.RequestGroupStopContext(ctx, p.Identity))
			}
		}
		if page.NextAfter == "" {
			return errors.Join(errs...)
		}
		cursor = page.NextAfter
	}
}
