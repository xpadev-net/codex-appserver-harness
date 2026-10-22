package control

import (
	"context"
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
	return &Loop{Runtime: runtime, Source: source, wake: make(chan struct{}, 1)}
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
	var workCursor, processCursor, waveCursor, actionCursor string
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
				if waveCursor != "" {
					scan = true
				}
			}
			e = l.maintain(ctx, flights, &processCursor, &actionCursor)
			if e != nil {
				l.report(e)
			} else if processCursor != "" || actionCursor != "" {
				scan = true
			}
			finish()
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
						go func(w SessionWork, sid, run string) {
							var out Outcome
							var e error
							if w.Execute != nil {
								out, e = l.Runtime.Execute(child, *w.Execute)
							} else {
								out, e = l.Runtime.Reconcile(child, w.Recovery.SessionID, w.Recovery.Run, w.Recovery.Validate)
							}
							completion := Completion{SessionID: sid, RunID: run, Outcome: out, Err: e}
							// Shutdown drains this channel until each process owner has returned.
							select {
							case results <- completion:
							case <-abandon:
							}
						}(work, sid, run)
					}
					// Continue a bounded page immediately, rather than waiting 60 seconds
					// when a source page has only busy or rejected entries.
					if page.NextAfter != "" && page.NextAfter != previousCursor {
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
			scan = true
		case <-l.wake:
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
			return errors.Join(ctx.Err(), process.ErrUnconfirmed)
		}
	}
	var errs []error
	for _, f := range flights {
		if f.completion != nil {
			errs = append(errs, l.Source.Complete(cleanup, *f.completion))
		}
	}
	return errors.Join(append(errs, ctx.Err())...)
}

func (l *Loop) maintain(ctx context.Context, flights map[string]*flight, processCursor, actionCursor *string) error {
	k := l.Runtime.Store
	page, err := k.PendingManagedProcesses(ctx, *processCursor, 128)
	if err != nil {
		return err
	}
	*processCursor = page.NextAfter
	for _, p := range page.Processes {
		f := flights[p.Request.Binding.ID]
		if f != nil {
			_, run, _ := f.work.identity()
			if run == p.Request.RunID && f.completion == nil {
				continue
			}
		}
		proof := storage.ManagedStopProof{OwnerID: p.Request.OwnerID}
		if p.State == "admitted" {
			proof.Disposition = "not_started"
		} else {
			if !p.Identity.Valid() {
				continue
			}
			alive, e := process.GroupAlive(p.Identity)
			if e != nil {
				return e
			}
			if alive {
				continue
			}
			proof.Disposition, proof.Identity = "group_stopped", p.Identity
		}
		if err = k.ConfirmManagedProcessStopped(ctx, proof); err != nil {
			return err
		}
	}
	actions, err := k.PendingDeadlineActions(ctx, *actionCursor, 128)
	if err != nil {
		return err
	}
	*actionCursor = actions.NextAfter
	for _, a := range actions.Actions {
		if a.Kind != "stop_process" {
			continue
		}
		if f := flights[a.SessionID]; f != nil && f.completion == nil {
			_, run, _ := f.work.identity()
			var ownerID string
			if err = k.Record(ctx, "managed_run_claim", run, &ownerID); err == nil {
				p, e := k.ManagedProcess(ctx, ownerID)
				if e != nil {
					return e
				}
				if p.Request.Binding.ID == a.SessionID && p.PreparationID == a.PreparationID {
					f.cancel()
				}
			} else if !errors.Is(err, context.Canceled) {
				l.report(err)
			}
		}
		if err = k.CompleteDeadlineAction(ctx, a.ID); err != nil && !errors.Is(err, storage.ErrDeadlineHeld) {
			return err
		}
	}
	return nil
}
