package product

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// SplitCreationReconciliationWorker polls stopped creations without model turns
// or mutation transport. Run joins every read and leaves active senders alone.
type SplitCreationReconciliationWorker struct {
	Reconciler SplitCreationReconciler
	// Trusted source reader for a new adoption, outside its SQL transaction.
	// Historical reads and exact replays never refresh source authority.
	RefreshSource func(context.Context, storage.ProductSplitChildPlanRequest) error
	Workers       int
	Every         time.Duration
	OnMutation    func()
	Report        func(error)
}

func (d SplitCreationReconciliationWorker) Run(ctx context.Context) error {
	if d.Reconciler.Store == nil || d.Reconciler.API == nil || d.Reconciler.MaxPages < 1 || d.Reconciler.MaxPages > 32 {
		return errors.New("split creation reconciliation store and read transport required")
	}
	workers := d.Workers
	if workers == 0 {
		workers = 2
	}
	if workers < 1 || workers > 8 {
		return errors.New("split creation reconciliation workers must be 1..8")
	}
	every := d.Every
	if every == 0 {
		every = time.Minute
	}
	if every <= 0 {
		return errors.New("positive split creation reconciliation interval required")
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	type result struct {
		id      string
		changed bool
		err     error
	}
	results := make(chan result, workers)
	flights := map[string]bool{}
	next := map[string]time.Time{}
	cursor := ""
	scan := true
	report := func(err error) {
		// A wake can cancel the owner between the loop's cancellation check
		// and the next short DB scan. Preserve other/joined failures, while
		// treating the exact owner cancellation as ordinary worker shutdown.
		if err != nil && !mergeDispatchCancellation(err, ctx.Err()) && (!errors.Is(err, storage.ErrProductSplitHeld) || errors.Is(err, storage.ErrTaskEvidence)) && d.Report != nil {
			d.Report(err)
		}
	}
	for {
		if ctx.Err() != nil {
			for len(flights) > 0 {
				r := <-results
				delete(flights, r.id)
				report(r.err)
			}
			return ctx.Err()
		}
		if scan && len(flights) < workers {
			page, err := d.Reconciler.Store.SplitCreationReconciliationTargets(ctx, cursor, 128)
			if err != nil {
				report(err)
				scan = false
			} else {
				complete := true
				for _, target := range page.Targets {
					id := target.ID
					if target.Unavailable {
						id = target.Cursor
					}
					if flights[id] || time.Now().Before(next[id]) {
						cursor = target.Cursor
						continue
					}
					if len(flights) >= workers {
						complete = false
						break
					}
					flights[id] = true
					cursor = target.Cursor
					go func(target storage.SplitCreationReconciliationTarget, flightID string) {
						changed, err := d.process(ctx, target)
						results <- result{flightID, changed, err}
					}(target, id)
				}
				if complete {
					if page.NextAfter == "" {
						cursor = ""
						scan = false
					} else {
						cursor = page.NextAfter
					}
				}
			}
			if scan && len(flights) < workers {
				continue
			}
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
			for id, due := range next {
				if !time.Now().Before(due) {
					delete(next, id)
				}
			}
			scan = true
		case r := <-results:
			delete(flights, r.id)
			next[r.id] = time.Now().Add(every)
			report(r.err)
			if r.changed && d.OnMutation != nil {
				d.OnMutation()
			}
			scan = true
		}
	}
}

func (d SplitCreationReconciliationWorker) process(ctx context.Context, target storage.SplitCreationReconciliationTarget) (bool, error) {
	if target.Unavailable {
		return false, storage.ErrTaskEvidence
	}
	job, err := d.Reconciler.Store.ReadProductSplitCreate(ctx, target.ID)
	if err != nil {
		if errors.Is(err, storage.ErrProductSplitHeld) || errors.Is(err, storage.ErrTaskEvidence) {
			return false, errors.Join(storage.ErrTaskEvidence, err)
		}
		return false, err
	}
	// The sending supervisor owns active groups. A GET-only worker must never
	// stop a healthy POST to manufacture its own reconciliation boundary.
	if job.State == "observed" {
		return d.finishRegistrations(ctx, job.Value.Request)
	}
	if job.State != "stopped" {
		return false, nil
	}
	observed, changed, err := d.Reconciler.Reconcile(ctx, target.ID)
	if err != nil || observed.State != "observed" {
		return changed, err
	}
	registered, err := d.finishRegistrations(ctx, observed.Value.Request)
	return changed || registered, err
}

// Each stage is a durable whole-set transaction. An interrupted adoption can
// resume from an observed creation without another GET or any mutation call.
func (d SplitCreationReconciliationWorker) finishRegistrations(ctx context.Context, request storage.ProductSplitChildPlanRequest) (bool, error) {
	_, err := d.Reconciler.Store.ReadProductSplitChildScopes(ctx, request)
	changed := false
	if errors.Is(err, sql.ErrNoRows) && !errors.Is(err, storage.ErrProductSplitHeld) {
		if d.RefreshSource != nil {
			if err := d.RefreshSource(ctx, request); err != nil {
				return changed, err
			}
		}
		if _, err = d.Reconciler.Store.BindProductSplitChildScopes(ctx, request); err == nil {
			changed = true
		}
	}
	if err != nil {
		return changed, err
	}
	_, err = d.Reconciler.Store.ReadProductSplitRegistrations(ctx, request)
	if err == nil {
		return changed, nil
	}
	if !errors.Is(err, sql.ErrNoRows) || errors.Is(err, storage.ErrProductSplitHeld) {
		return changed, err
	}
	if d.RefreshSource != nil {
		if err := d.RefreshSource(ctx, request); err != nil {
			return changed, err
		}
	}
	_, err = d.Reconciler.Store.RegisterProductSplitChildren(ctx, request)
	return changed || err == nil, err
}
