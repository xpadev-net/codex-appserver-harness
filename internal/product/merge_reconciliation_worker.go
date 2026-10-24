package product

import (
	"context"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// MergeReconciliationWorker polls persisted original requests without model turns
// or mutation transport. Run joins every read before returning to its owner.
type MergeReconciliationWorker struct {
	Reconciler MergeReconciler
	Workers    int
	Every      time.Duration
	OnMutation func()
	Report     func(error)
}

func (d MergeReconciliationWorker) Run(ctx context.Context) error {
	if d.Reconciler.Store == nil || d.Reconciler.API == nil {
		return errors.New("merge reconciliation store and read transport required")
	}
	workers := d.Workers
	if workers == 0 {
		workers = 2
	}
	if workers < 1 || workers > 8 {
		return errors.New("merge reconciliation workers must be 1..8")
	}
	every := d.Every
	if every == 0 {
		every = time.Minute
	}
	if every <= 0 {
		return errors.New("positive merge reconciliation interval required")
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	type result struct {
		id  string
		err error
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
		if err != nil && !(ctx.Err() != nil && err == ctx.Err()) && !errors.Is(err, storage.ErrProductMergeHeld) && d.Report != nil {
			d.Report(err)
		}
	}
	for {
		if ctx.Err() != nil {
			for len(flights) > 0 {
				r := <-results
				delete(flights, r.id)
			}
			return ctx.Err()
		}
		if scan && len(flights) < workers {
			page, err := d.Reconciler.Store.MergeReconciliationTargets(ctx, cursor, 128)
			if err != nil {
				report(err)
				scan = false
			} else {
				complete := true
				for _, id := range page.OperationIDs {
					if flights[id] || time.Now().Before(next[id]) {
						cursor = id
						continue
					}
					if len(flights) >= workers {
						complete = false
						break
					}
					flights[id] = true
					cursor = id
					go func(id string) {
						_, err := d.Reconciler.Reconcile(ctx, id)
						results <- result{id, err}
					}(id)
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
			if r.err == nil && d.OnMutation != nil {
				d.OnMutation()
			}
			scan = true
		}
	}
}
