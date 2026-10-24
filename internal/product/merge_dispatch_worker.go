package product

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// MergeDispatchWorker belongs to the single controller/authority owner. It
// discovers durable requests and journals, without model turns or resend of a
// previously claimed transport. Request and journal scans share flight keys.
type MergeDispatchWorker struct {
	Driver     MergeDriver
	Methods    map[int64]string
	Workers    int
	Every      time.Duration
	OnMutation func()
	Report     func(error)
}

func (d MergeDispatchWorker) process(ctx context.Context, target storage.MergeDispatchTarget) (bool, error) {
	if target.Kind == "request" {
		round, err := d.Driver.Store.ReadProductCloudRound(ctx, target.ID)
		if err != nil {
			return false, err
		}
		method, ok := d.Methods[round.Product.Value.WorkItem.RepositoryID]
		if !ok {
			return false, storage.ErrProductMergeHeld
		}
		// Waiting for formal approval must not acquire a mutation lease that
		// would block publication of the next fix candidate.
		if _, err := d.Driver.Gate(ctx, round.Product, target.ID); err != nil {
			return false, err
		}
		job, err := d.Driver.Merge(ctx, target.ID, dispatchOwner("merge:"+target.ID), method)
		return d.finishKnown(ctx, job, err)
	}
	if target.Kind != "merge" {
		return false, storage.ErrTaskEvidence
	}
	job, err := d.Driver.Store.ReadProductMerge(ctx, target.ID)
	if err != nil {
		return false, err
	}
	if job.Value.HTTPProtocol != "registered_http_v1" {
		return false, storage.ErrProductMergeHeld
	}
	if job.State == "merged" {
		_, changed, err := d.Driver.Store.RecordProductMergedWorkItem(ctx, job.ID)
		return changed, err
	}
	if job.Stop != nil || job.HTTPAbort != nil || job.Cancellation != nil {
		return false, nil
	}
	if job.State == "prepared" {
		current, changed, err := d.Driver.Store.CancelStalePreparedProductMerge(ctx, job.ID)
		if err != nil || changed {
			return changed, err
		}
		method, ok := d.Methods[job.Value.Product.Value.WorkItem.RepositoryID]
		if !ok || method != job.Value.Method {
			return false, storage.ErrProductMergeHeld
		}
		job, err := d.Driver.Merge(ctx, current.Value.RequestID, current.Value.OwnerID, method)
		return d.finishKnown(ctx, job, err)
	}
	if job.State != "launching" && job.State != "unknown" {
		return false, storage.ErrProductMergeHeld
	}
	job, err = d.Driver.Transport.Recover(ctx, job.ID)
	return d.finishKnown(ctx, job, err)
}

func (d MergeDispatchWorker) finishKnown(ctx context.Context, job storage.ProductMerge, err error) (bool, error) {
	if err == nil && job.State == "merged" {
		_, changed, err := d.Driver.Store.RecordProductMergedWorkItem(ctx, job.ID)
		return changed, err
	}
	return job.Stop != nil || job.HTTPAbort != nil || job.Cancellation != nil, err
}

// A single joined cause is still ordinary cancellation/hold. Joined unrelated
// failures (notably physical-stop uncertainty) must remain visible.
func mergeSoleCause(err, cause error) bool {
	if err == nil || cause == nil {
		return false
	}
	if err == cause {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		parts := joined.Unwrap()
		return len(parts) == 1 && mergeSoleCause(parts[0], cause)
	}
	return false
}

func (d MergeDispatchWorker) Run(ctx context.Context) error {
	if d.Driver.Store == nil || d.Driver.Transport.Store != d.Driver.Store || d.Driver.Gate == nil || !filepath.IsAbs(d.Driver.Transport.Executable) {
		return errors.New("shared merge driver, gate and trusted transport required")
	}
	d.Methods = maps.Clone(d.Methods)
	if len(d.Methods) < 1 || len(d.Methods) > 128 {
		return errors.New("explicit merge methods for 1..128 repositories required")
	}
	for id, method := range d.Methods {
		if id < 1 || method != "merge" && method != "squash" && method != "rebase" {
			return errors.New("positive merge repository and known method required")
		}
	}
	workers := d.Workers
	if workers == 0 {
		workers = 2
	}
	if workers < 1 || workers > 8 {
		return errors.New("merge dispatch workers must be 1..8")
	}
	every := d.Every
	if every == 0 {
		every = time.Minute
	}
	if every <= 0 {
		return errors.New("positive merge dispatch interval required")
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	type result struct {
		key     string
		changed bool
		err     error
	}
	results := make(chan result, workers)
	flights := map[string]bool{}
	next := map[string]time.Time{}
	cursor, scan := "", true
	report := func(err error) {
		if err != nil && !mergeSoleCause(err, ctx.Err()) && !mergeSoleCause(err, storage.ErrProductMergeHeld) && d.Report != nil {
			d.Report(err)
		}
	}
	for {
		if ctx.Err() != nil {
			for len(flights) > 0 {
				r := <-results
				delete(flights, r.key)
				report(r.err)
			}
			return ctx.Err()
		}
		if scan && len(flights) < workers {
			page, err := d.Driver.Store.MergeDispatchTargets(ctx, cursor, 128)
			if err != nil {
				report(err)
				scan = false
			} else {
				complete := true
				for _, target := range page.Targets {
					if flights[target.MergeID] || time.Now().Before(next[target.MergeID]) {
						cursor = target.Cursor
						continue
					}
					if len(flights) >= workers {
						complete = false
						break
					}
					flights[target.MergeID] = true
					cursor = target.Cursor
					go func(target storage.MergeDispatchTarget) {
						work, cancel := context.WithTimeout(ctx, 2*time.Minute)
						defer cancel()
						changed, err := d.process(work, target)
						results <- result{target.MergeID, changed, err}
					}(target)
				}
				if complete {
					if page.NextAfter == "" {
						cursor, scan = "", false
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
			delete(flights, r.key)
			next[r.key] = time.Now().Add(every)
			report(r.err)
			if r.changed && d.OnMutation != nil {
				d.OnMutation()
			}
			scan = true
		}
	}
}
