package product

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Automatic Issue writes require an explicit repository allowlist and trusted
// executable. This worker shares one flight key across every attempt of a child.
type SplitCreationDispatchWorker struct {
	Store          *storage.KernelStore
	Executable     string
	Repositories   map[int64]string
	SourcePolicies map[int64]IssueSourcePolicy
	Workers        int
	Every          time.Duration
	OnMutation     func()
	Report         func(error)
}

func (d SplitCreationDispatchWorker) process(ctx context.Context, id string) (bool, error) {
	job, err := d.Store.ReadProductSplitCreate(ctx, id)
	if err != nil {
		return false, err
	}
	latest, err := d.Store.ReadLatestProductSplitCreate(ctx, job.Value.Child.ID)
	if err != nil {
		return false, err
	}
	transport := SplitHTTPTransport{Store: d.Store, Executable: d.Executable}
	if latest.State == "unknown" {
		// A restarted controller must settle the original registered group even
		// when a repository was removed from the current write allowlist.
		got, err := transport.Recover(ctx, latest.ID)
		return d.finish(got, err)
	}
	if latest.State == "stopped" || latest.State == "observed" {
		return false, nil
	}
	child := latest.Value.Child.Value
	if d.Repositories[child.RepositoryID] != child.Repository {
		return false, storage.ErrProductSplitHeld
	}
	w, err := d.Store.Wave(ctx, latest.Value.Request.Target.WaveID)
	if err != nil {
		return false, err
	}
	if w.Source == "github" || latest.Value.Ancestry != nil {
		policy, ok := d.SourcePolicies[child.RepositoryID]
		if !ok {
			return false, storage.ErrProductSplitHeld
		}
		transport.SourcePolicy = &policy
	}
	reserved := false
	if latest.State == "aborted" {
		latest, err = d.Store.ReserveProductSplitCreateUnsentRetry(ctx, latest.ID, dispatchOwner("split:"+latest.ID))
		if err != nil {
			return false, err
		}
		reserved = true
	}
	if latest.State != "prepared" {
		return reserved, storage.ErrProductSplitHeld
	}
	// An existing/partial immutable spec means launch may already have started.
	// It is recovery work, never a second command invocation.
	specPath := filepath.Join(d.Store.Root, "split-http", latest.ID, "spec.json")
	if _, err := os.Lstat(specPath); err == nil {
		got, err := transport.Recover(ctx, latest.ID)
		changed, err := d.finish(got, err)
		return reserved || changed, err
	} else if !errors.Is(err, os.ErrNotExist) {
		return reserved, err
	}
	got, err := transport.Start(ctx, latest.ID)
	changed, err := d.finish(got, err)
	return reserved || changed, err
}

func (d SplitCreationDispatchWorker) Run(ctx context.Context) error {
	if d.Store == nil || !filepath.IsAbs(d.Executable) || len(d.Repositories) < 1 || len(d.Repositories) > 128 || d.Workers < 1 || d.Workers > 8 || d.Every <= 0 {
		return errors.New("explicit split store, executable, repositories, workers 1..8 and interval required")
	}
	d.Repositories = maps.Clone(d.Repositories)
	d.SourcePolicies = maps.Clone(d.SourcePolicies)
	for id, name := range d.Repositories {
		if id < 1 || name == "" {
			return errors.New("fixed split repository identity required")
		}
		if policy, ok := d.SourcePolicies[id]; ok && (policy.Validate() != nil || policy.Repository != name || policy.RepositoryID != id) {
			return errors.New("split source policy differs from allowlisted repository")
		}
	}
	type result struct {
		key     string
		changed bool
		err     error
	}
	results := make(chan result, d.Workers)
	flights := map[string]bool{}
	due := map[string]time.Time{}
	cursor, scan := "", true
	ticker := time.NewTicker(d.Every)
	defer ticker.Stop()
	report := func(err error) {
		if err != nil && !mergeDispatchCancellation(err, ctx.Err()) && !mergeSoleCause(err, storage.ErrProductSplitHeld) && d.Report != nil {
			d.Report(err)
		}
	}
	for {
		if ctx.Err() != nil {
			for len(flights) > 0 {
				r := <-results
				delete(flights, r.key)
				report(r.err)
				if r.changed && d.OnMutation != nil {
					d.OnMutation()
				}
			}
			return ctx.Err()
		}
		if scan && len(flights) < d.Workers {
			page, err := d.Store.SplitCreationReconciliationTargets(ctx, cursor, 128)
			if err != nil {
				report(err)
				scan = false
			} else {
				complete := true
				for _, target := range page.Targets {
					if target.Unavailable {
						report(storage.ErrTaskEvidence)
						cursor = target.Cursor
						continue
					}
					job, err := d.Store.ReadProductSplitCreate(ctx, target.ID)
					if err != nil {
						report(err)
						cursor = target.Cursor
						continue
					}
					key := job.Value.Child.ID
					if flights[key] || time.Now().Before(due[key]) {
						cursor = target.Cursor
						continue
					}
					if len(flights) >= d.Workers {
						complete = false
						break
					}
					cursor = target.Cursor
					flights[key] = true
					go func(id, key string) {
						work, cancel := context.WithTimeout(ctx, 2*time.Minute)
						defer cancel()
						changed, err := d.process(work, id)
						results <- result{key, changed, err}
					}(target.ID, key)
				}
				if complete {
					if page.NextAfter == "" {
						cursor, scan = "", false
					} else {
						cursor = page.NextAfter
					}
				}
			}
			if scan && len(flights) < d.Workers {
				continue
			}
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
			for key, at := range due {
				if !time.Now().Before(at) {
					delete(due, key)
				}
			}
			scan = true
		case r := <-results:
			delete(flights, r.key)
			due[r.key] = time.Now().Add(d.Every)
			report(r.err)
			if r.changed && d.OnMutation != nil {
				d.OnMutation()
			}
			scan = true
		}
	}
}

// A failed Commit may return an in-memory terminal value. Notify only after
// independently authenticating committed durable evidence. A canceled owner
// still joins this bounded read before releasing its Store/controller lock.
func (d SplitCreationDispatchWorker) finish(returned storage.ProductSplitCreate, cause error) (bool, error) {
	if returned.ID == "" || returned.Stop == nil && returned.Abort == nil {
		return false, cause
	}
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	saved, err := d.Store.ReadProductSplitCreate(cleanup, returned.ID)
	if err != nil {
		return false, errors.Join(cause, err)
	}
	return saved.Stop != nil || saved.Abort != nil, cause
}
