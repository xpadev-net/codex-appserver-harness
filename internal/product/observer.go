package product

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Observer owns bounded read-only collection outside KernelSource.Ready. The
// daemon must hold its controller lock. API methods must honor cancellation.
type Observer struct {
	Store      *storage.KernelStore
	API        func(storage.ProductTarget) ResourceAPI
	Workers    int
	Every      time.Duration
	OnMutation func()
	Report     func(error)
}
type observationResult struct {
	key     string
	changed bool
	err     error
}

func observationID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "scan-" + hex.EncodeToString(b[:]), nil
}

func (o Observer) Run(ctx context.Context) error {
	if o.Store == nil || o.API == nil {
		return errors.New("explicit PR observation store and read transport required")
	}
	workers := o.Workers
	if workers == 0 {
		workers = 2
	}
	if workers < 1 || workers > 8 {
		return errors.New("PR observer workers must be 1..8")
	}
	every := o.Every
	if every == 0 {
		every = 60 * time.Second
	}
	if every <= 0 {
		return errors.New("positive PR observation interval required")
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	results := make(chan observationResult, workers)
	flights := map[string]bool{}
	next := map[string]time.Time{}
	cursor := ""
	scan := true
	report := func(err error) {
		if err != nil && o.Report != nil {
			o.Report(err)
		}
	}
	for {
		if ctx.Err() != nil {
			// Collect and its transport honor the same cancellation. Keep the
			// store/controller owner alive until every read worker has returned.
			for len(flights) > 0 {
				r := <-results
				delete(flights, r.key)
			}
			return ctx.Err()
		}
		if scan && len(flights) < workers {
			page, err := o.Store.PRObservationTargets(ctx, cursor, 128)
			if err != nil {
				report(err)
				scan = false
			} else {
				completedPage := true
				for _, target := range page.Targets {
					key := target.Resource.ResourceKey
					if flights[key] || time.Now().Before(next[key]) {
						cursor = key
						continue
					}
					if len(flights) >= workers {
						completedPage = false
						break
					}
					id, err := observationID()
					if err != nil {
						report(err)
						completedPage = false
						scan = false
						break
					}
					flights[key] = true
					cursor = key
					go func(target storage.PRObservationTarget, id string) {
						receipt, err := (ResourceCollector{Store: o.Store, API: o.API}).Collect(ctx, target.Product, id)
						results <- observationResult{key: target.Resource.ResourceKey, changed: err == nil && len(receipt.Changes) > 0, err: err}
					}(target, id)
				}
				if completedPage {
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
			scan = true
		case r := <-results:
			delete(flights, r.key)
			next[r.key] = time.Now().Add(every)
			report(r.err)
			if r.changed && o.OnMutation != nil {
				o.OnMutation()
			}
			scan = true
		}
	}
}
