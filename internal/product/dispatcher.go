package product

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Dispatcher is a controller-owned external worker pool. Canonical gates and
// both transports must be supplied explicitly. It never launches a model or
// reconciles an unknown mutation without physical-stop evidence.
type Dispatcher struct {
	Store      *storage.KernelStore
	Publisher  Publisher
	Reviewer   ReviewRequester
	Workers    int
	Every      time.Duration
	OnMutation func()
	Report     func(error)
}

// Prepared retries use the same operation owner, including after restart.
// MarkPRMayHaveSent remains the atomic one-use send fence. Even an old live
// executor cannot consume that fence twice. This is not a quiescence proof.
func dispatchOwner(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "pr-dispatch-" + hex.EncodeToString(sum[:])
}

func (d Dispatcher) Run(ctx context.Context) error {
	if d.Store == nil || d.Publisher.Store != d.Store || d.Reviewer.Store != d.Store || d.Publisher.Gate == nil || d.Publisher.API == nil || d.Reviewer.Gate == nil || d.Reviewer.API == nil {
		return errors.New("explicit shared PR store, canonical publication/review gates and transports required")
	}
	workers := d.Workers
	if workers == 0 {
		workers = 2
	}
	if workers < 1 || workers > 8 {
		return errors.New("PR dispatch workers must be 1..8")
	}
	every := d.Every
	if every == 0 {
		every = time.Minute
	}
	if every <= 0 {
		return errors.New("positive PR dispatch retry interval required")
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
		if err != nil && d.Report != nil {
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
			page, err := d.Store.PRDispatchOperations(ctx, cursor, 128)
			if err != nil {
				report(err)
				scan = false
			} else {
				complete := true
				for _, op := range page.Operations {
					if flights[op.ID] || time.Now().Before(next[op.ID]) {
						cursor = op.ID
						continue
					}
					if len(flights) >= workers {
						complete = false
						break
					}
					flights[op.ID] = true
					cursor = op.ID
					go func(op storage.PRDispatchOperation) {
						work, cancel := context.WithTimeout(ctx, time.Minute)
						defer cancel()
						var err error
						if op.Name == "pr_publish" {
							_, err = d.Publisher.Publish(work, op.ID, dispatchOwner(op.ID))
						} else {
							_, err = d.Reviewer.Request(work, op.ID, dispatchOwner(op.ID))
						}
						results <- result{op.ID, err}
					}(op)
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
			if d.OnMutation != nil {
				d.OnMutation()
			}
			scan = true
		}
	}
}
