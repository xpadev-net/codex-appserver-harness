package product

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// CheckFinalSplitSources refreshes every registered child, including delegated
// parents without an own PR. The final store read authenticates closed Issues
// against actual own merge receipts; observation alone supplies no exception.
// This is a source-only check, not merged coverage or final acceptance.
func (g KernelSourceGate) CheckFinalSplitSources(ctx context.Context, target contract.Target) error {
	if g.Store == nil || g.API == nil {
		return storage.ErrProductFinalPlanHeld
	}
	coverage, err := g.Store.ProductCoverage(ctx, target)
	if err != nil {
		return err
	}
	if len(coverage.Value.Unavailable) != 0 {
		return storage.ErrProductFinalPlanHeld
	}
	type route struct{ observer SplitChildSourceObserver }
	byID := map[string]route{}
	repositories := map[int64]storage.ProductTargetSnapshot{}
	for _, group := range coverage.Value.Splits {
		for _, child := range group.Children {
			if _, duplicate := byID[child.WorkItemID]; duplicate {
				return storage.ErrProductFinalPlanHeld
			}
			registration, err := g.Store.ReadProductWorkItemRegistration(ctx, child.WorkItemID)
			if err != nil {
				return err
			}
			definition := registration.Value.Definition
			policy, ok := g.Policies[definition.RepositoryID]
			if !ok || policy.RepositoryID != definition.RepositoryID || policy.Repository != definition.Repository || registration.Digest != child.RegistrationDigest {
				return storage.ErrProductFinalPlanHeld
			}
			// Routing metadata is not a candidate or a publication grant. The observer
			// authenticates the actual registered binding before performing any GET.
			current := storage.ProductTargetSnapshot{Value: storage.ProductTarget{Target: target, Repository: definition.Repository, WorkItem: domain.WorkItem{ID: definition.ID, RepositoryID: definition.RepositoryID}}}
			api := g.API(current.Value)
			if api == nil {
				return storage.ErrProductFinalPlanHeld
			}
			byID[child.WorkItemID] = route{SplitChildSourceObserver{Store: g.Store, API: api, Policy: policy, Attempts: 1}}
			repositories[definition.RepositoryID] = current
		}
	}
	if len(byID) == 0 || len(byID) > 128 {
		return storage.ErrProductFinalPlanHeld
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	jobs := make(chan string, len(ids))
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(ids))
	var joined sync.WaitGroup
	for i := 0; i < min(4, len(ids)); i++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for id := range jobs {
				if work.Err() != nil {
					return
				}
				observer := byID[id].observer
				update, acknowledged, err := observer.Observe(work, target, id)
				if err == nil && (!acknowledged || update.Snapshot.Value.Facts.Hold || (update.Snapshot.Value.Status != "ready" && update.Snapshot.Value.Status != "closed")) {
					err = storage.ErrProductFinalPlanHeld
				}
				results <- err
				if err != nil {
					cancel()
					return
				}
			}
		}()
	}
	joined.Wait() // Cancellation never leaves detached GETs or source saves.
	close(results)
	completed := 0
	var failed error
	for err := range results {
		completed++
		failed = errors.Join(failed, err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if failed != nil {
		return failed
	}
	if completed != len(ids) {
		return storage.ErrProductFinalPlanHeld
	}
	repoIDs := make([]int64, 0, len(repositories))
	for id := range repositories {
		repoIDs = append(repoIDs, id)
	}
	sort.Slice(repoIDs, func(i, j int) bool { return repoIDs[i] < repoIDs[j] })
	for _, id := range repoIDs {
		if err := g.checkRoot(ctx, repositories[id]); err != nil {
			return err
		}
	}
	if _, err := g.Store.ReadProductSplitFinalSources(ctx, target); err != nil {
		return err
	}
	current, err := g.Store.ProductCoverage(ctx, target)
	if err != nil {
		return err
	}
	if current.Digest != coverage.Digest {
		return storage.ErrProductFinalPlanHeld
	}
	return nil
}
