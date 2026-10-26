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
	workItemsByRepository := map[int64][]string{}
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
			workItemsByRepository[definition.RepositoryID] = append(workItemsByRepository[definition.RepositoryID], child.WorkItemID)
		}
	}
	if len(byID) == 0 || len(byID) > 128 {
		return storage.ErrProductFinalPlanHeld
	}
	repoIDs := make([]int64, 0, len(workItemsByRepository))
	for id := range workItemsByRepository {
		repoIDs = append(repoIDs, id)
	}
	sort.Slice(repoIDs, func(i, j int) bool { return repoIDs[i] < repoIDs[j] })
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(repoIDs))
	var joined sync.WaitGroup
	for _, repositoryID := range repoIDs {
		ids := workItemsByRepository[repositoryID]
		sort.Strings(ids)
		observer := byID[ids[0]].observer
		joined.Add(1)
		go func(observer SplitChildSourceObserver, ids []string) {
			defer joined.Done()
			if work.Err() != nil {
				return
			}
			updates, acknowledged, err := observer.ObserveMany(work, target, ids)
			if err == nil && (!acknowledged || len(updates) != len(ids)) {
				err = storage.ErrProductFinalPlanHeld
			}
			if err == nil {
				for _, update := range updates {
					if update.Snapshot.Value.Facts.Hold || (update.Snapshot.Value.Status != "ready" && update.Snapshot.Value.Status != "closed") {
						err = storage.ErrProductFinalPlanHeld
						break
					}
				}
			}
			results <- err
			if err != nil {
				cancel()
			}
		}(observer, ids)
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
	if completed != len(repoIDs) {
		return storage.ErrProductFinalPlanHeld
	}
	// The final proof walk and fixed root-head reads are independent. Run them
	// together so the oldest checked child source does not age while SQLite
	// authenticates the complete tree.
	checks := make(chan error, len(repoIDs)+1)
	joined.Add(1)
	go func() {
		defer joined.Done()
		_, err := g.Store.ReadProductSplitFinalSources(ctx, target)
		checks <- err
	}()
	for _, id := range repoIDs {
		id := id
		joined.Add(1)
		go func() {
			defer joined.Done()
			checks <- g.checkRoot(ctx, repositories[id])
		}()
	}
	joined.Wait()
	close(checks)
	var checkErr error
	for err := range checks {
		checkErr = errors.Join(checkErr, err)
	}
	if checkErr != nil {
		return checkErr
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
