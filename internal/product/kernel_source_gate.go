package product

import (
	"context"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// KernelSourceGate refreshes an admitted Issue immediately before publication
// quality is evaluated. Periodic source polling alone is not a send-time proof.
// Direct roots use their saved version; their split children still require a
// fresh read of the child's own Issue. Other source kinds stay held.
type KernelSourceGate struct {
	Store    *storage.KernelStore
	Policies map[int64]IssueSourcePolicy
	API      func(storage.ProductTarget) ResourceAPI
}

func (g KernelSourceGate) Check(ctx context.Context, saved storage.ProductTargetSnapshot) error {
	if g.Store == nil {
		return errors.New("source journal required")
	}
	p := saved.Value
	w, err := g.Store.Wave(ctx, p.Target.WaveID)
	if err != nil {
		return err
	}
	if w.Target() != p.Target {
		return storage.ErrProductQualityHeld
	}
	child, err := g.Store.IsKnownProductSplitChildWorkItem(ctx, p.WorkItem.ID)
	if err != nil {
		return err
	}
	if child {
		policy, ok := g.Policies[p.WorkItem.RepositoryID]
		if !ok || policy.RepositoryID != p.WorkItem.RepositoryID || policy.Repository != p.Repository || g.API == nil {
			return storage.ErrProductQualityHeld
		}
		api := g.API(p)
		if api == nil {
			return storage.ErrProductQualityHeld
		}
		o := SplitChildSourceObserver{Store: g.Store, API: api, Policy: policy, Attempts: 1}
		update, acknowledged, err := o.Observe(ctx, p.Target, p.WorkItem.ID)
		// Periodic polling can finish a newer GET while this read is in flight.
		// Retry once with entirely new bookends and the current CAS identity;
		// never save old facts at a newer time or accept the winning cached read.
		if errors.Is(err, storage.ErrProductSplitSourceSuperseded) && ctx.Err() == nil {
			update, acknowledged, err = o.Observe(ctx, p.Target, p.WorkItem.ID)
		}
		if err != nil {
			return err
		}
		if !acknowledged || update.Snapshot.Value.Status != "ready" {
			return storage.ErrProductQualityHeld
		}
		if _, err := g.Store.RequireProductSplitChildSource(ctx, p.Target, p.WorkItem.ID, policy.Digest(), time.Minute); err != nil {
			return err
		}
	}
	return g.checkRoot(ctx, saved)
}

func (g KernelSourceGate) checkRoot(ctx context.Context, saved storage.ProductTargetSnapshot) error {
	if g.Store == nil {
		return storage.ErrProductQualityHeld
	}
	p := saved.Value
	w, err := g.Store.Wave(ctx, p.Target.WaveID)
	if err != nil {
		return err
	}
	if w.Target() != p.Target {
		return storage.ErrProductQualityHeld
	}
	if w.Source == "direct" {
		return nil
	}
	policy, ok := g.Policies[p.WorkItem.RepositoryID]
	if w.Source != "github" || !ok || policy.RepositoryID != p.WorkItem.RepositoryID || policy.Repository != p.Repository || g.API == nil {
		return errors.New("fresh Issue source route required for publication")
	}
	api := g.API(p)
	if api == nil {
		return errors.New("source read transport required")
	}
	o := IssueSourceObserver{Store: g.Store, API: api, Policy: policy, Attempts: 1}
	if err := o.Validate(); err != nil {
		return err
	}
	readCtx := github.WithResourceReadBudget(ctx, 16<<20)
	update, acknowledged, err := o.observe(ctx, readCtx, &budgetResourceAPI{ResourceAPI: api}, w, 5*time.Second)
	if err != nil {
		return err
	}
	if !acknowledged || update.Hold != nil || update.Snapshot.Value.Status != "ready" {
		return storage.ErrProductQualityHeld
	}
	fresh, err := g.Store.Wave(ctx, w.ID)
	if err != nil {
		return err
	}
	if fresh.Target() != p.Target || fresh.Source != w.Source || fresh.SourceID != w.SourceID || fresh.SourceHold != nil {
		return storage.ErrProductQualityHeld
	}
	return nil
}
