package product

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// KernelFinalGate checks current host verification policy, source and required
// capabilities after merge. It does not ask a closed PR to become mergeable.
type KernelFinalGate struct {
	SplitSources func(context.Context, contract.Target) error
	Executor     Executor
	Source       func(context.Context, storage.ProductTargetSnapshot) error
	Policies     map[int64]KernelMergePolicy
}

func (g KernelFinalGate) Check(ctx context.Context, target contract.Target) error {
	k := g.Executor.Store
	if k == nil || g.Source == nil {
		return storage.ErrProductFinalPlanHeld
	}
	coverage, err := k.ProductCoverage(ctx, target)
	if err != nil {
		return err
	}
	if len(coverage.Value.Merged) == 0 || len(coverage.Value.Residual)+len(coverage.Value.Pending)+len(coverage.Value.Unavailable)+len(coverage.Value.UnmergedWorkItems) != 0 {
		return fmt.Errorf("%w: incomplete merged coverage", storage.ErrProductFinalPlanHeld)
	}
	scoped := map[string]bool{}
	for _, group := range coverage.Value.Splits {
		for _, child := range group.Children {
			scoped[child.WorkItemID] = true
		}
	}
	if len(coverage.Value.Splits) > 0 && g.SplitSources == nil {
		return storage.ErrProductFinalPlanHeld
	}
	checked := map[int64]bool{}
	for _, fact := range coverage.Value.Merged {
		receipt, err := k.ReadProductMergedWorkItem(ctx, fact.MergeID)
		if err != nil {
			return err
		}
		if receipt.Digest != fact.ReceiptDigest {
			return storage.ErrTaskEvidence
		}
		p := receipt.Value.Product.Value
		host, ok := g.Executor.Repositories[p.WorkItem.RepositoryID]
		route, configured := g.Policies[p.WorkItem.RepositoryID]
		if !ok || !configured || route.ReadReport == nil || host.Name != p.Repository || host.BaseBranch != p.BaseBranch {
			return fmt.Errorf("%w: missing repository policy route", storage.ErrProductFinalPlanHeld)
		}
		pinned, err := k.ReadProductVerificationPolicy(ctx, target, p.WorkItem.RepositoryID)
		if err != nil {
			return err
		}
		expected := host.Policy
		expected.WaveID, expected.PolicyRevision = target.WaveID, target.PolicyRevision
		expected.VerifierPaths = slices.Clone(expected.VerifierPaths)
		slices.Sort(expected.VerifierPaths)
		if expected.Validate() != nil || !reflect.DeepEqual(expected, pinned.Value) {
			return fmt.Errorf("%w: host verification policy changed", storage.ErrProductFinalPlanHeld)
		}
		if !checked[p.WorkItem.RepositoryID] {
			report, err := route.ReadReport(ctx)
			if err != nil {
				return err
			}
			now := time.Now().UTC()
			if k.Now != nil {
				now = k.Now().UTC()
			}
			if report.Version != 1 || report.RepositoryID != p.WorkItem.RepositoryID || report.Repository != p.Repository || report.DefaultBranch != p.BaseBranch || report.ObservedAt.IsZero() || report.ObservedAt.After(now) || !report.AutoMergeReady() {
				return fmt.Errorf("%w: current final capability contract is unverified", storage.ErrProductFinalPlanHeld)
			}
			for _, name := range contract.MergeRequirements {
				if strings.TrimSpace(report.Capabilities[name].Evidence) == "" {
					return storage.ErrProductFinalPlanHeld
				}
			}
			checked[p.WorkItem.RepositoryID] = true
		}
		current, err := k.ReadProductTarget(ctx, target, p.WorkItem.ID)
		if err != nil {
			return err
		}
		if current.Value.Repository != p.Repository || current.Value.BaseBranch != p.BaseBranch || current.Value.WorkItem.Phase != domain.Merged || current.Value.Candidate != p.Candidate {
			return fmt.Errorf("%w: merged projection changed", storage.ErrProductFinalPlanHeld)
		}
		if !scoped[p.WorkItem.ID] {
			if err := g.Source(ctx, current); err != nil {
				return err
			}
		}
	}
	if len(coverage.Value.Splits) > 0 {
		if err := g.SplitSources(ctx, target); err != nil {
			return err
		}
	}
	fresh, err := k.ProductCoverage(ctx, target)
	if err != nil {
		return err
	}
	if fresh.Digest != coverage.Digest {
		return fmt.Errorf("%w: coverage changed during final gate", storage.ErrProductFinalPlanHeld)
	}
	return nil
}
