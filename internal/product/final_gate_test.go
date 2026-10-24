package product

import (
	"context"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Called from the real Git/SQLite/runner/merge fixture. Capabilities remain
// synthetic here; this is component evidence, never native P acceptance.
func checkKernelFinalGate(t *testing.T, k *storage.KernelStore, target contract.Target, receipt storage.ProductMergedWorkItem) {
	t.Helper()
	ctx := context.Background()
	p := receipt.Value.Product.Value
	policy, err := k.ReadProductVerificationPolicy(ctx, target, p.WorkItem.RepositoryID)
	if err != nil {
		t.Fatal(err)
	}
	host := ExecutionRepository{Name: p.Repository, BaseBranch: p.BaseBranch, Policy: policy.Value}
	report := contract.NewReport(p.Repository)
	report.RepositoryID, report.DefaultBranch = p.WorkItem.RepositoryID, p.BaseBranch
	if k.Now != nil {
		report.ObservedAt = k.Now()
	}
	for _, name := range contract.MergeRequirements {
		report.Capabilities[name] = contract.Capability{State: contract.Verified, Evidence: "fixture only " + name}
	}
	g := KernelFinalGate{Executor: Executor{Store: k, Repositories: map[int64]ExecutionRepository{p.WorkItem.RepositoryID: host}}, Source: func(ctx context.Context, saved storage.ProductTargetSnapshot) error {
		// This fixture predates Issue admission. Live Issue refresh is covered
		// by KernelSourceGate tests; retain the current scope check here.
		w, err := k.Wave(ctx, target.WaveID)
		if err != nil {
			return err
		}
		if w.Target() != saved.Value.Target {
			return storage.ErrProductFinalPlanHeld
		}
		return nil
	}, Policies: map[int64]KernelMergePolicy{p.WorkItem.RepositoryID: {ReadReport: func(context.Context) (contract.Report, error) { return report, nil }}}}
	if err := g.Check(ctx, target); err != nil {
		t.Fatal("current merged source rejected", err)
	}
	for _, name := range contract.MergeRequirements {
		old := report.Capabilities[name]
		report.Capabilities[name] = contract.Capability{State: contract.Unknown, Evidence: "fixture unknown"}
		if err := g.Check(ctx, target); err == nil {
			t.Fatal("unknown capability accepted", name)
		}
		report.Capabilities[name] = old
	}
	host.Policy.Verification.Revision++
	g.Executor.Repositories[p.WorkItem.RepositoryID] = host
	if err := g.Check(ctx, target); err == nil {
		t.Fatal("changed host policy accepted")
	}
	host.Policy = policy.Value
	g.Executor.Repositories[p.WorkItem.RepositoryID] = host
	g.Source = func(context.Context, storage.ProductTargetSnapshot) error { return storage.ErrProductQualityHeld }
	if err := g.Check(ctx, target); err == nil {
		t.Fatal("held current source accepted")
	}
}
