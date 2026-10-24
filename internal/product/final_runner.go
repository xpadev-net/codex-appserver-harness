package product

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// FinalRunner refreshes actual GitHub heads/source policy before reserving and
// claiming an ordinary trusted runner. The repository must already contain the
// fixed commit; fetch/hydration is a separate leased host operation.
type FinalRunner struct {
	Planner FinalPlanner
	Runner  Runner
}

func (f FinalRunner) Start(ctx context.Context, target contract.Target, repositoryID int64, profileID string, runtime storage.ProductRunnerRuntime) (storage.ProductRunnerJob, error) {
	var job storage.ProductRunnerJob
	if f.Runner.Store == nil || f.Runner.Store != f.Planner.Store || !filepath.IsAbs(f.Runner.Executable) {
		return job, errors.New("same store and explicit trusted final runner required")
	}
	plan, err := f.Planner.Prepare(ctx, target)
	if err != nil {
		return job, err
	}
	job, err = f.Runner.Store.ReserveProductFinalRunnerJob(ctx, plan.ID, repositoryID, profileID, runtime)
	if err != nil {
		return job, err
	}
	return f.Runner.startJob(ctx, job)
}
