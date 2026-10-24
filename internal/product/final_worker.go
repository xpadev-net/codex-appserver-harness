package product

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// FinalVerificationWorker advances fixed-head host work without polling a model.
// The common execution worker reconciles detached runners; this worker only
// launches an unclaimed planned profile or queues a terminal parent observation.
type FinalVerificationWorker struct {
	Final        FinalRunner
	Repositories map[int64]ExecutionRepository
	Every        time.Duration
	OnMutation   func()
	Report       func(error)
}

func (w FinalVerificationWorker) Advance(ctx context.Context, target contract.Target) (bool, error) {
	_, err := w.Final.Planner.Prepare(ctx, target)
	if err != nil {
		return false, err
	}
	evidence, err := w.Final.Runner.Store.ReadProductFinalVerificationEvidence(ctx, target)
	if err != nil {
		return false, err
	}
	if evidence.Value.Complete && evidence.Value.Current {
		return w.Final.Runner.Store.NotifyProductFinalVerification(ctx, target)
	}
	for _, repo := range evidence.Value.Repositories {
		host, ok := w.Repositories[repo.RepositoryID]
		if !ok || host.Name != repo.Repository || host.BaseBranch != repo.BaseBranch {
			return false, storage.ErrProductFinalPlanHeld
		}
		for _, job := range repo.Jobs {
			if job.State != "not_run" && job.State != "prepared" {
				continue
			}
			_, err := w.Final.Start(ctx, target, repo.RepositoryID, job.ProfileID, host.Runner)
			if err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}
func (w FinalVerificationWorker) Run(ctx context.Context) (runErr error) {
	var stoppingErr error
	defer func() { runErr = errors.Join(runErr, stoppingErr) }()
	if w.Final.Planner.Store == nil || w.Final.Runner.Store != w.Final.Planner.Store || w.Every <= 0 {
		return errors.New("same final planner/runner store and positive interval required")
	}
	ticker := time.NewTicker(w.Every)
	defer ticker.Stop()
	cursor := ""
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		page, err := w.Final.Planner.Store.ProductFinalExecutionTargets(ctx, cursor, 16)
		if err != nil {
			if w.Report != nil {
				w.Report(err)
			}
		} else {
			for _, item := range page.Unavailable {
				if w.Report != nil {
					w.Report(fmt.Errorf("final target %s: %s", item.WaveID, item.Reason))
				}
			}
			for _, target := range page.Targets {
				changed, err := w.Advance(ctx, target)
				if errors.Is(err, workspace.ErrGitStopUnconfirmed) {
					stoppingErr = errors.Join(stoppingErr, err)
				}
				expected := errors.Is(err, storage.ErrProductFinalPlanHeld) || errors.Is(err, storage.ErrProductFinalFetchHeld) || errors.Is(err, storage.ErrProductRunnerHeld) || errors.Is(err, storage.ErrExecutionCapacity) || errors.Is(err, storage.ErrResourceBusy)
				if err != nil && !expected && ctx.Err() == nil && w.Report != nil {
					w.Report(err)
				}
				if changed && w.OnMutation != nil {
					w.OnMutation()
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			cursor = page.NextAfter
			if cursor != "" {
				continue
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
