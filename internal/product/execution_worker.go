package product

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// ExecutionWorker is controller-owned local work, outside model selection.
// Synchronous Git owners join before Run returns; detached jobs remain durably
// owned and are reconciled independently of current Task/Candidate membership.
type ExecutionWorker struct {
	Executor   Executor
	Every      time.Duration
	OnMutation func()
	Report     func(error)
	wake       chan struct{}
}

func NewExecutionWorker(executor Executor, every time.Duration) *ExecutionWorker {
	return &ExecutionWorker{Executor: executor, Every: every, wake: make(chan struct{}, 1)}
}

func (w *ExecutionWorker) Hint() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func executionPending(err error) bool {
	return errors.Is(err, storage.ErrProductWorkspaceHeld) || errors.Is(err, storage.ErrProductSubjectHeld) || errors.Is(err, storage.ErrProductRunnerHeld) || errors.Is(err, storage.ErrTemplateNeedsMain) || errors.Is(err, storage.ErrExecutionCapacity)
}

func (w *ExecutionWorker) task(ctx context.Context, id string) (bool, error) {
	k := w.Executor.Store
	task, err := k.Task(ctx, id)
	if err != nil {
		return false, err
	}
	switch task.State {
	case contract.TaskReady:
		// Preparing an already saved workspace is not permission to repeat Git.
		if saved, err := k.ProductWorkspace(ctx, task.ID); err == nil && saved.State == "ready" {
			return false, nil
		} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		_, err := w.Executor.Prepare(ctx, id)
		return err == nil, err
	case contract.TaskCompleted:
		if task.Spec.Kind != "verification" {
			return false, nil
		}
		source, err := k.ReadProductTaskCandidate(ctx, task.ResultID)
		if err != nil {
			return false, err
		}
		if source.Value.VerificationPlan == nil {
			return false, storage.ErrTaskEvidence
		}
		before, err := k.ReadProductVerificationEvidence(ctx, source.Value.VerificationPlan.ID)
		if err != nil {
			return false, err
		}
		after, err := w.Executor.AdvanceVerification(ctx, source.Value.VerificationPlan.ID)
		return before.Digest != after.Digest && after.Digest != "", err
	default:
		state, err := k.SessionDispatchState(ctx, task.SessionID)
		if err != nil {
			return false, err
		}
		if state.Run == nil || state.Run.State != "completed" || !state.ProcessStopped || !state.RunStopped {
			return false, nil
		}
		_, err = w.Executor.Apply(ctx, task.ID, state.Run.ID)
		return err == nil, err
	}
}

func (w *ExecutionWorker) Run(ctx context.Context) (runErr error) {
	if w.Executor.Store == nil || w.Executor.Runner.Store != w.Executor.Store || w.Every <= 0 || w.wake == nil {
		return errors.New("configured product executor and positive interval required")
	}
	ticker := time.NewTicker(w.Every)
	defer ticker.Stop()
	var tasksAfter, jobsAfter string
	var stoppingErr error
	defer func() { runErr = errors.Join(runErr, stoppingErr) }()
	report := func(err error) {
		if errors.Is(err, workspace.ErrGitStopUnconfirmed) {
			stoppingErr = errors.Join(stoppingErr, err)
		}
		if err != nil && !executionPending(err) && w.Report != nil {
			w.Report(err)
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		jobs, err := w.Executor.Store.ProductRunningJobs(ctx, jobsAfter, 128)
		if err != nil {
			report(err)
		} else {
			jobsAfter = jobs.NextAfter
			for _, id := range jobs.IDs {
				job, err := w.Executor.Runner.Reconcile(ctx, id)
				if errors.Is(err, runner.ErrJobBusy) || errors.Is(err, runner.ErrCompletionUnconfirmed) || os.IsNotExist(err) {
					continue
				}
				report(err)
				if err == nil && job.State == "completed" && w.OnMutation != nil {
					w.OnMutation()
				}
			}
		}
		page, err := w.Executor.Store.ProductExecutionTasks(ctx, tasksAfter, 128)
		if err != nil {
			report(err)
		} else {
			tasksAfter = page.NextAfter
			for _, id := range page.IDs {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				changed, err := w.task(ctx, id)
				report(err)
				if changed && w.OnMutation != nil {
					w.OnMutation()
				}
			}
		}
		if tasksAfter != "" || jobsAfter != "" {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.wake:
		case <-ticker.C:
		}
	}
}
