package product

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductExecutionWorkerPreparesAndResumesOnlyUnclaimedGitIntent(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "prepared_restart"}[prepared], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p, request, wave, task := workspacePreparerFixture(t)
			k := p.Store
			repo := executorRepositoryFixture(request)
			if prepared {
				policy := repo.Policy
				policy.WaveID, policy.PolicyRevision = wave.ID, wave.Policy.Revision
				if _, err := k.RegisterProductVerificationPolicy(ctx, wave.Target(), policy); err != nil {
					t.Fatal(err)
				}
				if _, err := k.ReserveProductWorkspace(ctx, request); err != nil {
					t.Fatal(err)
				}
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			worker := NewExecutionWorker(Executor{Store: k, Repositories: map[int64]ExecutionRepository{request.RepositoryID: repo}, Runner: Runner{Store: k, Executable: exe}}, time.Hour)
			changed := make(chan struct{}, 4)
			worker.OnMutation = func() { changed <- struct{}{} }
			errorsSeen := make(chan error, 8)
			worker.Report = func(err error) { errorsSeen <- err }
			done := make(chan error, 1)
			joined := make(chan struct{})
			go func() { defer close(joined); done <- worker.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-joined:
				case <-time.After(15 * time.Second):
					t.Error("worker cleanup did not join")
				}
			})
			select {
			case <-changed:
			case err := <-errorsSeen:
				t.Fatal(err)
			case <-time.After(10 * time.Second):
				t.Fatal("startup scan did not prepare product Task")
			}
			ready, err := k.ProductWorkspace(ctx, task.ID)
			if err != nil || ready.State != "ready" {
				t.Fatal(ready, err)
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Git owner did not join")
			}
		})
	}
}

func TestProductExecutionWorkerAppliesSavedStoppedRunWithoutModelDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sealer, ready, task, _ := candidateSealerFixture(t)
	k := sealer.Store
	repo := executorRepositoryFixture(ready.Value.Request)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	worker := NewExecutionWorker(Executor{Store: k, Repositories: map[int64]ExecutionRepository{repo.Policy.RepositoryID: repo}, Runner: Runner{Store: k, Executable: exe}}, time.Hour)
	changed := make(chan struct{}, 4)
	worker.OnMutation = func() { changed <- struct{}{} }
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() { defer close(joined); done <- worker.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(15 * time.Second):
			t.Error("worker cleanup did not join")
		}
	})
	select {
	case <-changed:
	case <-time.After(10 * time.Second):
		t.Fatal("saved run was not applied")
	}
	applied, err := k.Task(ctx, task.ID)
	if err != nil || applied.State != contract.TaskCompleted {
		t.Fatal(applied, err)
	}
	var runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='agent_run'").Scan(&runs); err != nil || runs != 1 {
		t.Fatal("worker added a model run", runs, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("sealer owner did not join")
	}
}

func TestIssueAdmittedProductWavePreparesAndSealsStoppedCandidate(t *testing.T) {
	ctx := context.Background()
	preparer, request, wave, task := workspacePreparerFixture(t)
	k := preparer.Store
	repo := executorRepositoryFixture(request)
	worker := NewExecutionWorker(Executor{Store: k, Repositories: map[int64]ExecutionRepository{request.RepositoryID: repo}}, time.Hour)

	changed, err := worker.task(ctx, task.ID)
	if err != nil || !changed {
		t.Fatal("Issue-admitted product Task was not prepared by the executor", changed, err)
	}
	ready, err := k.ProductWorkspace(ctx, task.ID)
	if err != nil || ready.State != "ready" {
		t.Fatal("executor did not prepare the source Task workspace", ready, err)
	}

	stoppedTask, run := stoppedProductWriterRunFixture(t, k, ready, task)
	changed, err = worker.task(ctx, stoppedTask.ID)
	if err != nil || !changed {
		t.Fatal("executor did not adopt the normally stopped product result", changed, err)
	}
	applied, err := k.Task(ctx, task.ID)
	if err != nil || applied.State != contract.TaskCompleted {
		t.Fatal("source Task result did not complete atomically", applied, err)
	}
	candidate, err := k.ReadProductTaskCandidate(ctx, applied.ResultID)
	if err != nil || candidate.Value.RunID != run.ID || candidate.Value.Candidate.SHA == request.BaseSHA || candidate.Value.Target.WaveID != wave.ID {
		t.Fatal("executor did not seal the source Task into its Wave candidate", candidate, err)
	}
	product, err := k.ReadProductTarget(ctx, task.Target(), wave.SourceID)
	if err != nil || product.Value.WorkItem.Phase != domain.Verifying || product.Value.Candidate.SHA != candidate.Value.Candidate.SHA {
		t.Fatal("sealed candidate did not update the registered product WorkItem", product, err)
	}
}
