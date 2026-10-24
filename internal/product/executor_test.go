package product

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func executorRepositoryFixture(request storage.ProductWorkspaceRequest) ExecutionRepository {
	return ExecutionRepository{Name: "owner/repo", BaseBranch: "trunk", InitialSHA: request.BaseSHA, Policy: storage.ProductVerificationPolicy{RepositoryID: request.RepositoryID, Repository: request.Repository, VerifierPaths: []string{"tests/"}, Verification: policy.Verification{Revision: 1, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"sh", "-c", "printf PASS"}, TimeoutSeconds: 30, SuccessPattern: "PASS"}}}}}
}

func TestProductExecutorPreparesOnlyCurrentPhaseAndConfiguredRepository(t *testing.T) {
	for _, mode := range []string{"success", "future_phase", "wrong_repository"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			preparer, request, w, task := workspacePreparerFixture(t)
			k := preparer.Store
			repo := executorRepositoryFixture(request)
			if mode == "wrong_repository" {
				repo.Name = "different/repository"
			}
			if mode == "future_phase" {
				tasks, err := k.Tasks(ctx, w.ID)
				if err != nil {
					t.Fatal(err)
				}
				for _, item := range tasks {
					if item.Spec.Kind == "review" {
						task = item
					}
				}
			}
			e := Executor{Store: k, Repositories: map[int64]ExecutionRepository{request.RepositoryID: repo}}
			ready, err := e.Prepare(ctx, task.ID)
			if mode != "success" {
				if err == nil {
					t.Fatal("unready/unconfigured Task prepared", ready)
				}
				if _, err := k.ProductWorkspace(ctx, task.ID); err == nil {
					t.Fatal("rejected preparation saved a workspace")
				}
				return
			}
			if err != nil || ready.State != "ready" || ready.Value.Request != request {
				t.Fatal(ready, err)
			}
			again, err := e.Prepare(ctx, task.ID)
			if err != nil || !reflect.DeepEqual(ready, again) {
				t.Fatal("preparation replay changed input", again, err)
			}
			set, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: w.Target()})
			if err != nil || len(set.Tasks) != 0 {
				t.Fatal("host composition enabled model dispatch", set, err)
			}
		})
	}
}

func TestProductExecutorAppliesStoppedWriterAndReplaysReceipt(t *testing.T) {
	ctx := context.Background()
	sealer, ready, task, run := candidateSealerFixture(t)
	repo := executorRepositoryFixture(ready.Value.Request)
	e := Executor{Store: sealer.Store, Repositories: map[int64]ExecutionRepository{ready.Value.Request.RepositoryID: repo}}
	if _, err := e.Apply(ctx, task.ID, "unrelated-run"); err == nil {
		t.Fatal("unrelated run applied")
	}
	if _, err := e.Store.DB.Exec("UPDATE kernel_run_bindings SET process_stopped=0 WHERE run_id=?", run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Apply(ctx, task.ID, run.ID); err == nil {
		t.Fatal("run without physical stopping proof applied")
	}
	if _, err := e.Store.ProductSeal(ctx, task.ID, run.ID); err == nil {
		t.Fatal("unconfirmed run started sealing")
	}
	if _, err := e.Store.DB.Exec("UPDATE kernel_run_bindings SET process_stopped=1 WHERE run_id=?", run.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := e.Apply(ctx, task.ID, run.ID)
	if err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal(receipt, err)
	}
	source, err := e.Store.ReadProductTaskCandidate(ctx, receipt.ResultID)
	if err != nil || source.Value.Candidate.SHA == ready.Value.Request.BaseSHA {
		t.Fatal("host did not seal changed checkout", source, err)
	}
	again, err := e.Apply(ctx, task.ID, run.ID)
	if err != nil || !reflect.DeepEqual(receipt, again) {
		t.Fatal("host replay changed receipt", again, err)
	}
}

func checkProductExecutorVerification(t *testing.T, k *storage.KernelStore, plan storage.ProductVerificationPlanSnapshot) {
	t.Helper()
	ctx := context.Background()
	ready, err := k.ProductWorkspace(ctx, plan.Value.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	repo := executorRepositoryFixture(ready.Value.Request)
	repo.Runner = storage.ProductRunnerRuntime{Root: filepath.Join(k.Root, "runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"env"}}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	e := Executor{Store: k, Repositories: map[int64]ExecutionRepository{repo.Policy.RepositoryID: repo}, Runner: Runner{Store: k, Executable: exe}}
	deadline := time.Now().Add(10 * time.Second)
	for {
		evidence, err := e.AdvanceVerification(ctx, plan.ID)
		if err != nil {
			t.Fatal(err)
		}
		if evidence.Value.Complete {
			if !evidence.Value.Passed {
				t.Fatal("host execution failed", evidence)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("host did not reconcile detached runner")
		}
		time.Sleep(10 * time.Millisecond)
	}
	before, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.AdvanceVerification(ctx, plan.ID); err != nil {
		t.Fatal(err)
	}
	after, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("completed verification was restarted", after, err)
	}
}
