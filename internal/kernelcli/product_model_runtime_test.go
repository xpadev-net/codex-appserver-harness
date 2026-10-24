package kernelcli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func productModelRuntimeFixture(t *testing.T, k *storage.KernelStore, model string) (ProductExecutorConfig, control.IsolatedConfig) {
	t.Helper()
	c := productExecutorConfigFixture(t)
	binary := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	if err := os.Chmod(home, 0700); err != nil {
		t.Fatal(err)
	}
	runtime := control.IsolatedConfig{ProductTools: true, ProductToolProfile: "product_v2", PrivateRoot: k.Root, Model: model, CodexBinary: binary, WorkerCodexHome: home, Launcher: append([]string{}, c.Repositories[0].Runner.Launcher...), LauncherContract: "argv_passthrough_v1", IsolationVerified: true}
	return c, runtime
}
func TestProductModelRuntimeRequiresCompletePrivateCurrentConfiguration(t *testing.T) {
	k, main, err := openKernel(context.Background(), cliRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for _, mode := range []string{"valid", "disabled", "legacy", "root", "model", "isolation", "binary", "writable_binary", "public_home", "launcher", "profile", "auth"} {
		t.Run(mode, func(t *testing.T) {
			c, runtime := productModelRuntimeFixture(t, k, main.Model)
			switch mode {
			case "disabled":
				runtime.ProductTools = false
			case "legacy":
				runtime.ProductToolProfile = "product_v1"
			case "root":
				runtime.PrivateRoot = t.TempDir()
			case "model":
				runtime.Model = ""
			case "isolation":
				runtime.IsolationVerified = false
			case "binary":
				runtime.CodexBinary = filepath.Join(t.TempDir(), "missing")
			case "writable_binary":
				if err := os.Chmod(runtime.CodexBinary, 0777); err != nil {
					t.Fatal(err)
				}
			case "public_home":
				if err := os.Chmod(runtime.WorkerCodexHome, 0755); err != nil {
					t.Fatal(err)
				}
			case "launcher":
				runtime.Launcher = nil
			case "profile":
				runtime.ProductToolProfile = "unknown"
			case "auth":
				runtime.SessionAuthFile = filepath.Join(t.TempDir(), "auth")
				if err := os.WriteFile(runtime.SessionAuthFile, []byte("private"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			config, err := productModelRuntime(k, runtime, map[int64]product.ExecutionRepository{17: c.Repositories[0]})
			if mode == "disabled" || mode == "legacy" {
				if err != nil || config != nil {
					t.Fatal("local-only executor enabled models", config, err)
				}
				return
			}
			if mode != "valid" {
				if err == nil {
					t.Fatal("incomplete runtime activated", mode, config)
				}
				return
			}
			if err != nil || config == nil || len(config.RuntimeDigest) != 64 || config.Model != main.Model || config.Repositories[0].Policy.RepositoryID != 17 {
				t.Fatal(config, err)
			}
		})
	}
}
func TestProductExecutorBindsReadinessOnlyWhileSameStoreWorkerRuns(t *testing.T) {
	ctx := context.Background()
	k, main, err := openKernel(ctx, cliRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	c, runtime := productModelRuntimeFixture(t, k, main.Model)
	repo := c.Repositories[0].Policy.Repository
	for _, args := range [][]string{{"init", "--initial-branch=trunk"}, {"config", "user.name", "readiness fixture"}, {"config", "user.email", "readiness@example.invalid"}, {"-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "base"}} {
		if _, err := workspace.Git(ctx, repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	c.Repositories[0].InitialSHA, err = workspace.Resolve(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.CreateProductWave(ctx, storage.ProductWaveRequest{Wave: storage.WaveRequest{OrchestratorSessionID: main.SessionID, Source: "direct", SourceID: "bound-product-executor", OriginalRequest: "implement original behavior", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "verified original behavior"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: main.Limits}}, WorkItems: []storage.ProductWorkItemDefinition{{ID: "work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}})
	if err != nil {
		t.Fatal(err)
	}
	spec := contract.TaskSpec{ID: "implementation", Kind: "work", Instruction: "implement original behavior", Criteria: []string{"AC"}, WorkItemIDs: []string{"work"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "bound-executor-plan", contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "implementation", Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}})
	if err != nil {
		t.Fatal(err)
	}
	worker, _, err := loadProductExecutor(operatorFile(t, c), k, runtime)
	if err != nil {
		t.Fatal(err)
	}
	before, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: w.Target()})
	if err != nil || len(before.Tasks) != 0 {
		t.Fatal("config load granted dispatch", before, err)
	}
	run, stop := context.WithCancel(ctx)
	defer stop()
	errorsSeen := make(chan error, 8)
	done := make(chan error, 1)
	go func() { done <- worker(run, k, func() {}, func(err error) { errorsSeen <- err }) }()
	joined := false
	defer func() {
		stop()
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("executor cleanup did not join")
			}
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-errorsSeen:
			t.Fatal("product preparation failed", err)
		default:
		}
		ready, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: w.Target()})
		if err != nil {
			t.Fatal(err)
		}
		if len(ready.Tasks) == 1 {
			if ready.Tasks[0].Task.ID != admitted.CreatedTaskIDs[0] {
				t.Fatal(ready)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bound worker never produced ready Task", ready)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor did not join/revoke")
	}
	after, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: w.Target()})
	if err != nil || len(after.Tasks) != 0 {
		t.Fatal("stopped worker left readiness active", after, err)
	}
	var runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("readiness/preparation launched a model", runs, err)
	}
}
