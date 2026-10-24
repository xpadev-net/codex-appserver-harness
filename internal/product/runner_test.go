package product

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestMain(m *testing.M) {
	if len(os.Args) >= 3 && os.Args[1] == "_runner" {
		raw, err := os.ReadFile(os.Args[2])
		var spec runner.Spec
		if err == nil {
			err = json.Unmarshal(raw, &spec)
		}
		socket := ""
		if len(os.Args) == 5 && os.Args[3] == "--wake-socket" {
			socket = os.Args[4]
		}
		if err == nil {
			_, err = runner.ExecuteWithWake(context.Background(), spec, socket)
		}
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// A missing trusted launcher creates a real infrastructure failure. Restoring
// that executable lets a fresh attempt use the same frozen argv and Candidate.
func checkProductRunnerRetryExecution(t *testing.T, k *storage.KernelStore, plan storage.ProductVerificationPlanSnapshot) {
	t.Helper()
	ctx := context.Background()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := Runner{Store: k, Executable: exe}
	launcher := filepath.Join(k.Root, "restorable-runner-launcher")
	runtime := storage.ProductRunnerRuntime{Root: filepath.Join(k.Root, "runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{launcher}}
	job, err := r.Start(ctx, plan.ID, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	await := func(id string) storage.ProductRunnerJob {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			result, err := r.Reconcile(ctx, id)
			if err == nil {
				return result
			}
			if time.Now().After(deadline) {
				t.Fatal("actual retry reconciliation", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	first := await(job.ID)
	if first.Completion.Result.Status != "infrastructure_failure" {
		t.Fatal(first)
	}
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\nexec \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	workspace, err := k.ProductWorkspace(ctx, plan.Value.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	repo := executorRepositoryFixture(workspace.Value.Request)
	repo.Runner = runtime
	repo.Runner.ForbiddenPaths = append([]string{"/new-protected-host-path"}, runtime.ForbiddenPaths...)
	executor := Executor{Store: k, Repositories: map[int64]ExecutionRepository{repo.Policy.RepositoryID: repo}, Runner: r}
	if _, err := executor.AdvanceVerification(ctx, plan.ID); !errors.Is(err, storage.ErrProductRunnerHeld) {
		t.Fatal("changed host runtime reused old retry inputs", err)
	}
	unchanged, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
	if err != nil || unchanged.ValidationJobs != 1 || unchanged.RunnerRetries != 0 {
		t.Fatal("held runtime spent retry budget", unchanged, err)
	}
	repo.Runner = runtime
	executor.Repositories[repo.Policy.RepositoryID] = repo
	pending, err := executor.AdvanceVerification(ctx, plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := k.ReadProductRunnerJob(ctx, pending.Value.Jobs[0].JobID)
	if err != nil || retry.ID == first.ID || retry.Value.RetryOf != first.ID || retry.Value.Spec.Candidate != first.Value.Spec.Candidate {
		t.Fatal(retry, err)
	}
	if _, err := r.StartRetry(ctx, first.ID); !errors.Is(err, storage.ErrProductRunnerHeld) {
		t.Fatal("retry started twice", err)
	}
	finished := await(retry.ID)
	if finished.Completion.Result.Status != "passed" || finished.Completion.Result.TestedSHA != plan.Value.Candidate.SHA {
		t.Fatal(finished)
	}
	evidence, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || !evidence.Value.Passed || evidence.Value.Jobs[0].JobID != retry.ID {
		t.Fatal(evidence, err)
	}
	old, err := k.ReadProductRunnerJob(ctx, first.ID)
	if err != nil || !reflect.DeepEqual(old, first) {
		t.Fatal("retry changed measured first attempt", old, err)
	}
	budget, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
	if err != nil || budget.ValidationJobs != 2 || budget.RunnerRetries != 1 {
		t.Fatal(budget, err)
	}
}

// Extends the real-temp-Git seal/adoption fixture. This is measured subprocess
// execution of a trusted smoke profile, not a native model or P acceptance.
func checkProductRunnerExecution(t *testing.T, k *storage.KernelStore, plan storage.ProductVerificationPlanSnapshot, cancelJob bool) {
	t.Helper()
	ctx := context.Background()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := Runner{Store: k, Executable: exe}
	runtime := storage.ProductRunnerRuntime{Root: filepath.Join(k.Root, "runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"env"}, ForbiddenPaths: []string{"/unavailable-private-credentials"}}
	job, err := r.Start(ctx, plan.ID, "unit", runtime)
	if err != nil || job.State != "launching" {
		t.Fatal(job, err)
	}
	if _, err := r.Start(ctx, plan.ID, "unit", runtime); !errors.Is(err, storage.ErrProductRunnerHeld) {
		t.Fatal("launch replay", err)
	}
	if cancelJob {
		if err := r.RequestStop(ctx, job.ID); err != nil {
			t.Fatal(err)
		}
		reservation, err := k.OperationReservation(ctx, job.ID)
		if err != nil || reservation.State != "held" {
			t.Fatal("stop request released capacity", reservation, err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		job, err = r.Reconcile(ctx, job.ID)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("actual runner reconciliation", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.State != "completed" || job.Completion == nil || (cancelJob && job.Completion.Result.Status != "canceled" || !cancelJob && (job.Completion.Result.Status != "passed" || job.Completion.Result.TestedSHA != plan.Value.Candidate.SHA || job.Completion.LogDigest == "")) {
		t.Fatal(job)
	}
	if !cancelJob {
		checkout := filepath.Join(runtime.Root, job.ID, "checkout")
		data, err := os.ReadFile(filepath.Join(checkout, "implementation.txt"))
		if err != nil || string(data) != "implemented behavior\n" {
			t.Fatal("runner did not test implementation", err)
		}
		if _, err := os.Stat(filepath.Join(checkout, "tests", "behavior_test.go")); err != nil {
			t.Fatal("runner did not test final candidate with tests", err)
		}
	}
	p, err := k.ReadProductTarget(ctx, plan.Value.Target, plan.Value.Plan.WorkItemID)
	if err != nil || p.Value.WorkItem.Phase != domain.Verifying {
		t.Fatal("runner bypassed quality", p, err)
	}
	evidence, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || !evidence.Value.Complete || evidence.Value.Passed == cancelJob {
		t.Fatal("actual execution was not bound to current full profile gate", evidence, err)
	}
	root := k.Root
	k.Close()
	k, err = storage.OpenExistingKernel(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	r.Store = k
	again, err := r.Reconcile(ctx, job.ID)
	if err != nil || again.State != "completed" || again.Completion.ResultDigest != job.Completion.ResultDigest {
		t.Fatal("restart lost actual result", again, err)
	}
}
