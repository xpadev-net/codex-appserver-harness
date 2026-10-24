package product

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func checkFinalRunner(t *testing.T, k *storage.KernelStore, target contract.Target, receipt storage.ProductMergedWorkItem) {
	t.Helper()
	t.Run("final_runner", func(t *testing.T) {
		// The fake native merge/ancestry API is fixture evidence only. The actual
		// runner checks out the real verifier commit and executes the pinned argv.
		t.Setenv("PATH", "/usr/bin:/bin")
		ctx := context.Background()
		p := receipt.Value.Product.Value
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		launcher := filepath.Join(k.Root, "final-launcher")
		if err := os.WriteFile(launcher, []byte("#!/bin/sh\nexec \"$@\"\n"), 0700); err != nil {
			t.Fatal(err)
		}
		runtime := storage.ProductRunnerRuntime{Root: filepath.Join(k.Root, "final-runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{launcher}}
		api := &finalReadFixture{repository: Repository{ID: p.WorkItem.RepositoryID, Name: p.Repository, DefaultBranch: p.BaseBranch}, head: p.Candidate.SHA, merge: receipt.Value.MergeSHA}
		gates := 0
		f := FinalRunner{Planner: FinalPlanner{Store: k, Gate: func(context.Context, contract.Target) error { gates++; return nil }, API: func(storage.ProductTarget) ResourceAPI { return api }}, Runner: Runner{Store: k, Executable: exe}}
		job, err := f.Start(ctx, target, p.WorkItem.RepositoryID, "unit", runtime)
		if err != nil || job.State != "launching" || job.Value.Final == nil || job.Value.Spec.Candidate.SHA != p.Candidate.SHA || gates != 2 {
			t.Fatal(job, err, gates)
		}
		if _, err := f.Start(ctx, target, p.WorkItem.RepositoryID, "unit", runtime); !errors.Is(err, storage.ErrProductRunnerHeld) {
			t.Fatal("unknown launch repeated", err)
		}
		deadline := time.Now().Add(15 * time.Second)
		for {
			job, err = f.Runner.Reconcile(ctx, job.ID)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("final runner did not stop/reconcile", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if job.State != "completed" || job.Completion.Result.Status != "passed" || job.Completion.Result.TestedSHA != p.Candidate.SHA {
			t.Fatal("final runner did not execute exact merged-base plan", job)
		}
		if replay, err := f.Start(ctx, target, p.WorkItem.RepositoryID, "unit", runtime); err != nil || replay.ID != job.ID || replay.State != "completed" {
			t.Fatal("completed final runner repeated", replay, err)
		}
		// An observed remote SHA that is not in the local repository must produce
		// an actual infrastructure failure, never a pass for the old local checkout.
		api.head = strings.Repeat("b", 40)
		missing, err := f.Start(ctx, target, p.WorkItem.RepositoryID, "unit", runtime)
		if err != nil {
			t.Fatal(err)
		}
		deadline = time.Now().Add(15 * time.Second)
		for {
			missing, err = f.Runner.Reconcile(ctx, missing.ID)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("missing fixed commit not settled", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if missing.Completion.Result.Status != "infrastructure_failure" {
			t.Fatal("missing remote commit passed", missing)
		}
	})
}
