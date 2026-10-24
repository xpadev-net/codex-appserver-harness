package product

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
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
	checkKernelFinalGate(t, k, target, receipt)
	checkFinalCommitFetch(t, k, target, receipt)

}

func checkFinalCommitFetch(t *testing.T, k *storage.KernelStore, target contract.Target, receipt storage.ProductMergedWorkItem) {
	t.Helper()
	t.Run("fetched_final_runner", func(t *testing.T) {
		t.Setenv("PATH", "/usr/bin:/bin")
		ctx := context.Background()
		p := receipt.Value.Product.Value
		policy, err := k.ReadProductVerificationPolicy(ctx, target, p.WorkItem.RepositoryID)
		if err != nil {
			t.Fatal(err)
		}
		remote := t.TempDir()
		if _, err := workspace.Git(ctx, policy.Value.Repository, "clone", "--no-local", "--", policy.Value.Repository, remote); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"checkout", "-B", p.BaseBranch, p.Candidate.SHA}, {"config", "user.name", "final fixture"}, {"config", "user.email", "fixture@example.invalid"}} {
			if _, err := workspace.Git(ctx, remote, args...); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(remote, "after-merge.txt"), []byte("actual remote base advance\n"), 0600); err != nil {
			t.Fatal(err)
		}
		sha, err := workspace.Freeze(workspace.WithStoppedCommands(ctx), remote, "actual remote commit for final runner")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := workspace.Resolve(ctx, policy.Value.Repository, sha); err == nil {
			t.Fatal("fixture SHA already local")
		}
		oldHead, err := workspace.Resolve(ctx, policy.Value.Repository, "HEAD")
		if err != nil {
			t.Fatal(err)
		}
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		launcher := filepath.Join(k.Root, "fetch-final-launcher")
		if err := os.WriteFile(launcher, []byte("#!/bin/sh\nexec \"$@\"\n"), 0700); err != nil {
			t.Fatal(err)
		}
		api := &finalReadFixture{repository: Repository{ID: p.WorkItem.RepositoryID, Name: p.Repository, DefaultBranch: p.BaseBranch}, head: sha, merge: receipt.Value.MergeSHA}
		gates := 0
		temporary := errors.New("fixture source read temporarily unavailable")
		f := FinalRunner{Planner: FinalPlanner{Store: k, Gate: func(context.Context, contract.Target) error {
			gates++
			if gates == 5 {
				return temporary
			}
			return nil
		}, API: func(storage.ProductTarget) ResourceAPI { return api }}, Runner: Runner{Store: k, Executable: exe}, FetchRemotes: map[int64]string{p.WorkItem.RepositoryID: remote}}
		runtime := storage.ProductRunnerRuntime{Root: filepath.Join(k.Root, "fetched-final-runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{launcher}}
		if _, err := f.Start(ctx, target, p.WorkItem.RepositoryID, "unit", runtime); !errors.Is(err, temporary) {
			t.Fatal("did not exercise pre-send transient read", err, gates)
		}
		if _, err := workspace.Resolve(ctx, policy.Value.Repository, sha); err == nil {
			t.Fatal("held pre-send attempt fetched the object")
		}
		job, err := f.Start(ctx, target, p.WorkItem.RepositoryID, "unit", runtime)
		if err != nil || job.State != "launching" || job.Value.Spec.Candidate.SHA != sha {
			t.Fatal("remote commit did not reach trusted runner", job, err)
		}
		currentHead, err := workspace.Resolve(ctx, policy.Value.Repository, "HEAD")
		if err != nil || currentHead != oldHead {
			t.Fatal("fetch moved host checkout", currentHead, err)
		}
		deadline := time.Now().Add(15 * time.Second)
		for {
			job, err = f.Runner.Reconcile(ctx, job.ID)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("fetched final runner did not stop", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if job.State != "completed" || job.Completion.Result.Status != "passed" || job.Completion.Result.TestedSHA != sha {
			t.Fatal("runner used old checkout instead of fixed fetched SHA", job)
		}
		var id string
		if err := k.DB.QueryRow("SELECT id FROM records WHERE kind='product_final_fetch' AND json_extract(payload,'$.value.sha')=? AND json_extract(payload,'$.state')='available'", sha).Scan(&id); err != nil {
			t.Fatal(err)
		}
		fetch, err := k.ReadProductFinalFetch(ctx, id)
		if err != nil || fetch.State != "available" || fetch.Proof == nil || fetch.Value.RetryOf == "" || !fetch.Proof.Observation.Attempted || fetch.MayHaveSentAt.IsZero() {
			t.Fatal("fetch lacks physical owner/send/stop receipt", fetch, err)
		}
		replay, err := f.Start(ctx, target, p.WorkItem.RepositoryID, "unit", runtime)
		if err != nil || replay.ID != job.ID || replay.State != "completed" {
			t.Fatal("replay launched another runner", replay, err)
		}
		same, err := k.ReadProductFinalFetch(ctx, id)
		if err != nil || !reflect.DeepEqual(same, fetch) {
			t.Fatal("replay repeated fetch", same, err)
		}

		// A new remote HEAD is discovered by the ordinary daemon worker. It
		// starts a real fixed-commit profile, then produces one parent input.
		if err := os.WriteFile(filepath.Join(remote, "daemon-advance.txt"), []byte("next observed base\n"), 0600); err != nil {
			t.Fatal(err)
		}
		api.head, err = workspace.Freeze(workspace.WithStoppedCommands(ctx), remote, "daemon final verification")
		if err != nil {
			t.Fatal(err)
		}
		f.ResolveRemote = func(context.Context, contract.Target, int64) (string, error) { return remote, nil }
		f.FetchRemotes = nil
		worker := FinalVerificationWorker{Final: f, Repositories: map[int64]ExecutionRepository{p.WorkItem.RepositoryID: {Name: p.Repository, BaseBranch: p.BaseBranch, Policy: policy.Value, Runner: runtime}}}
		if changed, err := worker.Advance(ctx, target); err != nil || !changed {
			t.Fatal("daemon did not start fixed profile", changed, err)
		}
		evidence, err := k.ReadProductFinalVerificationEvidence(ctx, target)
		if err != nil || len(evidence.Value.Repositories) != 1 || len(evidence.Value.Repositories[0].Jobs) != 1 {
			t.Fatal(evidence, err)
		}
		jobID := evidence.Value.Repositories[0].Jobs[0].JobID
		deadline = time.Now().Add(15 * time.Second)
		for {
			job, err = f.Runner.Reconcile(ctx, jobID)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("daemon final runner did not stop", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if job.Completion.Result.Status != "passed" || job.Completion.Result.TestedSHA != api.head {
			t.Fatal("daemon ran stale commit", job)
		}
		for i := 0; i < 2; i++ {
			if changed, err := worker.Advance(ctx, target); err != nil || changed != (i == 0) {
				t.Fatal("final notice missing or repeated", changed, err)
			}
		}
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='parent_attention' AND json_extract(payload,'$.reason')='product_final_verification'").Scan(&count); err != nil || count != 1 {
			t.Fatal(count, err)
		}
	})
}
