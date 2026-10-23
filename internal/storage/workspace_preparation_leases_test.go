package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func TestWorkspacePreparationLeaseSurvivesGitIdentityChangeAliasesAndReopen(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	repo := operationRepo(t)
	destination := filepath.Join(t.TempDir(), "prepared")
	request := OperationCapacityRequest{ID: "prepare-workspace", Kind: "git", SessionID: "main", Target: w.Target(), Repository: repo, Worktree: destination}
	initialIdentity, err := worktreeResource(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := k.ReserveOperationCapacity(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := k.ReserveOperationCapacity(ctx, request)
	if err != nil || !reflect.DeepEqual(reserved, replay) {
		t.Fatal(replay, err)
	}
	runner := OperationCapacityRequest{ID: "before-creation", Kind: "runner", SessionID: "main", Target: w.Target(), Worktree: destination}
	if _, err := k.ReserveOperationCapacity(ctx, runner); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("runner bypassed absent destination lease", err)
	}
	if err := os.Mkdir(destination, 0700); err != nil {
		t.Fatal(err)
	}
	b := capacityMain(t, k, "prepared-worker", destination)
	if err := k.ReserveSessionThread(ctx, b); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("process bypassed absent destination lease", err)
	}
	if err := workspace.Create(ctx, repo, destination, "harness/prepared", "HEAD"); err != nil {
		t.Fatal(err)
	}
	createdIdentity, err := worktreeResource(ctx, destination)
	if err != nil || createdIdentity == initialIdentity {
		t.Fatal("fixture did not change Git identity", initialIdentity, createdIdentity, err)
	}
	child := filepath.Join(destination, "child")
	if err := os.Mkdir(child, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(destination, alias); err != nil {
		t.Fatal(err)
	}
	for i, path := range []string{destination, child, alias, filepath.Join(alias, "child"), filepath.Dir(destination)} {
		runner.ID = "runner-during-creation-" + string(rune('a'+i))
		runner.Worktree = path
		if _, err := k.ReserveOperationCapacity(ctx, runner); !errors.Is(err, ErrResourceBusy) {
			t.Fatal("changed identity/path bypassed preparation", path, err)
		}
	}
	if err := k.ReserveSessionThread(ctx, b); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("process bypassed linked worktree lease", err)
	}
	if err := k.ReleaseOperationCapacity(ctx, OperationReleaseProof{OperationID: request.ID, ProofID: "not-a-stop", Disposition: "unknown", Reason: "child status is unknown"}); err == nil {
		t.Fatal("unknown preparation released destination")
	}
	file := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenExistingKernel(file)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if err := k.ReserveSessionThread(ctx, b); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("reopen forgot preparation destination", err)
	}
	// A sibling checkout does not overlap the pinned path.
	independent := OperationCapacityRequest{ID: "independent-runner", Kind: "runner", SessionID: "main", Target: w.Target(), Worktree: t.TempDir()}
	if _, err := k.ReserveOperationCapacity(ctx, independent); err != nil {
		t.Fatal("unrelated workspace was serialized", err)
	}
	releaseOperation(t, k, independent.ID, "process_stopped")
	releaseOperation(t, k, request.ID, "process_stopped")
	if err := k.ReserveSessionThread(ctx, b); err != nil {
		t.Fatal("verified preparation stop did not release destination", err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, current.ID, current.PreparationID); err != nil {
		t.Fatal(err)
	}
	runner.ID = "after-creation"
	runner.Worktree = child
	if _, err := k.ReserveOperationCapacity(ctx, runner); err != nil {
		t.Fatal("completed preparation retained destination lock", err)
	}
}
func TestWorkspacePreparationRejectsLiveProcessAndRunnerInUncreatedSubtree(t *testing.T) {
	for _, kind := range []string{"process", "runner"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, w, _ := operationFixture(t)
			root := t.TempDir()
			destination := filepath.Join(root, "future-worktree")
			child := filepath.Join(destination, "future-child")
			if kind == "process" {
				b := capacityMain(t, k, "live-workspace-user", root)
				reserveSessionFixture(t, k, b)
			} else {
				running := OperationCapacityRequest{ID: "live-child-runner", Kind: "runner", SessionID: "main", Target: w.Target(), Worktree: child}
				if _, err := k.ReserveOperationCapacity(ctx, running); err != nil {
					t.Fatal(err)
				}
			}
			request := OperationCapacityRequest{ID: "overlapping-preparation", Kind: "git", SessionID: "main", Target: w.Target(), Repository: operationRepo(t), Worktree: destination}
			if _, err := k.ReserveOperationCapacity(ctx, request); !errors.Is(err, ErrResourceBusy) {
				t.Fatal("preparation ignored existing destination user", err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_dispatch_intents WHERE run_id=?", request.ID).Scan(&count); err != nil || count != 0 {
				t.Fatal("failed preparation leaked reservation", count, err)
			}
		})
	}
}
func TestWorkspacePreparationDestinationSerializesDifferentRepositories(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	destination := filepath.Join(t.TempDir(), "future")
	requests := []OperationCapacityRequest{{ID: "prepare-a", Kind: "git", SessionID: "main", Target: w.Target(), Repository: operationRepo(t), Worktree: destination}, {ID: "prepare-b", Kind: "git", SessionID: "main", Target: w.Target(), Repository: operationRepo(t), Worktree: destination}}
	errs := make([]error, 2)
	var group sync.WaitGroup
	for i := range requests {
		group.Add(1)
		go func(i int) { defer group.Done(); _, errs[i] = k.ReserveOperationCapacity(ctx, requests[i]) }(i)
	}
	group.Wait()
	winners, busy := 0, 0
	for _, err := range errs {
		if err == nil {
			winners++
		} else if errors.Is(err, ErrResourceBusy) {
			busy++
		} else {
			t.Fatal(err)
		}
	}
	if winners != 1 || busy != 1 {
		t.Fatal("two preparers acquired one destination", errs)
	}
}
func TestWorkspacePreparationLeaseFailureRollsBackAllResources(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	request := OperationCapacityRequest{ID: "rollback-preparation", Kind: "git", SessionID: "main", Target: w.Target(), Repository: operationRepo(t), Worktree: filepath.Join(t.TempDir(), "future")}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_preparation_lease BEFORE INSERT ON kernel_resource_leases WHEN NEW.resource LIKE 'workspace_prepare:%' BEGIN SELECT RAISE(FAIL,'preparation lease rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveOperationCapacity(ctx, request); err == nil || !strings.Contains(err.Error(), "preparation lease rollback") {
		t.Fatal("failure injection not reached", err)
	}
	for _, table := range []string{"kernel_resource_leases", "kernel_dispatch_intents"} {
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("partial preparation committed", table, count, err)
		}
	}
	request.ID = "relative-destination"
	request.Worktree = "relative"
	if _, err := k.ReserveOperationCapacity(ctx, request); err == nil {
		t.Fatal("relative preparation destination accepted")
	}
}

func TestWorkspacePreparationExcludesExistingGitOperationBelowDestination(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	usedRepo := operationRepo(t)
	existing := OperationCapacityRequest{ID: "existing-git-user", Kind: "git", SessionID: "main", Target: w.Target(), Repository: usedRepo}
	if _, err := k.ReserveOperationCapacity(ctx, existing); err != nil {
		t.Fatal(err)
	}
	preparation := OperationCapacityRequest{ID: "prepare-containing-directory", Kind: "git", SessionID: "main", Target: w.Target(), Repository: operationRepo(t), Worktree: filepath.Dir(usedRepo)}
	if _, err := k.ReserveOperationCapacity(ctx, preparation); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("preparation overlapped live Git directory", err)
	}
}

func TestWorkspacePreparationPinsDanglingSymlinkTargetsAndRejectsAliasBypass(t *testing.T) {
	for _, mode := range []string{"absolute", "relative", "parent_alias", "link_dotdot"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, _ := operationFixture(t)
			root := t.TempDir()
			target := filepath.Join(root, "future")
			alias := filepath.Join(root, "alias")
			linkTarget := target
			switch mode {
			case "relative":
				linkTarget = "future"
			case "parent_alias":
				target = filepath.Join(root, "missing-parent", "future")
				if err := os.Symlink(filepath.Join(root, "missing-parent"), alias); err != nil {
					t.Fatal(err)
				}
				alias = filepath.Join(alias, "future")
			case "link_dotdot":
				actual := filepath.Join(root, "actual", "nested")
				if err := os.MkdirAll(actual, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(actual, filepath.Join(root, "jump")); err != nil {
					t.Fatal(err)
				}
				linkTarget = "jump/../future"
				target = filepath.Join(root, "actual", "future")
			}
			if mode != "parent_alias" {
				if err := os.Symlink(linkTarget, alias); err != nil {
					t.Fatal(err)
				}
			}
			resolved, err := resourcePath(alias)
			if err != nil || resolved != target {
				t.Fatal("dangling symlink lost intended target", mode, resolved, target, err)
			}
			first := OperationCapacityRequest{ID: "prepare-dangling-alias", Kind: "git", SessionID: "main", Target: w.Target(), Repository: operationRepo(t), Worktree: alias}
			saved, err := k.ReserveOperationCapacity(ctx, first)
			if err != nil {
				t.Fatal(err)
			}
			pinned := false
			for _, lease := range saved.Resources {
				pinned = pinned || lease.Resource == workspacePreparationPrefix+target
			}
			if !pinned {
				t.Fatal("did not pin resolved destination", saved.Resources, target)
			}
			second := first
			second.ID = "prepare-direct-target"
			second.Repository = operationRepo(t)
			second.Worktree = target
			if _, err := k.ReserveOperationCapacity(ctx, second); !errors.Is(err, ErrResourceBusy) {
				t.Fatal("dangling alias permitted two preparations", err)
			}
			runner := OperationCapacityRequest{ID: "runner-at-dangling-target", Kind: "runner", SessionID: "main", Target: w.Target(), Worktree: target}
			if _, err := k.ReserveOperationCapacity(ctx, runner); !errors.Is(err, ErrResourceBusy) {
				t.Fatal("runner bypassed dangling alias", err)
			}
		})
	}
}
func TestWorkspacePreparationPathResolutionRejectsSymlinkCyclesAndNonDirectories(t *testing.T) {
	root := t.TempDir()
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	if err := os.Symlink(b, a); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(a, b); err != nil {
		t.Fatal(err)
	}
	if _, err := resourcePath(a); err == nil {
		t.Fatal("symlink cycle accepted")
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := resourcePath(file + "/../future"); err == nil {
		t.Fatal("non-directory traversal accepted")
	}
}
