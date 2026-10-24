package product

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func TestProductBaseWorkspacePreparerFetchesAndChecksOutExactObservedCommit(t *testing.T) {
	for _, mode := range []string{"already_local", "new_remote", "source_hold_before_send", "head_moves_after_fetch", "unknown_stop"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			preparer, oldRequest, _, task := workspacePreparerFixture(t)
			repo := executorRepositoryFixture(oldRequest)
			remote := t.TempDir()
			if _, err := workspace.Git(ctx, oldRequest.Repository, "clone", "--no-local", "--", oldRequest.Repository, remote); err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{{"config", "user.name", "base fixture"}, {"config", "user.email", "base@example.invalid"}} {
				if _, err := workspace.Git(ctx, remote, args...); err != nil {
					t.Fatal(err)
				}
			}
			sha := repo.InitialSHA
			if mode != "already_local" {
				if err := os.WriteFile(filepath.Join(remote, "predecessor.txt"), []byte("merged predecessor behavior\n"), 0600); err != nil {
					t.Fatal(err)
				}
				var err error
				sha, err = workspace.Freeze(workspace.WithStoppedCommands(ctx), remote, "actual new product base")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := workspace.Resolve(ctx, oldRequest.Repository, sha); err == nil {
					t.Fatal("new SHA was already local")
				}
			}
			api := &finalReadFixture{repository: Repository{ID: 17, Name: repo.Name, DefaultBranch: repo.BaseBranch, CloneURL: remote}, head: sha, merge: repo.InitialSHA}
			sourceCalls := 0
			blocked := errors.New("fixture source unavailable immediately before send")
			planner := BasePlanner{Executor: Executor{Store: preparer.Store, Repositories: map[int64]ExecutionRepository{17: repo}}, API: func(storage.ProductTarget) ResourceAPI { return api }, Source: func(context.Context, storage.ProductTargetSnapshot) error {
				sourceCalls++
				if mode == "unknown_stop" && sourceCalls == 7 {
					return errors.Join(context.Canceled, workspace.ErrGitStopUnconfirmed)
				}
				if mode == "source_hold_before_send" && sourceCalls == 7 {
					return blocked
				}
				return nil
			}}
			plan, err := planner.Prepare(ctx, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			request := storage.ProductWorkspaceRequest{TaskID: task.ID, RepositoryID: 17, Repository: oldRequest.Repository, BaseSHA: sha, BasePlanID: plan.ID, BasePlanDigest: plan.Digest}
			refreshCalls := 0
			preparer.RefreshBase = func(ctx context.Context, id string) (storage.ProductBasePlan, error) {
				refreshCalls++
				if mode == "head_moves_after_fetch" && refreshCalls == 4 {
					if err := os.WriteFile(filepath.Join(remote, "later-base.txt"), []byte("another concurrent merge\n"), 0600); err != nil {
						return storage.ProductBasePlan{}, err
					}
					next, err := workspace.Freeze(workspace.WithStoppedCommands(ctx), remote, "actual concurrent base advance")
					if err != nil {
						return storage.ProductBasePlan{}, err
					}
					api.head = next
				}
				return planner.Prepare(ctx, id)
			}
			ready, err := preparer.Prepare(ctx, request)
			if mode == "unknown_stop" {
				if !errors.Is(err, workspace.ErrGitStopUnconfirmed) || ready.State != "launching" || ready.Proof != nil || ready.BaseFetchAt != nil {
					t.Fatal("unknown stop published terminal readiness", ready, err)
				}
			} else if mode == "source_hold_before_send" || mode == "head_moves_after_fetch" {
				if err == nil || ready.State != "failed" || ready.Proof == nil || ready.Proof.Fetch == nil {
					t.Fatal("changed source acquired workspace", ready, err)
				}
				if mode == "source_hold_before_send" && (!errors.Is(err, blocked) || ready.Proof.Fetch.Attempted || ready.BaseFetchAt != nil) {
					t.Fatal("held source sent commit fetch", ready, err)
				}
				if mode == "head_moves_after_fetch" && (!ready.Proof.Fetch.Attempted || !ready.Proof.Fetch.Available || ready.BaseFetchAt == nil) {
					t.Fatal("lost historical fetch after HEAD changed", ready, err)
				}
				if _, err := os.Stat(ready.Value.CWD); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("held source created checkout", err)
				}
			} else {
				if err != nil || ready.State != "ready" || ready.Proof == nil || ready.Proof.Fetch == nil || !ready.Proof.Fetch.Available || ready.Proof.HeadSHA != sha {
					t.Fatal("fixed observed base not prepared", ready, err)
				}
				if (mode == "new_remote") != ready.Proof.Fetch.Attempted || (mode == "new_remote") != (ready.BaseFetchAt != nil) {
					t.Fatal("fetch fence did not match actual import", ready)
				}
				head, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD")
				if err != nil || head != sha {
					t.Fatal("checkout used old static initial SHA", head, err)
				}
				if err := workspace.Clean(ctx, ready.Value.CWD); err != nil {
					t.Fatal(err)
				}
				if mode == "new_remote" {
					raw, err := os.ReadFile(filepath.Join(ready.Value.CWD, "predecessor.txt"))
					if err != nil || string(raw) != "merged predecessor behavior\n" {
						t.Fatal(string(raw), err)
					}
				}
			}
			head, err := workspace.Resolve(ctx, oldRequest.Repository, "HEAD")
			if err != nil || head != repo.InitialSHA {
				t.Fatal("base preparation moved host checkout", head, err)
			}
			before := refreshCalls
			replay, replayErr := preparer.Prepare(ctx, request)
			if mode == "already_local" || mode == "new_remote" {
				if replayErr != nil || !reflect.DeepEqual(replay, ready) {
					t.Fatal("ready replay repeated physical work", replay, replayErr)
				}
			} else if !errors.Is(replayErr, storage.ErrProductWorkspaceHeld) {
				t.Fatal("failed preparation implicitly retried", replayErr)
			}
			if refreshCalls != before {
				t.Fatal("settled/unknown preparation refreshed instead of replaying", refreshCalls, before)
			}
			r, err := preparer.Store.OperationReservation(ctx, ready.ID)
			if mode == "unknown_stop" {
				if err != nil || r.State != "held" || r.Proof != nil {
					t.Fatal("unknown stop released capacity", r, err)
				}
				return
			}
			if err != nil || r.State != "released" || r.Proof == nil {
				t.Fatal("known stopped owner retained capacity", r, err)
			}
			var runs int
			if err := preparer.Store.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
				t.Fatal("host base preparation started model", runs, err)
			}
		})
	}
}
