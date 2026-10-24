package product

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func TestProductBasePlannerPinsActualTaskAndBookendsDefaultHead(t *testing.T) {
	for _, mode := range []string{"success", "same_head", "repository", "retarget", "moved", "bad_sha", "tag", "wrong_ref", "diverged", "wrong_base", "wrong_ancestor", "read_error", "oversize", "credentials", "gate_initial", "gate_final", "pause", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			preparer, request, w, task := workspacePreparerFixture(t)
			repo := executorRepositoryFixture(request)
			api := &finalReadFixture{repository: Repository{ID: repo.Policy.RepositoryID, Name: repo.Name, DefaultBranch: repo.BaseBranch, CloneURL: "https://github.com/owner/repo.git"}, head: strings.Repeat("f", 40), merge: repo.InitialSHA, mode: mode}
			if mode == "same_head" {
				api.head = repo.InitialSHA
			}
			if mode == "credentials" {
				api.repository.CloneURL = "https://user:secret@example.invalid/repo.git"
			}
			gates := 0
			blocked := errors.New("fixture current source held")
			planner := BasePlanner{Executor: Executor{Store: preparer.Store, Repositories: map[int64]ExecutionRepository{repo.Policy.RepositoryID: repo}}, API: func(p storage.ProductTarget) ResourceAPI {
				if p.Target != task.Target() || p.Repository != repo.Name || p.WorkItem.RepositoryID != repo.Policy.RepositoryID {
					t.Fatal("transport used caller-selected scope", p)
				}
				return api
			}, Source: func(ctx context.Context, p storage.ProductTargetSnapshot) error {
				gates++
				if p.Value.Target != task.Target() {
					t.Fatal("source target changed", p)
				}
				if mode == "gate_initial" || mode == "gate_final" && gates == 2 {
					return blocked
				}
				if mode == "pause" && gates == 2 {
					_, err := preparer.Store.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause-base-reader", Target: w.Target(), Action: "pause", Reason: "fixture"})
					return err
				}
				return ctx.Err()
			}}
			call := ctx
			if mode == "cancel" {
				var stop context.CancelFunc
				call, stop = context.WithCancel(ctx)
				stop()
			}
			plan, err := planner.Prepare(call, task.ID)
			if mode == "success" || mode == "same_head" {
				if err != nil || plan.Value.Head.HeadSHA != api.head || plan.Value.Input.Value.Preparation.Task.ID != task.ID || gates != 2 {
					t.Fatal(plan, gates, err)
				}
				first := plan
				replay, err := planner.Prepare(ctx, task.ID)
				if err != nil || !reflect.DeepEqual(replay, first) {
					t.Fatal("fresh bookend rewrote semantic plan", replay, err)
				}
				current, err := preparer.Store.CurrentProductBasePlan(ctx, task.ID)
				if err != nil || current.ID != plan.ID {
					t.Fatal(current, err)
				}
				expected := []string{"", "git/ref/heads/" + repo.BaseBranch}
				if mode != "same_head" {
					expected = append(expected, "compare/"+repo.InitialSHA+"..."+api.head+"?per_page=1")
				}
				expected = append(expected, "git/ref/heads/"+repo.BaseBranch, "")
				if !reflect.DeepEqual(api.paths[:len(expected)], expected) {
					t.Fatal("missing repository/ref/ancestry bookends", api.paths)
				}
			} else if err == nil {
				t.Fatal("unproven base admitted", mode, plan)
			}
			if mode == "gate_initial" && len(api.paths) != 0 {
				t.Fatal("held source still read remote", api.paths)
			}
			head, readErr := workspace.Resolve(ctx, request.Repository, "HEAD")
			if readErr != nil || head != repo.InitialSHA {
				t.Fatal("base observation changed repository", head, readErr)
			}
			var effects int
			if err := preparer.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_workspace','product_runner_job','product_seal')").Scan(&effects); err != nil || effects != 0 {
				t.Fatal("plan started Git/model/runner", effects, err)
			}
		})
	}
}
