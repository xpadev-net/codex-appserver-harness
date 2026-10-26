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

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func TestExecutionWorkerSelectsActualBaseRefreshesThenNotifiesChangedHead(t *testing.T) {
	ctx := context.Background()
	preparer, request, w, task := workspacePreparerFixture(t)
	k := preparer.Store
	repo := executorRepositoryFixture(request)
	remote := t.TempDir()
	if _, err := workspace.Git(ctx, request.Repository, "clone", "--no-local", "--", request.Repository, remote); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"config", "user.name", "executor fixture"}, {"config", "user.email", "executor@example.invalid"}} {
		if _, err := workspace.Git(ctx, remote, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(remote, "merged-base.txt"), []byte("actual successor base\n"), 0600); err != nil {
		t.Fatal(err)
	}
	head, err := workspace.Freeze(workspace.WithStoppedCommands(ctx), remote, "merged predecessor base")
	if err != nil {
		t.Fatal(err)
	}
	api := &finalReadFixture{repository: Repository{ID: 17, Name: repo.Name, DefaultBranch: repo.BaseBranch, CloneURL: remote}, head: head, merge: repo.InitialSHA}
	e := Executor{Store: k, Repositories: map[int64]ExecutionRepository{17: repo}, Runner: Runner{Store: k}}
	e.BasePlanner = &BasePlanner{Executor: e, Source: func(context.Context, storage.ProductTargetSnapshot) error { return nil }, API: func(storage.ProductTarget) ResourceAPI { return api }}
	worker := NewExecutionWorker(e, time.Second)
	changed, err := worker.task(ctx, task.ID)
	if err != nil || !changed {
		t.Fatal("daemon did not prepare observed base", changed, err)
	}
	ready, err := k.ProductWorkspace(ctx, task.ID)
	if err != nil || ready.State != "ready" || ready.Value.Request.BaseSHA != head || ready.Value.Request.BasePlanID == "" {
		t.Fatal(ready, err)
	}
	before := len(api.paths)
	if changed, err := worker.task(ctx, task.ID); err != nil || changed || len(api.paths) != before {
		t.Fatal("fresh ready workspace polled remote/repeated Git", changed, err, len(api.paths), before)
	}
	now := ready.FinishedAt.Add(31 * time.Second)
	k.Now = func() time.Time { return now }
	if changed, err := worker.task(ctx, task.ID); err != nil || !changed {
		t.Fatal("expired observation did not refresh", changed, err)
	}
	refreshed, err := k.ProductWorkspace(ctx, task.ID)
	if err != nil || !reflect.DeepEqual(refreshed, ready) {
		t.Fatal("read refresh rewrote prepared workspace", refreshed, err)
	}
	if _, err := k.CurrentProductBasePlan(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(remote, "later-merge.txt"), []byte("concurrent default branch advance\n"), 0600); err != nil {
		t.Fatal(err)
	}
	next, err := workspace.Freeze(workspace.WithStoppedCommands(ctx), remote, "concurrent merged base")
	if err != nil {
		t.Fatal(err)
	}
	api.head = next
	now = now.Add(31 * time.Second)
	if changed, err := worker.task(ctx, task.ID); !errors.Is(err, storage.ErrProductWorkspaceHeld) || !changed {
		t.Fatal("changed base did not hold and notify", changed, err)
	}
	for i := 0; i < 2; i++ {
		if changed, err := worker.task(ctx, task.ID); !errors.Is(err, storage.ErrProductWorkspaceHeld) || changed {
			t.Fatal("changed base repeatedly woke parent", changed, err)
		}
	}
	if _, err := workspace.Resolve(ctx, request.Repository, next); err == nil {
		t.Fatal("changed head silently imported/rewrote ready workspace")
	}
	same, err := k.ProductWorkspace(ctx, task.ID)
	if err != nil || !reflect.DeepEqual(same, ready) {
		t.Fatal(same, err)
	}
	if _, err := k.ReconcileNotifications(ctx, 128); err != nil {
		t.Fatal(err)
	}
	notes, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 128)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, n := range notes {
		if n.Reason == "product_base_changed" {
			count++
			if n.Preparation == nil || n.Preparation.CurrentHead != next || n.Preparation.BaseSHA != head {
				t.Fatal("parent lacks safe exact base summary", n)
			}
		}
	}
	if count != 1 {
		t.Fatal("missing/duplicate parent exception", count, notes)
	}
	var runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("host refresh/notification polled model", runs, err)
	}
}

func TestExecutionWorkerNotifiesFailedOrChangedUnstartedPreparationOnce(t *testing.T) {
	for _, mode := range []string{"known_failed", "unstarted_changed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			preparer, request, w, task := workspacePreparerFixture(t)
			k := preparer.Store
			repo := executorRepositoryFixture(request)
			api := &finalReadFixture{repository: Repository{ID: 17, Name: repo.Name, DefaultBranch: repo.BaseBranch, CloneURL: "https://github.com/owner/repo.git"}, head: repo.InitialSHA, merge: repo.InitialSHA}
			calls := 0
			failed := false
			blocked := errors.New("fixture source read failed after claim")
			e := Executor{Store: k, Repositories: map[int64]ExecutionRepository{17: repo}, Runner: Runner{Store: k}}
			e.BasePlanner = &BasePlanner{Executor: e, API: func(storage.ProductTarget) ResourceAPI { return api }, Source: func(context.Context, storage.ProductTargetSnapshot) error {
				calls++
				if mode == "known_failed" && !failed {
					// Exercise an admitted owner failure independently of the
					// number of fresh source reads surrounding preparation.
					if saved, err := k.ProductWorkspace(ctx, task.ID); err == nil && saved.State == "launching" {
						failed = true
						return blocked
					}
				}
				return nil
			}}
			if mode == "unstarted_changed" {
				plan, err := e.BasePlanner.Prepare(ctx, task.ID)
				if err != nil {
					t.Fatal(err)
				}
				request.BasePlanID, request.BasePlanDigest = plan.ID, plan.Digest
				if _, err := k.ReserveProductWorkspace(ctx, request); err != nil {
					t.Fatal(err)
				}
				api.head = strings.Repeat("f", 40)
			}
			worker := NewExecutionWorker(e, time.Second)
			changed, err := worker.task(ctx, task.ID)
			if !changed || mode == "known_failed" && !errors.Is(err, blocked) || mode == "unstarted_changed" && err != nil {
				t.Fatal("exception did not resolve into parent input", mode, changed, err)
			}
			saved, err := k.ProductWorkspace(ctx, task.ID)
			if err != nil || saved.State != "failed" || saved.Proof == nil {
				t.Fatal(saved, err)
			}
			if mode == "unstarted_changed" && saved.Proof.Outcome != "not_started" {
				t.Fatal("unclaimed profile not recorded as unstarted", saved)
			}
			before := calls
			for i := 0; i < 2; i++ {
				if changed, err := worker.task(ctx, task.ID); err != nil || changed || calls != before {
					t.Fatal("settled exception retried or woke model", changed, err, calls, before)
				}
			}
			r, err := k.OperationReservation(ctx, saved.ID)
			if err != nil || r.State != "released" {
				t.Fatal(r, err)
			}
			if _, err := k.ReconcileNotifications(ctx, 128); err != nil {
				t.Fatal(err)
			}
			notes, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 128)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, n := range notes {
				if n.Reason == "product_workspace_failed" {
					count++
					if n.Preparation == nil || n.Preparation.Outcome != saved.Proof.Outcome {
						t.Fatal(n)
					}
				}
			}
			if count != 1 {
				t.Fatal(count, notes)
			}
			var runs int
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
				t.Fatal("exception handler polled model", runs, err)
			}
		})
	}
}
