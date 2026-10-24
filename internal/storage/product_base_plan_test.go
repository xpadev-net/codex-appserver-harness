package storage

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func productBasePlanFixture(t *testing.T) (*KernelStore, ProductBaseInput, ProductBaseObservation) {
	t.Helper()
	k, w, task, request := productWorkspaceFixture(t)
	if _, err := k.RegisterProductVerificationPolicy(context.Background(), w.Target(), verificationPolicyFixture(w.ID, request.Repository, w.Policy.Revision)); err != nil {
		t.Fatal(err)
	}
	input, err := k.ReadProductBaseInput(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	ancestors, err := input.RequiredAncestors(request.BaseSHA)
	if err != nil {
		t.Fatal(err)
	}
	return k, input, ProductBaseObservation{Head: ProductBaseHead{RepositoryID: 17, Repository: "owner/repo", BaseBranch: "master", InitialSHA: request.BaseSHA, HeadSHA: strings.Repeat("f", 40), Remote: "https://github.com/owner/repo.git", Ancestors: ancestors}, ObservedAt: k.clock()}
}
func TestProductBasePlanKeepsSemanticHistoryAndRequiresFreshCurrentSource(t *testing.T) {
	ctx := context.Background()
	k, input, observation := productBasePlanFixture(t)
	taskID := input.Value.Preparation.Task.ID
	now := observation.ObservedAt
	k.Now = func() time.Time { return now }
	plan, err := k.PrepareProductBasePlan(ctx, taskID, input.Digest, observation)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		now = now.Add(time.Second)
		observation.ObservedAt = now
		replay, err := k.PrepareProductBasePlan(ctx, taskID, input.Digest, observation)
		if err != nil || !reflect.DeepEqual(replay, plan) {
			t.Fatal("same HEAD rewrote original plan", replay, err)
		}
	}
	current, err := k.CurrentProductBasePlan(ctx, taskID)
	if err != nil || current.ID != plan.ID {
		t.Fatal(current, err)
	}
	now = now.Add(time.Second)
	observation.ObservedAt = now
	observation.Head.HeadSHA = strings.Repeat("e", 40)
	next, err := k.PrepareProductBasePlan(ctx, taskID, input.Digest, observation)
	if err != nil || next.ID == plan.ID {
		t.Fatal("new HEAD relabeled old plan", next, err)
	}
	old, err := k.ReadProductBasePlan(ctx, plan.ID)
	if err != nil || !reflect.DeepEqual(old, plan) {
		t.Fatal(old, err)
	}
	now = now.Add(31 * time.Second)
	if _, err := k.CurrentProductBasePlan(ctx, taskID); !errors.Is(err, ErrProductBasePlanHeld) {
		t.Fatal("stale observation admitted effect", err)
	}
	observation.ObservedAt = now
	refreshed, err := k.PrepareProductBasePlan(ctx, taskID, input.Digest, observation)
	if err != nil || !reflect.DeepEqual(refreshed, next) {
		t.Fatal(refreshed, err)
	}
	target := input.Value.Preparation.Task.Target()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-base", Target: target, Action: "pause", Reason: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.CurrentProductBasePlan(ctx, taskID); err == nil {
		t.Fatal("paused Task acquired current base")
	}
	if old, err := k.ReadProductBasePlan(ctx, plan.ID); err != nil || !reflect.DeepEqual(old, plan) {
		t.Fatal("pause destroyed history", err)
	}
	root := k.Root
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	k.Now = func() time.Time { return now }
	reopened, err := k.ReadProductBasePlan(ctx, plan.ID)
	if err != nil || !reflect.DeepEqual(reopened, plan) {
		t.Fatal("reopen changed immutable plan", reopened, err)
	}
	if _, err := k.CurrentProductBasePlan(ctx, taskID); err == nil {
		t.Fatal("reopen discarded pause/source fences")
	}
	var effects int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_workspace','product_runner_job','product_seal')").Scan(&effects); err != nil || effects != 0 {
		t.Fatal("plan launched work", effects, err)
	}
}
func TestProductBasePlanMissingJournalNeverRegenerates(t *testing.T) {
	for _, mode := range []string{"plan", "history", "admission", "current", "selection_history", "all_selection"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, input, observation := productBasePlanFixture(t)
			taskID := input.Value.Preparation.Task.ID
			plan, err := k.PrepareProductBasePlan(ctx, taskID, input.Digest, observation)
			if err != nil {
				t.Fatal(err)
			}
			kind := map[string]string{"plan": "product_base_plan", "history": "product_base_plan_history", "admission": "product_base_plan_admission", "current": "product_base_current", "selection_history": "product_base_selection_history"}[mode]
			id := plan.ID
			if mode == "current" {
				id = taskID
			}
			if mode == "all_selection" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_base_current','product_base_selection_history')"); err != nil {
					t.Fatal(err)
				}
			} else if mode == "selection_history" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind=?", kind); err != nil {
					t.Fatal(err)
				}
			} else if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.CurrentProductBasePlan(ctx, taskID); err == nil {
				t.Fatal("lost journal admitted effect", mode)
			}
			observation.Head.HeadSHA = strings.Repeat("e", 40)
			if _, err := k.PrepareProductBasePlan(ctx, taskID, input.Digest, observation); err == nil {
				t.Fatal("reconstructed lost authority", mode)
			}
		})
	}
}
func TestProductBasePlanRejectsWrongBookendsAndRollsBackSelection(t *testing.T) {
	for _, mode := range []string{"input", "repository", "ancestors", "ref", "remote", "stale", "future", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, input, obs := productBasePlanFixture(t)
			digest := input.Digest
			switch mode {
			case "input":
				digest = strings.Repeat("a", 64)
			case "repository":
				obs.Head.RepositoryID++
			case "ancestors":
				obs.Head.Ancestors = nil
			case "ref":
				obs.Head.HeadSHA = "master"
			case "remote":
				obs.Head.Remote = "https://user:secret@example.invalid/repo.git"
			case "stale":
				obs.ObservedAt = obs.ObservedAt.Add(-31 * time.Second)
			case "future":
				obs.ObservedAt = obs.ObservedAt.Add(time.Hour)
			case "rollback":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_base_selection BEFORE INSERT ON records WHEN NEW.kind='product_base_current' BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
					t.Fatal(err)
				}
			}
			taskID := input.Value.Preparation.Task.ID
			if _, err := k.PrepareProductBasePlan(ctx, taskID, digest, obs); err == nil {
				t.Fatal("invalid observation admitted", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_base_%'").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial plan survived", count, err)
			}
			if mode == "rollback" {
				if _, err := k.DB.Exec("DROP TRIGGER fail_base_selection"); err != nil {
					t.Fatal(err)
				}
				if _, err := k.PrepareProductBasePlan(ctx, taskID, digest, obs); err != nil {
					t.Fatal("rollback consumed first admission", err)
				}
			}
		})
	}
}
func TestProductBaseInputKeepsAuthenticatedPredecessorIdentity(t *testing.T) {
	ctx := context.Background()
	k, _, task, job := productMergeSuccessorFixture(t, true)
	input, err := k.ReadProductBaseInput(ctx, task.ID)
	if err != nil || len(input.Value.Dependencies) != 1 {
		t.Fatal(input, err)
	}
	dep := input.Value.Dependencies[0]
	if dep.Fact.MergeID != job.ID || dep.RepositoryID != job.Value.Product.Value.WorkItem.RepositoryID {
		t.Fatal("dependency identity lost", dep)
	}
	seed := job.Value.Product.Value.Candidate.BaseSHA
	ancestors, err := input.RequiredAncestors(seed)
	if err != nil || !slices.Contains(ancestors, dep.Fact.MergeSHA) || !slices.Contains(ancestors, seed) {
		t.Fatal(ancestors, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merged_work_item_history' AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductBaseInput(ctx, task.ID); err == nil {
		t.Fatal("lost predecessor proof became base input")
	}
}
