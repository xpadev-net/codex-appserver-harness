package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestProductTaskPreparationReviewRequiresCurrentCompleteMachineEvidence(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime, _, _ := productQualityFixture(t, func(plan *contract.PlanProposal) {
		phase := &plan.Phases[len(plan.Phases)-1]
		review := phase.Tasks[0]
		review.ID = "host-review"
		phase.Tasks = append(phase.Tasks, review)
		phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, review.ID)
	})
	id := structureID("task-", w.ID, "host-review")
	if _, err := k.ReadProductTaskPreparation(ctx, id); !errors.Is(err, ErrProductRunnerHeld) {
		t.Fatal("review prepared before machine result", err)
	}
	completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
	prepared, err := k.ReadProductTaskPreparation(ctx, id)
	if err != nil || prepared.Task.ID != id || prepared.Product == nil || prepared.Product.Value.Candidate != plan.Value.Candidate {
		t.Fatal(prepared, err)
	}
	product, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	product.Value.WorkItem.Paused = true
	if _, err := k.SetProductTarget(ctx, product.Digest, product.Value); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, id); err == nil {
		t.Fatal("paused WorkItem prepared")
	}
}

func TestProductTaskPreparationReadHasNoExecutionEffects(t *testing.T) {
	ctx := context.Background()
	k, _, task, _ := productWorkspaceFixture(t)
	before, err := k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	after, err := k.Task(ctx, task.ID)
	if err != nil || before.InputDigest != after.InputDigest || before.Attempts != after.Attempts {
		t.Fatal(after, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_dispatch_intents").Scan(&count); err != nil || count != 0 {
		t.Fatal("read reserved execution", count, err)
	}
}
