package storage

import (
	"context"
	"testing"
)

func TestProductExecutionQueuesPageTasksAndRetainPausedRunnerOwnership(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime := productRunnerFixture(t)
	seen := map[string]bool{}
	for cursor := ""; ; {
		page, err := k.ProductExecutionTasks(ctx, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range page.IDs {
			if seen[id] {
				t.Fatal("duplicate Task page", id)
			}
			seen[id] = true
		}
		if page.NextAfter == "" {
			break
		}
		if page.NextAfter <= cursor {
			t.Fatal("cursor did not advance")
		}
		cursor = page.NextAfter
	}
	if !seen[plan.Value.TaskID] || len(seen) < 2 {
		t.Fatal("current product Tasks missing", seen)
	}
	job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil {
		t.Fatal(err)
	}
	w.Paused = true
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveWave(ctx, tx, w); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductVerificationEvidence(ctx, plan.ID); err == nil {
		t.Fatal("pause did not fence readiness")
	}
	running, err := k.ProductRunningJobs(ctx, "", 1)
	if err != nil || len(running.IDs) != 1 || running.IDs[0] != job.ID {
		t.Fatal("pause hid physical owner", running, err)
	}
	if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, productRunnerCompletionFixture(job)); err != nil {
		t.Fatal(err)
	}
	stopped, err := k.ProductRunningJobs(ctx, "", 1)
	if err != nil || len(stopped.IDs) != 0 {
		t.Fatal("stopped owner remained queued", stopped, err)
	}
}
