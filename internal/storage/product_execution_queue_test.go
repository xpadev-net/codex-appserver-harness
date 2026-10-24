package storage

import (
	"context"
	"fmt"
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

func TestProductExecutionPageBoundariesAndFreshScan(t *testing.T) {
	for _, count := range []int{0, 1, 128, 129, 256, 257} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ctx := context.Background()
			k, _ := waveStoreFixture(t)
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("queue-%04d", i)
				if err := kernelRecordTx(ctx, tx, "test_execution_queue", id, "", map[string]string{"id": id}); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			query := `SELECT id FROM records WHERE kind='test_execution_queue' AND id>? ORDER BY id LIMIT ?`
			for scan := 0; scan < 2; scan++ {
				seen := map[string]bool{}
				cursor, pages := "", 0
				for {
					page, err := k.productExecutionPage(ctx, cursor, 128, query)
					if err != nil || len(page.IDs) > 128 {
						t.Fatal(page, err)
					}
					pages++
					for _, id := range page.IDs {
						if seen[id] {
							t.Fatal("page repeated an ID", id)
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
				if len(seen) != count || pages != max(1, (count+127)/128) {
					t.Fatal("scan lost/repeated a page", scan, len(seen), pages)
				}
			}
		})
	}
}
