package storage

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func finishProductRunnerRetryFixture(t *testing.T, k *KernelStore, job ProductRunnerJob, status string) ProductRunnerJob {
	t.Helper()
	ctx := context.Background()
	claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest)
	if err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	completion := productRunnerCompletionFixture(job)
	completion.Result.Status = status
	finished, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, completion)
	if err != nil {
		t.Fatal(err)
	}
	return finished
}

func TestProductRunnerRetryPreservesInputsHistoryAndBudget(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime := productRunnerFixture(t)
	first := completeProductRunnerFixture(t, k, plan, runtime, "unit", "infrastructure_failure")
	retry, err := k.ReserveProductRunnerRetry(ctx, first.ID)
	if err != nil || retry.ID == first.ID || retry.Value.Spec.JobID != retry.ID || !productRunnerRetryValueMatches(first, retry.Value) {
		t.Fatal(retry, err)
	}
	firstLease, err := k.OperationReservation(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	retryLease, err := k.OperationReservation(ctx, retry.ID)
	if err != nil || retryLease.Request.Worktree == firstLease.Request.Worktree || retryLease.Request.RetryOf != first.ID {
		t.Fatal("retry reused files or lost lineage", retryLease, err)
	}
	usage, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || usage.ValidationJobs != 2 || usage.RunnerRetries != 1 {
		t.Fatal(usage, err)
	}
	again, err := k.ReserveProductRunnerRetry(ctx, first.ID)
	if err != nil || !reflect.DeepEqual(again, retry) {
		t.Fatal(again, err)
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(after, usage) {
		t.Fatal("replay spent budget", after, err)
	}
	pending, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || pending.Value.Complete || pending.Value.Passed || pending.Value.Jobs[0].JobID != retry.ID || pending.Value.Jobs[0].Completion != nil {
		t.Fatal("old completion replaced pending retry", pending, err)
	}
	finished := finishProductRunnerRetryFixture(t, k, retry, "passed")
	if claim, err := k.ClaimProductRunnerJob(ctx, retry.ID, retry.Digest); err != nil || claim.Start {
		t.Fatal(claim, err)
	}
	passed, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || !passed.Value.Complete || !passed.Value.Passed || passed.Value.Jobs[0].JobID != finished.ID {
		t.Fatal(passed, err)
	}
	old, err := k.ReadProductRunnerJob(ctx, first.ID)
	if err != nil || !reflect.DeepEqual(old, first) {
		t.Fatal("retry rewrote old completion", old, err)
	}
}

func TestProductRunnerRetryRejectsNonInfrastructureAndUnknown(t *testing.T) {
	for _, status := range []string{"prepared", "launching", "passed", "failed", "timeout", "canceled"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, runtime := productRunnerFixture(t)
			job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
			if err != nil {
				t.Fatal(err)
			}
			if status == "launching" {
				if _, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil {
					t.Fatal(err)
				}
			} else if status != "prepared" {
				finishProductRunnerRetryFixture(t, k, job, status)
			}
			before, err := k.WaveOperationBudget(ctx, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReserveProductRunnerRetry(ctx, job.ID); !errors.Is(err, ErrProductRunnerHeld) {
				t.Fatal("invalid retry", err)
			}
			after, err := k.WaveOperationBudget(ctx, w.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("refusal spent budget", after, err)
			}
		})
	}
}

func TestProductRunnerRetryLimitsAndCurrentControls(t *testing.T) {
	for _, mode := range []string{"retry_limit", "job_limit", "paused", "different_workspace", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, runtime := productRunnerFixture(t)
			first := completeProductRunnerFixture(t, k, plan, runtime, "unit", "infrastructure_failure")
			switch mode {
			case "retry_limit":
				retry, err := k.ReserveProductRunnerRetry(ctx, first.ID)
				if err != nil {
					t.Fatal(err)
				}
				first = finishProductRunnerRetryFixture(t, k, retry, "infrastructure_failure")
			case "job_limit":
				limits := DefaultWaveOperationLimits()
				limits.ValidationJobs = 1
				if err := k.Put(ctx, "wave_operation_limits", w.ID, w.ID, limits); err != nil {
					t.Fatal(err)
				}
			case "paused":
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
			case "different_workspace":
				old, err := k.OperationReservation(ctx, first.ID)
				if err != nil {
					t.Fatal(err)
				}
				request := old.Request
				request.ID, request.RetryOf, request.Worktree = "arbitrary-retry", first.ID, filepath.Join(runtime.Root, "unrelated")
				if _, err := k.ReserveOperationCapacity(ctx, request); err == nil {
					t.Fatal("arbitrary workspace bypassed retry provenance")
				}
				return
			case "rollback":
				if _, err := k.DB.Exec("CREATE TRIGGER reject_product_retry BEFORE INSERT ON records WHEN NEW.kind='product_runner_job' BEGIN SELECT RAISE(FAIL,'retry rollback'); END"); err != nil {
					t.Fatal(err)
				}
			}
			before, err := k.WaveOperationBudget(ctx, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReserveProductRunnerRetry(ctx, first.ID); err == nil {
				t.Fatal("held retry admitted")
			} else if mode == "rollback" && !strings.Contains(err.Error(), "retry rollback") {
				t.Fatal(err)
			}
			after, err := k.WaveOperationBudget(ctx, w.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("failed admission spent budget", after, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_dispatch_intents WHERE run_id=?", productRunnerRetryID(first.ID)).Scan(&count); err != nil || count != 0 {
				t.Fatal("failed admission retained capacity", count, err)
			}
		})
	}
}

func TestProductRunnerRetryLostEvidenceCannotFallBack(t *testing.T) {
	for _, kind := range []string{"product_runner_job", "product_runner_job_history", "product_runner_job_terminal"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, _, plan, runtime := productRunnerFixture(t)
			first := completeProductRunnerFixture(t, k, plan, runtime, "unit", "infrastructure_failure")
			retry, err := k.ReserveProductRunnerRetry(ctx, first.ID)
			if err != nil {
				t.Fatal(err)
			}
			finishProductRunnerRetryFixture(t, k, retry, "passed")
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, retry.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductVerificationEvidence(ctx, plan.ID); err == nil {
				t.Fatal("lost retry evidence fell back to old job")
			}
			if _, err := k.ReserveProductRunnerRetry(ctx, first.ID); err == nil {
				t.Fatal("lost retry evidence regenerated admission")
			}
		})
	}
}

func TestProductRunnerRetryFollowsTwoAttemptsAndRechecksBeforeClaim(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime := productRunnerFixture(t)
	limits := DefaultWaveOperationLimits()
	limits.RunnerRetries = 2
	if err := k.Put(ctx, "wave_operation_limits", w.ID, w.ID, limits); err != nil {
		t.Fatal(err)
	}
	first := completeProductRunnerFixture(t, k, plan, runtime, "unit", "infrastructure_failure")
	second, err := k.ReserveProductRunnerRetry(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	second = finishProductRunnerRetryFixture(t, k, second, "infrastructure_failure")
	third, err := k.ReserveProductRunnerRetry(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || evidence.Value.Complete || evidence.Value.Jobs[0].JobID != third.ID {
		t.Fatal(evidence, err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	p.Value.WorkItem.Paused = true
	if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
		t.Fatal(err)
	}
	if claim, err := k.ClaimProductRunnerJob(ctx, third.ID, third.Digest); err == nil || claim.Start {
		t.Fatal("retry ignored pause before launch", claim, err)
	}
	job, err := k.ReadProductRunnerJob(ctx, third.ID)
	if err != nil || job.State != "prepared" {
		t.Fatal("rejected claim changed job", job, err)
	}
}
