package storage

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
)

func productRunnerRetryID(previousID string) string {
	return structureID("product-runner-retry-", previousID, "v1")
}

func productRunnerRetryValueMatches(previous ProductRunnerJob, value ProductRunnerJobValue) bool {
	if previous.State != "completed" || previous.Completion == nil || previous.Completion.Result.Status != "infrastructure_failure" {
		return false
	}
	expected := previous.Value
	expected.RetryOf = previous.ID
	expected.Spec.JobID = productRunnerRetryID(previous.ID)
	return reflect.DeepEqual(expected, value)
}

// Generic retries keep their original workspace. Product attempts use distinct
// directories so their spec/log/result files remain immutable. Only a derived
// successor of an authenticated, stopped product job may change that lease.
func productRunnerRetryWorkspaceTx(ctx context.Context, tx *sql.Tx, previous OperationReservation, request OperationCapacityRequest) (bool, error) {
	if previous.Request.Worktree == request.Worktree {
		return true, nil
	}
	job, err := loadProductRunnerJob(ctx, tx, previous.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return job.State == "completed" && job.Completion.Result.Status == "infrastructure_failure" && request.RetryOf == job.ID && request.ID == productRunnerRetryID(job.ID) && request.Worktree == filepath.Join(job.Value.Spec.Root, request.ID), nil
}

// ReserveProductRunnerRetry creates at most one successor per stopped
// infrastructure failure. All execution inputs come from the previous immutable
// job, never a replacement command, Candidate or caller-supplied result.
func (k *KernelStore) ReserveProductRunnerRetry(ctx context.Context, previousID string) (ProductRunnerJob, error) {
	var job ProductRunnerJob
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	previous, err := loadProductRunnerJob(ctx, tx, previousID)
	if err != nil {
		return job, err
	}
	if previous.Value.Final != nil {
		return job, ErrProductRunnerHeld
	}
	value := previous.Value
	value.RetryOf, value.Spec.JobID = previous.ID, productRunnerRetryID(previous.ID)
	if !productRunnerRetryValueMatches(previous, value) {
		return job, ErrProductRunnerHeld
	}
	job, err = loadProductRunnerJob(ctx, tx, value.Spec.JobID)
	if err == nil {
		if !reflect.DeepEqual(job.Value, value) {
			return job, ErrTaskEvidence
		}
		return job, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return job, err
	}
	if admitted, err := productRunnerJobAdmittedTx(ctx, tx, value.Spec.JobID); err != nil || admitted {
		if err != nil {
			return job, err
		}
		return job, ErrTaskEvidence
	}
	plan, err := loadProductVerificationPlan(ctx, tx, value.PlanID)
	if err != nil {
		return job, err
	}
	w, err := k.productRunnerPreflightTx(ctx, tx, plan, false)
	if err != nil {
		return job, err
	}
	spec := value.Spec
	if err := validateProductRunnerRuntime(ProductRunnerRuntime{Root: spec.Root, LauncherContract: spec.LauncherContract, Launcher: spec.Launcher, ForbiddenPaths: spec.ForbiddenPaths}); err != nil {
		return job, err
	}
	reservation, err := k.reserveOperationCapacityTx(ctx, tx, OperationCapacityRequest{ID: spec.JobID, Kind: "runner", SessionID: value.SessionID, Target: w.Target(), Worktree: filepath.Join(spec.Root, spec.JobID), RetryOf: previous.ID})
	if err != nil {
		return job, err
	}
	if reservation.State != "held" {
		return job, ErrProductRunnerHeld
	}
	digest, err := contentDigest(value)
	if err != nil {
		return job, err
	}
	job = ProductRunnerJob{ID: spec.JobID, Value: value, Digest: digest, State: "prepared", CreatedAt: k.clock()}
	if err := kernelRecordTx(ctx, tx, "product_runner_job_history", job.ID, plan.ID, value); err != nil {
		return job, err
	}
	if err := saveProductRunnerJobTx(ctx, tx, job); err != nil {
		return job, err
	}
	return job, tx.Commit()
}

func productRunnerJobAdmittedTx(ctx context.Context, tx queryer, id string) (bool, error) {
	var admitted bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_runner_job_history','product_runner_job_terminal')) OR EXISTS(SELECT 1 FROM kernel_dispatch_intents WHERE run_id=?)`, id, id).Scan(&admitted)
	return admitted, err
}

// Follow only authenticated successors. Lost admitted evidence must never fall
// back to an older result, and an outstanding retry makes the profile pending.
func latestProductRunnerJobTx(ctx context.Context, tx queryer, planID, profileID string) (ProductRunnerJob, error) {
	job, err := loadProductRunnerJob(ctx, tx, productRunnerJobID(planID, profileID))
	if err != nil {
		return job, err
	}
	for {
		id := productRunnerRetryID(job.ID)
		next, err := loadProductRunnerJob(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			admitted, err := productRunnerJobAdmittedTx(ctx, tx, id)
			if err != nil {
				return job, err
			}
			if admitted {
				return job, ErrTaskEvidence
			}
			return job, nil
		}
		if err != nil {
			return job, err
		}
		if !productRunnerRetryValueMatches(job, next.Value) {
			return job, ErrTaskEvidence
		}
		job = next
	}
}
