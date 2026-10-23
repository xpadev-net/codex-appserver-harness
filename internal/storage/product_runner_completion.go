package storage

import (
	"context"
	"path/filepath"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

func productRunnerReleaseProof(job ProductRunnerJob) OperationReleaseProof {
	c := job.Completion
	return OperationReleaseProof{OperationID: job.ID, ProofID: job.ID + ":" + c.ResultDigest + ":" + c.LogDigest, Disposition: "process_stopped", Reason: "trusted runner owner stopped; exact result and log measured", RetryableInfrastructureFailure: c.Result.Status == "infrastructure_failure"}
}

// FinishProductRunnerJob is exclusively for the trusted process reconciler,
// after runner.ReadStoppedResult. It saves historical execution facts and frees
// their own capacity; it never changes Candidate, quality, approval or phase.
func (k *KernelStore) FinishProductRunnerJob(ctx context.Context, id, digest string, completion runner.Completion) (ProductRunnerJob, error) {
	var job ProductRunnerJob
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	job, err = loadProductRunnerJob(ctx, tx, id)
	if err != nil {
		return job, err
	}
	if job.Digest != digest || completion.Validate(job.Value.Spec) != nil || completion.Result.Started.Before(job.ClaimedAt) {
		return job, ErrProductRunnerHeld
	}
	if job.State == "completed" {
		if !reflect.DeepEqual(*job.Completion, completion) {
			return job, ErrProductRunnerHeld
		}
		return job, tx.Commit()
	}
	if job.State != "launching" {
		return job, ErrProductRunnerHeld
	}
	r, err := loadOperationReservationTx(ctx, tx, id)
	if err != nil {
		return job, err
	}
	s := job.Value.Spec
	if r.Kind != "runner" || r.State != "held" || r.SessionID != job.Value.SessionID || r.Request.Worktree != filepath.Join(s.Root, id) {
		return job, ErrProductRunnerHeld
	}
	finished := k.clock()
	job.State, job.FinishedAt, job.Completion = "completed", &finished, &completion
	if err := k.releaseOperationCapacityTx(ctx, tx, productRunnerReleaseProof(job)); err != nil {
		return job, err
	}
	if err := kernelRecordTx(ctx, tx, "product_runner_job_terminal", id, job.Value.PlanID, job); err != nil {
		return job, err
	}
	if err := saveProductRunnerJobTx(ctx, tx, job); err != nil {
		return job, err
	}
	return job, tx.Commit()
}
