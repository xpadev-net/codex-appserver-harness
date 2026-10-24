package storage

import (
	"context"
	"strings"
	"unicode/utf8"
)

func productBranchCancellationProof(job ProductBranchPublication) OperationReleaseProof {
	return OperationReleaseProof{OperationID: job.ID, ProofID: job.Cancellation.ProofID, Disposition: "not_started", Reason: job.Cancellation.Reason}
}

// CancelPreparedProductBranch releases only an intent that never granted any
// Git process start. A launching/unknown owner is never canceled as unsent.
func (k *KernelStore) CancelPreparedProductBranch(ctx context.Context, operationID, ownerID, reason string) (ProductBranchPublication, error) {
	var job ProductBranchPublication
	if strings.TrimSpace(reason) == "" || len(reason) > 16384 || !utf8.ValidString(reason) || strings.ContainsRune(reason, '\x00') {
		return job, ErrProductBranchHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	job, err = latestProductBranch(ctx, tx, operationID)
	if err != nil {
		return job, err
	}
	if job.Value.OwnerID != ownerID {
		return job, ErrProductBranchHeld
	}
	if job.State == "canceled" {
		if job.Cancellation.Reason != reason {
			return job, ErrProductBranchHeld
		}
		return job, tx.Commit()
	}
	if job.State != "prepared" {
		return job, ErrProductBranchHeld
	}
	job.State = "canceled"
	job.Cancellation = &ProductBranchCancellation{ProofID: job.ID + ":not-started", Reason: reason, At: k.clock()}
	if job.Cancellation.At.Before(job.CreatedAt) {
		return job, ErrProductBranchHeld
	}
	if err := k.releaseOperationCapacityTx(ctx, tx, productBranchCancellationProof(job)); err != nil {
		return job, err
	}
	if err := kernelRecordTx(ctx, tx, "product_branch_cancel", job.ID, job.Value.Product.Value.Target.WaveID, *job.Cancellation); err != nil {
		return job, err
	}
	if err := saveProductBranchTx(ctx, tx, job); err != nil {
		return job, err
	}
	return job, tx.Commit()
}
