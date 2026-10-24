package storage

import (
	"context"
	"os"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func validateProductBranchStop(job ProductBranchPublication, proof ProductBranchStop) error {
	o := proof.Observation
	if proof.PublicationID != job.ID || proof.ProofID == "" || strings.TrimSpace(proof.Reason) == "" || !utf8.ValidString(proof.ProofID+proof.Reason) || len(proof.ProofID) > 16384 || len(proof.Reason) > 16384 || strings.ContainsRune(proof.ProofID+proof.Reason, '\x00') || o.Source != job.Value.Source || o.Remote != job.Value.Remote.URL || o.Branch != job.Value.Branch || o.RemoteSHA != "" && !fixedSHA(o.RemoteSHA) || proof.StartedAt.Before(job.CreatedAt) || proof.FinishedAt.Before(proof.StartedAt) || proof.StartedAt.IsZero() || proof.FinishedAt.IsZero() {
		return ErrProductBranchHeld
	}
	if proof.RemoteVerified && o.RemoteSHA != job.Value.Source.SHA {
		return ErrProductBranchHeld
	}
	if job.ClaimedAt.IsZero() || proof.StartedAt.Before(job.ClaimedAt) {
		return ErrProductBranchHeld
	}
	if o.Attempted && (job.MayHaveSentAt.IsZero() || job.MayHaveSentAt.Before(proof.StartedAt) || job.MayHaveSentAt.After(proof.FinishedAt)) {
		return ErrProductBranchHeld
	}
	return nil
}

func productBranchReleaseProof(job ProductBranchPublication) OperationReleaseProof {
	return OperationReleaseProof{OperationID: job.ID, ProofID: job.Stop.ProofID, Disposition: "process_stopped", Reason: "trusted branch Git owner joined; remote outcome recorded separately"}
}

// RecordProductBranchStopped is for the synchronous physical Git owner only.
// A restarted controller cannot certify that the old owner's process stopped.
// Even a known remote SHA never frees capacity without physical-stop evidence.
func (k *KernelStore) RecordProductBranchStopped(ctx context.Context, operationID, ownerID, digest string, proof ProductBranchStop) (ProductBranchPublication, error) {
	var job ProductBranchPublication
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	job, err = loadProductBranch(ctx, tx, productBranchID(operationID))
	if err != nil {
		return job, err
	}
	if job.Value.OwnerID != ownerID || job.Digest != digest || validateProductBranchStop(job, proof) != nil || proof.FinishedAt.After(k.clock()) {
		return job, ErrProductBranchHeld
	}
	if job.Stop != nil {
		if !reflect.DeepEqual(*job.Stop, proof) {
			return job, ErrProductBranchHeld
		}
		return job, tx.Commit()
	}
	instanceID, err := k.branchOwnerInstanceID()
	if err != nil {
		return job, err
	}
	if job.State != "launching" || job.OwnerProcess != process.Identify(os.Getpid()) || job.OwnerInstanceID != instanceID {
		return job, ErrProductBranchHeld
	}
	job.Stop, job.StoppedAt, job.State = &proof, k.clock(), "stopped"
	if proof.RemoteVerified && proof.Observation.RemoteSHA == job.Value.Source.SHA {
		job.State = "published"
		job.Published = &ProductBranchRemoteProof{PublicationID: job.ID, ProofID: proof.ProofID + ":remote", StopProofID: proof.ProofID, RemoteSHA: proof.Observation.RemoteSHA, ReadAt: proof.FinishedAt}
		if err := kernelRecordTx(ctx, tx, "product_branch_remote", job.ID, job.Value.Product.Value.Target.WaveID, *job.Published); err != nil {
			return job, err
		}
	}
	if err := k.releaseOperationCapacityTx(ctx, tx, productBranchReleaseProof(job)); err != nil {
		return job, err
	}
	if err := kernelRecordTx(ctx, tx, "product_branch_stop", job.ID, job.Value.Product.Value.Target.WaveID, proof); err != nil {
		return job, err
	}
	if err := saveProductBranchTx(ctx, tx, job); err != nil {
		return job, err
	}
	return job, tx.Commit()
}

// ConfirmProductBranchRemote records a fresh read after a saved physical stop.
// It cannot settle an unconfirmed live writer, grant a new push, or adopt a
// different SHA. The original stop receipt and released reservation stay intact.
func (k *KernelStore) ConfirmProductBranchRemote(ctx context.Context, operationID, ownerID, digest string, proof ProductBranchRemoteProof) (ProductBranchPublication, error) {
	var job ProductBranchPublication
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	job, err = loadProductBranch(ctx, tx, productBranchID(operationID))
	if err != nil {
		return job, err
	}
	if job.Value.OwnerID != ownerID || job.Digest != digest || job.Stop == nil || proof.PublicationID != job.ID || proof.StopProofID != job.Stop.ProofID || proof.RemoteSHA != job.Value.Source.SHA || proof.ProofID == "" || len(proof.ProofID) > 16384 || !utf8.ValidString(proof.ProofID) || strings.ContainsAny(proof.ProofID, "\x00\r\n") || proof.ReadAt.After(k.clock()) {
		return job, ErrProductBranchHeld
	}
	if job.Published != nil {
		if *job.Published != proof {
			return job, ErrProductBranchHeld
		}
		return job, tx.Commit()
	}
	if job.State != "stopped" || proof.ReadAt.Before(job.StoppedAt) {
		return job, ErrProductBranchHeld
	}
	job.State, job.Published = "published", &proof
	if err := kernelRecordTx(ctx, tx, "product_branch_remote", job.ID, job.Value.Product.Value.Target.WaveID, proof); err != nil {
		return job, err
	}
	if err := saveProductBranchTx(ctx, tx, job); err != nil {
		return job, err
	}
	return job, tx.Commit()
}
