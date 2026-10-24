package storage

import (
	"context"
	"reflect"
)

// A trusted read-only adapter supplies a positively identified merged PR after
// a saved physical stop. Open/absent is never evidence of an unsent request.
type ProductMergeReconciliation struct {
	MergeID     string                `json:"merge_id"`
	StopProofID string                `json:"stop_proof_id"`
	MergeSHA    string                `json:"merge_sha"`
	Observation PRResourceObservation `json:"observation"`
}

func validateProductMergeReconciliation(job ProductMerge, proof ProductMergeReconciliation) error {
	p := job.Value.Product.Value
	o := proof.Observation
	if job.Stop == nil || job.Stop.Disposition != "outcome_unknown" || proof.MergeID != job.ID || proof.StopProofID != job.Stop.ProofID || !fixedSHA(proof.MergeSHA) || !validPRProofID(o.ProofID) || o.RepositoryID != p.WorkItem.RepositoryID || o.HeadRepositoryID != p.WorkItem.RepositoryID || o.BaseRepositoryID != p.WorkItem.RepositoryID || o.Number != p.PRNumber || o.MarkerWorkItemID != p.WorkItem.ID || o.HeadSHA != p.Candidate.SHA || o.HeadBranch != p.WorkItem.Branch || o.BaseBranch != p.BaseBranch || o.State != "closed" || !o.Merged || o.Draft || o.ObservedAt.IsZero() || o.ObservedAt.Before(job.Stop.FinishedAt) {
		return ErrProductMergeHeld
	}
	return nil
}

// ConfirmProductMergeObserved records only a positive merged read. It cannot
// resend, certify the old physical owner stopped, upgrade an unknown capability
// or use a closed/unmerged PR as success. Original stop and gate stay immutable.
func (k *KernelStore) ConfirmProductMergeObserved(ctx context.Context, id, owner, digest string, proof ProductMergeReconciliation) (ProductMerge, error) {
	var empty ProductMerge
	proof.Observation.ObservedAt = proof.Observation.ObservedAt.Round(0).UTC()
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	job, err := k.readProductMergeTx(ctx, tx, id)
	if err != nil {
		return empty, err
	}
	if job.Value.OwnerID != owner || job.Digest != digest || validateProductMergeReconciliation(job, proof) != nil || proof.Observation.ObservedAt.After(k.clock()) {
		return empty, ErrProductMergeHeld
	}
	if job.Reconciliation != nil {
		if !reflect.DeepEqual(*job.Reconciliation, proof) {
			return empty, ErrProductMergeHeld
		}
		return job, tx.Commit()
	}
	if job.State != "stopped_unknown" {
		return empty, ErrProductMergeHeld
	}
	if err := releaseProductMergeMutationTx(ctx, tx, job); err != nil {
		return empty, err
	}
	job.PRLifetimeRelease, err = k.releaseProductMergePRTx(ctx, tx, job)
	if err != nil {
		return empty, err
	}
	job.State, job.Reconciliation = "merged", &proof
	if err := kernelRecordTx(ctx, tx, "product_merge_reconciliation", id, job.Value.Product.Value.Target.WaveID, proof); err != nil {
		return empty, err
	}
	if err := saveProductMergeTx(ctx, tx, job); err != nil {
		return empty, err
	}
	return job, tx.Commit()
}
