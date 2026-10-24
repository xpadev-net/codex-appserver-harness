package product

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// MergeDriver connects the canonical parent gate to the registered transport.
// It accepts only an authenticated Cloud request ID and trusted host method/
// owner; candidate, repository and operation identity come from the journal.
type MergeDriver struct {
	Store     *storage.KernelStore
	Gate      func(context.Context, storage.ProductTargetSnapshot, string) (storage.ProductMergeGateObservation, error)
	Transport MergeHTTPTransport
}

// Merge never relaunches a previously claimed intent. Recovery is explicit and
// separate, so another scan cannot stop a still-running controller's transport.
// An initial gate hold leaves a prepared intent for a later fresh assessment.
func (d MergeDriver) Merge(ctx context.Context, requestID, owner, method string) (storage.ProductMerge, error) {
	var job storage.ProductMerge
	if d.Store == nil || d.Transport.Store != d.Store || d.Gate == nil || !filepath.IsAbs(d.Transport.Executable) {
		return job, errors.New("shared merge store, canonical gate and trusted transport required")
	}
	job, err := d.Store.PrepareProductMergeHTTP(ctx, requestID, owner, method)
	if err != nil {
		return job, err
	}
	if job.Stop != nil || job.HTTPAbort != nil || job.Cancellation != nil {
		return job, nil
	}
	if job.State != "prepared" {
		return job, storage.ErrProductMergeHeld
	}
	if _, err := d.Gate(ctx, job.Value.Product, requestID); err != nil {
		return job, err
	}
	claim, err := d.Store.ClaimProductMerge(ctx, job.ID, owner, job.Digest)
	if err != nil {
		return job, err
	}
	job = claim.Merge
	if !claim.Start {
		return job, storage.ErrProductMergeHeld
	}
	// The pre-claim observation is deliberately discarded. Fence admission
	// requires a newly measured packet after this exact physical claim.
	fresh, err := d.Gate(ctx, job.Value.Product, requestID)
	if err == nil {
		job, err = d.Store.MarkProductMergeMayHaveSent(ctx, job.ID, owner, job.Digest, fresh)
	}
	if err != nil {
		return d.stopClaim(claim.Merge, err)
	}
	result, err := d.Transport.Start(ctx, job.ID)
	if err == nil || result.Stop != nil || result.HTTPAbort != nil {
		return result, err
	}
	// Spec creation/start can fail before a wrapper registers. Resolve through
	// the protocol, never through absence of a PID, result or native request.
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	recovered, recoverErr := d.Transport.Recover(cleanup, job.ID)
	return recovered, errors.Join(err, recoverErr)
}

func (d MergeDriver) stopClaim(claim storage.ProductMerge, cause error) (storage.ProductMerge, error) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	job, err := d.Store.ReadProductMerge(cleanup, claim.ID)
	if err != nil {
		return job, errors.Join(cause, err)
	}
	if !job.MayHaveSentAt.IsZero() {
		// A failed commit acknowledgement can still have persisted the fence.
		// Only atomic HTTP admission sealing/actual group recovery resolves it.
		result, err := d.Transport.Recover(cleanup, job.ID)
		return result, errors.Join(cause, err)
	}
	now := time.Now().UTC()
	if d.Store.Now != nil {
		now = d.Store.Now().UTC()
	}
	proof := storage.ProductMergeStop{MergeID: job.ID, ProofID: "merge-parent-unsent-" + job.Digest, Reason: "own parent claim stopped before HTTP admission fence", StartedAt: job.ClaimedAt, FinishedAt: now, Disposition: "not_started"}
	result, err := d.Store.RecordProductMergeStopped(cleanup, job.ID, job.Value.OwnerID, job.Digest, proof)
	return result, errors.Join(cause, err)
}
