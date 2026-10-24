package storage

import (
	"context"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

// ProductMergeHTTPStop records the reconciler's actual quiescence observation
// of the immutable registered process group. It is never a caller-supplied
// stopped boolean or a replacement of the original parent's process identity.
type ProductMergeHTTPStop struct {
	MergeID    string           `json:"merge_id"`
	SpecDigest string           `json:"spec_digest"`
	Process    process.Identity `json:"process"`
	ProofID    string           `json:"proof_id"`
	VerifiedAt time.Time        `json:"verified_at"`
}

// FinishProductMergeHTTP is usable after controller restart. The trusted host
// supplies facts from the bound wrapper/result; storage independently verifies
// the registered group's physical stopping before committing or releasing any
// ownership. A live/unconfirmed group or missing owner stays held.
func (k *KernelStore) FinishProductMergeHTTP(ctx context.Context, id, owner, digest, specDigest string, proof ProductMergeStop) (ProductMerge, error) {
	var empty ProductMerge
	job, err := k.ReadProductMerge(ctx, id)
	if err != nil {
		return empty, err
	}
	if job.Value.HTTPProtocol != productMergeHTTPProtocol || job.Value.OwnerID != owner || job.Digest != digest || job.HTTP == nil || job.HTTP.SpecDigest != specDigest || job.HTTPAbort != nil {
		return empty, ErrProductMergeHeld
	}
	if job.Stop != nil {
		// Replay authenticates the saved witness; it must not inspect or signal a
		// recycled PID/group or release a successor's lease again.
		if job.HTTPStop == nil {
			return empty, ErrTaskEvidence
		}
		return k.recordProductMergeStopped(ctx, id, owner, digest, proof, job.HTTPStop)
	}
	work, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := process.WaitStopped(work, job.HTTP.Process); err != nil {
		return empty, err
	}
	verified := ProductMergeHTTPStop{MergeID: id, SpecDigest: specDigest, Process: job.HTTP.Process, ProofID: proof.ProofID, VerifiedAt: k.clock()}
	return k.recordProductMergeStopped(ctx, id, owner, digest, proof, &verified)
}
