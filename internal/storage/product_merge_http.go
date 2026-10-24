package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

const productMergeHTTPProtocol = "registered_http_v1"

type ProductMergeHTTPOwner struct {
	MergeID     string           `json:"merge_id"`
	SpecDigest  string           `json:"spec_digest"`
	GateDigest  string           `json:"gate_digest"`
	Process     process.Identity `json:"process"`
	ActivatedAt time.Time        `json:"activated_at"`
}
type ProductMergeHTTPAbort struct {
	MergeID    string    `json:"merge_id"`
	ProofID    string    `json:"proof_id"`
	Reason     string    `json:"reason"`
	GateDigest string    `json:"gate_digest"`
	At         time.Time `json:"at"`
}

// PrepareProductMergeHTTP pins the registered transport protocol before any
// possible send. Legacy intents cannot be upgraded by absence of an HTTP owner.
// The actual trusted wrapper must acquire ActivateProductMergeHTTP before it
// may start a native mutation; the database can atomically revoke that ability.
func (k *KernelStore) PrepareProductMergeHTTP(ctx context.Context, requestID, owner, method string) (ProductMerge, error) {
	return k.prepareProductMerge(ctx, requestID, owner, method, productMergeHTTPProtocol)
}
func validateProductMergeHTTP(ctx context.Context, q queryer, job ProductMerge) error {
	var owner ProductMergeHTTPOwner
	var abort ProductMergeHTTPAbort
	ownerErr := taskEvidenceRecord(ctx, q, "product_merge_http_owner", job.ID, &owner)
	abortErr := taskEvidenceRecord(ctx, q, "product_merge_http_abort", job.ID, &abort)
	var stopped ProductMergeHTTPStop
	stopErr := taskEvidenceRecord(ctx, q, "product_merge_http_stop", job.ID, &stopped)
	if job.HTTPStop != nil {
		if stopErr != nil || !reflect.DeepEqual(stopped, *job.HTTPStop) || job.HTTP == nil || job.Stop == nil || stopped.MergeID != job.ID || stopped.Process != job.HTTP.Process || stopped.SpecDigest != job.HTTP.SpecDigest || stopped.ProofID != job.Stop.ProofID || stopped.VerifiedAt.IsZero() || stopped.VerifiedAt.Before(job.Stop.FinishedAt) || stopped.VerifiedAt.Before(job.HTTP.ActivatedAt) {
			return ErrTaskEvidence
		}
	} else if !errors.Is(stopErr, sql.ErrNoRows) || job.HTTP != nil && job.Stop != nil {
		return ErrTaskEvidence
	}
	if job.Value.HTTPProtocol == "" {
		if job.HTTP != nil || job.HTTPAbort != nil || job.HTTPStop != nil || job.State == "aborted_http" || !errors.Is(ownerErr, sql.ErrNoRows) || !errors.Is(abortErr, sql.ErrNoRows) {
			return ErrTaskEvidence
		}
		return nil
	}
	if job.Value.HTTPProtocol != productMergeHTTPProtocol {
		return ErrTaskEvidence
	}
	if job.HTTP != nil {
		if ownerErr != nil || !reflect.DeepEqual(owner, *job.HTTP) || owner.MergeID != job.ID || owner.GateDigest != job.GateDigest || !owner.Process.Valid() || len(owner.SpecDigest) != 64 || strings.Trim(owner.SpecDigest, "0123456789abcdef") != "" || owner.ActivatedAt.IsZero() || job.MayHaveSentAt.IsZero() || owner.ActivatedAt.Before(job.MayHaveSentAt) || job.HTTPAbort != nil || job.State == "aborted_http" {
			return ErrTaskEvidence
		}
	} else if !errors.Is(ownerErr, sql.ErrNoRows) {
		return ErrTaskEvidence
	}
	if job.HTTPAbort != nil {
		if abortErr != nil || abort != *job.HTTPAbort || abort.MergeID != job.ID || abort.GateDigest != job.GateDigest || !validPRProofID(abort.ProofID) || !validPRProofID(abort.Reason) || abort.At.Before(job.MayHaveSentAt) || job.MayHaveSentAt.IsZero() || job.HTTP != nil || job.State != "aborted_http" {
			return ErrTaskEvidence
		}
	} else if !errors.Is(abortErr, sql.ErrNoRows) || job.State == "aborted_http" {
		return ErrTaskEvidence
	}
	return nil
}

// ActivateProductMergeHTTP is the protocol's one-use native-client admission.
// The trusted wrapper registers itself as the real process-group leader.
// It must never send on Start=false; no caller-selected process is accepted.
func (k *KernelStore) ActivateProductMergeHTTP(ctx context.Context, id, owner, digest, specDigest string) (ProductMergeClaim, error) {
	physical := process.Identify(os.Getpid())
	var out ProductMergeClaim
	if len(specDigest) != 64 || strings.Trim(specDigest, "0123456789abcdef") != "" || !physical.Valid() || !process.Alive(physical) {
		return out, ErrProductMergeHeld
	}
	_, group, _, err := process.ReadStat(physical.PID)
	if err != nil || group != physical.PID {
		return out, ErrProductMergeHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	job, err := k.readProductMergeTx(ctx, tx, id)
	if err != nil {
		return out, err
	}
	out.Merge = job
	if job.Value.HTTPProtocol != productMergeHTTPProtocol || job.Value.OwnerID != owner || job.Digest != digest || job.State != "unknown" || job.Stop != nil || job.HTTPAbort != nil {
		return out, ErrProductMergeHeld
	}
	if job.HTTP != nil {
		if job.HTTP.SpecDigest != specDigest || job.HTTP.Process != physical {
			return out, ErrProductMergeHeld
		}
		return out, tx.Commit()
	}
	var gate ProductMergeGateObservation
	if err := taskEvidenceRecord(ctx, tx, "product_merge_gate", id, &gate); err != nil {
		return out, err
	}
	now := k.clock()
	if gate.ObservedAt.After(now) || now.Sub(gate.ObservedAt) > 30*time.Second {
		return out, ErrProductMergeHeld
	}
	round, err := k.productCloudRoundTx(ctx, tx, job.Value.RequestID)
	if err != nil {
		return out, err
	}
	ready, seat, err := k.mergePreflightTx(ctx, tx, round)
	if err != nil {
		return out, err
	}
	if ready.ID != job.Value.ReadinessID || ready.Digest != job.Value.ReadinessDigest || seat.ID != job.Value.PRReservationID {
		return out, ErrProductMergeHeld
	}
	activation := ProductMergeHTTPOwner{MergeID: id, SpecDigest: specDigest, GateDigest: job.GateDigest, Process: physical, ActivatedAt: now}
	job.HTTP = &activation
	if err := kernelRecordTx(ctx, tx, "product_merge_http_owner", id, job.Value.Product.Value.Target.WaveID, activation); err != nil {
		return out, err
	}
	if err := saveProductMergeTx(ctx, tx, job); err != nil {
		return out, err
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	out.Merge, out.Start = job, true
	return out, nil
}

// AbortUnactivatedProductMergeHTTP seals the registered admission before any
// native client won it. This is an atomic protocol proof, not an inference from
// missing files, an absent PR, an expired lease or a restarted owner. After this
// commit every delayed wrapper must fail activation and therefore cannot send.
func (k *KernelStore) AbortUnactivatedProductMergeHTTP(ctx context.Context, id, owner, digest, proofID, reason string) (ProductMerge, error) {
	var empty ProductMerge
	if !validPRProofID(proofID) || !validPRProofID(reason) {
		return empty, ErrProductMergeHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	job, err := k.readProductMergeTx(ctx, tx, id)
	if err != nil {
		return empty, err
	}
	if job.Value.HTTPProtocol != productMergeHTTPProtocol || job.Value.OwnerID != owner || job.Digest != digest || job.HTTP != nil {
		return empty, ErrProductMergeHeld
	}
	if job.HTTPAbort != nil {
		if job.HTTPAbort.ProofID != proofID || job.HTTPAbort.Reason != reason {
			return empty, ErrProductMergeHeld
		}
		return job, tx.Commit()
	}
	if job.State != "unknown" || job.Stop != nil || job.MayHaveSentAt.IsZero() {
		return empty, ErrProductMergeHeld
	}
	abort := ProductMergeHTTPAbort{MergeID: id, ProofID: proofID, Reason: reason, GateDigest: job.GateDigest, At: k.clock()}
	if err := releaseProductMergeMutationTx(ctx, tx, job); err != nil {
		return empty, err
	}
	job.HTTPAbort, job.State = &abort, "aborted_http"
	if err := kernelRecordTx(ctx, tx, "product_merge_http_abort", id, job.Value.Product.Value.Target.WaveID, abort); err != nil {
		return empty, err
	}
	if err := saveProductMergeTx(ctx, tx, job); err != nil {
		return empty, err
	}
	return job, tx.Commit()
}
