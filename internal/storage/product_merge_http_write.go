package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

type ProductMergeHTTPWrite struct {
	MergeID    string                      `json:"merge_id"`
	SpecDigest string                      `json:"spec_digest"`
	Process    process.Identity            `json:"process"`
	Gate       ProductMergeGateObservation `json:"gate"`
	At         time.Time                   `json:"at"`
}

func validateProductMergeHTTPWrite(ctx context.Context, q queryer, job ProductMerge) error {
	var write ProductMergeHTTPWrite
	err := taskEvidenceRecord(ctx, q, "product_merge_http_write", job.ID, &write)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if job.HTTPWrite == nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return ErrTaskEvidence
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(write, *job.HTTPWrite) || job.HTTP == nil || write.MergeID != job.ID || write.Process != job.HTTP.Process || write.SpecDigest != job.HTTP.SpecDigest || validateMergeGateObservation(job, write.Gate) != nil || write.Gate.ObservedAt.Before(job.HTTP.ActivatedAt) || write.At.Before(write.Gate.ObservedAt) || write.At.Sub(write.Gate.ObservedAt) > 30*time.Second || job.HTTPAbort != nil {
		return ErrTaskEvidence
	}
	if job.Stop != nil && (job.Stop.Disposition == "client_not_started" || job.Stop.FinishedAt.Before(write.At)) {
		return ErrTaskEvidence
	}
	return nil
}

// MarkProductMergeHTTPMayHaveSent is the registered client's separate one-use
// native mutation fence, immediately after fresh controller authorization and
// before the fixed PUT. Registration alone never substitutes for this check.
func (k *KernelStore) MarkProductMergeHTTPMayHaveSent(ctx context.Context, id, owner, digest, specDigest string, gate ProductMergeGateObservation) (ProductMerge, error) {
	var empty ProductMerge
	gate = canonicalMergeGateObservation(gate)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	job, err := k.readProductMergeTx(ctx, tx, id)
	if err != nil {
		return empty, err
	}
	physical := process.Identify(os.Getpid())
	if job.Value.HTTPProtocol != productMergeHTTPProtocol || job.Value.OwnerID != owner || job.Digest != digest || job.HTTP == nil || job.HTTP.Process != physical || job.HTTP.SpecDigest != specDigest || job.State != "unknown" || job.Stop != nil || job.HTTPAbort != nil || job.HTTPWrite != nil {
		return empty, ErrProductMergeHeld
	}
	now := k.clock()
	if validateMergeGateObservation(job, gate) != nil || gate.ObservedAt.Before(job.HTTP.ActivatedAt) || gate.ObservedAt.After(now) || now.Sub(gate.ObservedAt) > 30*time.Second {
		return empty, ErrProductMergeHeld
	}
	round, err := k.productCloudRoundTx(ctx, tx, job.Value.RequestID)
	if err != nil {
		return empty, err
	}
	ready, seat, err := k.mergePreflightTx(ctx, tx, round)
	if err != nil {
		return empty, err
	}
	if ready.ID != job.Value.ReadinessID || ready.Digest != job.Value.ReadinessDigest || seat.ID != job.Value.PRReservationID {
		return empty, ErrProductMergeHeld
	}
	write := ProductMergeHTTPWrite{MergeID: id, SpecDigest: specDigest, Process: physical, Gate: gate, At: now}
	job.HTTPWrite = &write
	if err := kernelRecordTx(ctx, tx, "product_merge_http_write", id, job.Value.Product.Value.Target.WaveID, write); err != nil {
		return empty, err
	}
	if err := saveProductMergeTx(ctx, tx, job); err != nil {
		return empty, err
	}
	return job, tx.Commit()
}
