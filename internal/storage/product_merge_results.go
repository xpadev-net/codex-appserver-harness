package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

// ProductMergeResponse contains only normalized facts from the actual bound
// expected-head merge request. The trusted HTTP owner must join before recording
// this receipt. Model tools cannot supply response or physical-stop facts.
type ProductMergeResponse struct {
	RepositoryID    int64  `json:"repository_id"`
	PRNumber        int    `json:"pr_number"`
	ExpectedHeadSHA string `json:"expected_head_sha"`
	Method          string `json:"method"`
	Merged          bool   `json:"merged"`
	MergeSHA        string `json:"merge_sha"`
}
type ProductMergeStop struct {
	MergeID     string                `json:"merge_id"`
	ProofID     string                `json:"proof_id"`
	Reason      string                `json:"reason"`
	StartedAt   time.Time             `json:"started_at"`
	FinishedAt  time.Time             `json:"finished_at"`
	Disposition string                `json:"disposition"` // not_started, client_not_started, response_received, outcome_unknown
	Response    *ProductMergeResponse `json:"response,omitempty"`
}

func validateProductMergeStop(job ProductMerge, proof ProductMergeStop) error {
	if proof.MergeID != job.ID || !validPRProofID(proof.ProofID) || strings.TrimSpace(proof.Reason) == "" || len(proof.Reason) > 16384 || !utf8.ValidString(proof.Reason) || strings.ContainsRune(proof.Reason, '\x00') || proof.StartedAt.IsZero() || proof.FinishedAt.IsZero() || proof.StartedAt.Before(job.ClaimedAt) || proof.FinishedAt.Before(proof.StartedAt) || job.ClaimedAt.IsZero() {
		return ErrProductMergeHeld
	}
	if job.MayHaveSentAt.IsZero() {
		if proof.Disposition != "not_started" || proof.Response != nil {
			return ErrProductMergeHeld
		}
		return nil
	}
	if job.Value.HTTPProtocol == productMergeHTTPProtocol && job.HTTP != nil {
		// This protocol fences before native-client admission. The registered
		// activation, rather than the parent claim, starts this transport span.
		if !proof.StartedAt.Equal(job.HTTP.ActivatedAt) || job.MayHaveSentAt.After(proof.StartedAt) {
			return ErrProductMergeHeld
		}
	} else if job.MayHaveSentAt.Before(proof.StartedAt) || job.MayHaveSentAt.After(proof.FinishedAt) {
		return ErrProductMergeHeld
	}
	if proof.Disposition == "client_not_started" && proof.Response == nil && job.Value.HTTPProtocol == productMergeHTTPProtocol && job.HTTP != nil {
		return nil
	}
	if proof.Disposition == "outcome_unknown" && proof.Response == nil {
		return nil
	}
	if proof.Disposition != "response_received" || proof.Response == nil {
		return ErrProductMergeHeld
	}
	r := proof.Response
	p := job.Value.Product.Value
	if r.RepositoryID != p.WorkItem.RepositoryID || r.PRNumber != p.PRNumber || r.ExpectedHeadSHA != p.Candidate.SHA || r.Method != job.Value.Method || r.Merged && !fixedSHA(r.MergeSHA) || !r.Merged && r.MergeSHA != "" {
		return ErrProductMergeHeld
	}
	return nil
}
func mergeStoppedState(proof ProductMergeStop) string {
	if proof.Disposition == "not_started" {
		return "stopped_unsent"
	}
	if proof.Disposition == "client_not_started" {
		return "stopped_before_http"
	}
	if proof.Disposition == "outcome_unknown" {
		return "stopped_unknown"
	}
	if proof.Response.Merged {
		return "merged"
	}
	return "rejected"
}
func productMergeTerminal(job ProductMerge) bool {
	return job.State == "stopped_unsent" || job.State == "rejected" || job.State == "merged" || job.State == "aborted_http" || job.State == "stopped_before_http"
}
func validateProductMergeResult(ctx context.Context, q queryer, job ProductMerge) error {
	var proof ProductMergeStop
	err := taskEvidenceRecord(ctx, q, "product_merge_stop", job.ID, &proof)
	var observed ProductMergeReconciliation
	observedErr := taskEvidenceRecord(ctx, q, "product_merge_reconciliation", job.ID, &observed)
	if job.Stop == nil {
		if !errors.Is(err, sql.ErrNoRows) || job.PRLifetimeRelease != nil || job.Reconciliation != nil || !errors.Is(observedErr, sql.ErrNoRows) {
			return ErrTaskEvidence
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(proof, *job.Stop) || validateProductMergeStop(job, proof) != nil {
		return ErrTaskEvidence
	}
	expected := mergeStoppedState(proof)
	if job.Reconciliation != nil {
		if observedErr != nil || !reflect.DeepEqual(observed, *job.Reconciliation) || proof.Disposition != "outcome_unknown" || validateProductMergeReconciliation(job, observed) != nil {
			return ErrTaskEvidence
		}
		expected = "merged"
	} else if !errors.Is(observedErr, sql.ErrNoRows) {
		return ErrTaskEvidence
	}
	if job.State != expected {
		return ErrTaskEvidence
	}

	if job.State == "merged" {
		seat, err := loadOperationReservationTx(ctx, q, job.Value.PRReservationID)
		if err != nil {
			return err
		}
		if job.PRLifetimeRelease == nil || seat.State != "released" || seat.Proof == nil || *job.PRLifetimeRelease != *seat.Proof || (seat.Proof.Disposition != "merged" && seat.Proof.Disposition != "closed") {
			return ErrTaskEvidence
		}
	} else if job.PRLifetimeRelease != nil {
		return ErrTaskEvidence
	}
	return nil
}
func releaseProductMergeMutationTx(ctx context.Context, tx *sql.Tx, job ProductMerge) error {
	result, err := tx.ExecContext(ctx, "UPDATE kernel_resource_leases SET state='released' WHERE resource=? AND run_id=? AND generation=? AND state='held'", job.MutationLease.Resource, job.ID, job.MutationLease.Generation)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrProductMergeHeld
	}
	return nil
}

// RecordProductMergeStopped is only for the synchronous physical owner. It
// records historical facts even after pause/cancel/a new candidate, never
// mutates current Product or grants Wave acceptance. Unknown outcome retains
// the mutation lease despite the positively recorded client stop.
func (k *KernelStore) RecordProductMergeStopped(ctx context.Context, id, owner, digest string, proof ProductMergeStop) (ProductMerge, error) {
	return k.recordProductMergeStopped(ctx, id, owner, digest, proof, nil)
}
func (k *KernelStore) recordProductMergeStopped(ctx context.Context, id, owner, digest string, proof ProductMergeStop, httpStop *ProductMergeHTTPStop) (ProductMerge, error) {
	var empty ProductMerge
	proof.StartedAt = proof.StartedAt.Round(0).UTC()
	proof.FinishedAt = proof.FinishedAt.Round(0).UTC()
	if proof.Response != nil {
		copy := *proof.Response
		proof.Response = &copy
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
	registered := job.Value.HTTPProtocol == productMergeHTTPProtocol && !job.MayHaveSentAt.IsZero()
	if registered {
		if httpStop == nil || job.HTTP == nil || httpStop.MergeID != id || httpStop.SpecDigest != job.HTTP.SpecDigest || httpStop.Process != job.HTTP.Process || httpStop.ProofID != proof.ProofID || httpStop.VerifiedAt.Before(proof.FinishedAt) || httpStop.VerifiedAt.Before(job.HTTP.ActivatedAt) || httpStop.VerifiedAt.After(k.clock()) {
			return empty, ErrProductMergeHeld
		}
	} else if httpStop != nil {
		return empty, ErrProductMergeHeld
	}

	if job.Value.OwnerID != owner || job.Digest != digest || validateProductMergeStop(job, proof) != nil || proof.FinishedAt.After(k.clock()) {
		return empty, ErrProductMergeHeld
	}
	if job.Stop != nil {
		if !reflect.DeepEqual(*job.Stop, proof) || !reflect.DeepEqual(job.HTTPStop, httpStop) {
			return empty, ErrProductMergeHeld
		}
		return job, tx.Commit()
	}
	if job.State != "launching" && job.State != "unknown" {
		return empty, ErrProductMergeHeld
	}
	if httpStop == nil {
		instance, e := k.branchOwnerInstanceID()
		if e != nil {
			return empty, e
		}
		if job.OwnerProcess != process.Identify(os.Getpid()) || job.OwnerInstanceID != instance {
			return empty, ErrProductMergeHeld
		}
	} else {
		copy := *httpStop
		job.HTTPStop = &copy
		if err := kernelRecordTx(ctx, tx, "product_merge_http_stop", id, job.Value.Product.Value.Target.WaveID, copy); err != nil {
			return empty, err
		}
	}

	job.Stop, job.State = &proof, mergeStoppedState(proof)
	if job.State != "stopped_unknown" {
		if err := releaseProductMergeMutationTx(ctx, tx, job); err != nil {
			return empty, err
		}
	}
	if job.State == "merged" {
		job.PRLifetimeRelease, err = k.releaseProductMergePRTx(ctx, tx, job)
		if err != nil {
			return empty, err
		}
	}

	if err := kernelRecordTx(ctx, tx, "product_merge_stop", id, job.Value.Product.Value.Target.WaveID, proof); err != nil {
		return empty, err
	}
	if err := saveProductMergeTx(ctx, tx, job); err != nil {
		return empty, err
	}
	return job, tx.Commit()
}

func (k *KernelStore) releaseProductMergePRTx(ctx context.Context, tx *sql.Tx, job ProductMerge) (*OperationReleaseProof, error) {
	seat, err := loadOperationReservationTx(ctx, tx, job.Value.PRReservationID)
	if err != nil {
		return nil, err
	}
	p := job.Value.Product.Value
	if seat.Kind != "pr" || seat.WaveID != p.Target.WaveID || seat.Request.ResourceKey != productMergeResource(p) {
		return nil, ErrTaskEvidence
	}
	if seat.State == "held" {
		release := OperationReleaseProof{OperationID: seat.ID, ProofID: job.ID + ":merged", Disposition: "merged", Reason: "native original-head merge confirmed after physical owner joined"}
		if err := k.releaseOperationCapacityTx(ctx, tx, release); err != nil {
			return nil, err
		}
		return &release, nil
	} else if seat.State == "released" && seat.Proof != nil && (seat.Proof.Disposition == "merged" || seat.Proof.Disposition == "closed") {
		copy := *seat.Proof
		return &copy, nil
	} else {
		return nil, ErrProductMergeHeld
	}
}
