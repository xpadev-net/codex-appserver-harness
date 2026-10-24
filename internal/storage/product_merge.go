package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

var ErrProductMergeHeld = errors.New("product merge requires fresh gates or reconciliation")

type ProductMergeValue struct {
	HTTPProtocol    string                `json:"http_protocol,omitempty"`
	RequestID       string                `json:"request_id"`
	RoundDigest     string                `json:"round_digest"`
	OwnerID         string                `json:"owner_id"`
	Method          string                `json:"method"`
	Product         ProductTargetSnapshot `json:"product"`
	ReadinessID     string                `json:"readiness_id"`
	ReadinessDigest string                `json:"readiness_digest"`
	PRReservationID string                `json:"pr_reservation_id"`
}
type ProductMergeCancellation struct {
	ProofID string    `json:"proof_id"`
	Reason  string    `json:"reason"`
	At      time.Time `json:"at"`
}
type ProductMerge struct {
	HTTPWrite         *ProductMergeHTTPWrite      `json:"http_write,omitempty"`
	HTTPStop          *ProductMergeHTTPStop       `json:"http_stop,omitempty"`
	HTTP              *ProductMergeHTTPOwner      `json:"http,omitempty"`
	HTTPAbort         *ProductMergeHTTPAbort      `json:"http_abort,omitempty"`
	Reconciliation    *ProductMergeReconciliation `json:"reconciliation,omitempty"`
	Stop              *ProductMergeStop           `json:"stop,omitempty"`
	PRLifetimeRelease *OperationReleaseProof      `json:"pr_lifetime_release,omitempty"`
	GateDigest        string                      `json:"gate_digest,omitempty"`
	Cancellation      *ProductMergeCancellation   `json:"cancellation,omitempty"`
	ID                string                      `json:"id"`
	Value             ProductMergeValue           `json:"value"`
	Digest            string                      `json:"digest"`
	State             string                      `json:"state"` // prepared, launching, unknown, canceled, stopped_unsent, stopped_unknown, rejected, merged, aborted_http
	MutationLease     ResourceLease               `json:"mutation_lease"`
	CreatedAt         time.Time                   `json:"created_at"`
	ClaimedAt         time.Time                   `json:"claimed_at"`
	OwnerProcess      process.Identity            `json:"owner_process"`
	OwnerInstanceID   string                      `json:"owner_instance_id"`
	MayHaveSentAt     time.Time                   `json:"may_have_sent_at"`
}
type ProductMergeClaim struct {
	Merge ProductMerge `json:"merge"`
	Start bool         `json:"start"`
}
type productMergeRecord struct {
	Merge  ProductMerge `json:"merge"`
	Digest string       `json:"digest"`
}
type productMergePhysicalClaim struct {
	ID              string           `json:"id"`
	OwnerProcess    process.Identity `json:"owner_process"`
	OwnerInstanceID string           `json:"owner_instance_id"`
	ClaimedAt       time.Time        `json:"claimed_at"`
}

func productMergeID(requestID string) string {
	return structureID("product-merge-", requestID, "merge")
}
func productMergeResource(p ProductTarget) string {
	return fmt.Sprintf("github:%d:work:%s", p.WorkItem.RepositoryID, p.WorkItem.ID)
}
func saveProductMergeTx(ctx context.Context, tx *sql.Tx, job ProductMerge) error {
	digest, err := contentDigest(job)
	if err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, "product_merge", job.ID, job.Value.Product.Value.Target.WaveID, productMergeRecord{Merge: job, Digest: digest})
}

// Absence is distinct from damaged admitted evidence. Read never recreates a
// missing head/history or a consumed physical/send claim.
func lookupProductMerge(ctx context.Context, q queryer, id string) (job ProductMerge, found bool, err error) {
	var record productMergeRecord
	if err = taskEvidenceRecord(ctx, q, "product_merge", id, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return job, false, nil
		}
		return job, false, err
	}
	found = true
	defer func() {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrTaskEvidence
		}
	}()
	job = record.Merge
	digest, e := contentDigest(job)
	if e != nil {
		return job, true, e
	}
	valueDigest, e := contentDigest(job.Value)
	if e != nil {
		return job, true, e
	}
	v := job.Value
	p := v.Product.Value
	if job.ID != id || id != productMergeID(v.RequestID) || record.Digest != digest || job.Digest != valueDigest || job.CreatedAt.IsZero() || !validPRProofID(v.OwnerID) || (v.Method != "merge" && v.Method != "squash" && v.Method != "rebase") || job.MutationLease.Resource != "product-pr-mutation:"+productMergeResource(p) || job.MutationLease.Generation < 1 {
		return job, true, ErrTaskEvidence
	}
	var history ProductMergeValue
	var admitted string
	if err = taskEvidenceRecord(ctx, q, "product_merge_history", id, &history); err != nil {
		return job, true, err
	}
	if err = taskEvidenceRecord(ctx, q, "product_merge_admission", id, &admitted); err != nil {
		return job, true, err
	}
	if !reflect.DeepEqual(history, v) || admitted != job.Digest {
		return job, true, ErrTaskEvidence
	}
	var ready ProductPublicationReadiness
	if err = taskEvidenceRecord(ctx, q, "product_publication_readiness_history", v.ReadinessID, &ready); err != nil {
		return job, true, err
	}
	readyDigest, e := contentDigest(ready.Value)
	if e != nil {
		return job, true, e
	}
	if ready.ID != v.ReadinessID || ready.Digest != v.ReadinessDigest || ready.Digest != readyDigest || !reflect.DeepEqual(ready.Value.Product, v.Product) {
		return job, true, ErrTaskEvidence
	}
	var claim productMergePhysicalClaim
	var send time.Time
	claimErr := taskEvidenceRecord(ctx, q, "product_merge_claim", id, &claim)
	sendErr := taskEvidenceRecord(ctx, q, "product_merge_send", id, &send)
	var hasGate, hasCancel bool
	if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_merge_gate' AND id=?),EXISTS(SELECT 1 FROM records WHERE kind='product_merge_cancel' AND id=?)", id, id).Scan(&hasGate, &hasCancel); err != nil {
		return job, true, err
	}
	hasSent := !job.MayHaveSentAt.IsZero()
	if hasSent != hasGate || (job.State == "canceled") != hasCancel {
		return job, true, ErrTaskEvidence
	}
	switch job.State {
	case "prepared", "canceled":
		if !errors.Is(claimErr, sql.ErrNoRows) || !errors.Is(sendErr, sql.ErrNoRows) || !job.ClaimedAt.IsZero() || job.OwnerProcess != (process.Identity{}) || job.OwnerInstanceID != "" || !job.MayHaveSentAt.IsZero() || job.GateDigest != "" {
			return job, true, ErrTaskEvidence
		}
		var cancellation ProductMergeCancellation
		cancelErr := taskEvidenceRecord(ctx, q, "product_merge_cancel", id, &cancellation)
		if job.State == "canceled" {
			if cancelErr != nil || job.Cancellation == nil || cancellation != *job.Cancellation || !validPRProofID(cancellation.ProofID) || strings.TrimSpace(cancellation.Reason) == "" || cancellation.At.Before(job.CreatedAt) {
				return job, true, ErrTaskEvidence
			}
		} else if !errors.Is(cancelErr, sql.ErrNoRows) || job.Cancellation != nil {
			return job, true, ErrTaskEvidence
		}

	case "launching", "unknown", "stopped_unsent", "stopped_unknown", "rejected", "merged", "aborted_http", "stopped_before_http":
		if job.Cancellation != nil || claimErr != nil || claim.ID != id || claim.OwnerProcess != job.OwnerProcess || claim.OwnerInstanceID != job.OwnerInstanceID || !claim.ClaimedAt.Equal(job.ClaimedAt) || !job.OwnerProcess.Valid() || job.OwnerInstanceID == "" || job.ClaimedAt.Before(job.CreatedAt) {
			return job, true, ErrTaskEvidence
		}
		if job.State == "launching" || job.State == "stopped_unsent" || (job.State == "aborted_http" && !hasSent) {
			if !errors.Is(sendErr, sql.ErrNoRows) || !job.MayHaveSentAt.IsZero() || job.GateDigest != "" {
				return job, true, ErrTaskEvidence
			}
		} else if sendErr != nil || !send.Equal(job.MayHaveSentAt) || send.IsZero() || send.Before(job.ClaimedAt) {
			return job, true, ErrTaskEvidence
		}
		if hasSent {
			var gate ProductMergeGateObservation
			if err = taskEvidenceRecord(ctx, q, "product_merge_gate", id, &gate); err != nil {
				return job, true, err
			}
			gateDigest, e := contentDigest(gate)
			if e != nil {
				return job, true, e
			}
			if job.GateDigest != gateDigest || validateMergeGateObservation(job, gate) != nil || gate.ObservedAt.After(job.MayHaveSentAt) || job.MayHaveSentAt.Sub(gate.ObservedAt) > 30*time.Second {
				return job, true, ErrTaskEvidence
			}
		}

	default:
		return job, true, ErrTaskEvidence
	}
	if err := validateProductMergeHTTPWrite(ctx, q, job); err != nil {
		return job, true, err
	}
	if err := validateProductMergeHTTP(ctx, q, job); err != nil {
		return job, true, err
	}
	if err := validateProductMergeResult(ctx, q, job); err != nil {
		return job, true, err
	}
	if (job.State == "stopped_unsent" || job.State == "stopped_unknown" || job.State == "rejected" || job.State == "merged" || job.State == "stopped_before_http") && job.Stop == nil {
		return job, true, ErrTaskEvidence
	}
	if (job.State == "launching" || job.State == "unknown" || job.State == "prepared" || job.State == "canceled") && job.Stop != nil {
		return job, true, ErrTaskEvidence
	}

	return job, true, nil
}

// mergePreflightTx rechecks current controls and independent quality without
// requiring the original request Task to remain running. Completed Task threads
// are never reopened to perform a later merge.
func (k *KernelStore) mergePreflightTx(ctx context.Context, tx *sql.Tx, round ProductCloudRound) (ProductPublicationReadiness, OperationReservation, error) {
	var empty ProductPublicationReadiness
	var seat OperationReservation
	p := round.Product.Value
	w, err := loadWave(ctx, tx, p.Target.WaveID)
	if err != nil {
		return empty, seat, err
	}
	if w.Target() != p.Target || w.Policy.Stage != "P" || !w.Policy.Product || w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return empty, seat, ErrProductMergeHeld
	}
	parent, err := sessionBindingTx(ctx, tx, w.OrchestratorSessionID)
	if err != nil {
		return empty, seat, err
	}
	if parent.Closed {
		return empty, seat, ErrProductMergeHeld
	}
	current, err := loadProductTargetTx(ctx, tx, p.Target, p.WorkItem.ID)
	if err != nil {
		return empty, seat, err
	}
	if !reflect.DeepEqual(current, round.Product) || p.WorkItem.Paused || p.WorkItem.MergeHold || p.WorkItem.Phase != domain.Reviewing || p.PRNumber < 1 || len(round.Packet.CloudReviewerIDs) == 0 {
		return empty, seat, ErrProductMergeHeld
	}
	ready, err := loadProductPublicationReadiness(ctx, tx, p.Target, p.WorkItem.ID)
	if err != nil {
		return empty, seat, err
	}
	quality, err := k.productQualityTx(ctx, tx, p.Target, p.WorkItem.ID)
	if err != nil {
		return empty, seat, err
	}
	if !reflect.DeepEqual(ready.Value.Product, current) || !reflect.DeepEqual(ready.Value.Quality, quality) {
		return empty, seat, ErrProductMergeHeld
	}
	seat, err = loadPRLifetimeSeatTx(ctx, tx, w.ID, productMergeResource(p))
	if err != nil {
		return empty, seat, err
	}
	if seat.State != "held" || seat.SessionID != w.OrchestratorSessionID {
		return empty, seat, ErrProductMergeHeld
	}
	return ready, seat, nil
}
func (k *KernelStore) readProductMergeTx(ctx context.Context, tx *sql.Tx, id string) (ProductMerge, error) {
	job, found, err := lookupProductMerge(ctx, tx, id)
	if err != nil {
		return job, err
	}
	if !found {
		return job, sql.ErrNoRows
	}
	round, err := k.productCloudRoundTx(ctx, tx, job.Value.RequestID)
	if err != nil {
		return job, err
	}
	if round.Digest != job.Value.RoundDigest || !reflect.DeepEqual(round.Product, job.Value.Product) {
		return job, ErrTaskEvidence
	}
	if job.State == "canceled" || productMergeTerminal(job) {
		return job, nil
	}
	var owner, state, wave string
	var generation int64
	if err := tx.QueryRowContext(ctx, "SELECT run_id,state,wave_id,generation FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&owner, &state, &wave, &generation); err != nil {
		return job, err
	}
	if owner != job.ID || state != "held" || wave != job.Value.Product.Value.Target.WaveID || generation != job.MutationLease.Generation {
		return job, ErrTaskEvidence
	}
	return job, nil
}

// PrepareProductMerge reserves only the existing PR mutation lease. It grants
// no HTTP send or formal approval: the trusted adapter must freshly run all
// source/quality/Cloud/CI/rules gates before the separate one-use send fence.
func (k *KernelStore) PrepareProductMerge(ctx context.Context, requestID, owner, method string) (ProductMerge, error) {
	return k.prepareProductMerge(ctx, requestID, owner, method, "")
}
func (k *KernelStore) prepareProductMerge(ctx context.Context, requestID, owner, method, protocol string) (ProductMerge, error) {
	var empty ProductMerge
	if !validPRProofID(owner) || (method != "merge" && method != "squash" && method != "rebase") {
		return empty, ErrProductMergeHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	id := productMergeID(requestID)
	old, found, err := lookupProductMerge(ctx, tx, id)
	if err != nil {
		return empty, err
	}
	if found {
		old, err = k.readProductMergeTx(ctx, tx, id)
		if err != nil {
			return empty, err
		}
		if old.Value.OwnerID != owner || old.Value.Method != method || old.Value.HTTPProtocol != protocol {
			return empty, ErrProductMergeHeld
		}
		return old, tx.Commit()
	}
	var admitted bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_merge_history','product_merge_admission','product_merge_claim','product_merge_send','product_merge_gate')) OR EXISTS(SELECT 1 FROM kernel_resource_leases WHERE run_id=?)`, id, id).Scan(&admitted); err != nil {
		return empty, err
	}
	if admitted {
		return empty, ErrTaskEvidence
	}
	round, err := k.productCloudRoundTx(ctx, tx, requestID)
	if err != nil {
		return empty, err
	}
	ready, seat, err := k.mergePreflightTx(ctx, tx, round)
	if err != nil {
		return empty, err
	}
	lease, err := claimResourceTx(ctx, tx, "product-pr-mutation:"+productMergeResource(round.Product.Value), id, round.Product.Value.Target.WaveID)
	if err != nil {
		return empty, err
	}
	value := ProductMergeValue{HTTPProtocol: protocol, RequestID: requestID, RoundDigest: round.Digest, OwnerID: owner, Method: method, Product: round.Product, ReadinessID: ready.ID, ReadinessDigest: ready.Digest, PRReservationID: seat.ID}
	digest, err := contentDigest(value)
	if err != nil {
		return empty, err
	}
	job := ProductMerge{ID: id, Value: value, Digest: digest, State: "prepared", MutationLease: lease, CreatedAt: k.clock()}
	for _, r := range []struct {
		kind  string
		value any
	}{{"product_merge_history", value}, {"product_merge_admission", digest}} {
		if err := kernelRecordTx(ctx, tx, r.kind, id, round.Product.Value.Target.WaveID, r.value); err != nil {
			return empty, err
		}
	}
	if err := saveProductMergeTx(ctx, tx, job); err != nil {
		return empty, err
	}
	return job, tx.Commit()
}

func (k *KernelStore) ClaimProductMerge(ctx context.Context, id, owner, digest string) (ProductMergeClaim, error) {
	var out ProductMergeClaim
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
	if job.Value.OwnerID != owner || job.Digest != digest {
		return out, ErrProductMergeHeld
	}
	// Terminal receipts are readonly acknowledgements. In particular a
	// protocol abort seals every delayed physical/native start permanently.
	if productMergeTerminal(job) || job.State == "canceled" {
		return out, tx.Commit()
	}

	if job.State != "prepared" {
		return out, tx.Commit()
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
	job.OwnerProcess = process.Identify(os.Getpid())
	job.OwnerInstanceID, err = k.branchOwnerInstanceID()
	if err != nil {
		return out, err
	}
	if !job.OwnerProcess.Valid() {
		return out, ErrProductMergeHeld
	}
	job.ClaimedAt, job.State = k.clock(), "launching"
	claim := productMergePhysicalClaim{ID: id, OwnerProcess: job.OwnerProcess, OwnerInstanceID: job.OwnerInstanceID, ClaimedAt: job.ClaimedAt}
	if err := kernelRecordTx(ctx, tx, "product_merge_claim", id, job.Value.Product.Value.Target.WaveID, claim); err != nil {
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

// MarkProductMergeMayHaveSent binds the freshly measured host gate packet to
// the one-use HTTP fence atomically. A cached assessment cannot be replayed.
func (k *KernelStore) MarkProductMergeMayHaveSent(ctx context.Context, id, owner, digest string, gate ProductMergeGateObservation) (ProductMerge, error) {
	gate = canonicalMergeGateObservation(gate)
	var empty ProductMerge
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	job, err := k.readProductMergeTx(ctx, tx, id)
	if err != nil {
		return empty, err
	}
	instance, err := k.branchOwnerInstanceID()
	if err != nil {
		return empty, err
	}
	if job.Value.OwnerID != owner || job.Digest != digest || job.State != "launching" || job.HTTPAbort != nil || !job.MayHaveSentAt.IsZero() || job.OwnerProcess != process.Identify(os.Getpid()) || job.OwnerInstanceID != instance {
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
	now := k.clock()
	if validateMergeGateObservation(job, gate) != nil || gate.ObservedAt.After(now) || now.Sub(gate.ObservedAt) > 30*time.Second {
		return empty, ErrProductMergeHeld
	}
	job.GateDigest, err = contentDigest(gate)
	if err != nil {
		return empty, err
	}
	if err := kernelRecordTx(ctx, tx, "product_merge_gate", id, job.Value.Product.Value.Target.WaveID, gate); err != nil {
		return empty, err
	}

	job.State, job.MayHaveSentAt = "unknown", now
	if err := kernelRecordTx(ctx, tx, "product_merge_send", id, job.Value.Product.Value.Target.WaveID, job.MayHaveSentAt); err != nil {
		return empty, err
	}
	if err := saveProductMergeTx(ctx, tx, job); err != nil {
		return empty, err
	}
	return job, tx.Commit()
}
func (k *KernelStore) ReadProductMerge(ctx context.Context, id string) (ProductMerge, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductMerge{}, err
	}
	defer tx.Rollback()
	job, err := k.readProductMergeTx(ctx, tx, id)
	if err != nil {
		return job, err
	}
	return job, tx.Commit()
}

// CancelPreparedProductMerge proves no physical claimant or send exists. A
// launching/unknown operation needs its actual owner's stop/reconciliation,
// never an inferred not_started disposition after restart.
func (k *KernelStore) CancelPreparedProductMerge(ctx context.Context, id, owner, digest, proofID, reason string) (ProductMerge, error) {
	var empty ProductMerge
	if !validPRProofID(proofID) || strings.TrimSpace(reason) == "" || len(reason) > 16384 || !utf8.ValidString(reason) || strings.ContainsRune(reason, '\x00') {
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
	if job.Value.OwnerID != owner || job.Digest != digest {
		return empty, ErrProductMergeHeld
	}
	job, err = k.cancelPreparedProductMergeTx(ctx, tx, job, proofID, reason)
	if err != nil {
		return empty, err
	}
	return job, tx.Commit()
}

func (k *KernelStore) cancelPreparedProductMergeTx(ctx context.Context, tx *sql.Tx, job ProductMerge, proofID, reason string) (ProductMerge, error) {
	var empty ProductMerge
	if job.State == "canceled" {
		if job.Cancellation.ProofID != proofID || job.Cancellation.Reason != reason {
			return empty, ErrProductMergeHeld
		}
		return job, nil
	}
	if job.State != "prepared" {
		return empty, ErrProductMergeHeld
	}
	if err := releaseProductMergeMutationTx(ctx, tx, job); err != nil {
		return empty, err
	}
	cancellation := ProductMergeCancellation{ProofID: proofID, Reason: reason, At: k.clock()}
	job.State, job.Cancellation = "canceled", &cancellation
	if err := kernelRecordTx(ctx, tx, "product_merge_cancel", job.ID, job.Value.Product.Value.Target.WaveID, cancellation); err != nil {
		return empty, err
	}
	if err := saveProductMergeTx(ctx, tx, job); err != nil {
		return empty, err
	}
	return job, nil
}
