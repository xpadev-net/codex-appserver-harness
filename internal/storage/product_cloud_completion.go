package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

var ErrProductCloudHeld = errors.New("Cloud work requires a mapped terminal provider response")

// Normalized native API facts supplied by the trusted host reader. No model
// tool accepts these fields, a completion boolean or a caller-selected actor set.
type ProductNativeReviewFact struct {
	ID          int64     `json:"id"`
	ReviewerID  int64     `json:"reviewer_id"`
	State       string    `json:"state"`
	SHA         string    `json:"sha"`
	SubmittedAt time.Time `json:"submitted_at"`
}
type ProductCloudCompletionValue struct {
	OperationID string                    `json:"operation_id"`
	RoundDigest string                    `json:"round_digest"`
	ProofID     string                    `json:"proof_id"`
	ReadAt      time.Time                 `json:"read_at"`
	Report      contract.Report           `json:"report"`
	Reviews     []ProductNativeReviewFact `json:"reviews"`
}
type ProductCloudReviewCompletion struct {
	Value  ProductCloudCompletionValue `json:"value"`
	Digest string                      `json:"digest"`
}

func validateCloudCompletion(round ProductCloudRound, value ProductCloudCompletionValue) error {
	p := round.Product.Value
	if value.OperationID != round.OperationID || value.RoundDigest != round.Digest || !validPRProofID(value.ProofID) || value.ReadAt.IsZero() || value.ReadAt.Before(round.RequestedAt) || len(round.Packet.CloudReviewerIDs) == 0 || len(value.Reviews) == 0 || len(value.Reviews) > 128 {
		return ErrProductCloudHeld
	}
	report := value.Report
	if report.Version != 1 || report.ObservedAt.IsZero() || report.ObservedAt.Before(round.RequestedAt) || report.ObservedAt.After(value.ReadAt) || report.RepositoryID != p.WorkItem.RepositoryID || report.Repository != p.Repository || report.DefaultBranch != p.BaseBranch || len(report.Capabilities) > 128 {
		return ErrProductCloudHeld
	}
	for _, name := range []string{"cloud_request", "review_sha"} {
		c := report.Capabilities[name]
		if c.State != contract.Verified || strings.TrimSpace(c.Evidence) == "" {
			return ErrProductCloudHeld
		}
	}
	for i, v := range value.Reviews {
		if v.ID < 1 || i > 0 && value.Reviews[i-1].ID >= v.ID || !slices.Contains(round.Packet.CloudReviewerIDs, v.ReviewerID) || v.SHA != p.Candidate.SHA || v.SubmittedAt.IsZero() || v.SubmittedAt.Before(round.RequestedAt) || v.SubmittedAt.After(value.ReadAt) {
			return ErrProductCloudHeld
		}
		switch v.State {
		case "APPROVED", "COMMENTED", "CHANGES_REQUESTED", "DISMISSED":
		default:
			return ErrProductCloudHeld
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return ErrProductCloudHeld
	}
	return nil
}

func cloudCompletionRelease(c ProductCloudReviewCompletion) OperationReleaseProof {
	return OperationReleaseProof{OperationID: c.Value.OperationID, ProofID: c.Value.ProofID, Disposition: "completed", Reason: "verified assigned provider responded for this Cloud request; no approval or merge grant"}
}

func (k *KernelStore) loadCloudCompletionTx(ctx context.Context, tx *sql.Tx, id string) (out ProductCloudReviewCompletion, err error) {
	if err := taskEvidenceRecord(ctx, tx, "product_cloud_completion", id, &out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			var admitted bool
			if e := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_cloud_completion_history' AND id=?)", id).Scan(&admitted); e != nil {
				return out, e
			}
			if admitted {
				return out, ErrTaskEvidence
			}
			reservation, e := loadOperationReservationTx(ctx, tx, id)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return out, e
			}
			if e == nil && reservation.State == "released" && reservation.Proof != nil && reservation.Proof.Disposition == "completed" {
				return out, ErrTaskEvidence
			}
		}
		return out, err
	}
	defer func() {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrTaskEvidence
		}
	}()
	var history ProductCloudCompletionValue
	if err := taskEvidenceRecord(ctx, tx, "product_cloud_completion_history", id, &history); err != nil {
		return out, err
	}
	digest, err := contentDigest(out.Value)
	if err != nil {
		return out, err
	}
	if digest != out.Digest || out.Value.OperationID != id || !reflect.DeepEqual(history, out.Value) {
		return out, ErrTaskEvidence
	}
	round, err := k.productCloudRoundTx(ctx, tx, id)
	if err != nil {
		return out, err
	}
	if err := validateCloudCompletion(round, out.Value); err != nil {
		return out, err
	}
	r, err := loadOperationReservationTx(ctx, tx, id)
	if err != nil {
		return out, err
	}
	if r.State != "released" || r.Proof == nil || *r.Proof != cloudCompletionRelease(out) {
		return out, ErrTaskEvidence
	}
	return out, nil
}

// RecordProductCloudReviewCompleted releases only this request's known external
// work. It does not require current business authority: pause, cancellation or
// a new candidate must not prevent recording a positively identified old result.
// Completion is deliberately distinct from formal approval and Wave acceptance.
func (k *KernelStore) RecordProductCloudReviewCompleted(ctx context.Context, value ProductCloudCompletionValue) (ProductCloudReviewCompletion, error) {
	var empty ProductCloudReviewCompletion
	value.ReadAt = value.ReadAt.Round(0).UTC()
	value.Report.ObservedAt = value.Report.ObservedAt.Round(0).UTC()
	value.Report.Capabilities = maps.Clone(value.Report.Capabilities)
	value.Reviews = slices.Clone(value.Reviews)
	for i := range value.Reviews {
		value.Reviews[i].SubmittedAt = value.Reviews[i].SubmittedAt.Round(0).UTC()
	}
	slices.SortFunc(value.Reviews, func(a, b ProductNativeReviewFact) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	old, err := k.loadCloudCompletionTx(ctx, tx, value.OperationID)
	if err == nil {
		if !reflect.DeepEqual(old.Value, value) {
			return empty, ErrTaskEvidence
		}
		return old, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return empty, err
	}
	var admitted bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_cloud_completion_history' AND id=?)", value.OperationID).Scan(&admitted); err != nil {
		return empty, err
	}
	if admitted {
		return empty, ErrTaskEvidence
	}
	round, err := k.productCloudRoundTx(ctx, tx, value.OperationID)
	if err != nil {
		return empty, err
	}
	if err := validateCloudCompletion(round, value); err != nil {
		return empty, err
	}
	if value.ReadAt.After(k.clock()) {
		return empty, ErrProductCloudHeld
	}
	r, err := loadOperationReservationTx(ctx, tx, round.ReservationID)
	if err != nil {
		return empty, err
	}
	if r.State != "held" || r.Proof != nil {
		return empty, ErrProductCloudHeld
	}
	out := ProductCloudReviewCompletion{Value: value}
	out.Digest, err = contentDigest(value)
	if err != nil {
		return empty, err
	}
	if err := k.releaseOperationCapacityTx(ctx, tx, cloudCompletionRelease(out)); err != nil {
		return empty, err
	}
	if err := kernelRecordTx(ctx, tx, "product_cloud_completion_history", value.OperationID, round.Resource.WaveID, value); err != nil {
		return empty, err
	}
	if err := kernelRecordTx(ctx, tx, "product_cloud_completion", value.OperationID, round.Resource.WaveID, out); err != nil {
		return empty, err
	}
	return out, tx.Commit()
}

func (k *KernelStore) ReadProductCloudReviewCompletion(ctx context.Context, id string) (ProductCloudReviewCompletion, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductCloudReviewCompletion{}, err
	}
	defer tx.Rollback()
	out, err := k.loadCloudCompletionTx(ctx, tx, id)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}
