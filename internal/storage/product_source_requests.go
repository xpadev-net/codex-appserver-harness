package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ProductSourceRequestAdoptionRequest is trusted operator/source-controller
// input. Models and Issue text cannot invoke this metadata authority.
type ProductSourceRequestAdoptionRequest struct {
	Target            contract.Target `json:"target"`
	DirectionID       string          `json:"direction_id"`
	ObservationID     string          `json:"observation_id"`
	ObservationDigest string          `json:"observation_digest"`
	WorkItemID        string          `json:"work_item_id"`
}
type ProductSourceRequestAdoptionValue struct {
	Request               ProductSourceRequestAdoptionRequest `json:"request"`
	RequestDigest         string                              `json:"request_digest"`
	OriginalRequestDigest string                              `json:"original_request_digest"`
	DirectionDigest       string                              `json:"direction_digest"`
	ContinuationID        string                              `json:"continuation_id"`
	ContinuationDigest    string                              `json:"continuation_digest"`
	PolicyDigest          string                              `json:"policy_digest"`
	CreatedAt             time.Time                           `json:"created_at"`
}
type ProductSourceRequestAdoption struct {
	ID     string                            `json:"id"`
	Value  ProductSourceRequestAdoptionValue `json:"value"`
	Digest string                            `json:"digest"`
}

func sourceRequestEpoch(t contract.Target) string {
	return structureID("source-request-epoch-", t.WaveID, strconv.Itoa(t.ScopeRevision)+":"+strconv.Itoa(t.PolicyRevision))
}

func (k *KernelStore) checkProductSourceRequestTx(ctx context.Context, tx *sql.Tx, w domain.Wave, saved ProductSourceRequestAdoption) error {
	v, r := saved.Value, saved.Value.Request
	digest, err := contentDigest(v)
	if err != nil {
		return err
	}
	epoch := sourceRequestEpoch(r.Target)
	if saved.Digest != digest || saved.ID != structureID("source-request-", epoch, digest) || !r.Target.Valid() || w.Source != "github" || w.Policy.Stage != "P" || !w.Policy.Product || r.Target.WaveID != w.ID || r.Target.ScopeRevision != w.ScopeRevision || r.Target.PolicyRevision != w.Policy.Revision || v.CreatedAt.IsZero() || v.OriginalRequestDigest != ProductSourceRequestDigest(w.OriginalRequest) || !validSourceDigest(v.RequestDigest) || w.DirectionID != r.DirectionID {
		return ErrTaskEvidence
	}
	var direction WaveDirection
	if err := taskEvidenceRecord(ctx, tx, "wave_direction", r.DirectionID, &direction); err != nil {
		return errors.Join(ErrTaskEvidence, err)
	}
	directionDigest, err := contentDigest(direction.Request)
	if err != nil {
		return err
	}
	if direction.ID != r.DirectionID || direction.Digest != directionDigest || directionDigest != v.DirectionDigest || direction.State != "applied" || direction.AppliedTarget.WaveID != w.ID || direction.AppliedTarget.ScopeRevision != w.ScopeRevision || direction.AppliedTarget.PolicyRevision != w.Policy.Revision || ProductSourceRequestDigest(direction.Request.Instruction) != v.RequestDigest {
		return ErrTaskEvidence
	}
	var observed ProductSourceSnapshot
	if err := taskEvidenceRecord(ctx, tx, "product_source_observation_history", r.ObservationID, &observed); err != nil {
		return errors.Join(ErrTaskEvidence, err)
	}
	observationDigest, err := contentDigest(observed.Value)
	if err != nil {
		return err
	}
	o, f := observed.Value, observed.Value.Facts
	if observed.ID != r.ObservationID || observed.Digest != observationDigest || observationDigest != r.ObservationDigest || observed.Version < 1 || observed.ObservedAt.IsZero() || o.WaveID != w.ID || o.SourceID != w.SourceID || o.ScopeRevision != w.ScopeRevision || o.PolicyRevision != w.Policy.Revision || o.Status != "request_changed" || f.validate() != nil || !f.Available || f.IssueState != "open" || !f.Ready || f.Hold || f.RequestDigest != v.RequestDigest || f.PolicyDigest != v.PolicyDigest {
		return ErrTaskEvidence
	}
	policy, err := productSourcePolicyTx(ctx, tx, w)
	if err != nil {
		return err
	}
	var policyHead, policyHistory productSourcePolicyRecord
	if err := taskEvidenceRecord(ctx, tx, "product_source_policy", productSourcePolicyID(w), &policyHead); err != nil {
		return errors.Join(ErrTaskEvidence, err)
	}
	if err := taskEvidenceRecord(ctx, tx, "product_source_policy_history", productSourcePolicyID(w), &policyHistory); err != nil {
		return errors.Join(ErrTaskEvidence, err)
	}
	if policy != v.PolicyDigest || !reflect.DeepEqual(policyHead, policyHistory) {
		return ErrTaskEvidence
	}
	lineage, err := k.productContinuationInputsTx(ctx, tx, w, r.WorkItemID)
	if err != nil {
		return errors.Join(ErrTaskEvidence, err)
	}
	if len(lineage) == 0 || lineage[0].ID != v.ContinuationID || lineage[0].Digest != v.ContinuationDigest {
		return ErrTaskEvidence
	}
	coverage, err := k.productCoverageTx(ctx, tx, w)
	if err != nil {
		return errors.Join(ErrTaskEvidence, err)
	}
	for _, retired := range coverage.Value.Retired {
		if retired.WorkItemID == w.SourceID && retired.CurrentWorkItemID == r.WorkItemID {
			return nil
		}
	}
	return ErrTaskEvidence
}

// Missing records never fall back to the original request once independently
// retained admission/history establishes this epoch's adopted request.
func (k *KernelStore) productSourceRequestTx(ctx context.Context, tx *sql.Tx, w domain.Wave) (*ProductSourceRequestAdoption, error) {
	return k.loadProductSourceRequestTx(ctx, tx, w, false)
}

func knownProductSourceRequestTx(ctx context.Context, tx *sql.Tx, w domain.Wave) (bool, error) {
	var known bool
	err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE (kind IN ('product_source_request','product_source_request_admission') AND work_item_id=?) OR (kind='product_source_request_history' AND json_extract(payload,'$.value.request.target.wave_id')=?))", w.ID, w.ID).Scan(&known)
	return known, err
}

// Only observation and explicit adoption may inspect an as-yet unadopted new
// epoch. Task inputs must hold once any earlier adopted request is known.
func (k *KernelStore) loadProductSourceRequestTx(ctx context.Context, tx *sql.Tx, w domain.Wave, allowNewEpoch bool) (*ProductSourceRequestAdoption, error) {
	if w.Source != "github" || w.Policy.Stage != "P" || !w.Policy.Product {
		return nil, nil
	}
	epoch := sourceRequestEpoch(w.Target())
	var saved ProductSourceRequestAdoption
	err := taskEvidenceRecord(ctx, tx, "product_source_request", epoch, &saved)
	if errors.Is(err, sql.ErrNoRows) {
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_source_request_admission' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_source_request_history' AND work_item_id=?)", epoch, epoch).Scan(&known); err != nil {
			return nil, err
		}
		if known {
			return nil, ErrTaskEvidence
		}
		if !allowNewEpoch {
			known, err := knownProductSourceRequestTx(ctx, tx, w)
			if err != nil {
				return nil, err
			}
			if known {
				return nil, ErrTaskEvidence
			}
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var history ProductSourceRequestAdoption
	var admission string
	if err := taskEvidenceRecord(ctx, tx, "product_source_request_history", saved.ID, &history); err != nil {
		return nil, errors.Join(ErrTaskEvidence, err)
	}
	if err := taskEvidenceRecord(ctx, tx, "product_source_request_admission", epoch, &admission); err != nil {
		return nil, errors.Join(ErrTaskEvidence, err)
	}
	if !reflect.DeepEqual(saved, history) || saved.Digest != admission {
		return nil, ErrTaskEvidence
	}
	if err := k.checkProductSourceRequestTx(ctx, tx, w, saved); err != nil {
		return nil, err
	}
	return &saved, nil
}

// AdoptProductSourceRequest saves one immutable adoption per scope/policy epoch.
// It grants no dispatch, clears no hold and changes no original request/budget.
// A new plan must capture it; a fresh host GET must then confirm source readiness.
func (k *KernelStore) AdoptProductSourceRequest(ctx context.Context, r ProductSourceRequestAdoptionRequest) (ProductSourceRequestAdoption, error) {
	var saved ProductSourceRequestAdoption
	if !r.Target.Valid() || r.DirectionID == "" || r.ObservationID == "" || !validSourceDigest(r.ObservationDigest) || r.WorkItemID == "" {
		return saved, ErrTaskEvidence
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return saved, err
	}
	prior, err := k.loadProductSourceRequestTx(ctx, tx, w, true)
	if err != nil {
		return saved, err
	}
	if prior != nil {
		if prior.Value.Request != r {
			return saved, ErrTaskEvidence
		}
		return *prior, nil
	}
	if w.Source != "github" || !productContinuationRegistrationAllowed(w, r.Target, k.clock()) || w.SourceHold == nil || w.SourceHold.Reason != "request_changed" || w.SourceHold.Digest != r.ObservationDigest {
		return saved, ErrTaskEvidence
	}
	observation, err := loadProductSourceSnapshot(ctx, tx, w.ID)
	if err != nil {
		return saved, err
	}
	if observation.ID != r.ObservationID || observation.Digest != r.ObservationDigest {
		return saved, ErrTaskEvidence
	}
	var direction WaveDirection
	if err := taskEvidenceRecord(ctx, tx, "wave_direction", r.DirectionID, &direction); err != nil {
		return saved, err
	}
	lineage, err := k.productContinuationInputsTx(ctx, tx, w, r.WorkItemID)
	if err != nil {
		return saved, err
	}
	if len(lineage) == 0 {
		return saved, ErrTaskEvidence
	}
	saved.Value = ProductSourceRequestAdoptionValue{Request: r, RequestDigest: observation.Value.Facts.RequestDigest, OriginalRequestDigest: ProductSourceRequestDigest(w.OriginalRequest), DirectionDigest: direction.Digest, ContinuationID: lineage[0].ID, ContinuationDigest: lineage[0].Digest, PolicyDigest: observation.Value.Facts.PolicyDigest, CreatedAt: k.clock()}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	epoch := sourceRequestEpoch(r.Target)
	saved.ID = structureID("source-request-", epoch, saved.Digest)
	if err := k.checkProductSourceRequestTx(ctx, tx, w, saved); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_source_request", epoch, w.ID, saved); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_source_request_history", saved.ID, epoch, saved); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_source_request_admission", epoch, w.ID, saved.Digest); err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}
