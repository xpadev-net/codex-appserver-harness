package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type ProductMergedProjection struct {
	Before ProductTargetSnapshot `json:"before"`
	After  ProductTargetSnapshot `json:"after"`
}

// ProductMergedWorkItemValue records a positively known original PR merge. It
// is not approval, criterion acceptance or permission to complete a Wave.
type ProductMergedWorkItemValue struct {
	MergeID            string                   `json:"merge_id"`
	MergeDigest        string                   `json:"merge_digest"`
	Product            ProductTargetSnapshot    `json:"product"`
	RegistrationDigest string                   `json:"registration_digest"`
	ReadinessID        string                   `json:"readiness_id"`
	ReadinessDigest    string                   `json:"readiness_digest"`
	GateDigest         string                   `json:"gate_digest"`
	StopProofID        string                   `json:"stop_proof_id"`
	Criteria           []string                 `json:"criteria"`
	MergeSHA           string                   `json:"merge_sha"`
	ObservedAt         time.Time                `json:"observed_at"`
	RecordedAt         time.Time                `json:"recorded_at"`
	Projection         *ProductMergedProjection `json:"projection,omitempty"`
}

type ProductMergedWorkItem struct {
	Value  ProductMergedWorkItemValue `json:"value"`
	Digest string                     `json:"digest"`
}

func mergedWorkItemFacts(ctx context.Context, q queryer, job ProductMerge) (ProductMergedWorkItemValue, error) {
	var v ProductMergedWorkItemValue
	if job.Value.HTTPProtocol != productMergeHTTPProtocol || job.State != "merged" || job.HTTP == nil || job.HTTPStop == nil || job.Stop == nil || job.PRLifetimeRelease == nil {
		return v, ErrProductMergeHeld
	}
	var ready ProductPublicationReadiness
	if err := taskEvidenceRecord(ctx, q, "product_publication_readiness_history", job.Value.ReadinessID, &ready); err != nil {
		return v, err
	}
	quality := ready.Value.Quality
	qualityDigest, err := contentDigest(quality.Value)
	if err != nil {
		return v, err
	}
	p := job.Value.Product.Value
	if quality.Digest != qualityDigest || quality.Value.ProductDigest != job.Value.Product.Digest || quality.Value.Target != p.Target || quality.Value.Candidate != p.Candidate || quality.Value.WorkItemID != p.WorkItem.ID {
		return v, ErrTaskEvidence
	}
	var registration ProductWorkItemRegistrationSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_work_item_registration_version", structureID("product-work-", p.WorkItem.ID, quality.Value.RegistrationDigest), &registration); err != nil {
		return v, err
	}
	digest, err := contentDigest(registration.Value)
	if err != nil {
		return v, err
	}
	r := registration.Value
	if registration.Digest != digest || digest != quality.Value.RegistrationDigest || r.WaveID != p.Target.WaveID || r.ScopeRevision != p.Target.ScopeRevision || r.PolicyRevision != p.Target.PolicyRevision || r.Definition.ID != p.WorkItem.ID || r.Definition.RepositoryID != p.WorkItem.RepositoryID || r.Definition.Repository != p.Repository || len(r.Definition.Criteria) == 0 {
		return v, ErrTaskEvidence
	}
	for _, id := range r.Definition.Criteria {
		if len(quality.Value.Criteria[id]) == 0 {
			return v, ErrTaskEvidence
		}
	}
	v = ProductMergedWorkItemValue{MergeID: job.ID, MergeDigest: job.Digest, Product: job.Value.Product, RegistrationDigest: digest, ReadinessID: job.Value.ReadinessID, ReadinessDigest: job.Value.ReadinessDigest, GateDigest: job.GateDigest, StopProofID: job.Stop.ProofID, Criteria: slices.Clone(r.Definition.Criteria)}
	if job.Reconciliation != nil {
		v.MergeSHA, v.ObservedAt = job.Reconciliation.MergeSHA, job.Reconciliation.Observation.ObservedAt
	} else if job.Stop.Response != nil && job.Stop.Response.Merged {
		v.MergeSHA, v.ObservedAt = job.Stop.Response.MergeSHA, job.Stop.FinishedAt
	} else {
		return v, ErrTaskEvidence
	}
	return v, nil
}

func mergedProjectionValue(before ProductTargetSnapshot, observed, recorded time.Time) ProductTarget {
	after := before.Value
	after.WorkItem.Phase = domain.Merged
	after.WorkItem.PhaseStartedAt, after.WorkItem.MergeObservedAt = recorded, observed
	after.WorkItem.Version++
	return after
}

func readMergedWorkItem(ctx context.Context, q queryer, job ProductMerge, now time.Time) (ProductMergedWorkItem, error) {
	var saved ProductMergedWorkItem
	err := taskEvidenceRecord(ctx, q, "product_merged_work_item", job.ID, &saved)
	if errors.Is(err, sql.ErrNoRows) {
		var orphan bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_merged_work_item_history','product_merged_work_item_admission'))", job.ID).Scan(&orphan); err != nil {
			return saved, err
		}
		if orphan {
			return saved, ErrTaskEvidence
		}
		return saved, sql.ErrNoRows
	}
	if err != nil {
		return saved, err
	}
	var history ProductMergedWorkItem
	var admitted string
	if err := taskEvidenceRecord(ctx, q, "product_merged_work_item_history", job.ID, &history); err != nil {
		return saved, errors.Join(ErrTaskEvidence, err)
	}
	if err := taskEvidenceRecord(ctx, q, "product_merged_work_item_admission", job.ID, &admitted); err != nil {
		return saved, errors.Join(ErrTaskEvidence, err)
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	want, err := mergedWorkItemFacts(ctx, q, job)
	if err != nil {
		return saved, errors.Join(ErrTaskEvidence, err)
	}
	want.RecordedAt, want.Projection = saved.Value.RecordedAt, saved.Value.Projection
	if digest != saved.Digest || admitted != digest || !reflect.DeepEqual(saved, history) || !reflect.DeepEqual(want, saved.Value) || saved.Value.RecordedAt.Before(saved.Value.ObservedAt) || saved.Value.RecordedAt.IsZero() || saved.Value.RecordedAt.After(now) {
		return saved, ErrTaskEvidence
	}
	if projection := saved.Value.Projection; projection != nil {
		before := projection.Before.Value
		original := job.Value.Product.Value
		if !sameMergedCandidate(before, original) || before.WorkItem.Phase != domain.Reviewing {
			return saved, ErrTaskEvidence
		}
		for _, snapshot := range []ProductTargetSnapshot{projection.Before, projection.After} {
			digest, err := contentDigest(snapshot.Value)
			if err != nil || snapshot.Digest != digest || snapshot.Value.validate() != nil {
				return saved, ErrTaskEvidence
			}
			var observed ProductTargetSnapshot
			if err := taskEvidenceRecord(ctx, q, "product_target_observation", digest, &observed); err != nil || !reflect.DeepEqual(snapshot, observed) {
				return saved, errors.Join(ErrTaskEvidence, err)
			}
		}
		if !reflect.DeepEqual(projection.After.Value, mergedProjectionValue(projection.Before, saved.Value.ObservedAt, saved.Value.RecordedAt)) {
			return saved, ErrTaskEvidence
		}
	}
	return saved, nil
}

func sameMergedCandidate(a, b ProductTarget) bool {
	return a.Target == b.Target && a.Repository == b.Repository && a.BaseBranch == b.BaseBranch && a.PRNumber == b.PRNumber && a.Candidate == b.Candidate && a.WorkItem.ID == b.WorkItem.ID && a.WorkItem.RepositoryID == b.WorkItem.RepositoryID && a.WorkItem.Version == b.WorkItem.Version && a.WorkItem.ScopeRevision == b.WorkItem.ScopeRevision && a.WorkItem.PolicyRevision == b.WorkItem.PolicyRevision && a.WorkItem.Branch == b.WorkItem.Branch
}

func (k *KernelStore) ReadProductMergedWorkItem(ctx context.Context, id string) (ProductMergedWorkItem, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductMergedWorkItem{}, err
	}
	defer tx.Rollback()
	job, err := k.readProductMergeTx(ctx, tx, id)
	if err != nil {
		return ProductMergedWorkItem{}, err
	}
	return readMergedWorkItem(ctx, tx, job, k.clock())
}

// RecordProductMergedWorkItem derives all facts from the saved registered
// transport. It never accepts a caller-selected candidate, SHA or merged bool.
// Projection and immutable receipt commit together; old results retain history
// without overwriting a newer revision/candidate or completing its Wave.
func (k *KernelStore) RecordProductMergedWorkItem(ctx context.Context, id string) (ProductMergedWorkItem, bool, error) {
	var saved ProductMergedWorkItem
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, false, err
	}
	defer tx.Rollback()
	job, err := k.readProductMergeTx(ctx, tx, id)
	if err != nil {
		return saved, false, err
	}
	if old, err := readMergedWorkItem(ctx, tx, job, k.clock()); err == nil {
		return old, false, tx.Commit()
	} else if err != sql.ErrNoRows {
		return saved, false, err
	}
	saved.Value, err = mergedWorkItemFacts(ctx, tx, job)
	if err != nil {
		return saved, false, err
	}
	saved.Value.RecordedAt = k.clock()
	if saved.Value.RecordedAt.Before(saved.Value.ObservedAt) {
		return saved, false, ErrTaskEvidence
	}
	p := job.Value.Product.Value
	w, err := loadWave(ctx, tx, p.Target.WaveID)
	if err != nil {
		return saved, false, err
	}
	var current ProductTargetSnapshot
	if err := taskEvidenceRecord(ctx, tx, "product_target", productTargetID(p.Target, p.WorkItem.ID), &current); err != nil {
		return saved, false, err
	}
	digest, err := contentDigest(current.Value)
	if err != nil || digest != current.Digest || current.Value.validate() != nil || current.Value.Target.WaveID != p.Target.WaveID || current.Value.WorkItem.ID != p.WorkItem.ID {
		return saved, false, ErrTaskEvidence
	}
	var observed ProductTargetSnapshot
	if err := taskEvidenceRecord(ctx, tx, "product_target_observation", digest, &observed); err != nil || !reflect.DeepEqual(observed, current) {
		return saved, false, errors.Join(ErrTaskEvidence, err)
	}
	if w.Target() == p.Target && w.State != domain.WaveCanceled && w.State != domain.WaveCompleted && sameMergedCandidate(current.Value, p) && current.Value.WorkItem.Phase == domain.Reviewing {
		after, err := k.setProductTargetTx(ctx, tx, current.Digest, mergedProjectionValue(current, saved.Value.ObservedAt, saved.Value.RecordedAt))
		if err != nil {
			return saved, false, err
		}
		saved.Value.Projection = &ProductMergedProjection{Before: current, After: after}
	}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return saved, false, err
	}
	for kind, value := range map[string]any{"product_merged_work_item": saved, "product_merged_work_item_history": saved, "product_merged_work_item_admission": saved.Digest} {
		if err := kernelRecordTx(ctx, tx, kind, id, p.Target.WaveID, value); err != nil {
			return saved, false, err
		}
	}
	if err := k.waveEventTx(ctx, tx, w, "product_merge_recorded", "", id, saved); err != nil {
		return saved, false, err
	}
	if err := tx.Commit(); err != nil {
		return saved, false, err
	}
	return saved, true, nil
}
