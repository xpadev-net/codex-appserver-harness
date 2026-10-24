package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type ProductPublicationReadinessValue struct {
	Scope      *ProductPublicationScope `json:"scope,omitempty"`
	Product    ProductTargetSnapshot    `json:"product"`
	Quality    ProductQualityProof      `json:"quality"`
	PreparedAt time.Time                `json:"prepared_at"`
}

type ProductPublicationReadiness struct {
	ID     string                           `json:"id"`
	Value  ProductPublicationReadinessValue `json:"value"`
	Digest string                           `json:"digest"`
}

func loadProductPublicationReadiness(ctx context.Context, q queryer, target contract.Target, workID string) (ProductPublicationReadiness, error) {
	var saved, history ProductPublicationReadiness
	if err := taskEvidenceRecord(ctx, q, "product_publication_readiness", productTargetID(target, workID), &saved); err != nil {
		return saved, err
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	qualityDigest, err := contentDigest(saved.Value.Quality.Value)
	if err != nil {
		return saved, err
	}
	productDigest, err := contentDigest(saved.Value.Product.Value)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.Value.PreparedAt.IsZero() || saved.Value.Quality.Digest != qualityDigest || saved.Value.Product.Digest != productDigest || saved.ID != structureID("product-publication-ready-", target.WaveID, qualityDigest) || saved.Value.Product.Value.WorkItem.ID != workID || saved.Value.Quality.Value.ProductDigest != productDigest || saved.Value.Product.Value.Target != saved.Value.Quality.Value.Target || saved.Value.Quality.Value.WorkItemID != workID || saved.Value.Quality.Value.Candidate != saved.Value.Product.Value.Candidate || saved.Value.Product.Value.WorkItem.Phase != domain.Reviewing {
		return saved, ErrTaskEvidence
	}
	if err := taskEvidenceRecord(ctx, q, "product_publication_readiness_history", saved.ID, &history); err != nil {
		return saved, errors.Join(ErrTaskEvidence, err)
	}
	if !reflect.DeepEqual(saved, history) {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

// PrepareProductPublication derives local quality and changes only the current
// WorkItem to REVIEWING. It creates no PR, approval, merge or Wave acceptance.
// All source/result/runner/reviewer facts and the projection update share a Tx.
func (k *KernelStore) PrepareProductPublication(ctx context.Context, target contract.Target, workID string) (ProductPublicationReadiness, error) {
	var saved ProductPublicationReadiness
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	quality, err := k.productQualityTx(ctx, tx, target, workID)
	if err != nil {
		return saved, err
	}
	product, err := loadProductTargetTx(ctx, tx, target, workID)
	if err != nil {
		return saved, err
	}
	if product.Value.WorkItem.MergeHold {
		return saved, ErrProductQualityHeld
	}
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return saved, err
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, workID)
	if err != nil {
		return saved, err
	}
	var scope *ProductPublicationScope
	full, err := productPublicationFullScope(w, registration)
	if err != nil {
		return saved, err
	}
	if !full {
		value, err := k.prepareProductPublicationScopeTx(ctx, tx, target, workID)
		if err != nil {
			return saved, err
		}
		scope = &value
	}
	switch product.Value.WorkItem.Phase {
	case domain.Verifying:
		value := product.Value
		value.WorkItem.Phase, value.WorkItem.PhaseStartedAt = domain.Reviewing, k.clock()
		value.WorkItem.Version++
		product, err = k.setProductTargetTx(ctx, tx, product.Digest, value)
		if err != nil {
			return saved, err
		}
		quality, err = k.productQualityTx(ctx, tx, target, workID)
		if err != nil {
			return saved, err
		}
	case domain.Reviewing:
		prior, err := loadProductPublicationReadiness(ctx, tx, target, workID)
		if err != nil {
			return saved, errors.Join(ErrTaskEvidence, err)
		}
		if !reflect.DeepEqual(prior.Value.Scope, scope) {
			return saved, ErrTaskEvidence
		}
		old := prior.Value.Product.Value
		if old.Target.WaveID != target.WaveID || old.Target.ScopeRevision != target.ScopeRevision || old.Target.PolicyRevision != target.PolicyRevision || old.Candidate != product.Value.Candidate {
			return saved, ErrTaskEvidence
		}
		if reflect.DeepEqual(prior.Value.Product, product) && reflect.DeepEqual(prior.Value.Quality, quality) {
			return prior, tx.Commit()
		}
	default:
		return saved, ErrProductQualityHeld
	}
	saved.Value = ProductPublicationReadinessValue{Scope: scope, Product: product, Quality: quality, PreparedAt: k.clock()}
	saved.ID = structureID("product-publication-ready-", target.WaveID, quality.Digest)
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	var original ProductPublicationReadiness
	if err := taskEvidenceRecord(ctx, tx, "product_publication_readiness_history", saved.ID, &original); err == nil {
		return saved, ErrTaskEvidence
	} else if !errors.Is(err, sql.ErrNoRows) {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_publication_readiness_history", saved.ID, target.WaveID, saved); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_publication_readiness", productTargetID(target, workID), target.WaveID, saved); err != nil {
		return saved, err
	}
	if err := k.waveEventTx(ctx, tx, w, "product_publication_ready", "", saved.ID+":ready", saved); err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

// ReadProductPublicationReadiness is a fresh proof read, not a cached send grant.
func (k *KernelStore) ReadProductPublicationReadiness(ctx context.Context, target contract.Target, workID string) (ProductPublicationReadiness, error) {
	var saved ProductPublicationReadiness
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	saved, err = loadProductPublicationReadiness(ctx, tx, target, workID)
	if err != nil {
		return saved, err
	}
	quality, err := k.productQualityTx(ctx, tx, target, workID)
	if err != nil {
		return saved, err
	}
	product, err := loadProductTargetTx(ctx, tx, target, workID)
	if err != nil {
		return saved, err
	}
	if product.Value.WorkItem.MergeHold || product.Value.WorkItem.Phase != domain.Reviewing || !reflect.DeepEqual(saved.Value.Product, product) || !reflect.DeepEqual(saved.Value.Quality, quality) {
		return saved, ErrProductQualityHeld
	}
	if err := k.checkProductPublicationScopeTx(ctx, tx, saved); err != nil {
		return saved, err
	}
	return saved, nil
}

func (k *KernelStore) checkProductPublicationScopeTx(ctx context.Context, tx *sql.Tx, ready ProductPublicationReadiness) error {
	p := ready.Value.Product.Value
	w, err := loadWave(ctx, tx, p.Target.WaveID)
	if err != nil {
		return err
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, p.WorkItem.ID)
	if err != nil {
		return err
	}
	if w.Target() != p.Target {
		return ErrProductPublicationScopeHeld
	}
	full, err := productPublicationFullScope(w, registration)
	if err != nil {
		return err
	}
	if full {
		if ready.Value.Scope != nil {
			return ErrTaskEvidence
		}
		return nil
	}
	if ready.Value.Scope == nil {
		return ErrProductPublicationScopeHeld
	}
	current, err := k.readProductPublicationScopeTx(ctx, tx, p.Target, p.WorkItem.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(*ready.Value.Scope, current) {
		return ErrTaskEvidence
	}
	return nil
}
