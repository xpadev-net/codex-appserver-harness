package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrProductContinuationHeld = errors.New("versioned product continuation requires an authenticated original merge and current new contract")

// ProductContinuationRequest is a trusted source-controller operation, never
// policy supplied by an Issue body or a model. It records lineage only.
type ProductContinuationRequest struct {
	Target              contract.Target           `json:"target"`
	Key                 string                    `json:"key"`
	ParentMergeID       string                    `json:"parent_merge_id"`
	ParentReceiptDigest string                    `json:"parent_receipt_digest"`
	Definition          ProductWorkItemDefinition `json:"definition"`
}

type ProductContinuationValue struct {
	Request                  ProductContinuationRequest          `json:"request"`
	Registration             ProductWorkItemRegistrationSnapshot `json:"registration"`
	ParentWorkItemID         string                              `json:"parent_work_item_id"`
	ParentTarget             contract.Target                     `json:"parent_target"`
	ParentRegistrationDigest string                              `json:"parent_registration_digest"`
	ParentProjectionDigest   string                              `json:"parent_projection_digest"`
	ParentCandidateSHA       string                              `json:"parent_candidate_sha"`
	ParentMergeSHA           string                              `json:"parent_merge_sha"`
	DirectionID              string                              `json:"direction_id,omitempty"`
	CreatedAt                time.Time                           `json:"created_at"`
}

type ProductContinuation struct {
	ID     string                   `json:"id"`
	Value  ProductContinuationValue `json:"value"`
	Digest string                   `json:"digest"`
}

type productContinuationRequestBinding struct {
	WorkItemID    string `json:"work_item_id"`
	ID            string `json:"id"`
	Digest        string `json:"digest"`
	RequestDigest string `json:"request_digest"`
}

func productContinuationRequestID(r ProductContinuationRequest) string {
	return structureID("product-continuation-request-", r.Target.WaveID, r.Key)
}

func loadProductContinuation(ctx context.Context, q queryer, workID string) (ProductContinuation, error) {
	var saved, history ProductContinuation
	err := taskEvidenceRecord(ctx, q, "product_continuation", workID, &saved)
	if errors.Is(err, sql.ErrNoRows) {
		var known bool
		if e := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_continuation_admission' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_continuation_history' AND work_item_id=?)", workID, workID).Scan(&known); e != nil {
			return saved, e
		}
		if known {
			return saved, ErrTaskEvidence
		}
	}
	if err != nil {
		return saved, err
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.ID != structureID("product-continuation-", workID, digest) || saved.Value.Request.Definition.ID != workID || saved.Value.Registration.Value.Definition.ID != workID || saved.Value.CreatedAt.IsZero() {
		return saved, ErrTaskEvidence
	}
	var admitted string
	if err := taskEvidenceRecord(ctx, q, "product_continuation_admission", workID, &admitted); err != nil {
		return saved, errors.Join(ErrTaskEvidence, err)
	}
	if err := taskEvidenceRecord(ctx, q, "product_continuation_history", saved.ID, &history); err != nil {
		return saved, errors.Join(ErrTaskEvidence, err)
	}
	if admitted != digest || !reflect.DeepEqual(saved, history) {
		return saved, ErrTaskEvidence
	}
	var request productContinuationRequestBinding
	if err := taskEvidenceRecord(ctx, q, "product_continuation_request", productContinuationRequestID(saved.Value.Request), &request); err != nil {
		return saved, errors.Join(ErrTaskEvidence, err)
	}
	requestDigest, err := contentDigest(saved.Value.Request)
	if err != nil {
		return saved, err
	}
	if request != (productContinuationRequestBinding{WorkItemID: workID, ID: saved.ID, Digest: digest, RequestDigest: requestDigest}) {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

func (k *KernelStore) continuationParentTx(ctx context.Context, q queryer, request ProductContinuationRequest, w domain.Wave) (ProductMergedWorkItem, error) {
	var empty ProductMergedWorkItem
	job, err := k.readProductMergeTx(ctx, q, request.ParentMergeID)
	if err != nil {
		return empty, err
	}
	parent, err := readMergedWorkItem(ctx, q, job, k.clock())
	if err != nil {
		return empty, err
	}
	p := parent.Value.Product.Value
	if parent.Digest != request.ParentReceiptDigest || parent.Value.Projection == nil || p.Target.WaveID != w.ID || p.WorkItem.ID == request.Definition.ID || p.WorkItem.RepositoryID != request.Definition.RepositoryID || p.Repository != request.Definition.Repository || p.Target.ScopeRevision > w.ScopeRevision || p.Target.PolicyRevision > w.Policy.Revision || p.Target.ScopeRevision == w.ScopeRevision && p.Target.PolicyRevision == w.Policy.Revision {
		return empty, ErrProductContinuationHeld
	}
	var registration ProductWorkItemRegistrationSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_work_item_registration_version", structureID("product-work-", p.WorkItem.ID, parent.Value.RegistrationDigest), &registration); err != nil {
		return empty, err
	}
	if registration.Value.Source != w.Source || registration.Value.SourceID != w.SourceID {
		return empty, ErrProductContinuationHeld
	}
	var current ProductTargetSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_target", productTargetID(p.Target, p.WorkItem.ID), &current); err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(current, parent.Value.Projection.After) || current.Value.WorkItem.Phase != domain.Merged {
		return empty, ErrProductContinuationHeld
	}
	return parent, nil
}

// RegisterProductContinuation atomically registers a new WorkItem and its
// immutable link to an earlier version's known merged WorkItem. It never edits
// the original PR/projection, creates Tasks, accepts quality or resets budgets.
// Task-input/coverage/gateway integration is deliberately a separate port.
func (k *KernelStore) RegisterProductContinuation(ctx context.Context, request ProductContinuationRequest) (ProductContinuation, error) {
	var saved ProductContinuation
	if !request.Target.Valid() || request.Key == "" || len(request.Key) > 256 || !utf8.ValidString(request.Key) || strings.ContainsAny(request.Key, "\x00\r\n") || request.ParentMergeID == "" || !validSourceDigest(request.ParentReceiptDigest) {
		return saved, ErrProductContinuationHeld
	}
	request.Definition.Criteria = slices.Clone(request.Definition.Criteria)
	slices.Sort(request.Definition.Criteria)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	prior, readErr := loadProductContinuation(ctx, tx, request.Definition.ID)
	if readErr == nil {
		if !reflect.DeepEqual(prior.Value.Request, request) {
			return saved, ErrProductContinuationHeld
		}
		if err := k.checkProductContinuationTx(ctx, tx, prior); err != nil {
			return saved, err
		}
		return prior, tx.Commit()
	}
	if !errors.Is(readErr, sql.ErrNoRows) {
		return saved, readErr
	}
	var previousRequest productContinuationRequestBinding
	if err := taskEvidenceRecord(ctx, tx, "product_continuation_request", productContinuationRequestID(request), &previousRequest); err == nil {
		return saved, ErrProductContinuationHeld
	} else if !errors.Is(err, sql.ErrNoRows) {
		return saved, err
	}
	w, err := loadWave(ctx, tx, request.Target.WaveID)
	if err != nil {
		return saved, err
	}
	if w.Target() != request.Target || w.Policy.Stage != "P" || !w.Policy.Product || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return saved, ErrProductContinuationHeld
	}
	parent, err := k.continuationParentTx(ctx, tx, request, w)
	if err != nil {
		return saved, err
	}
	var known bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_work_item_registration','wave_work_item')) OR EXISTS(SELECT 1 FROM records WHERE kind='product_work_item_registration_version' AND json_extract(payload,'$.value.definition.id')=?)", request.Definition.ID, request.Definition.ID).Scan(&known); err != nil {
		return saved, err
	}
	if known {
		return saved, ErrProductContinuationHeld
	}
	registration, err := k.registerProductWorkItemTx(ctx, tx, request.Target, "", request.Definition)
	if err != nil {
		return saved, err
	}
	p := parent.Value.Product.Value
	saved.Value = ProductContinuationValue{Request: request, Registration: registration, ParentWorkItemID: p.WorkItem.ID, ParentTarget: p.Target, ParentRegistrationDigest: parent.Value.RegistrationDigest, ParentProjectionDigest: parent.Value.Projection.After.Digest, ParentCandidateSHA: p.Candidate.SHA, ParentMergeSHA: parent.Value.MergeSHA, DirectionID: w.DirectionID, CreatedAt: k.clock()}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	saved.ID = structureID("product-continuation-", request.Definition.ID, saved.Digest)
	requestDigest, err := contentDigest(request)
	if err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_continuation_request", productContinuationRequestID(request), w.ID, productContinuationRequestBinding{WorkItemID: request.Definition.ID, ID: saved.ID, Digest: saved.Digest, RequestDigest: requestDigest}); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_continuation_history", saved.ID, request.Definition.ID, saved); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_continuation_admission", request.Definition.ID, w.ID, saved.Digest); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_continuation", request.Definition.ID, w.ID, saved); err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

func (k *KernelStore) checkProductContinuationTx(ctx context.Context, q queryer, saved ProductContinuation) error {
	w, err := loadWave(ctx, q, saved.Value.Request.Target.WaveID)
	if err != nil {
		return err
	}
	r, err := loadProductWorkItemRegistration(ctx, q, saved.Value.Request.Definition.ID)
	if err != nil {
		return err
	}
	if !productRegistrationScope(r, w) || !reflect.DeepEqual(r, saved.Value.Registration) {
		return ErrProductContinuationHeld
	}
	var version ProductWorkItemRegistrationSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_work_item_registration_version", structureID("product-work-", r.Value.Definition.ID, r.Digest), &version); err != nil {
		return err
	}
	if !reflect.DeepEqual(version, r) {
		return ErrTaskEvidence
	}
	parent, err := k.continuationParentTx(ctx, q, saved.Value.Request, w)
	if err != nil {
		return err
	}
	p := parent.Value.Product.Value
	if saved.Value.ParentWorkItemID != p.WorkItem.ID || saved.Value.ParentTarget != p.Target || saved.Value.ParentRegistrationDigest != parent.Value.RegistrationDigest || saved.Value.ParentProjectionDigest != parent.Value.Projection.After.Digest || saved.Value.ParentCandidateSHA != p.Candidate.SHA || saved.Value.ParentMergeSHA != parent.Value.MergeSHA || !reflect.DeepEqual(saved.Value.Request.Definition, r.Value.Definition) {
		return ErrTaskEvidence
	}
	return nil
}

func (k *KernelStore) ReadProductContinuation(ctx context.Context, workID string) (ProductContinuation, error) {
	var saved ProductContinuation
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	saved, err = loadProductContinuation(ctx, tx, workID)
	if err != nil {
		return saved, err
	}
	if err := k.checkProductContinuationTx(ctx, tx, saved); err != nil {
		return saved, err
	}
	return saved, nil
}
