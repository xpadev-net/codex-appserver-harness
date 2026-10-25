package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"
)

type ProductSplitCreationReference struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}
type ProductSplitPreparationValue struct {
	ParentHeadID     string                          `json:"parent_head_id"`
	WaveID           string                          `json:"wave_id"`
	RequestDigest    string                          `json:"request_digest"`
	ProposalID       string                          `json:"proposal_id"`
	ProposalDigest   string                          `json:"proposal_digest"`
	PlanID           string                          `json:"plan_id"`
	PlanDigest       string                          `json:"plan_digest"`
	AllocationID     string                          `json:"allocation_id"`
	AllocationDigest string                          `json:"allocation_digest"`
	Creations        []ProductSplitCreationReference `json:"creations"`
	CreatedAt        time.Time                       `json:"created_at"`
}
type ProductSplitPreparation struct {
	ID     string                       `json:"id"`
	Value  ProductSplitPreparationValue `json:"value"`
	Digest string                       `json:"digest"`
}

// PrepareProductSplitExecution is a trusted controller port. Proposal, all
// child intents/seats, initial creation journals and the complete-set receipt
// commit together. It neither sends HTTP nor registers child execution.
func (k *KernelStore) PrepareProductSplitExecution(ctx context.Context, r ProductSplitProposalRequest) (ProductSplitPreparation, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitPreparation{}, err
	}
	defer tx.Rollback()
	saved, err := k.prepareProductSplitExecutionTx(ctx, tx, r)
	if err != nil {
		return ProductSplitPreparation{}, err
	}
	if err := tx.Commit(); err != nil {
		return ProductSplitPreparation{}, err
	}
	return saved, nil
}

func loadProductSplitPreparationTx(ctx context.Context, tx *sql.Tx, head string) (ProductSplitPreparation, error) {
	var saved, history ProductSplitPreparation
	err := taskEvidenceRecord(ctx, tx, "product_split_preparation", head, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_preparation_admission' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_preparation_history','product_split_preparation_creation') AND (work_item_id=? OR json_extract(payload,'$.parent_head_id')=?))", head, head, head).Scan(&known); err != nil {
			return saved, err
		}
		if known {
			return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
		}
		return saved, sql.ErrNoRows
	}
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.ID != structureID("split-preparation-", head, digest) || len(saved.Value.Creations) < 2 || len(saved.Value.Creations) > maxProductWorkItems || saved.Value.CreatedAt.IsZero() {
		return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_preparation_history", saved.ID, &history); err != nil {
		return saved, splitEvidenceErr(err)
	}
	var admission ProductSplitPreparation
	if err := taskEvidenceRecord(ctx, tx, "product_split_preparation_admission", head, &admission); err != nil {
		return saved, splitEvidenceErr(err)
	}
	if !reflect.DeepEqual(saved, history) || !reflect.DeepEqual(saved, admission) {
		return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	for _, creation := range saved.Value.Creations {
		var witness splitPreparationCreation
		if err := taskEvidenceRecord(ctx, tx, "product_split_preparation_creation", creation.ID, &witness); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if witness != (splitPreparationCreation{head, saved.Value.ProposalID, creation.Digest}) {
			return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
		}
	}
	return saved, nil
}

func (k *KernelStore) prepareProductSplitExecutionTx(ctx context.Context, tx *sql.Tx, r ProductSplitProposalRequest) (ProductSplitPreparation, error) {
	var empty ProductSplitPreparation
	head := splitProposalHead(r)
	prior, err := loadProductSplitPreparationTx(ctx, tx, head)
	exists := err == nil
	if err != nil && err != sql.ErrNoRows {
		return empty, err
	}
	proposal, err := k.prepareProductSplitProposalTx(ctx, tx, r)
	if err != nil {
		return empty, err
	}
	request := ProductSplitChildPlanRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID, ProposalID: proposal.ID, ProposalDigest: proposal.Digest}
	plan, err := k.prepareProductSplitChildPlanTx(ctx, tx, request)
	if err != nil {
		return empty, err
	}
	allocation, err := k.reserveProductSplitChildrenTx(ctx, tx, request)
	if err != nil {
		return empty, err
	}
	requestDigest, err := contentDigest(r)
	if err != nil {
		return empty, err
	}
	value := ProductSplitPreparationValue{ParentHeadID: head, WaveID: r.Target.WaveID, RequestDigest: requestDigest, ProposalID: proposal.ID, ProposalDigest: proposal.Digest, PlanID: plan.Set.ID, PlanDigest: plan.Set.Digest, AllocationID: allocation.ID, AllocationDigest: allocation.Digest, Creations: []ProductSplitCreationReference{}, CreatedAt: k.clock()}
	for _, child := range plan.Children {
		owner := structureID("split-controller-", proposal.ID, child.ID)
		var job ProductSplitCreate
		if exists {
			// A complete-set receipt is independent proof that this journal
			// already existed, even if every per-child index/lease was lost.
			job, err = loadSplitCreate(ctx, tx, splitCreateID(child.ID))
		} else {
			job, err = k.prepareProductSplitCreateTx(ctx, tx, request, child.ID, owner)
		}
		if err != nil {
			return empty, splitEvidenceErr(err)
		}
		if job.Value.Request != request || job.Value.Child.ID != child.ID || job.Value.OwnerID != owner {
			return empty, ErrProductSplitHeld
		}
		value.Creations = append(value.Creations, ProductSplitCreationReference{ID: job.ID, Digest: job.Digest})
	}
	if exists {
		value.CreatedAt = prior.Value.CreatedAt
		if !reflect.DeepEqual(value, prior.Value) {
			return empty, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
		}
		return prior, nil
	}
	digest, err := contentDigest(value)
	if err != nil {
		return empty, err
	}
	saved := ProductSplitPreparation{ID: structureID("split-preparation-", head, digest), Value: value, Digest: digest}
	for _, record := range []struct {
		kind, id, owner string
		value           any
	}{
		{"product_split_preparation", head, r.Target.WaveID, saved},
		{"product_split_preparation_history", saved.ID, head, saved},
		{"product_split_preparation_admission", head, r.Target.WaveID, saved},
	} {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, record.owner, record.value); err != nil {
			return empty, err
		}
	}
	for _, creation := range value.Creations {
		if err := kernelRecordTx(ctx, tx, "product_split_preparation_creation", creation.ID, r.Target.WaveID, splitPreparationCreation{head, proposal.ID, creation.Digest}); err != nil {
			return empty, err
		}
	}
	return saved, nil
}

type splitPreparationCreation struct {
	ParentHeadID string `json:"parent_head_id"`
	ProposalID   string `json:"proposal_id"`
	Digest       string `json:"digest"`
}
