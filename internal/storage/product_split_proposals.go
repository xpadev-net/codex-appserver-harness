package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

var ErrProductSplitHeld = errors.New("product split requires authenticated current local scope and bounded lossless mapping")

// This trusted metadata port neither creates children nor retires the parent.
// InspectProductSplitProposalContext derives registered children's local scope
// and original root mapping; durable adoption remains limited to initial roots.
type ProductSplitProposalRequest struct {
	Target             contract.Target    `json:"target"`
	Key                string             `json:"key"`
	ParentWorkItemID   string             `json:"parent_work_item_id"`
	RegistrationDigest string             `json:"registration_digest"`
	Proposal           splitting.Proposal `json:"proposal"`
}
type ProductSplitParentScopeReference struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

type ProductSplitProposalValue struct {
	ParentScopeProof          *ProductSplitParentScopeReference `json:"parent_scope_proof,omitempty"`
	RootSourceDigest          string                            `json:"root_source_digest,omitempty"`
	ParentSplitScope          *ProductSplitChildScopeReference  `json:"parent_split_scope,omitempty"`
	ParentCriterionRoots      map[string]string                 `json:"parent_criterion_roots,omitempty"`
	Request                   ProductSplitProposalRequest       `json:"request"`
	InitialRegistrationDigest string                            `json:"initial_registration_digest"`
	ParentScopeDigest         string                            `json:"parent_scope_digest"`
	SourceDigest              string                            `json:"source_digest"`
	Limits                    WaveOperationLimits               `json:"limits"`
	Depth                     int                               `json:"depth"`
	Product                   *ProductTargetSnapshot            `json:"product,omitempty"`
	CreatedAt                 time.Time                         `json:"created_at"`
}
type ProductSplitProposal struct {
	ID     string                    `json:"id"`
	Value  ProductSplitProposalValue `json:"value"`
	Digest string                    `json:"digest"`
}
type splitProposalRequestBinding struct {
	HeadID        string `json:"head_id"`
	ID            string `json:"id"`
	Digest        string `json:"digest"`
	RequestDigest string `json:"request_digest"`
}

func splitProposalHead(r ProductSplitProposalRequest) string {
	target, _ := contentDigest(r.Target)
	return structureID("product-split-head-", target, r.ParentWorkItemID)
}
func splitProposalRequestID(r ProductSplitProposalRequest) string {
	return structureID("product-split-request-", r.Target.WaveID, r.Key)
}
func splitEvidenceErr(err error) error {
	err = sourceRequestEvidenceErr(err)
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrTaskEvidence) || errors.Is(err, ErrProductPublicationScopeHeld) {
		return errors.Join(ErrProductSplitHeld, err)
	}
	return err
}

func (k *KernelStore) productSplitContextTx(ctx context.Context, tx *sql.Tx, r ProductSplitProposalRequest) (ProductSplitProposalValue, error) {
	value := ProductSplitProposalValue{Request: r}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return value, err
	}
	if w.Target() != r.Target || w.Policy.Stage != "P" || !w.Policy.Product {
		return value, ErrProductSplitHeld
	}
	registration, err := publicationScopeRegistrationTx(ctx, tx, w, r.ParentWorkItemID)
	if err != nil {
		return value, splitEvidenceErr(err)
	}
	if registration.Digest != r.RegistrationDigest {
		return value, ErrProductSplitHeld
	}
	binding, err := productSplitChildScopeTx(ctx, tx, w, registration)
	if err != nil {
		return value, splitEvidenceErr(err)
	}
	parentPolicy := ""
	if binding == nil {
		var admission productAdmissionReceipt
		if err := taskEvidenceRecord(ctx, tx, "product_wave_admission", structureID("product-admission-", w.Source, w.SourceID), &admission); err != nil {
			return value, splitEvidenceErr(err)
		}
		digest, err := contentDigest(admission.Value)
		if err != nil {
			return value, err
		}
		originalDigest := admission.Value.Registrations[r.ParentWorkItemID]
		if digest != admission.Digest || admission.Value.WaveID != w.ID || admission.Value.Source != w.Source || admission.Value.SourceID != w.SourceID || !validSourceDigest(originalDigest) {
			return value, ErrProductSplitHeld
		}
		var original ProductWorkItemRegistrationSnapshot
		if err := taskEvidenceRecord(ctx, tx, "product_work_item_registration_version", structureID("product-work-", r.ParentWorkItemID, originalDigest), &original); err != nil {
			return value, splitEvidenceErr(err)
		}
		originalChecksum, err := contentDigest(original.Value)
		if err != nil {
			return value, err
		}
		if original.Digest != originalDigest || originalChecksum != originalDigest || original.Value.WaveID != w.ID || original.Value.Source != w.Source || original.Value.SourceID != w.SourceID || original.Value.Definition.ID != r.ParentWorkItemID || original.Value.Definition.Repository != registration.Value.Definition.Repository || original.Value.Definition.RepositoryID != registration.Value.Definition.RepositoryID {
			return value, ErrProductSplitHeld
		}
		value.InitialRegistrationDigest = originalDigest
	} else {
		value.ParentSplitScope = registration.Value.SplitScope
		value.Depth = binding.Value.Child.Value.Depth
		value.InitialRegistrationDigest = registration.Digest
		value.ParentCriterionRoots, err = nestedSplitCriterionRootsTx(ctx, tx, w, registration, *binding)
		if err != nil {
			return value, err
		}
		source, e := loadSplitChildSource(ctx, tx, *binding)
		now := k.clock()
		if e != nil {
			return value, splitEvidenceErr(e)
		}
		if source.Value.Status != "ready" || now.Before(source.CheckedAt) || now.Sub(source.CheckedAt) > time.Minute {
			return value, ErrProductSplitHeld
		}
		parentPolicy = source.Value.Facts.PolicyDigest
	}

	value.Limits, err = waveOperationLimitsTx(ctx, tx, w.ID)
	if err != nil {
		return value, splitEvidenceErr(err)
	}
	if value.Limits.SplitDepth == 0 || value.Limits.SplitChildren == 0 {
		return value, ErrProductSplitHeld
	}
	sourceRequest, err := k.productSourceRequestTx(ctx, tx, w)
	if err != nil {
		return value, splitEvidenceErr(err)
	}
	directionDigest := ""
	if w.DirectionID != "" {
		var direction WaveDirection
		if err := taskEvidenceRecord(ctx, tx, "wave_direction", w.DirectionID, &direction); err != nil {
			return value, splitEvidenceErr(err)
		}
		directionDigest, err = contentDigest(direction.Request)
		if err != nil {
			return value, err
		}
		if direction.ID != w.DirectionID || direction.Digest != directionDigest || direction.State != "applied" || direction.AppliedTarget.ScopeRevision != w.ScopeRevision || direction.AppliedTarget.PolicyRevision != w.Policy.Revision || direction.AppliedTarget.WaveID != w.ID {
			return value, ErrProductSplitHeld
		}
	}
	value.SourceDigest, err = contentDigest(struct {
		Source, SourceID, DirectionDigest string
		Request                           *ProductSourceRequestAdoption
	}{w.Source, w.SourceID, directionDigest, sourceRequest})
	if err != nil {
		return value, err
	}
	if binding != nil {
		value.RootSourceDigest = value.SourceDigest
		value.SourceDigest, err = contentDigest(struct{ RootSourceDigest, BindingDigest, PolicyDigest string }{value.SourceDigest, binding.Digest, parentPolicy})
		if err != nil {
			return value, err
		}
	}

	scope := verification.Scope{Revision: w.ScopeRevision, OriginalRequest: w.OriginalRequest, Included: append([]string{}, registration.Value.Definition.Criteria...), Remaining: []string{}, PreviouslyMerged: []string{}}
	for _, id := range scope.Included {
		for _, criterion := range w.Criteria {
			if criterion.ID == id {
				scope.Criteria = append(scope.Criteria, verification.Criterion{ID: id, Requirement: criterion.Requirement})
			}
		}
	}
	if binding != nil {
		scope = binding.Value.Scope
	}

	if err := r.Proposal.Validate(r.ParentWorkItemID, scope, value.Depth, value.Limits.SplitDepth, value.Limits.SplitChildren); err != nil {
		return value, errors.Join(ErrProductSplitHeld, err)
	}
	value.ParentScopeDigest, err = contentDigest(scope)
	if err != nil {
		return value, err
	}
	var product ProductTargetSnapshot
	err = taskEvidenceRecord(ctx, tx, "product_target", productTargetID(w.Target(), r.ParentWorkItemID), &product)
	if errors.Is(err, sql.ErrNoRows) {
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_target_observation' AND json_extract(payload,'$.value.work_item.id')=?)", r.ParentWorkItemID).Scan(&known); err != nil {
			return value, err
		}
		if known {
			return value, ErrProductSplitHeld
		}
	} else if err != nil {
		return value, splitEvidenceErr(err)
	} else {
		p := product.Value
		checksum, digestErr := contentDigest(p)
		if digestErr != nil {
			return value, digestErr
		}
		if invalid := p.validate(); invalid != nil {
			return value, errors.Join(ErrProductSplitHeld, ErrTaskEvidence, invalid)
		}
		if product.Digest != checksum || p.Target != w.Target() {
			return value, ErrProductSplitHeld
		}
		if p.Target.WaveID != w.ID || p.WorkItem.ID != r.ParentWorkItemID || p.WorkItem.RepositoryID != registration.Value.Definition.RepositoryID || p.Repository != registration.Value.Definition.Repository || p.Target.ScopeRevision != w.ScopeRevision || p.Target.PolicyRevision != w.Policy.Revision || p.WorkItem.Phase == domain.Merged || p.WorkItem.Phase == domain.Split || p.WorkItem.Phase == domain.Canceled {
			return value, ErrProductSplitHeld
		}
		value.Product = &product
	}
	held, err := productWorkItemProjectionHeld(ctx, tx, w, r.ParentWorkItemID)
	if err != nil {
		return value, splitEvidenceErr(err)
	}
	if held {
		return value, ErrProductSplitHeld
	}
	other, err := productSplitParentPreparationHeld(ctx, tx, w, r.ParentWorkItemID, splitProposalHead(r))
	if err != nil {
		return value, splitEvidenceErr(err)
	}
	if other {
		return value, ErrProductSplitHeld
	}
	return value, nil
}

func loadProductSplitProposal(ctx context.Context, tx *sql.Tx, r ProductSplitProposalRequest) (ProductSplitProposal, error) {
	var saved, history ProductSplitProposal
	head := splitProposalHead(r)
	err := taskEvidenceRecord(ctx, tx, "product_split_proposal", head, &saved)
	if errors.Is(err, sql.ErrNoRows) {
		nested, nestedErr := loadNestedSplitProposal(ctx, tx, r)
		if nestedErr == nil {
			return nested.Value.Proposal, nil
		}
		if nestedErr != sql.ErrNoRows {
			return saved, nestedErr
		}
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_proposal_admission' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_proposal_history' AND work_item_id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_proposal_request' AND work_item_id=? AND json_extract(payload,'$.head_id')=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_children','product_split_children_history','product_split_child_intent','product_split_child_intent_history','product_split_allocation','product_split_allocation_history') AND json_extract(payload,'$.value.parent_head_id')=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_children_admission','product_split_child_intent_admission','product_split_allocation_admission','product_split_work_item_seat') AND json_extract(payload,'$.parent_head_id')=?) OR EXISTS(SELECT 1 FROM kernel_budget_ledger WHERE kind='split_children' AND json_extract(payload,'$.value.parent_head_id')=?)", head, head, r.Target.WaveID, head, head, head, head).Scan(&known); err != nil {
			return saved, err
		}
		if !known {
			var witnessErr error
			known, witnessErr = splitCreationWitnessTx(ctx, tx, r.Target.WaveID, head)
			if witnessErr != nil {
				return saved, witnessErr
			}
		}
		if known {
			return saved, ErrProductSplitHeld
		}
	}
	if err != nil {
		return saved, err
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if digest != saved.Digest || saved.ID != structureID("product-split-", head, digest) || saved.Value.CreatedAt.IsZero() || splitProposalHead(saved.Value.Request) != head {
		return saved, ErrProductSplitHeld
	}
	var admitted string
	if err := taskEvidenceRecord(ctx, tx, "product_split_proposal_admission", head, &admitted); err != nil {
		return saved, splitEvidenceErr(err)
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_proposal_history", saved.ID, &history); err != nil {
		return saved, splitEvidenceErr(err)
	}
	if admitted != digest || !reflect.DeepEqual(saved, history) {
		return saved, ErrProductSplitHeld
	}
	var binding splitProposalRequestBinding
	if err := taskEvidenceRecord(ctx, tx, "product_split_proposal_request", splitProposalRequestID(saved.Value.Request), &binding); err != nil {
		return saved, splitEvidenceErr(err)
	}
	requestDigest, err := contentDigest(saved.Value.Request)
	if err != nil {
		return saved, err
	}
	if binding != (splitProposalRequestBinding{HeadID: head, ID: saved.ID, Digest: digest, RequestDigest: requestDigest}) {
		return saved, ErrProductSplitHeld
	}
	return saved, nil
}

func (k *KernelStore) PrepareProductSplitProposal(ctx context.Context, r ProductSplitProposalRequest) (ProductSplitProposal, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitProposal{}, err
	}
	defer tx.Rollback()
	saved, err := k.prepareProductSplitProposalTx(ctx, tx, r)
	if err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

func (k *KernelStore) prepareProductSplitProposalTx(ctx context.Context, tx *sql.Tx, r ProductSplitProposalRequest) (ProductSplitProposal, error) {
	var saved ProductSplitProposal
	if !r.Target.Valid() || r.Key == "" || len(r.Key) > 256 || !utf8.ValidString(r.Key) || r.ParentWorkItemID == "" || !validSourceDigest(r.RegistrationDigest) || len(r.Proposal.Children) > maxProductWorkItems {
		return saved, ErrProductSplitHeld
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return saved, err
	}
	if len(raw) > 1<<20 {
		return saved, ErrProductSplitHeld
	}
	var detached ProductSplitProposalRequest
	if err := json.Unmarshal(raw, &detached); err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(r, detached) {
		return saved, ErrProductSplitHeld
	}
	prior, err := loadProductSplitProposal(ctx, tx, r)
	if err == nil {
		if prior.Value.ParentSplitScope != nil {
			return ProductSplitProposal{}, ErrProductSplitHeld
		}
		if !reflect.DeepEqual(prior.Value.Request, r) {
			return saved, ErrProductSplitHeld
		}
		current, err := k.productSplitContextTx(ctx, tx, r)
		if err != nil {
			return saved, err
		}
		current.CreatedAt = prior.Value.CreatedAt
		if !reflect.DeepEqual(prior.Value, current) {
			return saved, ErrProductSplitHeld
		}
		return prior, nil
	}
	if err != sql.ErrNoRows {
		return saved, err
	}
	var known bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_proposal_request','product_nested_split_proposal_request') AND id=?)", splitProposalRequestID(r)).Scan(&known); err != nil {
		return saved, err
	}
	if known {
		return saved, ErrProductSplitHeld
	}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return saved, err
	}
	if w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return saved, ErrProductSplitHeld
	}
	saved.Value, err = k.productSplitContextTx(ctx, tx, r)
	if err != nil {
		return saved, err
	}
	// Nested context derivation is read-only until durable ancestry witnesses
	// and the parent Issue send fence are connected together.
	if saved.Value.ParentSplitScope != nil {
		return ProductSplitProposal{}, ErrProductSplitHeld
	}
	saved.Value.CreatedAt = k.clock()
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	head := splitProposalHead(r)
	saved.ID = structureID("product-split-", head, saved.Digest)
	requestDigest, err := contentDigest(r)
	if err != nil {
		return saved, err
	}
	for _, record := range []struct {
		kind, id, owner string
		value           any
	}{
		{"product_split_proposal", head, w.ID, saved},
		{"product_split_proposal_history", saved.ID, head, saved},
		{"product_split_proposal_admission", head, w.ID, saved.Digest},
		{"product_split_proposal_request", splitProposalRequestID(r), w.ID, splitProposalRequestBinding{HeadID: head, ID: saved.ID, Digest: saved.Digest, RequestDigest: requestDigest}},
	} {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, record.owner, record.value); err != nil {
			return saved, err
		}
	}
	return saved, nil
}

// ReadProductSplitProposal authenticates current host metadata without granting
// child creation or parent retirement, including while the Wave is paused.
// WorkItem projection changes (including its own pause) invalidate this current
// context read; the immutable saved journal remains historical evidence.
func (k *KernelStore) ReadProductSplitProposal(ctx context.Context, target contract.Target, parentID string) (ProductSplitProposal, error) {
	var saved ProductSplitProposal
	if !target.Valid() || parentID == "" {
		return saved, ErrProductSplitHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	return k.readProductSplitProposalTx(ctx, tx, target, parentID)
}

func (k *KernelStore) readProductSplitProposalTx(ctx context.Context, tx *sql.Tx, target contract.Target, parentID string) (ProductSplitProposal, error) {
	var saved ProductSplitProposal
	var err error
	saved, err = loadProductSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: target, ParentWorkItemID: parentID})
	if err != nil {
		return saved, err
	}
	current, err := k.productSplitContextTx(ctx, tx, saved.Value.Request)
	if err != nil {
		return saved, err
	}
	if saved.Value.ParentScopeProof != nil {
		if err := bindNestedParentProof(ctx, tx, &current, *saved.Value.ParentScopeProof); err != nil {
			return saved, err
		}
		if saved.Value.CreatedAt.After(k.clock()) {
			return saved, ErrProductSplitHeld
		}
	}
	current.CreatedAt = saved.Value.CreatedAt
	if !reflect.DeepEqual(current, saved.Value) {
		return saved, ErrProductSplitHeld
	}
	return saved, nil
}
