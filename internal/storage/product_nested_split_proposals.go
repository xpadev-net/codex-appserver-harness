package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Each independent envelope keeps the complete ancestor group at the existing
// group-witness JSON paths. Missing proof is never permission to forget siblings.
type ProductNestedSplitProposalValue struct {
	ProductSplitParentScopeValue
	ParentProof    ProductSplitParentScopeReference `json:"parent_proof"`
	Proposal       ProductSplitProposal             `json:"proposal"`
	ProposalHeadID string                           `json:"proposal_head_id"`
	RequestKeyID   string                           `json:"request_key_id"`
}
type ProductNestedSplitProposal struct {
	ID     string                          `json:"id"`
	Value  ProductNestedSplitProposalValue `json:"value"`
	Digest string                          `json:"digest"`
}

func bindNestedParentProof(ctx context.Context, tx *sql.Tx, value *ProductSplitProposalValue, ref ProductSplitParentScopeReference) error {
	if value.ParentSplitScope == nil || value.Depth < 1 || ref.ID == "" || !validSourceDigest(ref.Digest) {
		return ErrProductSplitHeld
	}
	var binding ProductSplitChildScopeBinding
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_scope", value.Request.ParentWorkItemID, &binding); err != nil {
		return splitEvidenceErr(err)
	}
	proof, err := loadSplitParentScope(ctx, tx, binding)
	if err != nil {
		return err
	}
	if proof.ID != ref.ID || proof.Digest != ref.Digest || proof.Value.BindingID != value.ParentSplitScope.BindingID || proof.Value.BindingDigest != value.ParentSplitScope.BindingDigest {
		return ErrProductSplitHeld
	}
	value.ParentScopeProof = &ref
	return nil
}

func loadNestedSplitProposal(ctx context.Context, tx *sql.Tx, r ProductSplitProposalRequest) (ProductNestedSplitProposal, error) {
	var saved ProductNestedSplitProposal
	head := splitProposalHead(r)
	err := taskEvidenceRecord(ctx, tx, "product_nested_split_proposal", head, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_nested_split_proposal_admission' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_nested_split_proposal_history','product_nested_split_proposal_request') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.proposal_head_id')=?)`, head, head).Scan(&known); err != nil {
			return saved, err
		}
		if known {
			return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
		}
		return saved, err
	}
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	v := saved.Value
	p := v.Proposal
	request := p.Value.Request
	digest, err := contentDigest(v)
	if err != nil {
		return saved, err
	}
	proposalDigest, err := contentDigest(p.Value)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.ID != structureID("nested-split-envelope-", head, digest) || v.ProposalHeadID != head || splitProposalHead(request) != head || v.RequestKeyID != splitProposalRequestID(request) || p.Digest != proposalDigest || p.ID != structureID("product-split-", head, proposalDigest) || p.Value.CreatedAt.IsZero() || p.Value.CreatedAt.Before(v.SavedAt) || p.Value.ParentScopeProof == nil || *p.Value.ParentScopeProof != v.ParentProof || p.Value.ParentSplitScope == nil || request.ParentWorkItemID != v.ParentWorkItemID {
		return saved, ErrProductSplitHeld
	}
	var binding ProductSplitChildScopeBinding
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_scope", v.ParentWorkItemID, &binding); err != nil {
		return saved, splitEvidenceErr(err)
	}
	proof, err := loadSplitParentScope(ctx, tx, binding)
	if err != nil {
		return saved, err
	}
	if proof.ID != v.ParentProof.ID || proof.Digest != v.ParentProof.Digest || !reflect.DeepEqual(proof.Value, v.ProductSplitParentScopeValue) {
		return saved, ErrProductSplitHeld
	}
	var registration ProductWorkItemRegistrationSnapshot
	for _, candidate := range v.Registrations {
		if candidate.Value.Definition.ID == v.ParentWorkItemID {
			registration = candidate
		}
	}
	sourceProposal, err := loadProductSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: binding.Value.Request.Target, ParentWorkItemID: binding.Value.Request.ParentWorkItemID})
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	roots, err := nestedSplitRootsFromBinding(registration, binding, sourceProposal)
	if err != nil {
		return saved, err
	}
	rootSource := sourceProposal.Value.SourceDigest
	if sourceProposal.Value.ParentSplitScope != nil {
		rootSource = sourceProposal.Value.RootSourceDigest
	}
	sourceDigest, err := contentDigest(struct{ RootSourceDigest, BindingDigest, PolicyDigest string }{rootSource, binding.Digest, v.SourcePolicyDigest})
	if err != nil {
		return saved, err
	}
	if registration.Digest != request.RegistrationDigest || !reflect.DeepEqual(registration.Value.SplitScope, p.Value.ParentSplitScope) || p.Value.Depth != binding.Value.Child.Value.Depth || p.Value.Depth != sourceProposal.Value.Depth+1 || p.Value.InitialRegistrationDigest != registration.Digest || p.Value.ParentScopeDigest != binding.Value.ScopeDigest || p.Value.RootSourceDigest != rootSource || p.Value.SourceDigest != sourceDigest || !reflect.DeepEqual(p.Value.ParentCriterionRoots, roots) || p.Value.Limits != sourceProposal.Value.Limits || request.Proposal.Validate(v.ParentWorkItemID, binding.Value.Scope, p.Value.Depth, p.Value.Limits.SplitDepth, p.Value.Limits.SplitChildren) != nil {
		return saved, ErrProductSplitHeld
	}
	for _, record := range []struct{ kind, id string }{{"product_nested_split_proposal_history", saved.ID}, {"product_nested_split_proposal_admission", head}, {"product_nested_split_proposal_request", v.RequestKeyID}} {
		var witness ProductNestedSplitProposal
		if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &witness); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(saved, witness) {
			return saved, ErrProductSplitHeld
		}
	}
	return saved, nil
}

// PrepareProductNestedSplitProposal adopts metadata with an explicit host proof
// reference. It creates no intents, seats, Issues, Tasks or send permission.
// The ordinary root port remains unable to adopt a nested preview implicitly.
func (k *KernelStore) PrepareProductNestedSplitProposal(ctx context.Context, r ProductSplitProposalRequest, parent ProductSplitParentScopeReference) (ProductNestedSplitProposal, error) {
	var empty ProductNestedSplitProposal
	if !r.Target.Valid() || r.Key == "" || len(r.Key) > 256 || !utf8.ValidString(r.Key) || r.ParentWorkItemID == "" || !validSourceDigest(r.RegistrationDigest) || len(r.Proposal.Children) > maxProductWorkItems || parent.ID == "" || !validSourceDigest(parent.Digest) {
		return empty, ErrProductSplitHeld
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > 1<<20 {
		return empty, ErrProductSplitHeld
	}
	var request ProductSplitProposalRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return empty, err
	}
	r = request
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	prior, err := loadNestedSplitProposal(ctx, tx, r)
	if err == nil {
		if !reflect.DeepEqual(prior.Value.Proposal.Value.Request, r) || prior.Value.ParentProof != parent {
			return empty, ErrProductSplitHeld
		}
		current, err := k.readProductSplitProposalTx(ctx, tx, r.Target, r.ParentWorkItemID)
		if err != nil || !reflect.DeepEqual(current, prior.Value.Proposal) {
			if err != nil {
				return empty, err
			}
			return empty, ErrProductSplitHeld
		}
		return prior, nil
	}
	if err != sql.ErrNoRows {
		return empty, err
	}
	var known bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_proposal_request','product_nested_split_proposal_request') AND id=?)", splitProposalRequestID(r)).Scan(&known); err != nil {
		return empty, err
	}
	if known {
		return empty, ErrProductSplitHeld
	}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return empty, err
	}
	if w.DispatchPaused() || w.State == domain.WaveBlocked || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || !k.clock().Before(w.Deadline) {
		return empty, ErrProductSplitHeld
	}
	value, err := k.productSplitContextTx(ctx, tx, r)
	if err != nil {
		return empty, err
	}
	if err := bindNestedParentProof(ctx, tx, &value, parent); err != nil {
		return empty, err
	}
	var binding ProductSplitChildScopeBinding
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_scope", r.ParentWorkItemID, &binding); err != nil {
		return empty, splitEvidenceErr(err)
	}
	proof, err := loadSplitParentScope(ctx, tx, binding)
	if err != nil {
		return empty, err
	}
	value.CreatedAt = k.clock()
	if value.CreatedAt.Before(proof.Value.SavedAt) {
		return empty, ErrProductSplitHeld
	}
	digest, err := contentDigest(value)
	if err != nil {
		return empty, err
	}
	head := splitProposalHead(r)
	proposal := ProductSplitProposal{ID: structureID("product-split-", head, digest), Value: value, Digest: digest}
	saved := ProductNestedSplitProposal{Value: ProductNestedSplitProposalValue{ProductSplitParentScopeValue: proof.Value, ParentProof: parent, Proposal: proposal, ProposalHeadID: head, RequestKeyID: splitProposalRequestID(r)}}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return empty, err
	}
	saved.ID = structureID("nested-split-envelope-", head, saved.Digest)
	raw, err = json.Marshal(saved)
	if err != nil {
		return empty, err
	}
	if len(raw) > 16<<20 {
		return empty, ErrProductSplitHeld
	}
	for _, record := range []struct{ kind, id string }{{"product_nested_split_proposal", head}, {"product_nested_split_proposal_history", saved.ID}, {"product_nested_split_proposal_admission", head}, {"product_nested_split_proposal_request", saved.Value.RequestKeyID}} {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, r.Target.WaveID, saved); err != nil {
			return empty, err
		}
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	return saved, nil
}
