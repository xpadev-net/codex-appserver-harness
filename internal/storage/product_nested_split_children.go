package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
)

type ProductNestedSplitChildPlanValue struct {
	ProductNestedSplitProposalValue
	EnvelopeID     string                `json:"envelope_id"`
	EnvelopeDigest string                `json:"envelope_digest"`
	Plan           ProductSplitChildPlan `json:"plan"`
}
type ProductNestedSplitChildPlan struct {
	ID     string                           `json:"id"`
	Value  ProductNestedSplitChildPlanValue `json:"value"`
	Digest string                           `json:"digest"`
}

func deriveNestedSplitPlan(proposal ProductSplitProposal, registration ProductWorkItemRegistrationSnapshot, r ProductSplitChildPlanRequest, created ProductSplitChildSetValue) (ProductSplitChildPlan, error) {
	var plan ProductSplitChildPlan
	children, err := deriveProductSplitChildIntents(proposal, registration)
	if err != nil {
		return plan, err
	}
	if created.CreatedAt.IsZero() || created.CreatedAt.Before(proposal.Value.CreatedAt) {
		return plan, ErrProductSplitHeld
	}
	value := ProductSplitChildSetValue{Request: r, ParentHeadID: splitProposalHead(proposal.Value.Request), ChildIDs: []string{}, CreatedAt: created.CreatedAt}
	for _, child := range children {
		value.ChildIDs = append(value.ChildIDs, child.ID)
	}
	digest, err := contentDigest(value)
	if err != nil {
		return plan, err
	}
	return ProductSplitChildPlan{Set: ProductSplitChildSet{ID: structureID("split-children-", r.ProposalID, digest), Value: value, Digest: digest}, Children: children}, nil
}

func loadNestedSplitChildPlan(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductNestedSplitChildPlan, error) {
	var saved ProductNestedSplitChildPlan
	err := taskEvidenceRecord(ctx, tx, "product_nested_split_children", r.ProposalID, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, splitScopeWitnessSQL(`SELECT EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind='product_nested_split_children_admission' AND id=?) OR EXISTS(SELECT 1 FROM split_scope_witnesses WHERE kind IN ('product_nested_split_children_history','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission','product_nested_split_allocation','product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat','product_nested_split_allocation_ledger','product_nested_split_creation_ancestor','product_nested_split_retry_ancestor') AND json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.plan.set.value.request.proposal_id')=?)`), r.ProposalID, r.ProposalID).Scan(&known); err != nil {
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
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.ID != structureID("nested-split-children-", r.ProposalID, digest) || saved.Value.Plan.Set.Value.Request != r {
		return saved, ErrProductSplitHeld
	}
	envelope, err := loadNestedSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID})
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	proposal := envelope.Value.Proposal
	if saved.Value.EnvelopeID != envelope.ID || saved.Value.EnvelopeDigest != envelope.Digest || !reflect.DeepEqual(saved.Value.ProductNestedSplitProposalValue, envelope.Value) || proposal.ID != r.ProposalID || proposal.Digest != r.ProposalDigest {
		return saved, ErrProductSplitHeld
	}
	registration, err := loadSplitIntentRegistrationTx(ctx, tx, proposal)
	if err != nil {
		return saved, err
	}
	plan, err := deriveNestedSplitPlan(proposal, registration, r, saved.Value.Plan.Set.Value)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(saved.Value.Plan, plan) {
		return saved, ErrProductSplitHeld
	}
	records := []struct{ kind, id string }{{"product_nested_split_children_history", saved.ID}, {"product_nested_split_children_admission", r.ProposalID}}
	for _, child := range plan.Children {
		for _, kind := range []string{"product_nested_split_child_intent", "product_nested_split_child_intent_history", "product_nested_split_child_intent_admission"} {
			records = append(records, struct{ kind, id string }{kind, child.ID})
		}
	}
	for _, record := range records {
		var witness ProductNestedSplitChildPlan
		if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &witness); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(saved, witness) {
			return saved, ErrProductSplitHeld
		}
	}
	return saved, nil
}

// The surrounding generic preparation already checked the live Wave and fresh
// nested proposal. All descendant records carry the complete original group;
// no naked intent can outlive the ancestor's ownership or retry prohibitions.
func (k *KernelStore) prepareNestedSplitChildPlanTx(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest, proposal ProductSplitProposal) (ProductSplitChildPlan, error) {
	var empty ProductSplitChildPlan
	envelope, err := loadNestedSplitProposal(ctx, tx, proposal.Value.Request)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(envelope.Value.Proposal, proposal) {
		return empty, ErrProductSplitHeld
	}
	registration, err := loadSplitIntentRegistrationTx(ctx, tx, proposal)
	if err != nil {
		return empty, err
	}
	plan, err := deriveNestedSplitPlan(proposal, registration, r, ProductSplitChildSetValue{CreatedAt: k.clock()})
	if err != nil {
		return empty, err
	}
	saved := ProductNestedSplitChildPlan{Value: ProductNestedSplitChildPlanValue{ProductNestedSplitProposalValue: envelope.Value, EnvelopeID: envelope.ID, EnvelopeDigest: envelope.Digest, Plan: plan}}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return empty, err
	}
	saved.ID = structureID("nested-split-children-", r.ProposalID, saved.Digest)
	raw, err := json.Marshal(saved)
	if err != nil {
		return empty, err
	}
	if len(raw) > 16<<20 {
		return empty, ErrProductSplitHeld
	}
	records := []struct{ kind, id string }{{"product_nested_split_children", r.ProposalID}, {"product_nested_split_children_history", saved.ID}, {"product_nested_split_children_admission", r.ProposalID}}
	for _, child := range plan.Children {
		for _, kind := range []string{"product_nested_split_child_intent", "product_nested_split_child_intent_history", "product_nested_split_child_intent_admission"} {
			records = append(records, struct{ kind, id string }{kind, child.ID})
		}
	}
	for _, record := range records {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, r.Target.WaveID, saved); err != nil {
			return empty, err
		}
	}
	return plan, nil
}
