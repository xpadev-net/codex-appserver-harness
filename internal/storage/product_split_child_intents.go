package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
)

type ProductSplitChildPlanRequest struct {
	Target           contract.Target `json:"target"`
	ParentWorkItemID string          `json:"parent_work_item_id"`
	ProposalID       string          `json:"proposal_id"`
	ProposalDigest   string          `json:"proposal_digest"`
}
type ProductSplitChildIntentValue struct {
	Target               contract.Target `json:"target"`
	ParentHeadID         string          `json:"parent_head_id"`
	ProposalID           string          `json:"proposal_id"`
	ProposalDigest       string          `json:"proposal_digest"`
	ParentWorkItemID     string          `json:"parent_work_item_id"`
	WorkItemID           string          `json:"work_item_id"`
	Repository           string          `json:"repository"`
	RepositoryID         int64           `json:"repository_id"`
	Depth                int             `json:"depth"`
	Child                splitting.Child `json:"child"`
	ParentCriteria       []string        `json:"parent_criteria"`
	DependsOnWorkItemIDs []string        `json:"depends_on_work_item_ids"`
	Marker               string          `json:"marker"`
	Body                 string          `json:"body"`
}
type ProductSplitChildIntent struct {
	ID     string                       `json:"id"`
	Value  ProductSplitChildIntentValue `json:"value"`
	Digest string                       `json:"digest"`
}
type ProductSplitChildSetValue struct {
	Request      ProductSplitChildPlanRequest `json:"request"`
	ParentHeadID string                       `json:"parent_head_id"`
	ChildIDs     []string                     `json:"child_ids"`
	CreatedAt    time.Time                    `json:"created_at"`
}
type ProductSplitChildSet struct {
	ID     string                    `json:"id"`
	Value  ProductSplitChildSetValue `json:"value"`
	Digest string                    `json:"digest"`
}
type splitChildAdmission struct {
	Digest       string `json:"digest"`
	ParentHeadID string `json:"parent_head_id"`
}

type ProductSplitChildPlan struct {
	Set      ProductSplitChildSet      `json:"set"`
	Children []ProductSplitChildIntent `json:"children"`
}

func deriveProductSplitChildIntents(proposal ProductSplitProposal, registration ProductWorkItemRegistrationSnapshot) ([]ProductSplitChildIntent, error) {
	var out []ProductSplitChildIntent
	workIDs := map[string]string{}
	for _, child := range proposal.Value.Request.Proposal.Children {
		id := structureID("split-child-", proposal.ID, child.Key)
		workIDs[child.Key] = structureID("work-split-", proposal.Value.Request.Target.WaveID, id)
	}
	for _, child := range proposal.Value.Request.Proposal.Children {
		intent := ProductSplitChildIntent{ID: structureID("split-child-", proposal.ID, child.Key)}
		value := ProductSplitChildIntentValue{Target: proposal.Value.Request.Target, ParentHeadID: splitProposalHead(proposal.Value.Request), ProposalID: proposal.ID, ProposalDigest: proposal.Digest, ParentWorkItemID: proposal.Value.Request.ParentWorkItemID, WorkItemID: workIDs[child.Key], Repository: registration.Value.Definition.Repository, RepositoryID: registration.Value.Definition.RepositoryID, Depth: proposal.Value.Depth + 1, Child: child, ParentCriteria: []string{}, DependsOnWorkItemIDs: []string{}, Marker: "<!-- harness-split-child:" + intent.ID + " -->"}
		seen := map[string]bool{}
		for _, mapping := range child.Coverage {
			if !seen[mapping.ParentCriterion] {
				seen[mapping.ParentCriterion] = true
				value.ParentCriteria = append(value.ParentCriteria, mapping.ParentCriterion)
			}
		}
		sort.Strings(value.ParentCriteria)
		for _, dependency := range child.DependsOn {
			id, ok := workIDs[dependency]
			if !ok {
				return nil, ErrProductSplitHeld
			}
			value.DependsOnWorkItemIDs = append(value.DependsOnWorkItemIDs, id)
		}
		body, err := json.Marshal(struct {
			SchemaVersion    int             `json:"schema_version"`
			Target           contract.Target `json:"target"`
			WorkItemID       string          `json:"work_item_id"`
			ProposalID       string          `json:"proposal_id"`
			ProposalDigest   string          `json:"proposal_digest"`
			ParentWorkItemID string          `json:"parent_work_item_id"`
			Child            splitting.Child `json:"child"`
		}{1, value.Target, value.WorkItemID, proposal.ID, proposal.Digest, value.ParentWorkItemID, child})
		if err != nil {
			return nil, err
		}
		value.Body = child.Requirement + "\n\n```harness-split-part\n" + string(body) + "\n```\n\n" + value.Marker + "\n"
		if len(value.Body) > 1<<20 {
			return nil, ErrProductSplitHeld
		}
		intent.Value = value
		intent.Digest, err = contentDigest(value)
		if err != nil {
			return nil, err
		}
		out = append(out, intent)
	}
	return out, nil
}

func loadSplitIntentRegistrationTx(ctx context.Context, tx *sql.Tx, proposal ProductSplitProposal) (ProductWorkItemRegistrationSnapshot, error) {
	var registration ProductWorkItemRegistrationSnapshot
	r := proposal.Value.Request
	if err := taskEvidenceRecord(ctx, tx, "product_work_item_registration_version", structureID("product-work-", r.ParentWorkItemID, r.RegistrationDigest), &registration); err != nil {
		return registration, splitEvidenceErr(err)
	}
	digest, err := contentDigest(registration.Value)
	if err != nil {
		return registration, err
	}
	if registration.Digest != r.RegistrationDigest || digest != registration.Digest || registration.Value.Definition.ID != r.ParentWorkItemID || registration.Value.WaveID != r.Target.WaveID || registration.Value.ScopeRevision != r.Target.ScopeRevision || registration.Value.PolicyRevision != r.Target.PolicyRevision {
		return registration, ErrProductSplitHeld
	}
	return registration, nil
}

// Static proof load intentionally remains readable after a scope/pause change.
// It is not a send fence, budget reservation or child registration permission.
func loadProductSplitChildPlan(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductSplitChildPlan, error) {
	var plan ProductSplitChildPlan
	var set, history ProductSplitChildSet
	err := taskEvidenceRecord(ctx, tx, "product_split_children", r.ProposalID, &set)
	if errors.Is(err, sql.ErrNoRows) {
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_children_admission' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_children_history','product_split_child_intent','product_split_child_intent_history','product_split_child_intent_admission') AND work_item_id=?) OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_allocation','product_split_allocation_admission') AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_allocation_history' AND work_item_id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_work_item_seat' AND json_extract(payload,'$.proposal_id')=?) OR EXISTS(SELECT 1 FROM kernel_budget_ledger WHERE kind='split_children' AND operation_key=?)", r.ProposalID, r.ProposalID, r.ProposalID, r.ProposalID, r.ProposalID, r.ProposalID).Scan(&known); err != nil {
			return plan, err
		}
		if !known {
			var witnessErr error
			known, witnessErr = splitCreationWitnessTx(ctx, tx, r.Target.WaveID, splitProposalHead(ProductSplitProposalRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID}))
			if witnessErr != nil {
				return plan, witnessErr
			}
		}
		if known {
			return plan, ErrProductSplitHeld
		}
	}
	if err != nil {
		return plan, err
	}
	digest, err := contentDigest(set.Value)
	if err != nil {
		return plan, err
	}
	if set.Value.Request != r || set.Digest != digest || set.ID != structureID("split-children-", r.ProposalID, digest) || set.Value.CreatedAt.IsZero() {
		return plan, ErrProductSplitHeld
	}
	var admitted splitChildAdmission
	if err := taskEvidenceRecord(ctx, tx, "product_split_children_history", set.ID, &history); err != nil {
		return plan, splitEvidenceErr(err)
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_children_admission", r.ProposalID, &admitted); err != nil {
		return plan, splitEvidenceErr(err)
	}
	if admitted != (splitChildAdmission{digest, set.Value.ParentHeadID}) || !reflect.DeepEqual(history, set) {
		return plan, ErrProductSplitHeld
	}
	proposal, err := loadProductSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID})
	if err != nil {
		return plan, splitEvidenceErr(err)
	}
	if proposal.ID != r.ProposalID || proposal.Digest != r.ProposalDigest || set.Value.ParentHeadID != splitProposalHead(proposal.Value.Request) {
		return plan, ErrProductSplitHeld
	}
	registration, err := loadSplitIntentRegistrationTx(ctx, tx, proposal)
	if err != nil {
		return plan, err
	}
	expected, err := deriveProductSplitChildIntents(proposal, registration)
	if err != nil {
		return plan, err
	}
	if len(expected) != len(set.Value.ChildIDs) {
		return plan, ErrProductSplitHeld
	}
	for i, child := range expected {
		if set.Value.ChildIDs[i] != child.ID {
			return plan, ErrProductSplitHeld
		}
		var saved, original ProductSplitChildIntent
		var admission splitChildAdmission
		if err := taskEvidenceRecord(ctx, tx, "product_split_child_intent", child.ID, &saved); err != nil {
			return plan, splitEvidenceErr(err)
		}
		if err := taskEvidenceRecord(ctx, tx, "product_split_child_intent_history", child.ID, &original); err != nil {
			return plan, splitEvidenceErr(err)
		}
		if err := taskEvidenceRecord(ctx, tx, "product_split_child_intent_admission", child.ID, &admission); err != nil {
			return plan, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(saved, child) || !reflect.DeepEqual(original, child) || admission != (splitChildAdmission{child.Digest, child.Value.ParentHeadID}) {
			return plan, ErrProductSplitHeld
		}
	}
	plan.Set, plan.Children = set, expected
	return plan, nil
}

func (k *KernelStore) PrepareProductSplitChildPlan(ctx context.Context, r ProductSplitChildPlanRequest) (ProductSplitChildPlan, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitChildPlan{}, err
	}
	defer tx.Rollback()
	saved, err := k.prepareProductSplitChildPlanTx(ctx, tx, r)
	if err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

func (k *KernelStore) prepareProductSplitChildPlanTx(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductSplitChildPlan, error) {
	var plan ProductSplitChildPlan
	if !r.Target.Valid() || r.ParentWorkItemID == "" || r.ProposalID == "" || !validSourceDigest(r.ProposalDigest) {
		return plan, ErrProductSplitHeld
	}
	prior, err := loadProductSplitChildPlan(ctx, tx, r)
	if err == nil {
		return prior, nil
	}
	if err != sql.ErrNoRows {
		return plan, err
	}
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return plan, err
	}
	if w.Target() != r.Target || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return plan, ErrProductSplitHeld
	}
	proposal, err := k.readProductSplitProposalTx(ctx, tx, r.Target, r.ParentWorkItemID)
	if err != nil {
		return plan, err
	}
	if proposal.ID != r.ProposalID || proposal.Digest != r.ProposalDigest {
		return plan, ErrProductSplitHeld
	}
	registration, err := loadSplitIntentRegistrationTx(ctx, tx, proposal)
	if err != nil {
		return plan, err
	}
	children, err := deriveProductSplitChildIntents(proposal, registration)
	if err != nil {
		return plan, err
	}
	set := ProductSplitChildSet{Value: ProductSplitChildSetValue{Request: r, ParentHeadID: splitProposalHead(proposal.Value.Request), ChildIDs: []string{}, CreatedAt: k.clock()}}
	for _, child := range children {
		set.Value.ChildIDs = append(set.Value.ChildIDs, child.ID)
	}
	set.Digest, err = contentDigest(set.Value)
	if err != nil {
		return plan, err
	}
	set.ID = structureID("split-children-", r.ProposalID, set.Digest)
	for _, child := range children {
		for _, record := range []struct {
			kind  string
			value any
		}{{"product_split_child_intent", child}, {"product_split_child_intent_history", child}, {"product_split_child_intent_admission", splitChildAdmission{child.Digest, child.Value.ParentHeadID}}} {
			if err := kernelRecordTx(ctx, tx, record.kind, child.ID, r.ProposalID, record.value); err != nil {
				return plan, err
			}
		}
	}
	for _, record := range []struct {
		kind, id, owner string
		value           any
	}{{"product_split_children", r.ProposalID, w.ID, set}, {"product_split_children_history", set.ID, r.ProposalID, set}, {"product_split_children_admission", r.ProposalID, w.ID, splitChildAdmission{set.Digest, set.Value.ParentHeadID}}} {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, record.owner, record.value); err != nil {
			return plan, err
		}
	}
	return ProductSplitChildPlan{Set: set, Children: children}, nil
}
func (k *KernelStore) ReadProductSplitChildPlan(ctx context.Context, r ProductSplitChildPlanRequest) (ProductSplitChildPlan, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitChildPlan{}, err
	}
	defer tx.Rollback()
	return loadProductSplitChildPlan(ctx, tx, r)
}
