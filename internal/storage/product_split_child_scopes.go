package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

type ProductSplitChildCreationReference struct {
	ID         string `json:"id"`
	Digest     string `json:"digest"`
	Attempt    int    `json:"attempt"`
	PreviousID string `json:"previous_id"`
}
type ProductSplitChildScopeValue struct {
	Request                  ProductSplitChildPlanRequest         `json:"request"`
	ParentHeadID             string                               `json:"parent_head_id"`
	ParentRegistrationDigest string                               `json:"parent_registration_digest"`
	ParentScopeDigest        string                               `json:"parent_scope_digest"`
	SourceDigest             string                               `json:"source_digest"`
	AllocationID             string                               `json:"allocation_id"`
	AllocationDigest         string                               `json:"allocation_digest"`
	Child                    ProductSplitChildIntent              `json:"child"`
	InitialCreationID        string                               `json:"initial_creation_id"`
	CreationID               string                               `json:"creation_id"`
	CreationDigest           string                               `json:"creation_digest"`
	Creations                []ProductSplitChildCreationReference `json:"creations"`
	Issue                    ProductSplitCreatedIssue             `json:"issue"`
	SourceID                 string                               `json:"source_id"`
	Scope                    verification.Scope                   `json:"scope"`
	ScopeDigest              string                               `json:"scope_digest"`
	CreatedAt                time.Time                            `json:"created_at"`
}
type ProductSplitChildScopeBinding struct {
	ID     string                      `json:"id"`
	Value  ProductSplitChildScopeValue `json:"value"`
	Digest string                      `json:"digest"`
}
type ProductSplitChildScopesValue struct {
	Request      ProductSplitChildPlanRequest    `json:"request"`
	ParentHeadID string                          `json:"parent_head_id"`
	Children     []ProductSplitChildScopeBinding `json:"children"`
	CreatedAt    time.Time                       `json:"created_at"`
}
type ProductSplitChildScopes struct {
	ID     string                       `json:"id"`
	Value  ProductSplitChildScopesValue `json:"value"`
	Digest string                       `json:"digest"`
}
type splitChildScopeSource struct {
	Request           ProductSplitChildPlanRequest         `json:"request"`
	ParentHeadID      string                               `json:"parent_head_id"`
	SetID             string                               `json:"set_id"`
	SetDigest         string                               `json:"set_digest"`
	BindingID         string                               `json:"binding_id"`
	BindingDigest     string                               `json:"binding_digest"`
	ChildID           string                               `json:"child_id"`
	WorkItemID        string                               `json:"work_item_id"`
	InitialCreationID string                               `json:"initial_creation_id"`
	CreationID        string                               `json:"creation_id"`
	IssueID           int64                                `json:"issue_id"`
	Creations         []ProductSplitChildCreationReference `json:"creations"`
}

func deriveProductSplitChildScopes(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest, at time.Time) (ProductSplitChildScopes, error) {
	var out ProductSplitChildScopes
	if !r.Target.Valid() || r.ParentWorkItemID == "" || at.IsZero() {
		return out, ErrProductSplitHeld
	}
	plan, err := loadProductSplitChildPlan(ctx, tx, r)
	if err != nil {
		return out, splitEvidenceErr(err)
	}
	allocation, err := loadProductSplitAllocation(ctx, tx, r)
	if err != nil {
		return out, splitEvidenceErr(err)
	}
	proposal, err := loadProductSplitProposal(ctx, tx, ProductSplitProposalRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID})
	if err != nil {
		return out, splitEvidenceErr(err)
	}
	if proposal.ID != r.ProposalID || proposal.Digest != r.ProposalDigest || len(plan.Children) < 2 {
		return out, ErrProductSplitHeld
	}
	out.Value = ProductSplitChildScopesValue{Request: r, ParentHeadID: plan.Set.Value.ParentHeadID, Children: []ProductSplitChildScopeBinding{}, CreatedAt: at}
	issueIDs, numbers := map[int64]bool{}, map[int]bool{}
	for _, child := range plan.Children {
		job, err := loadLatestProductSplitCreate(ctx, tx, child.ID)
		if err != nil {
			return ProductSplitChildScopes{}, splitEvidenceErr(err)
		}
		if job.State != "observed" || job.Issue == nil || job.Stop == nil || !reflect.DeepEqual(job.Value.Child, child) || job.Value.Request != r || job.Issue.ObservedAt.After(at) || issueIDs[job.Issue.ID] || numbers[job.Issue.Number] {
			return ProductSplitChildScopes{}, ErrProductSplitHeld
		}
		chain := []ProductSplitChildCreationReference{}
		for cursor := job; ; {
			chain = append(chain, ProductSplitChildCreationReference{cursor.ID, cursor.Digest, cursor.Value.Attempt, cursor.Value.RetryOf})
			if cursor.Value.RetryOf == "" {
				break
			}
			if len(chain) > 32 {
				return ProductSplitChildScopes{}, ErrProductSplitHeld
			}
			cursor, err = loadSplitCreate(ctx, tx, cursor.Value.RetryOf)
			if err != nil {
				return ProductSplitChildScopes{}, splitEvidenceErr(err)
			}
		}
		for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
			chain[i], chain[j] = chain[j], chain[i]
		}
		issueIDs[job.Issue.ID], numbers[job.Issue.Number] = true, true
		scope := verification.Scope{Revision: r.Target.ScopeRevision, OriginalRequest: child.Value.Child.Requirement, Criteria: append([]verification.Criterion{}, child.Value.Child.Criteria...), Included: []string{}, Remaining: []string{}, PreviouslyMerged: []string{}}
		for _, criterion := range scope.Criteria {
			scope.Included = append(scope.Included, criterion.ID)
		}
		if err := scope.Validate(); err != nil {
			return ProductSplitChildScopes{}, errors.Join(ErrProductSplitHeld, err)
		}
		scopeDigest, err := contentDigest(scope)
		if err != nil {
			return ProductSplitChildScopes{}, err
		}
		creationDigest, err := contentDigest(job)
		if err != nil {
			return ProductSplitChildScopes{}, err
		}
		value := ProductSplitChildScopeValue{Request: r, ParentHeadID: plan.Set.Value.ParentHeadID, ParentRegistrationDigest: proposal.Value.Request.RegistrationDigest, ParentScopeDigest: proposal.Value.ParentScopeDigest, SourceDigest: proposal.Value.SourceDigest, AllocationID: allocation.ID, AllocationDigest: allocation.Digest, Child: child, InitialCreationID: splitCreateID(child.ID), CreationID: job.ID, CreationDigest: creationDigest, Creations: chain, Issue: *job.Issue, SourceID: fmt.Sprintf("r%d-i%d", job.Issue.RepositoryID, job.Issue.Number), Scope: scope, ScopeDigest: scopeDigest, CreatedAt: at}
		digest, err := contentDigest(value)
		if err != nil {
			return ProductSplitChildScopes{}, err
		}
		binding := ProductSplitChildScopeBinding{ID: structureID("split-child-scope-", child.Value.WorkItemID, digest), Value: value, Digest: digest}
		out.Value.Children = append(out.Value.Children, binding)
	}
	out.Digest, err = contentDigest(out.Value)
	if err != nil {
		return ProductSplitChildScopes{}, err
	}
	out.ID = structureID("split-child-scopes-", r.ProposalID, out.Digest)
	return out, nil
}

func splitChildScopeSourceValue(set ProductSplitChildScopes, binding ProductSplitChildScopeBinding) splitChildScopeSource {
	v := binding.Value
	return splitChildScopeSource{v.Request, v.ParentHeadID, set.ID, set.Digest, binding.ID, binding.Digest, v.Child.ID, v.Child.Value.WorkItemID, v.InitialCreationID, v.CreationID, v.Issue.ID, append([]ProductSplitChildCreationReference{}, v.Creations...)}
}

func loadProductSplitChildScopes(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest) (ProductSplitChildScopes, error) {
	var saved, history, admission ProductSplitChildScopes
	err := taskEvidenceRecord(ctx, tx, "product_split_child_scopes", r.ProposalID, &saved)
	if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scopes_admission' AND id=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scopes_history' AND work_item_id=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission') AND json_extract(payload,'$.value.request.proposal_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scope_source' AND json_extract(payload,'$.request.proposal_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_registrations','product_split_registrations_history','product_split_registrations_admission') AND json_extract(payload,'$.value.request.proposal_id')=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(payload,'$.value.split_scope.request.proposal_id'),json_extract(payload,'$.split_scope.request.proposal_id'))=?)`, r.ProposalID, r.ProposalID, r.ProposalID, r.ProposalID, r.ProposalID, r.ProposalID).Scan(&known); err != nil {
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
	if saved.Value.Request != r {
		return saved, ErrProductSplitHeld
	}
	expected, err := deriveProductSplitChildScopes(ctx, tx, r, saved.Value.CreatedAt)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(saved, expected) {
		return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_scopes_history", saved.ID, &history); err != nil {
		return saved, splitEvidenceErr(err)
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_child_scopes_admission", r.ProposalID, &admission); err != nil {
		return saved, splitEvidenceErr(err)
	}
	if !reflect.DeepEqual(saved, history) || !reflect.DeepEqual(saved, admission) {
		return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	for _, binding := range saved.Value.Children {
		for _, record := range []struct{ kind, id string }{{"product_split_child_scope", binding.Value.Child.Value.WorkItemID}, {"product_split_child_scope_history", binding.ID}, {"product_split_child_scope_admission", binding.Value.Child.Value.WorkItemID}} {
			var child ProductSplitChildScopeBinding
			if err := taskEvidenceRecord(ctx, tx, record.kind, record.id, &child); err != nil {
				return saved, splitEvidenceErr(err)
			}
			if !reflect.DeepEqual(binding, child) {
				return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
			}
		}
		var source splitChildScopeSource
		key := fmt.Sprintf("github:%d:issue:%d", binding.Value.Issue.RepositoryID, binding.Value.Issue.Number)
		if err := taskEvidenceRecord(ctx, tx, "product_split_child_scope_source", key, &source); err != nil {
			return saved, splitEvidenceErr(err)
		}
		if !reflect.DeepEqual(source, splitChildScopeSourceValue(saved, binding)) {
			return saved, errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
		}
	}
	return saved, nil
}

// BindProductSplitChildScopes records all authenticated created children and
// their complete local criterion/part/depth/source data atomically. This is
// historical metadata, not WorkItem registration, source freshness or quality.
func (k *KernelStore) BindProductSplitChildScopes(ctx context.Context, r ProductSplitChildPlanRequest) (ProductSplitChildScopes, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitChildScopes{}, err
	}
	defer tx.Rollback()
	saved, err := loadProductSplitChildScopes(ctx, tx, r)
	if err == nil {
		return saved, nil
	}
	if err != sql.ErrNoRows {
		return ProductSplitChildScopes{}, err
	}
	saved, err = deriveProductSplitChildScopes(ctx, tx, r, k.clock())
	if err != nil {
		return ProductSplitChildScopes{}, err
	}
	raw, err := json.Marshal(saved)
	if err != nil {
		return ProductSplitChildScopes{}, err
	}
	if len(raw) > 8<<20 {
		return ProductSplitChildScopes{}, ErrProductSplitHeld
	}
	for _, binding := range saved.Value.Children {
		var rootClaim bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM kernel_waves WHERE source='github' AND source_id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_work_item_registration' AND id=?)", binding.Value.SourceID, binding.Value.SourceID).Scan(&rootClaim); err != nil {
			return ProductSplitChildScopes{}, err
		}
		if rootClaim {
			return ProductSplitChildScopes{}, ErrProductSplitHeld
		}
		workID := binding.Value.Child.Value.WorkItemID
		for _, record := range []struct{ kind, id, owner string }{{"product_split_child_scope", workID, r.Target.WaveID}, {"product_split_child_scope_history", binding.ID, r.ProposalID}, {"product_split_child_scope_admission", workID, r.Target.WaveID}} {
			var exists bool
			if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind=? AND id=?)", record.kind, record.id).Scan(&exists); err != nil {
				return ProductSplitChildScopes{}, err
			}
			if exists {
				return ProductSplitChildScopes{}, ErrProductSplitHeld
			}
			if err := kernelRecordTx(ctx, tx, record.kind, record.id, record.owner, binding); err != nil {
				return ProductSplitChildScopes{}, err
			}
		}
		key := fmt.Sprintf("github:%d:issue:%d", binding.Value.Issue.RepositoryID, binding.Value.Issue.Number)
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_child_scope_source' AND id=?)", key).Scan(&known); err != nil {
			return ProductSplitChildScopes{}, err
		}
		if known {
			return ProductSplitChildScopes{}, ErrProductSplitHeld
		}
		if err := kernelRecordTx(ctx, tx, "product_split_child_scope_source", key, r.Target.WaveID, splitChildScopeSourceValue(saved, binding)); err != nil {
			return ProductSplitChildScopes{}, err
		}
	}
	for _, record := range []struct{ kind, id, owner string }{{"product_split_child_scopes", r.ProposalID, r.Target.WaveID}, {"product_split_child_scopes_history", saved.ID, r.ProposalID}, {"product_split_child_scopes_admission", r.ProposalID, r.Target.WaveID}} {
		if err := kernelRecordTx(ctx, tx, record.kind, record.id, record.owner, saved); err != nil {
			return ProductSplitChildScopes{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ProductSplitChildScopes{}, err
	}
	return saved, nil
}
func (k *KernelStore) ReadProductSplitChildScopes(ctx context.Context, r ProductSplitChildPlanRequest) (ProductSplitChildScopes, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitChildScopes{}, err
	}
	defer tx.Rollback()
	return loadProductSplitChildScopes(ctx, tx, r)
}
