package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"sort"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

// A split is delegation, never completion. Each mapping retains the local
// criterion and whole/part obligation until its own current merge is known.
type ProductSplitCoverageChild struct {
	WorkItemID         string               `json:"work_item_id"`
	RegistrationDigest string               `json:"registration_digest"`
	BindingID          string               `json:"binding_id"`
	BindingDigest      string               `json:"binding_digest"`
	ScopeDigest        string               `json:"scope_digest"`
	LocalCriteria      []string             `json:"local_criteria"`
	Coverage           []splitting.Coverage `json:"coverage"`
	Merged             bool                 `json:"merged"`
}
type ProductSplitCoverage struct {
	ID                       string                      `json:"id"`
	Digest                   string                      `json:"digest"`
	ParentWorkItemID         string                      `json:"parent_work_item_id"`
	ParentRegistrationDigest string                      `json:"parent_registration_digest"`
	Children                 []ProductSplitCoverageChild `json:"children"`
}

// Authenticate the complete group, not just a child head or a declared part.
// These are the initial-root splits admitted by the current split port.
func productSplitCoverageTx(ctx context.Context, tx *sql.Tx, w domain.Wave, heads map[string]ProductWorkItemRegistrationSnapshot) ([]ProductSplitCoverage, []ProductCoveragePending, error) {
	var groups []ProductSplitCoverage
	var unavailable []ProductCoveragePending
	ids := make([]string, 0, len(heads))
	for id := range heads {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	seen := map[string]bool{}
	parents := map[string]bool{}
	for _, id := range ids {
		registration := heads[id]
		ref := registration.Value.SplitScope
		if ref == nil || !productRegistrationScope(registration, w) || seen[ref.Request.ProposalID] {
			continue
		}
		seen[ref.Request.ProposalID] = true
		group, err := loadProductSplitRegistrations(ctx, tx, ref.Request)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			if !readinessHoldError(err) && !errors.Is(err, sql.ErrNoRows) {
				return nil, nil, err
			}
			unavailable = append(unavailable, ProductCoveragePending{WorkItemID: id, RecordID: ref.Request.ProposalID, Reason: "split_registration_evidence_unavailable"})
			continue
		}
		parent, ok := heads[group.Value.Request.ParentWorkItemID]
		if !ok || !productRegistrationScope(parent, w) || parent.Value.SplitScope != nil || len(group.Value.Children) < 2 || len(group.Value.Children) != len(group.Value.Registrations) {
			unavailable = append(unavailable, ProductCoveragePending{WorkItemID: group.Value.Request.ParentWorkItemID, RecordID: group.ID, Reason: "split_parent_registration_unavailable"})
			continue
		}
		descriptor := ProductSplitCoverage{ID: group.ID, Digest: group.Digest, ParentWorkItemID: parent.Value.Definition.ID, ParentRegistrationDigest: parent.Digest, Children: []ProductSplitCoverageChild{}}
		scope := verification.Scope{Revision: w.ScopeRevision, OriginalRequest: w.OriginalRequest, Included: slices.Clone(parent.Value.Definition.Criteria), Remaining: []string{}, PreviouslyMerged: []string{}}
		for _, criterion := range w.Criteria {
			if slices.Contains(scope.Included, criterion.ID) {
				scope.Criteria = append(scope.Criteria, verification.Criterion{ID: criterion.ID, Requirement: criterion.Requirement})
			}
		}
		scopeDigest, err := contentDigest(scope)
		if err != nil {
			return nil, nil, err
		}
		proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: parent.Value.Definition.ID, ScopeRevision: w.ScopeRevision, Outcome: "split", Children: []splitting.Child{}}
		valid := true
		for i, binding := range group.Value.Children {
			child := group.Value.Registrations[i]
			v := binding.Value
			if v.ParentRegistrationDigest != parent.Digest || v.ParentScopeDigest != scopeDigest || !reflect.DeepEqual(heads[child.Value.Definition.ID], child) || v.Child.Value.Depth != 1 {
				valid = false
				break
			}
			proposal.Children = append(proposal.Children, v.Child.Value.Child)
			descriptor.Children = append(descriptor.Children, ProductSplitCoverageChild{WorkItemID: child.Value.Definition.ID, RegistrationDigest: child.Digest, BindingID: binding.ID, BindingDigest: binding.Digest, ScopeDigest: v.ScopeDigest, LocalCriteria: slices.Clone(v.Scope.Included), Coverage: slices.Clone(v.Child.Value.Child.Coverage)})
		}
		if !valid || proposal.Validate(parent.Value.Definition.ID, scope, 0, 1, maxProductWorkItems) != nil {
			unavailable = append(unavailable, ProductCoveragePending{WorkItemID: parent.Value.Definition.ID, RecordID: group.ID, Reason: "split_scope_mapping_unavailable"})
			continue
		}
		if parents[parent.Value.Definition.ID] {
			return nil, nil, ErrProductCoverageHeld
		}
		parents[parent.Value.Definition.ID] = true
		groups = append(groups, descriptor)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].ParentWorkItemID < groups[j].ParentWorkItemID })
	return groups, unavailable, nil
}

// Only authenticated, current merge facts enter this reducer. A merged part
// contributes its local obligations; all declared parts must be present.
func aggregateProductSplitCoverage(groups []ProductSplitCoverage, facts map[string]ProductCoverageFact) map[string]bool {
	covered := map[string]bool{}
	for gi := range groups {
		group := &groups[gi]
		type parts struct {
			count    int
			whole    bool
			valid    bool
			expected map[string]bool
			merged   map[string]bool
		}
		obligations := map[string]*parts{}
		for ci := range group.Children {
			child := &group.Children[ci]
			fact, ok := facts[child.WorkItemID]
			child.Merged = ok && fact.WorkItemID == child.WorkItemID && fact.RegistrationDigest == child.RegistrationDigest && fact.SplitScopeDigest == child.ScopeDigest && sameStringSet(fact.LocalCriteria, child.LocalCriteria)
			for _, mapping := range child.Coverage {
				obligation := obligations[mapping.ParentCriterion]
				if obligation == nil {
					obligation = &parts{valid: true, expected: map[string]bool{}, merged: map[string]bool{}}
					obligations[mapping.ParentCriterion] = obligation
				}
				if mapping.PartID == "" {
					if len(obligation.expected) > 0 || mapping.PartCount != 0 {
						obligation.valid = false
					}
					obligation.whole = true
				} else {
					if obligation.whole || mapping.PartCount < 2 || obligation.count != 0 && obligation.count != mapping.PartCount {
						obligation.valid = false
					}
					obligation.count = mapping.PartCount
				}
				if obligation.expected[mapping.PartID] {
					obligation.valid = false
				}
				obligation.expected[mapping.PartID] = true
				if child.Merged && slices.Contains(fact.LocalCriteria, mapping.ChildCriterion) {
					obligation.merged[mapping.PartID] = true
				}
			}
		}
		for criterion, obligation := range obligations {
			expected := obligation.count
			if obligation.whole {
				expected = 1
			}
			if obligation.valid && expected > 0 && len(obligation.expected) == expected && len(obligation.merged) == expected {
				covered[criterion] = true
			}
		}
	}
	return covered
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	left, right := slices.Clone(a), slices.Clone(b)
	sort.Strings(left)
	sort.Strings(right)
	return slices.Equal(left, right)
}

// A saved quality snapshot must refer to an independently sealed verifier of
// the exact local scope. Global Task criterion claims are only bookkeeping.
// This static audit preserves merged history without requiring a new runner.
func splitMergedLocalScope(ctx context.Context, q queryer, registration ProductWorkItemRegistrationSnapshot, job ProductMerge) (string, []string, error) {
	ref := registration.Value.SplitScope
	if ref == nil {
		return "", nil, ErrProductCoverageHeld
	}
	var binding ProductSplitChildScopeBinding
	if err := taskEvidenceRecord(ctx, q, "product_split_child_scope_history", ref.BindingID, &binding); err != nil {
		return "", nil, err
	}
	digest, err := contentDigest(binding.Value)
	if err != nil || binding.ID != ref.BindingID || binding.Digest != ref.BindingDigest || digest != binding.Digest || binding.Value.Child.Value.WorkItemID != registration.Value.Definition.ID {
		return "", nil, ErrTaskEvidence
	}
	scopeDigest, err := contentDigest(binding.Value.Scope)
	if err != nil || scopeDigest != binding.Value.ScopeDigest || binding.Value.Scope.Validate() != nil {
		return "", nil, ErrTaskEvidence
	}
	var ready ProductPublicationReadiness
	if err := taskEvidenceRecord(ctx, q, "product_publication_readiness_history", job.Value.ReadinessID, &ready); err != nil {
		return "", nil, err
	}
	quality := ready.Value.Quality
	digest, err = contentDigest(quality.Value)
	if err != nil || digest != quality.Digest || quality.Value.RegistrationDigest != registration.Digest || len(quality.Value.Verifications) == 0 {
		return "", nil, ErrTaskEvidence
	}
	ids := make([]string, 0, len(quality.Value.Verifications))
	for id := range quality.Value.Verifications {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if !validSourceDigest(quality.Value.Verifications[id]) {
			return "", nil, ErrTaskEvidence
		}
		plan, err := loadProductVerificationPlan(ctx, q, id)
		if err != nil {
			return "", nil, err
		}
		sealed, err := sealedProductEvidence(ctx, q, plan.Value.TaskID, plan.Value.RunID)
		if err != nil {
			return "", nil, err
		}
		input := sealed.Value.Verification
		if input == nil || input.ScopeDigest != scopeDigest || input.Subject.Value.WorkItemID != registration.Value.Definition.ID || plan.Value.Target != quality.Value.Target || plan.Value.Candidate != quality.Value.Candidate || !plan.Value.Plan.Ready() || plan.Value.Plan.Validate(registration.Value.Definition.ID, plan.Value.Candidate, binding.Value.Scope, input.Policy.Value.Verification) != nil {
			return "", nil, ErrTaskEvidence
		}
	}
	return scopeDigest, slices.Clone(binding.Value.Scope.Included), nil
}
