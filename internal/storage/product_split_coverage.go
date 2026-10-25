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
	DelegatedComplete  bool                 `json:"delegated_complete,omitempty"`
}
type ProductSplitCoverage struct {
	ParentDepth              int                         `json:"parent_depth,omitempty"`
	ParentScopeDigest        string                      `json:"parent_scope_digest"`
	ParentLocalCriteria      []string                    `json:"parent_local_criteria"`
	CoveredLocalCriteria     []string                    `json:"covered_local_criteria,omitempty"`
	Complete                 bool                        `json:"complete,omitempty"`
	ID                       string                      `json:"id"`
	Digest                   string                      `json:"digest"`
	ParentWorkItemID         string                      `json:"parent_work_item_id"`
	ParentRegistrationDigest string                      `json:"parent_registration_digest"`
	Children                 []ProductSplitCoverageChild `json:"children"`
}

// Authenticate the complete group, not just a child head or a declared part.
// Each group maps only to its immediate parent scope, including nested groups.
func productSplitCoverageTx(ctx context.Context, tx *sql.Tx, w domain.Wave, heads map[string]ProductWorkItemRegistrationSnapshot) ([]ProductSplitCoverage, []ProductCoveragePending, error) {
	type proof struct {
		Groups      []ProductSplitCoverage
		Unavailable []ProductCoveragePending
	}
	input := struct {
		Wave  domain.Wave
		Heads map[string]ProductWorkItemRegistrationSnapshot
	}{w, heads}
	value, err := readSplitProof(ctx, tx, "productSplitCoverage", input, func(read context.Context) (proof, error) {
		groups, unavailable, err := collectProductSplitCoverageTx(read, tx, w, heads)
		return proof{groups, unavailable}, err
	})
	return value.Groups, value.Unavailable, err
}

func collectProductSplitCoverageTx(ctx context.Context, tx *sql.Tx, w domain.Wave, heads map[string]ProductWorkItemRegistrationSnapshot) ([]ProductSplitCoverage, []ProductCoveragePending, error) {
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
		if !ok || !productRegistrationScope(parent, w) || len(group.Value.Children) < 2 || len(group.Value.Children) != len(group.Value.Registrations) {
			unavailable = append(unavailable, ProductCoveragePending{WorkItemID: group.Value.Request.ParentWorkItemID, RecordID: group.ID, Reason: "split_parent_registration_unavailable"})
			continue
		}
		scope := verification.Scope{Revision: w.ScopeRevision, OriginalRequest: w.OriginalRequest, Included: slices.Clone(parent.Value.Definition.Criteria), Remaining: []string{}, PreviouslyMerged: []string{}}
		for _, criterion := range w.Criteria {
			if slices.Contains(scope.Included, criterion.ID) {
				scope.Criteria = append(scope.Criteria, verification.Criterion{ID: criterion.ID, Requirement: criterion.Requirement})
			}
		}
		depth := 0
		if parent.Value.SplitScope != nil {
			binding, err := productSplitChildScopeTx(ctx, tx, w, parent)
			if err != nil || binding == nil {
				if ctx.Err() != nil {
					return nil, nil, ctx.Err()
				}
				if err != nil && !readinessHoldError(err) && !errors.Is(err, sql.ErrNoRows) {
					return nil, nil, err
				}
				unavailable = append(unavailable, ProductCoveragePending{WorkItemID: parent.Value.Definition.ID, RecordID: group.ID, Reason: "split_parent_scope_unavailable"})
				continue
			}
			scope = binding.Value.Scope
			depth = binding.Value.Child.Value.Depth
		}
		scopeDigest, err := contentDigest(scope)
		if err != nil {
			return nil, nil, err
		}
		descriptor := ProductSplitCoverage{ID: group.ID, Digest: group.Digest, ParentWorkItemID: parent.Value.Definition.ID, ParentRegistrationDigest: parent.Digest, ParentDepth: depth, ParentScopeDigest: scopeDigest, ParentLocalCriteria: slices.Clone(scope.Included), Children: []ProductSplitCoverageChild{}}
		proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: parent.Value.Definition.ID, ScopeRevision: w.ScopeRevision, Outcome: "split", Children: []splitting.Child{}}
		valid := true
		for i, binding := range group.Value.Children {
			child := group.Value.Registrations[i]
			v := binding.Value
			if v.ParentRegistrationDigest != parent.Digest || v.ParentScopeDigest != scopeDigest || !reflect.DeepEqual(heads[child.Value.Definition.ID], child) || v.Child.Value.Depth != depth+1 {
				valid = false
				break
			}
			proposal.Children = append(proposal.Children, v.Child.Value.Child)
			descriptor.Children = append(descriptor.Children, ProductSplitCoverageChild{WorkItemID: child.Value.Definition.ID, RegistrationDigest: child.Digest, BindingID: binding.ID, BindingDigest: binding.Digest, ScopeDigest: v.ScopeDigest, LocalCriteria: slices.Clone(v.Scope.Included), Coverage: slices.Clone(v.Child.Value.Child.Coverage)})
		}
		if !valid || proposal.Validate(parent.Value.Definition.ID, scope, depth, depth+1, maxProductWorkItems) != nil {
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

// Only authenticated groups and current merge facts enter this reducer. A
// delegated node completes through every local obligation of its own group;
// it never acquires a synthetic merge fact. Only root groups project Wave IDs.
func aggregateProductSplitCoverage(groups []ProductSplitCoverage, facts map[string]ProductCoverageFact) map[string]bool {
	covered := map[string]bool{}
	if len(groups) > maxProductWorkItems {
		return covered
	}
	byParent := map[string]int{}
	duplicates := map[string]bool{}
	for i := range groups {
		group := &groups[i]
		group.Complete, group.CoveredLocalCriteria = false, nil
		for ci := range group.Children {
			group.Children[ci].Merged, group.Children[ci].DelegatedComplete = false, false
		}
		if _, exists := byParent[group.ParentWorkItemID]; exists {
			duplicates[group.ParentWorkItemID] = true
		}
		byParent[group.ParentWorkItemID] = i
	}
	state := make([]int, len(groups))
	var visit func(int) map[string]bool
	visit = func(gi int) map[string]bool {
		group := &groups[gi]
		local := map[string]bool{}
		if duplicates[group.ParentWorkItemID] || state[gi] == 1 {
			return local
		}
		if state[gi] == 2 {
			for _, id := range group.CoveredLocalCriteria {
				local[id] = true
			}
			return local
		}
		state[gi] = 1
		type parts struct {
			count              int
			whole, valid       bool
			expected, complete map[string]bool
		}
		obligations := map[string]*parts{}
		for ci := range group.Children {
			child := &group.Children[ci]
			fact, ok := facts[child.WorkItemID]
			child.Merged = ok && fact.WorkItemID == child.WorkItemID && fact.RegistrationDigest == child.RegistrationDigest && fact.SplitScopeDigest == child.ScopeDigest && sameStringSet(fact.LocalCriteria, child.LocalCriteria)
			contributes := child.Merged
			if index, delegated := byParent[child.WorkItemID]; delegated {
				contributes = false // An own PR cannot bypass incomplete delegation.
				nested := &groups[index]
				if nested.ParentDepth == group.ParentDepth+1 && nested.ParentRegistrationDigest == child.RegistrationDigest && nested.ParentScopeDigest == child.ScopeDigest && sameStringSet(nested.ParentLocalCriteria, child.LocalCriteria) {
					visit(index)
					child.DelegatedComplete = nested.Complete
					contributes = child.DelegatedComplete
				}
			}
			for _, mapping := range child.Coverage {
				obligation := obligations[mapping.ParentCriterion]
				if obligation == nil {
					obligation = &parts{valid: true, expected: map[string]bool{}, complete: map[string]bool{}}
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
				if contributes && slices.Contains(child.LocalCriteria, mapping.ChildCriterion) {
					obligation.complete[mapping.PartID] = true
				}
			}
		}
		for criterion, obligation := range obligations {
			expected := obligation.count
			if obligation.whole {
				expected = 1
			}
			if obligation.valid && expected > 0 && len(obligation.expected) == expected && len(obligation.complete) == expected && slices.Contains(group.ParentLocalCriteria, criterion) {
				local[criterion] = true
				group.CoveredLocalCriteria = append(group.CoveredLocalCriteria, criterion)
			}
		}
		sort.Strings(group.CoveredLocalCriteria)
		group.Complete = len(group.ParentLocalCriteria) > 0 && sameStringSet(group.CoveredLocalCriteria, group.ParentLocalCriteria)
		state[gi] = 2
		return local
	}
	for i := range groups {
		local := visit(i)
		if groups[i].ParentDepth == 0 {
			for criterion := range local {
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

// Static verifier audit selects the original namespace from the registered
// scope. A missing nested envelope never falls back to a legacy binding copy.
// Current group authority is checked separately by the coverage collector.
func splitMergedScopeBinding(ctx context.Context, q queryer, registration ProductWorkItemRegistrationSnapshot) (ProductSplitChildScopeBinding, error) {
	var binding ProductSplitChildScopeBinding
	ref := registration.Value.SplitScope
	if ref == nil {
		return binding, ErrProductCoverageHeld
	}
	if ref.AncestorSet == nil {
		err := taskEvidenceRecord(ctx, q, "product_split_child_scope_history", ref.BindingID, &binding)
		return binding, err
	}
	var saved ProductNestedSplitChildScopes
	if err := taskEvidenceRecord(ctx, q, "product_nested_split_child_scope_history", ref.BindingID, &saved); err != nil {
		return binding, err
	}
	digest, err := contentDigest(saved.Value)
	if err != nil || digest != saved.Digest || saved.ID != structureID("nested-split-scopes-", ref.Request.ProposalID, digest) {
		return binding, ErrTaskEvidence
	}
	scopes := saved.Value.Scopes
	digest, err = contentDigest(scopes.Value)
	if err != nil || scopes.Digest != digest || scopes.ID != structureID("split-child-scopes-", ref.Request.ProposalID, digest) || scopes.ID != ref.SetID || scopes.Digest != ref.SetDigest || scopes.Value.Request != ref.Request {
		return binding, ErrTaskEvidence
	}
	found := false
	for _, candidate := range scopes.Value.Children {
		if candidate.ID != ref.BindingID && candidate.Value.Child.Value.WorkItemID != registration.Value.Definition.ID {
			continue
		}
		if found || candidate.ID != ref.BindingID || candidate.Digest != ref.BindingDigest || candidate.Value.Child.Value.WorkItemID != registration.Value.Definition.ID || candidate.Value.Request != ref.Request {
			return binding, ErrTaskEvidence
		}
		binding, found = candidate, true
	}
	if !found {
		return binding, ErrTaskEvidence
	}
	return binding, nil
}

// A saved quality snapshot must refer to an independently sealed verifier of
// the exact local scope. Global Task criterion claims are only bookkeeping.
// This static audit preserves merged history without requiring a new runner.
func splitMergedLocalScope(ctx context.Context, q queryer, registration ProductWorkItemRegistrationSnapshot, job ProductMerge) (string, []string, error) {
	ref := registration.Value.SplitScope
	if ref == nil {
		return "", nil, ErrProductCoverageHeld
	}
	binding, err := splitMergedScopeBinding(ctx, q, registration)
	if err != nil {
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
		machine, err := productVerificationEvidenceFromPlan(ctx, q, plan)
		if err != nil {
			return "", nil, err
		}
		if !machine.Value.Complete || !machine.Value.Passed || machine.Value.Candidate != quality.Value.Candidate || machine.Digest != quality.Value.Verifications[id] {
			return "", nil, ErrTaskEvidence
		}
	}
	return scopeDigest, slices.Clone(binding.Value.Scope.Included), nil
}
