package storage

import (
	"reflect"
	"slices"
	"sort"
)

// An immutable base packet retains why every actual leaf merge was required.
// Delegation supplies group provenance, never a synthetic merge receipt or SHA.
type ProductSplitDependencyOrigin struct {
	WorkItemID         string   `json:"work_item_id"`
	RegistrationDigest string   `json:"registration_digest"`
	GroupID            string   `json:"group_id"`
	GroupDigest        string   `json:"group_digest"`
	ScopeDigest        string   `json:"scope_digest"`
	LeafWorkItemIDs    []string `json:"leaf_work_item_ids"`
}

// The caller supplies coverage authenticated in the same SQL transaction. This
// reducer rechecks completion from actual facts and returns every required leaf.
func productMergeDependencyLeaves(coverage ProductCoverageLedger, required []string) ([]string, []ProductSplitDependencyOrigin, error) {
	facts := map[string]ProductCoverageFact{}
	ambiguous := map[string]bool{}
	for _, fact := range coverage.Value.Merged {
		if prior, exists := facts[fact.WorkItemID]; exists && !reflect.DeepEqual(prior, fact) {
			ambiguous[fact.WorkItemID] = true
		}
		facts[fact.WorkItemID] = fact
	}
	groups := slices.Clone(coverage.Value.Splits)
	if len(groups) > maxProductWorkItems {
		return nil, nil, ErrProductMergeDependencyHeld
	}
	byParent := map[string]int{}
	for i := range groups {
		groups[i].Children = slices.Clone(groups[i].Children)
		if _, exists := byParent[groups[i].ParentWorkItemID]; exists {
			return nil, nil, ErrProductMergeDependencyHeld
		}
		byParent[groups[i].ParentWorkItemID] = i
	}
	unavailable := map[string]bool{}
	for _, pending := range coverage.Value.Unavailable {
		if pending.WorkItemID != "" {
			unavailable[pending.WorkItemID] = true
		}
	}
	aggregateProductSplitCoverage(groups, facts)
	origins := map[string]ProductSplitDependencyOrigin{}
	active := map[string]bool{}
	var visit func(string) ([]string, error)
	visit = func(id string) ([]string, error) {
		if id == "" || unavailable[id] || ambiguous[id] || active[id] || len(active) >= maxProductWorkItems {
			return nil, ErrProductMergeDependencyHeld
		}
		index, delegated := byParent[id]
		if !delegated {
			if _, exists := facts[id]; !exists {
				return nil, ErrProductMergeDependencyHeld
			}
			return []string{id}, nil
		}
		if origin, ok := origins[id]; ok {
			return slices.Clone(origin.LeafWorkItemIDs), nil
		}
		group := groups[index]
		if !group.Complete {
			return nil, ErrProductMergeDependencyHeld
		}
		active[id] = true
		defer delete(active, id)
		leaves := map[string]bool{}
		for _, child := range group.Children {
			ids, err := visit(child.WorkItemID)
			if err != nil {
				return nil, err
			}
			for _, leaf := range ids {
				if leaves[leaf] {
					return nil, ErrProductMergeDependencyHeld
				}
				leaves[leaf] = true
			}
		}
		ids := make([]string, 0, len(leaves))
		for leaf := range leaves {
			ids = append(ids, leaf)
		}
		sort.Strings(ids)
		origins[id] = ProductSplitDependencyOrigin{WorkItemID: id, RegistrationDigest: group.ParentRegistrationDigest, GroupID: group.ID, GroupDigest: group.Digest, ScopeDigest: group.ParentScopeDigest, LeafWorkItemIDs: ids}
		return slices.Clone(ids), nil
	}
	leaves := map[string]bool{}
	for _, id := range required {
		ids, err := visit(id)
		if err != nil {
			return nil, nil, err
		}
		for _, leaf := range ids {
			leaves[leaf] = true
		}
	}
	ids := make([]string, 0, len(leaves))
	for id := range leaves {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	provenance := make([]ProductSplitDependencyOrigin, 0, len(origins))
	for _, origin := range origins {
		provenance = append(provenance, origin)
	}
	sort.Slice(provenance, func(i, j int) bool { return provenance[i].WorkItemID < provenance[j].WorkItemID })
	return ids, provenance, nil
}
