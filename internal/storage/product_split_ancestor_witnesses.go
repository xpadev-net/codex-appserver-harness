package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Ancestor witnesses carry prohibitions only. They contain source identities,
// consumed creation attempts and reserved WorkItem identities, without copying
// requirement bodies, candidates or recursively embedded registration proofs.
type ProductSplitAncestorChildValue struct {
	Child             splitChildSourceChild                `json:"child"`
	Issue             splitChildSourceIssue                `json:"issue"`
	InitialCreationID string                               `json:"initial_creation_id"`
	CreationID        string                               `json:"creation_id"`
	Creations         []ProductSplitChildCreationReference `json:"creations"`
}
type ProductSplitAncestorChild struct {
	Value ProductSplitAncestorChildValue `json:"value"`
}
type ProductSplitAncestorGroup struct {
	Request            ProductSplitChildPlanRequest `json:"request"`
	ParentHeadID       string                       `json:"parent_head_id"`
	ParentWorkItemID   string                       `json:"parent_work_item_id,omitempty"`
	SourcePolicyDigest string                       `json:"source_policy_digest,omitempty"`
	ScopeSetID         string                       `json:"scope_set_id"`
	ScopeSetDigest     string                       `json:"scope_set_digest"`
	Children           []ProductSplitAncestorChild  `json:"children"`
}
type ProductSplitAncestorWitnessesValue struct {
	ScopeEnvelopeID     string                      `json:"scope_envelope_id"`
	ScopeEnvelopeDigest string                      `json:"scope_envelope_digest"`
	Groups              []ProductSplitAncestorGroup `json:"groups"`
}
type ProductSplitAncestorWitnesses struct {
	ID     string                             `json:"id"`
	Value  ProductSplitAncestorWitnessesValue `json:"value"`
	Digest string                             `json:"digest"`
}

func splitAncestorGroup(scopes ProductSplitChildScopesValue, setID, setDigest, parentWorkID, policyDigest string) ProductSplitAncestorGroup {
	out := ProductSplitAncestorGroup{Request: scopes.Request, ParentHeadID: scopes.ParentHeadID, ParentWorkItemID: parentWorkID, SourcePolicyDigest: policyDigest, ScopeSetID: setID, ScopeSetDigest: setDigest, Children: []ProductSplitAncestorChild{}}
	for _, binding := range scopes.Children {
		v := binding.Value
		out.Children = append(out.Children, ProductSplitAncestorChild{Value: ProductSplitAncestorChildValue{Child: splitChildSourceChild{ID: v.Child.ID, Value: splitChildSourceWork{WorkItemID: v.Child.Value.WorkItemID}}, Issue: splitChildSourceIssue{RepositoryID: v.Issue.RepositoryID, ID: v.Issue.ID, Number: v.Issue.Number}, InitialCreationID: v.InitialCreationID, CreationID: v.CreationID, Creations: append([]ProductSplitChildCreationReference{}, v.Creations...)}})
	}
	sort.Slice(out.Children, func(i, j int) bool {
		return out.Children[i].Value.Child.Value.WorkItemID < out.Children[j].Value.Child.Value.WorkItemID
	})
	return out
}

func canonicalSplitAncestorGroups(waveID string, groups []ProductSplitAncestorGroup) ([]ProductSplitAncestorGroup, error) {
	if len(groups) == 0 || len(groups) > maxProductWorkItems {
		return nil, ErrProductSplitHeld
	}
	unique := map[string]ProductSplitAncestorGroup{}
	work := map[string]string{}
	seats := map[string]bool{}
	issues := map[string]string{}
	issueIDs := map[string]string{}
	retries := map[string]bool{}
	creationOwners := map[string]string{}
	for _, group := range groups {
		r := group.Request
		if !r.Target.Valid() || r.Target.WaveID != waveID || r.ParentWorkItemID == "" || r.ProposalID == "" || !validSourceDigest(r.ProposalDigest) || group.ParentHeadID != splitProposalHead(ProductSplitProposalRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID}) || group.ScopeSetID == "" || !validSourceDigest(group.ScopeSetDigest) || len(group.Children) < 2 || len(group.Children) > maxProductWorkItems || ((group.ParentWorkItemID == "") != (group.SourcePolicyDigest == "")) || group.SourcePolicyDigest != "" && !validSourceDigest(group.SourcePolicyDigest) {
			return nil, ErrProductSplitHeld
		}
		group.Children = append([]ProductSplitAncestorChild{}, group.Children...)
		sort.Slice(group.Children, func(i, j int) bool {
			return group.Children[i].Value.Child.Value.WorkItemID < group.Children[j].Value.Child.Value.WorkItemID
		})
		seats[r.ParentWorkItemID] = true
		own := map[string]bool{}
		for _, child := range group.Children {
			v := child.Value
			id := v.Child.Value.WorkItemID
			if id == "" || v.Child.ID == "" || own[id] || v.Issue.RepositoryID < 1 || v.Issue.ID < 1 || v.Issue.Number < 1 || len(v.Creations) < 1 || len(v.Creations) > 33 || v.InitialCreationID != v.Creations[0].ID || v.CreationID != v.Creations[len(v.Creations)-1].ID {
				return nil, ErrProductSplitHeld
			}
			own[id] = true
			seats[id] = true
			for i, creation := range v.Creations {
				if !strings.HasPrefix(creation.ID, "split-create-") || len(creation.ID) > 192 || !validSourceDigest(creation.Digest) || creation.Attempt != i || i == 0 && creation.PreviousID != "" || i > 0 && creation.PreviousID != v.Creations[i-1].ID {
					return nil, ErrProductSplitHeld
				}
				if prior, ok := creationOwners[creation.ID]; ok && prior != id {
					return nil, ErrProductSplitHeld
				}
				creationOwners[creation.ID] = id
				if i > 0 {
					retries[creation.ID] = true
				}
			}
			signature, err := contentDigest(v)
			if err != nil {
				return nil, err
			}
			if prior, ok := work[id]; ok && prior != signature {
				return nil, ErrProductSplitHeld
			}
			issueKey := fmt.Sprintf("%d:%d", v.Issue.RepositoryID, v.Issue.Number)
			if prior, ok := issues[issueKey]; ok && prior != id {
				return nil, ErrProductSplitHeld
			}
			issueIDKey := fmt.Sprintf("%d:%d", v.Issue.RepositoryID, v.Issue.ID)
			if prior, ok := issueIDs[issueIDKey]; ok && prior != id {
				return nil, ErrProductSplitHeld
			}
			work[id], issues[issueKey], issueIDs[issueIDKey] = signature, id, id
		}
		if group.ParentWorkItemID != "" && group.ParentWorkItemID != r.ParentWorkItemID && !own[group.ParentWorkItemID] {
			return nil, ErrProductSplitHeld
		}
		key := group.ScopeSetID + ":" + group.ParentWorkItemID
		if prior, ok := unique[key]; ok {
			a, err := contentDigest(prior)
			if err != nil {
				return nil, err
			}
			b, err := contentDigest(group)
			if err != nil {
				return nil, err
			}
			if a != b {
				return nil, ErrProductSplitHeld
			}
		}
		unique[key] = group
	}
	if len(seats) > maxProductWorkItems || len(retries) > 32 {
		return nil, ErrProductSplitHeld
	}
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]ProductSplitAncestorGroup, 0, len(keys))
	for _, key := range keys {
		out = append(out, unique[key])
	}
	return out, nil
}

// InspectProductSplitAncestorWitnesses authenticates the canonical scope proof
// before deriving compact negative witnesses. It writes no record or admission
// and grants no registration, source readiness, quality or execution authority.
func (k *KernelStore) InspectProductSplitAncestorWitnesses(ctx context.Context, r ProductSplitChildPlanRequest) (ProductSplitAncestorWitnesses, error) {
	var out ProductSplitAncestorWitnesses
	if !r.Target.Valid() || r.ParentWorkItemID == "" || r.ProposalID == "" || !validSourceDigest(r.ProposalDigest) {
		return out, ErrProductSplitHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	scopes, err := loadNestedSplitChildScopes(ctx, tx, r)
	if err != nil {
		return out, splitEvidenceErr(err)
	}
	value := scopes.Value
	groups, err := canonicalSplitAncestorGroups(r.Target.WaveID, []ProductSplitAncestorGroup{
		splitAncestorGroup(value.ProductSplitChildScopesValue, value.ScopeSetID, value.ScopeSetDigest, value.ParentWorkItemID, value.SourcePolicyDigest),
		splitAncestorGroup(value.Scopes.Value, value.Scopes.ID, value.Scopes.Digest, value.ParentWorkItemID, value.SourcePolicyDigest),
	})
	if err != nil {
		return out, err
	}
	out.Value = ProductSplitAncestorWitnessesValue{ScopeEnvelopeID: scopes.ID, ScopeEnvelopeDigest: scopes.Digest, Groups: groups}
	out.Digest, err = contentDigest(out.Value)
	if err != nil {
		return out, err
	}
	out.ID = structureID("split-ancestor-witnesses-", scopes.ID, out.Digest)
	raw, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	if len(raw) > 16<<20 {
		return ProductSplitAncestorWitnesses{}, ErrProductSplitHeld
	}
	return out, nil
}
