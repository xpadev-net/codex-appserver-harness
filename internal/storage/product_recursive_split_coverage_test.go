package storage

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
)

func TestProductNestedSplitCoverageRetiresEveryAuthenticatedDelegatedNode(t *testing.T) {
	ctx := context.Background()
	k, r, _ := nestedRegistrationScopeFixture(t)
	registered, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := k.ProductCoverage(ctx, r.Target)
	if err != nil || len(ledger.Value.Splits) != 2 || len(ledger.Value.Retired) != 2 || len(ledger.Value.Unavailable) != 0 || len(ledger.Value.UnmergedWorkItems) != 3 || len(ledger.Value.Merged) != 0 || len(ledger.Value.Residual) != 2 {
		t.Fatal("nested delegation unavailable", ledger, err)
	}
	var nested ProductSplitCoverage
	for _, group := range ledger.Value.Splits {
		if group.ParentWorkItemID == r.ParentWorkItemID {
			nested = group
		}
		if slices.Contains(ledger.Value.UnmergedWorkItems, group.ParentWorkItemID) {
			t.Fatal("delegated parent still requires own PR", group)
		}
	}
	if nested.ID != registered.ID || nested.ParentDepth != 1 || nested.Complete || !sameStringSet(nested.ParentLocalCriteria, []string{"second-ac", "extra-ac"}) {
		t.Fatal("local group flattened", nested)
	}
	for i, registration := range registered.Value.Registrations {
		binding, err := splitMergedScopeBinding(ctx, k.DB, registration)
		if err != nil || !reflect.DeepEqual(binding, registered.Value.Children[i]) {
			t.Fatal("static verifier audit lost nested local scope", err)
		}
	}
	t.Run("legacy_copy_cannot_replace_nested_history", func(t *testing.T) {
		isolated := cloneNestedScopeStore(t, k)
		registration, binding := registered.Value.Registrations[0], registered.Value.Children[0]
		if err := isolated.Put(ctx, "product_split_child_scope_history", binding.ID, r.Target.WaveID, binding); err != nil {
			t.Fatal(err)
		}
		if _, err := isolated.DB.Exec("DELETE FROM records WHERE kind='product_nested_split_child_scope_history' AND id=?", binding.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := splitMergedScopeBinding(ctx, isolated.DB, registration); err == nil {
			t.Fatal("legacy copy replaced missing nested audit envelope")
		}
	})
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("coverage changed budget", err)
	}
	for _, loss := range []struct{ kind, id string }{
		{"product_nested_split_registrations", r.ProposalID},
		{"product_nested_split_registrations_admission", r.ProposalID},
		{"product_nested_split_child_scope_history", registered.Value.Children[0].ID},
		{"product_work_item_registration", registered.Value.Registrations[0].Value.Definition.ID},
	} {
		t.Run("loss_"+loss.kind, func(t *testing.T) {
			isolated := cloneNestedScopeStore(t, k)
			if _, err := isolated.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			held, err := isolated.ProductCoverage(ctx, r.Target)
			if err != nil || len(held.Value.Unavailable) == 0 || len(held.Value.Residual) != 2 || !slices.Contains(held.Value.UnmergedWorkItems, r.ParentWorkItemID) {
				t.Fatal("missing group retained retirement", held, err)
			}
			identified := false
			for _, pending := range held.Value.Unavailable {
				identified = identified || pending.WorkItemID == r.ParentWorkItemID
			}
			if !identified {
				t.Fatal("lost delegation omitted its parent identity", held.Value.Unavailable)
			}
		})
	}
}

// Reducer boundary: descriptors stand for groups already authenticated by the
// storage collector. Facts here are inputs to reduction, not real merge receipts.
func recursiveCoverageReducerFixture() ([]ProductSplitCoverage, map[string]ProductCoverageFact) {
	child := func(id, local, parent, part string) ProductSplitCoverageChild {
		return ProductSplitCoverageChild{WorkItemID: id, RegistrationDigest: "reg-" + id, ScopeDigest: "scope-" + id, LocalCriteria: []string{local}, Coverage: []splitting.Coverage{{ParentCriterion: parent, ChildCriterion: local, PartID: part, PartCount: 2}}}
	}
	root := ProductSplitCoverage{ID: "root-group", ParentWorkItemID: "root", ParentRegistrationDigest: "root-reg", ParentScopeDigest: "root-scope", ParentLocalCriteria: []string{"AC"}, Children: []ProductSplitCoverageChild{child("left", "local", "AC", "left"), child("right", "right-local", "AC", "right")}}
	nested := ProductSplitCoverage{ID: "nested-group", ParentWorkItemID: "left", ParentRegistrationDigest: "reg-left", ParentScopeDigest: "scope-left", ParentDepth: 1, ParentLocalCriteria: []string{"local"}, Children: []ProductSplitCoverageChild{child("grand-left", "AC", "local", "one"), child("grand-right", "grand-local", "local", "two")}}
	deeper := ProductSplitCoverage{ID: "deeper-group", ParentWorkItemID: "grand-left", ParentRegistrationDigest: "reg-grand-left", ParentScopeDigest: "scope-grand-left", ParentDepth: 2, ParentLocalCriteria: []string{"AC"}, Children: []ProductSplitCoverageChild{child("great-left", "great-one", "AC", "one"), child("great-right", "great-two", "AC", "two")}}
	facts := map[string]ProductCoverageFact{}
	for _, c := range []ProductSplitCoverageChild{root.Children[1], nested.Children[1], deeper.Children[0], deeper.Children[1]} {
		facts[c.WorkItemID] = ProductCoverageFact{WorkItemID: c.WorkItemID, RegistrationDigest: c.RegistrationDigest, SplitScopeDigest: c.ScopeDigest, LocalCriteria: slices.Clone(c.LocalCriteria)}
	}
	return []ProductSplitCoverage{deeper, root, nested}, facts // Deliberately not tree order.
}

func TestProductRecursiveSplitCoverageKeepsLocalIDsPartsAndActualMergesSeparate(t *testing.T) {
	for _, mode := range []string{"all", "missing_root_sibling", "missing_great_leaf", "wrong_leaf_scope", "wrong_parent_scope", "wrong_parent_registration", "wrong_parent_local", "wrong_depth", "duplicate_parent", "orphan", "duplicate_part", "missing_part", "own_merge_cannot_bypass"} {
		t.Run(mode, func(t *testing.T) {
			groups, facts := recursiveCoverageReducerFixture()
			switch mode {
			case "missing_root_sibling":
				delete(facts, "right")
			case "missing_great_leaf":
				delete(facts, "great-right")
			case "wrong_leaf_scope":
				f := facts["great-right"]
				f.SplitScopeDigest = "wrong"
				facts["great-right"] = f
			case "wrong_parent_scope":
				groups[0].ParentScopeDigest = "wrong"
			case "wrong_parent_registration":
				groups[0].ParentRegistrationDigest = "wrong"
			case "wrong_parent_local":
				groups[0].ParentLocalCriteria = []string{"other"}
			case "wrong_depth":
				groups[0].ParentDepth = 1
			case "duplicate_parent":
				groups = append(groups, groups[0])
			case "orphan":
				groups = groups[:1]
			case "duplicate_part":
				groups[0].Children[1].Coverage[0].PartID = "one"
			case "missing_part":
				groups[0].Children[1].Coverage[0].PartCount = 3
			case "own_merge_cannot_bypass":
				delete(facts, "great-right")
				c := groups[2].Children[0]
				facts[c.WorkItemID] = ProductCoverageFact{WorkItemID: c.WorkItemID, RegistrationDigest: c.RegistrationDigest, SplitScopeDigest: c.ScopeDigest, LocalCriteria: slices.Clone(c.LocalCriteria)}
			}
			covered := aggregateProductSplitCoverage(groups, facts)
			if covered["AC"] != (mode == "all") || covered["local"] || covered["right-local"] {
				t.Fatal("local coverage escaped its parent", covered, groups)
			}
			if mode == "all" {
				if !groups[0].Complete || !groups[1].Complete || !groups[2].Complete || groups[1].Children[0].Merged || !groups[1].Children[0].DelegatedComplete || groups[2].Children[0].Merged || !groups[2].Children[0].DelegatedComplete {
					t.Fatal("delegated completion fabricated an own merge", groups)
				}
				raw, _ := json.Marshal(groups)
				delete(facts, "great-right")
				if aggregateProductSplitCoverage(groups, facts)["AC"] || groups[0].Complete || groups[1].Complete || groups[2].Complete {
					t.Fatal("prior reduction survived lost leaf", string(raw), groups)
				}
			}
		})
	}
}
