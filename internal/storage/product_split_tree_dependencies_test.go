package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductSplitDescendantsInheritAncestorDependenciesWithoutTaskDAG(t *testing.T) {
	ctx := context.Background()
	k, r, proof := nestedRegistrationScopeFixture(t)
	group, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	expected := proof.Value.Children[0].Value.Child.Value.WorkItemID
	before, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, registration := range group.Value.Registrations {
		tx, err := k.kernelTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ids, err := productSplitDependencyIDsTx(ctx, tx, w, []string{registration.Value.Definition.ID})
		tx.Rollback()
		if err != nil || !reflect.DeepEqual(ids, []string{expected}) {
			t.Fatal("ancestor sibling dependency disappeared", ids, err)
		}
		for _, kind := range []string{"work", "fix", "verification", "review"} {
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			task := domain.Task{Spec: contract.TaskSpec{Kind: kind, WorkItemIDs: []string{registration.Value.Definition.ID}, DependsOn: []contract.TaskDependency{}}}
			err = k.productMergeDependenciesTx(ctx, tx, w, task)
			tx.Rollback()
			if !errors.Is(err, ErrProductMergeDependencyHeld) {
				t.Fatal("omitted DAG bypassed unmerged ancestor peer", kind, err)
			}
		}
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("dependency reads changed budget", err)
	}
	isolated := cloneNestedScopeStore(t, k)
	if _, err := isolated.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration' AND id=?", expected); err != nil {
		t.Fatal(err)
	}
	tx, err := isolated.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if ids, err := productSplitDependencyIDsTx(ctx, tx, w, []string{group.Value.Registrations[0].Value.Definition.ID}); err == nil || len(ids) != 0 {
		t.Fatal("lost ancestor proof became dependency free", ids, err)
	}
}

// A reducer boundary, not fabricated service acceptance: the real SQL caller
// authenticates these facts before expansion. Exact leaf receipts stay distinct.
func splitDependencyReducerFixture() ProductCoverageLedger {
	groups, facts := recursiveCoverageReducerFixture()
	ledger := ProductCoverageLedger{Value: ProductCoverageValue{Splits: groups}}
	ids := make([]string, 0, len(facts))
	for id := range facts {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for i, id := range ids {
		fact := facts[id]
		fact.MergeID = "merge-" + id
		fact.MergeSHA = fmt.Sprintf("%040x", i+1)
		fact.ReceiptDigest = strings.Repeat("a", 64)
		ledger.Value.Merged = append(ledger.Value.Merged, fact)
	}
	return ledger
}

func TestProductSplitDependencyExpansionRequiresEveryActualLeafAndRetainsOrigins(t *testing.T) {
	for _, mode := range []string{"all", "missing_leaf", "parent_own_merge", "wrong_scope", "wrong_registration", "wrong_local", "wrong_depth", "duplicate_part", "ambiguous_leaf", "unrelated_ambiguity"} {
		t.Run(mode, func(t *testing.T) {
			ledger := splitDependencyReducerFixture()
			switch mode {
			case "missing_leaf", "parent_own_merge":
				ledger.Value.Merged = slices.DeleteFunc(ledger.Value.Merged, func(f ProductCoverageFact) bool { return f.WorkItemID == "great-right" })
				if mode == "parent_own_merge" {
					ledger.Value.Merged = append(ledger.Value.Merged, ProductCoverageFact{WorkItemID: "left", RegistrationDigest: "reg-left", SplitScopeDigest: "scope-left", LocalCriteria: []string{"local"}, MergeID: "own-parent-merge", MergeSHA: strings.Repeat("e", 40)})
				}
			case "wrong_scope":
				ledger.Value.Splits[0].ParentScopeDigest = "wrong"
			case "wrong_registration":
				ledger.Value.Splits[0].ParentRegistrationDigest = "wrong"
			case "wrong_local":
				ledger.Value.Splits[0].ParentLocalCriteria = []string{"local"}
			case "wrong_depth":
				ledger.Value.Splits[0].ParentDepth = 1
			case "duplicate_part":
				ledger.Value.Splits[0].Children[1].Coverage[0].PartID = "one"
			case "ambiguous_leaf":
				f := ledger.Value.Merged[0]
				f.MergeID = "different-merge"
				ledger.Value.Merged = append(ledger.Value.Merged, f)
			case "unrelated_ambiguity":
				ledger.Value.Merged = append(ledger.Value.Merged, ProductCoverageFact{WorkItemID: "unrelated", MergeID: "one"}, ProductCoverageFact{WorkItemID: "unrelated", MergeID: "two"})
			}
			leaves, origins, err := productMergeDependencyLeaves(ledger, []string{"left", "left"})
			success := mode == "all" || mode == "unrelated_ambiguity"
			if !success {
				if !errors.Is(err, ErrProductMergeDependencyHeld) || len(leaves) != 0 || len(origins) != 0 {
					t.Fatal("incomplete tree authorized dependency", leaves, origins, err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(leaves, []string{"grand-right", "great-left", "great-right"}) || len(origins) != 2 {
				t.Fatal("missing or synthetic leaf", leaves, origins, err)
			}
			for _, origin := range origins {
				if origin.GroupID == "" || origin.ScopeDigest == "" || origin.RegistrationDigest == "" || len(origin.LeafWorkItemIDs) == 0 {
					t.Fatal("tree provenance omitted", origin)
				}
			}
			// Every repository-local leaf merge must enter the frozen base ancestry.
			input := ProductBaseInput{Value: ProductBaseInputValue{SplitDependencies: origins}}
			input.Value.Preparation.Registration.Value.Definition.RepositoryID = 17
			expected := []string{strings.Repeat("f", 40)}
			for _, fact := range ledger.Value.Merged {
				if !slices.Contains(leaves, fact.WorkItemID) {
					continue
				}
				input.Value.Dependencies = append(input.Value.Dependencies, ProductBaseDependency{Fact: fact, RepositoryID: 17})
				expected = append(expected, fact.MergeSHA)
			}
			input.Value.Dependencies = append(input.Value.Dependencies, ProductBaseDependency{Fact: ProductCoverageFact{MergeSHA: strings.Repeat("b", 40)}, RepositoryID: 99})
			slices.Sort(expected)
			got, err := input.RequiredAncestors(strings.Repeat("f", 40))
			if err != nil || !reflect.DeepEqual(got, expected) {
				t.Fatal("base omitted leaf SHA or imported foreign repository SHA", got, expected, err)
			}
		})
	}
}

func TestProductSplitDependencyUnavailableDelegationCannotBecomeOrdinaryLeaf(t *testing.T) {
	for _, reason := range []string{"split_scope_mapping_unavailable", "split_registration_evidence_unavailable", "split_parent_registration_unavailable", "split_parent_scope_unavailable", "split_local_verification_unavailable", "source_registration_unavailable"} {
		t.Run(reason, func(t *testing.T) {
			ledger := splitDependencyReducerFixture()
			ledger.Value.Splits = slices.DeleteFunc(ledger.Value.Splits, func(g ProductSplitCoverage) bool { return g.ParentWorkItemID == "left" })
			ledger.Value.Merged = append(ledger.Value.Merged, ProductCoverageFact{WorkItemID: "left", RegistrationDigest: "reg-left", SplitScopeDigest: "scope-left", LocalCriteria: []string{"local"}, MergeID: "own-parent-merge", MergeSHA: strings.Repeat("e", 40)})
			ledger.Value.Unavailable = []ProductCoveragePending{{WorkItemID: "left", RecordID: "lost-delegation", Reason: reason}}
			if leaves, origins, err := productMergeDependencyLeaves(ledger, []string{"left"}); !errors.Is(err, ErrProductMergeDependencyHeld) || len(leaves) != 0 || len(origins) != 0 {
				t.Fatal("unavailable delegation became an ordinary merged leaf", leaves, origins, err)
			}
			// A loss attributed to this parent must not block an independent leaf.
			leaves, _, err := productMergeDependencyLeaves(ledger, []string{"right"})
			if err != nil || !reflect.DeepEqual(leaves, []string{"right"}) {
				t.Fatal("unrelated contributor inherited the hold", leaves, err)
			}
		})
	}
}
