package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func flatAncestorProducerKinds() []string {
	return []string{
		"product_split_parent_scope", "product_split_parent_scope_history", "product_split_parent_scope_admission",
		"product_nested_split_proposal", "product_nested_split_proposal_history", "product_nested_split_proposal_admission", "product_nested_split_proposal_request",
		"product_nested_split_children", "product_nested_split_children_history", "product_nested_split_children_admission", "product_nested_split_child_intent", "product_nested_split_child_intent_history", "product_nested_split_child_intent_admission",
		"product_nested_split_allocation", "product_nested_split_allocation_history", "product_nested_split_allocation_admission", "product_nested_split_work_item_seat",
		"product_nested_split_child_scopes", "product_nested_split_child_scopes_history", "product_nested_split_child_scopes_admission", "product_nested_split_child_scope", "product_nested_split_child_scope_history", "product_nested_split_child_scope_admission", "product_nested_split_child_scope_source",
		"product_nested_split_registrations", "product_nested_split_registrations_history", "product_nested_split_registrations_admission",
		"product_split_create", "product_split_create_event", "product_split_create_history", "product_split_create_admission", "product_split_create_gate", "product_split_create_gate_history", "product_split_create_retry_budget", "product_split_created_resource",
		"ledger_allocation", "ledger_retry",
	}
}

func insertFlatAncestorProducer(t *testing.T, k *KernelStore, kind, owner string, value map[string]any) {
	t.Helper()
	envelope := map[string]any{"value": value}
	var payload any = envelope
	switch kind {
	case "product_split_create", "product_split_create_event":
		payload = map[string]any{"value": map[string]any{"value": map[string]any{"ancestry": envelope}}}
	case "product_split_create_gate", "product_split_create_gate_history":
		payload = map[string]any{"value": map[string]any{"ancestry": envelope}}
	case "product_split_create_history", "product_split_create_admission", "product_split_create_retry_budget", "product_split_created_resource", "ledger_retry":
		payload = map[string]any{"ancestry": envelope}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(kind, "ledger_") {
		ledgerKind := "split_children"
		if kind == "ledger_retry" {
			ledgerKind = "split_create_retry"
		}
		if _, err := k.DB.Exec("INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,?,1,0,?)", owner, kind, ledgerKind, raw); err != nil {
			t.Fatal(err)
		}
	} else if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", kind, "flat-ancestor:"+kind, owner, raw); err != nil {
		t.Fatal(err)
	}
}

func flatAncestorHeader(wave string, groups any) map[string]any {
	return map[string]any{
		"request": map[string]any{"target": map[string]any{"wave_id": wave}, "proposal_id": "flat-proposal"}, "parent_head_id": "flat-parent",
		"children":   []any{map[string]any{"value": map[string]any{"child": map[string]any{"value": map[string]any{"depth": 2}}}}},
		"allocation": map[string]any{}, "ancestor_witnesses": groups,
	}
}

func TestFlatAncestorWitnessesSurviveEachMetadataAndCreationProducerAlone(t *testing.T) {
	ctx := context.Background()
	base, r, scopes := splitRegistrationsFixture(t)
	group := splitAncestorGroup(scopes.Value, scopes.ID, scopes.Digest, "", "")
	if _, err := base.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' OR kind LIKE 'product_nested_split_%'"); err != nil {
		t.Fatal(err)
	}
	if _, err := base.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
		t.Fatal(err)
	}
	if _, err := base.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
		t.Fatal(err)
	}
	for _, kind := range flatAncestorProducerKinds() {
		t.Run(kind, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			insertFlatAncestorProducer(t, k, kind, r.Target.WaveID, flatAncestorHeader(r.Target.WaveID, []ProductSplitAncestorGroup{group}))
			for _, binding := range scopes.Value.Children {
				if _, err := k.CreateProductWave(ctx, splitRootRequest(binding.Value.Issue.RepositoryID, binding.Value.Issue.Number, "github", "flat-alone-"+kind)); !errors.Is(err, ErrProductSplitChildSourceHeld) {
					t.Fatal("old ancestor became a new root", err)
				}
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			cursors := map[string]bool{}
			for _, target := range page.Targets {
				if cursors[target.Cursor] {
					t.Fatal("flat cursor collision")
				}
				cursors[target.Cursor] = true
				found = found || target.ID == scopes.Value.Children[0].Value.CreationID || target.ID == scopes.Value.Children[1].Value.CreationID
			}
			if !found {
				t.Fatal("flat ancestor creation hidden from recovery")
			}
		})
	}
}

func TestFlatAncestorMalformedArraysHoldOnlyTheirOwningWave(t *testing.T) {
	ctx := context.Background()
	for _, shape := range []string{"string", "number", "object", "empty", "null", "missing", "oversize", "invalid_element"} {
		t.Run(shape, func(t *testing.T) {
			k, w, _ := productSplitProposalFixture(t)
			var groups any
			switch shape {
			case "string":
				groups = "broken"
			case "number":
				groups = 7
			case "object":
				groups = map[string]any{"broken": true}
			case "empty":
				groups = []any{}
			case "oversize":
				groups = make([]any, maxProductWorkItems+1)
			case "invalid_element":
				groups = []any{"broken"}
			}
			value := flatAncestorHeader("foreign-wave", groups)
			if shape == "missing" {
				delete(value, "ancestor_witnesses")
			}
			for _, kind := range flatAncestorProducerKinds() {
				insertFlatAncestorProducer(t, k, kind, "foreign-wave", value)
			}
			if _, err := k.WaveOperationBudget(ctx, w.ID); err != nil {
				t.Fatal("foreign flat data poisoned healthy budget", err)
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := splitAllocationIDsTx(ctx, tx, w.ID); err != nil {
				tx.Rollback()
				t.Fatal("foreign flat data poisoned healthy seats", err)
			}
			tx.Rollback()
			held, err := splitScopeUnreadableForWave(ctx, k.DB, "foreign-wave")
			if err != nil || !held {
				t.Fatal("own malformed flat witnesses ignored", held, err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal("malformed flat arrays broke recovery", err)
			}
			unavailable := false
			for _, target := range page.Targets {
				unavailable = unavailable || target.Unavailable
			}
			if !unavailable {
				t.Fatal("unreadable flat arrays hidden")
			}
		})
	}
}

func TestSplitAncestorWitnessesIncludeFlatOlderGroups(t *testing.T) {
	group := ancestorGroupFixture(t)
	local := ProductSplitChildScopesValue{Request: group.Request, ParentHeadID: group.ParentHeadID}
	for _, child := range group.Children {
		v := child.Value
		local.Children = append(local.Children, ProductSplitChildScopeBinding{Value: ProductSplitChildScopeValue{
			Child:             ProductSplitChildIntent{ID: v.Child.ID, Value: ProductSplitChildIntentValue{WorkItemID: v.Child.Value.WorkItemID}},
			Issue:             ProductSplitCreatedIssue{RepositoryID: v.Issue.RepositoryID, ID: v.Issue.ID, Number: v.Issue.Number},
			InitialCreationID: v.InitialCreationID, CreationID: v.CreationID, Creations: v.Creations,
		}})
	}
	var scopes ProductNestedSplitChildScopes
	scopes.ID, scopes.Digest = "scope-envelope", strings.Repeat("e", 64)
	scopes.Value.ProductSplitChildScopesValue = local
	scopes.Value.ScopeSetID, scopes.Value.ScopeSetDigest = group.ScopeSetID, group.ScopeSetDigest
	scopes.Value.ParentWorkItemID, scopes.Value.SourcePolicyDigest = group.ParentWorkItemID, group.SourcePolicyDigest
	scopes.Value.Scopes = ProductSplitChildScopes{ID: "own-scope", Value: local, Digest: strings.Repeat("f", 64)}
	older := cloneAncestorGroup(t, group)
	older.ScopeSetID = "older-scope"
	scopes.Value.AncestorWitnesses = []ProductSplitAncestorGroup{older}
	got, err := deriveSplitAncestorWitnesses(scopes)
	if err != nil || len(got.Value.Groups) != 3 {
		t.Fatal("older ancestor group discarded", got, err)
	}
	found := false
	for _, item := range got.Value.Groups {
		found = found || item.ScopeSetID == older.ScopeSetID
	}
	if !found {
		t.Fatal("oldest group missing")
	}
}
