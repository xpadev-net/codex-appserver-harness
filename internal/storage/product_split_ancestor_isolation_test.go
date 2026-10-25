package storage

import (
	"context"
	"encoding/json"
	"testing"
)

func TestSplitAncestorMalformedGroupsHoldOnlyTheirOwningWave(t *testing.T) {
	ctx := context.Background()
	for _, shape := range []string{"string", "number", "object", "empty", "null", "oversize", "invalid_element"} {
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
			case "null":
				groups = nil
			case "oversize":
				groups = make([]any, maxProductWorkItems+1)
			case "invalid_element":
				groups = []any{"broken"}
			}
			kinds := []string{"product_work_item_registration", "product_work_item_registration_version", "wave_work_item", "product_nested_split_registrations", "product_nested_split_registrations_history", "product_nested_split_registrations_admission", "product_split_child_source", "product_split_child_source_history", "product_split_child_source_policy", "product_split_child_source_policy_history"}
			for _, kind := range kinds {
				payload := map[string]any{"value": map[string]any{"wave_id": "foreign-wave", "definition": map[string]any{"id": "foreign-work"}, "request": map[string]any{"target": map[string]any{"wave_id": "foreign-wave"}, "proposal_id": "foreign-proposal"}, "parent_head_id": "foreign-head", "split_scope": map[string]any{"ancestor_set": map[string]any{"value": map[string]any{"groups": groups}}}, "ancestor_set": map[string]any{"value": map[string]any{"groups": groups}}}, "target": map[string]any{"wave_id": "foreign-wave"}, "split_scope": map[string]any{"ancestor_set": map[string]any{"value": map[string]any{"groups": groups}}}}
				raw, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", kind, kind+"-foreign", "foreign-wave", raw); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.WaveOperationBudget(ctx, w.ID); err != nil {
				t.Fatal("foreign groups poisoned healthy budget", err)
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := splitAllocationIDsTx(ctx, tx, w.ID); err != nil {
				tx.Rollback()
				t.Fatal("foreign groups poisoned healthy seats", err)
			}
			tx.Rollback()
			held, err := splitScopeUnreadableForWave(ctx, k.DB, "foreign-wave")
			if err != nil || !held {
				t.Fatal("own corruption ignored", held, err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal("corrupt groups broke recovery page", err)
			}
			unavailable := false
			for _, target := range page.Targets {
				unavailable = unavailable || target.Unavailable
			}
			if !unavailable {
				t.Fatal("unreadable groups hidden from recovery")
			}
		})
	}
}
