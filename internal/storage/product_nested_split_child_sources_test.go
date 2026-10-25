package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestProductNestedSplitChildSourceRetainsEveryAncestorWithoutExecution(t *testing.T) {
	ctx := context.Background()
	base, r, proof := nestedRegistrationScopeFixture(t)
	registered, err := base.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	before, err := base.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	var first ProductSplitChildSource
	for i, binding := range proof.Value.Scopes.Value.Children {
		id := binding.Value.Child.Value.WorkItemID
		update, err := base.ObserveProductSplitChildSource(ctx, r.Target, id, "", splitSourceFacts(binding))
		if err != nil || !update.Changed || update.Snapshot.Value.Status != "ready" || !reflect.DeepEqual(update.Snapshot.Value.AncestorSet, registered.Value.Registrations[i].Value.SplitScope.AncestorSet) {
			t.Fatal("grandchild source lost authenticated ancestors", err)
		}
		if i == 0 {
			first = update.Snapshot
		}
	}
	id := first.Value.Child.Value.WorkItemID
	if _, err := base.RequireProductSplitChildSource(ctx, r.Target, id, first.Value.Facts.PolicyDigest, time.Minute); err != nil {
		t.Fatal(err)
	}
	parent, err := base.PrepareProductSplitParentScope(ctx, r.Target, id)
	if err != nil || parent.Value.BindingID != proof.Value.Scopes.Value.Children[0].ID || !reflect.DeepEqual(parent.Value.AncestorWitnesses, first.Value.AncestorSet.Value.Groups) {
		t.Fatal("parent proof dropped older ancestors", err)
	}
	for _, kind := range []string{"product_split_parent_scope", "product_split_parent_scope_history", "product_split_parent_scope_admission"} {
		key := id
		if kind == "product_split_parent_scope_history" {
			key = parent.ID
		}
		var copy ProductSplitParentScope
		if err := base.Record(ctx, kind, key, &copy); err != nil || !reflect.DeepEqual(copy, parent) {
			t.Fatal("independent parent copy lost witnesses", kind, err)
		}
	}
	after, err := base.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("source observation changed budgets", err)
	}
	// Opaque discovery includes each nested head, history and admission exactly once.
	seen, cursors := map[string]int{}, map[string]bool{}
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 32 {
			t.Fatal("source discovery did not terminate")
		}
		page, err := base.ProductSplitChildSourceTargets(ctx, 17, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, target := range page.Targets {
			if cursors[target.Cursor] {
				t.Fatal("discovery cursor reused")
			}
			cursors[target.Cursor] = true
			if !target.Unavailable {
				seen[target.WorkItemID]++
			}
		}
		if page.NextAfter == "" {
			break
		}
		cursor = page.NextAfter
	}
	for _, binding := range proof.Value.Scopes.Value.Children {
		if seen[binding.Value.Child.Value.WorkItemID] != 3 {
			t.Fatal("grandchild discovery lost a receipt", seen)
		}
	}
	records := []struct{ kind, id string }{
		{"product_split_child_source", id}, {"product_split_child_source_history", first.ID},
		{"product_split_child_source_policy", id}, {"product_split_child_source_policy_history", id},
		{"product_split_parent_scope", id}, {"product_split_parent_scope_history", parent.ID}, {"product_split_parent_scope_admission", id},
	}
	for _, record := range records {
		t.Run("sole_"+record.kind, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			if _, err := k.DB.Exec("DELETE FROM records WHERE (kind LIKE 'product_split_%' OR kind LIKE 'product_nested_split_%') AND NOT(kind=? AND id=?)", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND (json_type(payload,'$.value.split_scope') IS NOT NULL OR json_type(payload,'$.split_scope') IS NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			for _, layer := range [][]ProductSplitChildScopeBinding{proof.Value.Children, proof.Value.Scopes.Value.Children} {
				for _, binding := range layer {
					if _, err := k.CreateProductWave(ctx, splitRootRequest(binding.Value.Issue.RepositoryID, binding.Value.Issue.Number, "github", fmt.Sprintf("source-sole-%s", record.kind))); !errors.Is(err, ErrProductSplitChildSourceHeld) {
						t.Fatal("ancestor or sibling became a new root", err)
					}
				}
			}
			if _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); !readinessHoldError(err) {
				t.Fatal("retry witness refunded", err)
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !readinessHoldError(err) {
				t.Fatal("seats refunded", err)
			}
			if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", first.Value.Facts); !readinessHoldError(err) {
				t.Fatal("missing scope reconstructed", err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			found := map[string]bool{}
			for _, target := range page.Targets {
				found[target.ID] = true
			}
			root, own := false, false
			for _, binding := range proof.Value.Children {
				root = root || found[binding.Value.CreationID]
			}
			for _, binding := range proof.Value.Scopes.Value.Children {
				own = own || found[binding.Value.CreationID]
			}
			if !root || !own {
				t.Fatal("ancestor or own recovery disappeared")
			}
		})
	}
	t.Run("resealed_policy_cannot_drop_ancestors", func(t *testing.T) {
		k := cloneNestedScopeStore(t, base)
		var policy splitChildSourcePolicy
		if err := k.Record(ctx, "product_split_child_source_policy", id, &policy); err != nil {
			t.Fatal(err)
		}
		policy.Value.AncestorSet = nil
		policy.Digest, err = contentDigest(policy.Value)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(policy)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := k.DB.Exec("UPDATE records SET payload=? WHERE kind IN ('product_split_child_source_policy','product_split_child_source_policy_history') AND id=?", raw, id); err != nil {
			t.Fatal(err)
		}
		if _, err := k.ReadProductSplitChildSource(ctx, r.Target, id); !readinessHoldError(err) {
			t.Fatal("resealed policy erased ancestors", err)
		}
	})
	t.Run("resealed_parent_cannot_drop_ancestors", func(t *testing.T) {
		k := cloneNestedScopeStore(t, base)
		changed := parent
		if len(parent.Value.AncestorWitnesses) < 2 {
			t.Fatal("fixture needs older ancestor groups")
		}
		changed.Value.AncestorWitnesses = append([]ProductSplitAncestorGroup{}, parent.Value.AncestorWitnesses[1:]...)
		changed.Digest, err = contentDigest(changed.Value)
		if err != nil {
			t.Fatal(err)
		}
		changed.ID = structureID("split-parent-scope-", id, changed.Digest)
		tx, err := k.kernelTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		for _, record := range []struct{ kind, key string }{{"product_split_parent_scope", id}, {"product_split_parent_scope_history", changed.ID}, {"product_split_parent_scope_admission", id}} {
			if err := kernelRecordTx(ctx, tx, record.kind, record.key, r.Target.WaveID, changed); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := loadSplitParentScope(ctx, tx, proof.Value.Scopes.Value.Children[0]); !readinessHoldError(err) {
			t.Fatal("resealed parent proof erased ancestors", err)
		}
	})
	t.Run("missing_parent_head", func(t *testing.T) {
		k := cloneNestedScopeStore(t, base)
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_parent_scope' AND id=?", id); err != nil {
			t.Fatal(err)
		}
		tx, err := k.kernelTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := loadSplitParentScope(ctx, tx, proof.Value.Scopes.Value.Children[0]); !readinessHoldError(err) {
			t.Fatal("missing parent proof could be reinitialized", err)
		}
	})
	t.Run("missing_source_head", func(t *testing.T) {
		k := cloneNestedScopeStore(t, base)
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_child_source' AND id=?", id); err != nil {
			t.Fatal(err)
		}
		if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", first.Value.Facts); !readinessHoldError(err) {
			t.Fatal("lost source was initialized again", err)
		}
	})
	var tasks, parentScopes int
	if err := base.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal("source observation created Tasks", tasks, err)
	}
	if err := base.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_parent_scope' AND id=?", id).Scan(&parentScopes); err != nil || parentScopes != 1 {
		t.Fatal("parent proof count changed", parentScopes, err)
	}
}

func TestProductSplitChildSourceAtKeepsExternalReadTime(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	binding := scopes.Value.Children[0]
	id := binding.Value.Child.Value.WorkItemID
	readAt := k.clock().Add(time.Minute)
	now := readAt.Add(10 * time.Second)
	k.Now = func() time.Time { return now }
	update, err := k.ObserveProductSplitChildSourceAt(ctx, r.Target, id, "", splitSourceFacts(binding), readAt)
	if err != nil || !update.Snapshot.CheckedAt.Equal(readAt) || !update.Snapshot.ObservedAt.Equal(readAt) {
		t.Fatal("save time replaced GET time", update, err)
	}
	if _, err := k.RequireProductSplitChildSource(ctx, r.Target, id, update.Snapshot.Value.Facts.PolicyDigest, 5*time.Second); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("slow save renewed source freshness", err)
	}
	for _, at := range []time.Time{{}, now.Add(time.Second), readAt.Add(-time.Second)} {
		if _, err := k.ObserveProductSplitChildSourceAt(ctx, r.Target, id, update.Snapshot.ID, splitSourceFacts(binding), at); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("invalid observation clock accepted", at, err)
		}
	}
}
