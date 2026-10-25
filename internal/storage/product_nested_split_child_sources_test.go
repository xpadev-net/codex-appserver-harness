package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestProductNestedSplitChildSourceRetainsEveryAncestorWithoutDeeperAdmission(t *testing.T) {
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
	if _, err := base.PrepareProductSplitParentScope(ctx, r.Target, id); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("deeper split admitted before ancestor proof integration", err)
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
	if err := base.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_parent_scope' AND id=?", id).Scan(&parentScopes); err != nil || parentScopes != 0 {
		t.Fatal("failed deeper admission left a proof", parentScopes, err)
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
