package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func nestedRegistrationScopeFixture(t *testing.T, depth ...int) (*KernelStore, ProductSplitChildPlanRequest, ProductNestedSplitChildScopes) {
	t.Helper()
	k, r := nestedObservedScopesFixture(t, depth...)
	if _, err := k.BindProductSplitChildScopes(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	var proof ProductNestedSplitChildScopes
	if err := k.Record(context.Background(), "product_nested_split_child_scopes", r.ProposalID, &proof); err != nil {
		t.Fatal(err)
	}
	return k, r, proof
}

func TestProductNestedSplitRegistrationsKeepLocalScopesAndCompactAncestorsWithoutExecution(t *testing.T) {
	ctx := context.Background()
	base, r, proof := nestedRegistrationScopeFixture(t)
	before, err := base.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	registered, err := base.RegisterProductSplitChildren(ctx, r)
	if err != nil || len(registered.Value.Registrations) != 2 {
		t.Fatal(registered, err)
	}
	var saved ProductNestedSplitRegistrations
	if err := base.Record(ctx, "product_nested_split_registrations", r.ProposalID, &saved); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saved.Value.ProductNestedSplitChildScopesValue, proof.Value) || saved.Value.ScopeEnvelopeID != proof.ID || saved.Value.ScopeEnvelopeDigest != proof.Digest || !reflect.DeepEqual(saved.Value.RegistrationSet, registered) {
		t.Fatal("scope authority not pinned")
	}
	wave, err := base.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	for i, registration := range registered.Value.Registrations {
		id := registration.Value.Definition.ID
		ref := registration.Value.SplitScope
		if ref == nil || ref.AncestorSet == nil || !reflect.DeepEqual(*ref.AncestorSet, saved.Value.AncestorSet) || len(ref.AncestorSet.Value.Groups) != 2 || !reflect.DeepEqual(registration.Value.Definition.Criteria, proof.Value.Scopes.Value.Children[i].Value.Child.Value.ParentCriteria) {
			t.Fatal("root mapping/compact ancestry lost")
		}
		binding, err := base.ReadProductSplitChildScope(ctx, r.Target, id)
		if err != nil || binding.Value.Child.Value.Depth != 2 || !reflect.DeepEqual(binding, proof.Value.Scopes.Value.Children[i]) {
			t.Fatal("registered local scope unavailable", err)
		}
		held, err := productWorkItemExecutionHeld(ctx, base.DB, wave, id)
		if err != nil || !held {
			t.Fatal("registration granted execution", held, err)
		}
		public := productSplitScopeInput(binding)
		if !reflect.DeepEqual(public.Scope, binding.Value.Scope) || public.Depth != 2 {
			t.Fatal("public local obligation flattened")
		}
	}
	after, err := base.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("registration reset budgets", err)
	}
	var tasks int
	if err := base.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal("registration created Tasks", tasks, err)
	}
	// The ordinary API cannot erase the reserved scoped identity.
	if _, err := base.RegisterProductWorkItem(ctx, r.Target, "", registered.Value.Registrations[0].Value.Definition); err == nil {
		t.Fatal("ordinary registration erased scope")
	}
	records := []struct{ kind, id string }{{"product_nested_split_registrations", r.ProposalID}, {"product_nested_split_registrations_history", saved.ID}, {"product_nested_split_registrations_admission", r.ProposalID}, {"product_work_item_registration", registered.Value.Registrations[0].Value.Definition.ID}, {"product_work_item_registration_version", structureID("product-work-", registered.Value.Registrations[0].Value.Definition.ID, registered.Value.Registrations[0].Digest)}, {"wave_work_item", registered.Value.Registrations[0].Value.Definition.ID}, {"product_nested_split_child_scopes", r.ProposalID}}
	for _, record := range records {
		t.Run("missing_"+record.kind, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", record.kind, record.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductSplitRegistrations(ctx, r); !readinessHoldError(err) {
				t.Fatal("lost registration accepted", err)
			}
			if _, err := k.RegisterProductSplitChildren(ctx, r); !readinessHoldError(err) {
				t.Fatal("lost registration reconstructed", err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", record.kind, record.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			if record.kind == "product_nested_split_registrations" {
				restored, err := k.ReadProductSplitRegistrations(ctx, r)
				if err != nil || !reflect.DeepEqual(restored, registered) {
					t.Fatal("exact restoration denied", err)
				}
			}
		})
	}
	t.Run("pause_audit", func(t *testing.T) {
		k := cloneNestedScopeStore(t, base)
		if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-nested-reg", Target: r.Target, Action: "pause", Reason: "audit"}); err != nil {
			t.Fatal(err)
		}
		replay, err := k.RegisterProductSplitChildren(ctx, r)
		if err != nil || !reflect.DeepEqual(replay, registered) {
			t.Fatal("paused replay changed registrations", err)
		}
	})
	for _, record := range records[:6] {
		t.Run("sole_"+record.kind, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			if _, err := k.DB.Exec("DELETE FROM records WHERE (kind LIKE 'product_split_%' OR kind LIKE 'product_nested_split_%') AND NOT(kind=? AND id=?)", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND (json_type(payload,'$.value.split_scope') IS NOT NULL OR json_type(payload,'$.split_scope') IS NOT NULL) AND NOT(kind=? AND id=?)", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			rootIDs, ownIDs := map[string]bool{}, map[string]bool{}
			for layer, bindings := range [][]ProductSplitChildScopeBinding{proof.Value.Children, proof.Value.Scopes.Value.Children} {
				for _, binding := range bindings {
					if _, err := k.CreateProductWave(ctx, splitRootRequest(binding.Value.Issue.RepositoryID, binding.Value.Issue.Number, "github", fmt.Sprintf("root-after-registration-%s", record.kind))); !errors.Is(err, ErrProductSplitChildSourceHeld) {
						t.Fatal("ancestor/own sibling became root", err)
					}
					if layer == 0 {
						rootIDs[binding.Value.CreationID] = true
					} else {
						ownIDs[binding.Value.CreationID] = true
					}
				}
			}
			if _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); !readinessHoldError(err) {
				t.Fatal("known retry refunded", err)
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !readinessHoldError(err) {
				t.Fatal("reserved seats refunded", err)
			}
			if _, err := k.PrepareProductSplitChildPlan(ctx, r); !readinessHoldError(err) {
				t.Fatal("lost plan reconstructed", err)
			}
			if _, err := k.BindProductSplitChildScopes(ctx, r); !readinessHoldError(err) {
				t.Fatal("lost scopes reconstructed", err)
			}
			if _, err := k.RegisterProductSplitChildren(ctx, r); !readinessHoldError(err) {
				t.Fatal("lost siblings reconstructed", err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			foundRoot, foundOwn := false, false
			cursors := map[string]bool{}
			for _, target := range page.Targets {
				if cursors[target.Cursor] {
					t.Fatal("capsule cursor collision", target.Cursor)
				}
				cursors[target.Cursor] = true
				foundRoot = foundRoot || rootIDs[target.ID]
				foundOwn = foundOwn || ownIDs[target.ID]
			}
			if !foundRoot || !foundOwn {
				t.Fatal("ancestor/own recovery hidden", page)
			}
		})
	}
}

func TestProductNestedSplitRegistrationIsAtomicAndRequiresCurrentFreshParent(t *testing.T) {
	ctx := context.Background()
	base, r, _ := nestedRegistrationScopeFixture(t)
	for _, mode := range []string{"pause", "stale", "late_failure"} {
		t.Run(mode, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			switch mode {
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-before-nested-reg", Target: r.Target, Action: "pause", Reason: "hold"}); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock()
				k.Now = func() time.Time { return now.Add(2 * time.Minute) }
			case "late_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_nested_registration BEFORE INSERT ON records WHEN NEW.kind='product_nested_split_registrations_admission' BEGIN SELECT RAISE(ABORT,'late registration failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.RegisterProductSplitChildren(ctx, r); err == nil {
				t.Fatal("invalid registration succeeded")
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_nested_split_registrations','product_nested_split_registrations_history','product_nested_split_registrations_admission') OR (kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND coalesce(json_extract(payload,'$.value.split_scope.request.proposal_id'),json_extract(payload,'$.split_scope.request.proposal_id'))=?)", r.ProposalID).Scan(&count); err != nil || count != 0 {
				t.Fatal("partial registration", count, err)
			}
		})
	}
}
