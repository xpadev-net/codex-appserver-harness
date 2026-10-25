package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSplitScopeWitnessesKeepMissingCreationsDiscoverableWithoutBlockingLaterRows(t *testing.T) {
	ctx := context.Background()
	for _, keep := range []string{
		"product_split_child_scope", "product_split_child_scope_history", "product_split_child_scope_admission",
		"product_split_child_scopes", "product_split_child_scopes_history", "product_split_child_scopes_admission", "product_split_child_scope_source",
		"product_split_registrations", "product_split_registrations_history", "product_split_registrations_admission",
		"product_work_item_registration", "product_work_item_registration_version", "wave_work_item",
	} {
		t.Run(keep, func(t *testing.T) {
			k, r, scopes := splitRegistrationsFixture(t)
			if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE (kind LIKE 'product_split_%' OR kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item')) AND kind<>?", keep); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			// Opaque row cursors stay bounded even when a damaged witness key is long.
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_scopes_history',?,?,'{')", strings.Repeat("bad", 300), r.Target.WaveID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_work_item_registration_version','malformed-split-version',?,'{')", r.Target.WaveID); err != nil {
				t.Fatal(err)
			}
			cursor := ""
			seen := map[string]bool{}
			healthy, unavailable := 0, 0
			complete := false
			for pageNumber := 0; pageNumber < 32; pageNumber++ {
				page, err := k.SplitCreationReconciliationTargets(ctx, cursor, 1)
				if err != nil {
					t.Fatal("bad witness poisoned paging", err)
				}
				for _, target := range page.Targets {
					if seen[target.Cursor] || len(target.Cursor) > 512 {
						t.Fatal("unstable/unbounded cursor", target)
					}
					seen[target.Cursor] = true
					if target.Unavailable {
						unavailable++
						continue
					}
					if target.ID != scopes.Value.Children[0].Value.CreationID && target.ID != scopes.Value.Children[1].Value.CreationID {
						t.Fatal("unrelated creation", target)
					}
					healthy++
					if _, err := k.ReadProductSplitCreate(ctx, target.ID); !errors.Is(err, ErrProductSplitHeld) {
						t.Fatal("discovery reconstructed authority", target, err)
					}
				}
				if page.NextAfter == "" {
					complete = true
					break
				}
				cursor = page.NextAfter
				if pageNumber == 0 {
					path := k.Root + "/state.db"
					k.Close()
					k, err = OpenExistingKernel(path)
					if err != nil {
						t.Fatal(err)
					}
					defer k.Close()
				}
			}
			if !complete || healthy < 1 || unavailable != 2 {
				t.Fatal("witness disappeared or starved", complete, healthy, unavailable)
			}
		})
	}
}

func TestMalformedSplitWitnessHoldsItsWaveWithoutPoisoningAnotherWave(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"product_split_registrations", "product_work_item_registration_version", "product_split_child_scope_source"} {
		for _, raw := range []string{"{", "{}"} {
			for _, own := range []bool{true, false} {
				t.Run(kind+raw+map[bool]string{true: "-own", false: "-foreign"}[own], func(t *testing.T) {
					k, r, _ := splitRegistrationsFixture(t)
					saved, err := k.RegisterProductSplitChildren(ctx, r)
					if err != nil {
						t.Fatal(err)
					}
					owner := "other-wave"
					if own {
						owner = r.Target.WaveID
					}
					if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,'unreadable-witness',?,?)", kind, owner, raw); err != nil {
						t.Fatal(err)
					}
					_, budgetErr := k.WaveOperationBudget(ctx, r.Target.WaveID)
					_, scopeErr := k.ReadProductSplitRegistrations(ctx, r)
					_, registerErr := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "healthy-independent-root", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC", "extra"}})
					if own {
						for _, err := range []error{budgetErr, scopeErr, registerErr} {
							if !errors.Is(err, ErrProductSplitHeld) {
								t.Fatal("unknown owned evidence released Wave", err)
							}
						}
					} else {
						if budgetErr != nil || scopeErr != nil || registerErr != nil {
							t.Fatal("foreign malformed evidence poisoned healthy Wave", budgetErr, scopeErr, registerErr)
						}
						actual, err := k.ReadProductSplitChildScope(ctx, r.Target, saved.Value.Registrations[0].Value.Definition.ID)
						if err != nil || actual.ID != saved.Value.Children[0].ID {
							t.Fatal("foreign corruption changed binding", actual, err)
						}
					}
				})
			}
		}
	}
}
