package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
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
							if !readinessHoldError(err) {
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

func TestUnreadableSplitHistoryAloneCannotReleaseParentOrLifetimeSeats(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"product_split_child_scope_history", "product_split_child_scopes_history", "product_split_registrations_history"} {
		for _, legacy := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "-wave-owner", true: "-legacy-owner"}[legacy], func(t *testing.T) {
				k, r, _ := splitRegistrationsFixture(t)
				if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
					t.Fatal(err)
				}
				proposal, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID)
				if err != nil {
					t.Fatal(err)
				}
				owner := r.Target.WaveID
				if legacy {
					owner = r.ProposalID
				}
				if _, err := k.DB.Exec("UPDATE records SET work_item_id=? WHERE kind=?", owner, kind); err != nil {
					t.Fatal(err)
				}
				if _, err := k.ReadProductSplitRegistrations(ctx, r); err != nil {
					t.Fatal("readable old owner changed payload authority", err)
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", kind); err != nil {
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
				if _, err := k.DB.Exec("UPDATE records SET payload='{}' WHERE kind=?", kind); err != nil {
					t.Fatal(err)
				}
				if _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); !readinessHoldError(err) {
					t.Fatal("unreadable history refunded seats", err)
				}
				w, err := k.Wave(ctx, r.Target.WaveID)
				if err != nil {
					t.Fatal(err)
				}
				held, err := productWorkItemExecutionHeld(ctx, k.DB, w, r.ParentWorkItemID)
				if err != nil || !held {
					t.Fatal("unreadable history released parent", held, err)
				}
				proposal.Value.Request.Key = "new-key-after-unreadable-history"
				if _, err := k.PrepareProductSplitProposal(ctx, proposal.Value.Request); !errors.Is(err, ErrProductSplitHeld) {
					t.Fatal("unknown history replaced with new split", err)
				}
			})
		}
	}
}

func TestUnreadableLegacySplitHistoryNeedsAuthenticatedProposalForWaveAttribution(t *testing.T) {
	ctx := context.Background()
	k, r, _ := splitRegistrationsFixture(t)
	var original ProductSplitProposal
	var owner string
	if err := k.Record(ctx, "product_split_proposal_history", r.ProposalID, &original); err != nil {
		t.Fatal(err)
	}
	if err := k.DB.QueryRow("SELECT work_item_id FROM records WHERE kind='product_split_proposal_history' AND id=?", r.ProposalID).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("UPDATE records SET work_item_id=?,payload='{}' WHERE kind='product_split_child_scopes_history'", r.ProposalID); err != nil {
		t.Fatal(err)
	}
	held, err := splitScopeUnreadableForWave(ctx, k.DB, "independent-wave")
	if err != nil || held {
		t.Fatal("authenticated foreign history blocked another Wave", held, err)
	}
	held, err = splitScopeUnreadableForWave(ctx, k.DB, r.Target.WaveID)
	if err != nil || !held {
		t.Fatal("own unknown history released", held, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_proposal_history' AND id=?", r.ProposalID); err != nil {
		t.Fatal(err)
	}
	held, err = splitScopeUnreadableForWave(ctx, k.DB, "independent-wave")
	if err != nil || !held {
		t.Fatal("unattributable old history inferred independent ownership", held, err)
	}
	generic, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "independent-generic", OriginalRequest: "research without product work", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "research report"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: domain.DefaultKernelLimits()}})
	if err != nil {
		t.Fatal(err)
	}
	held, err = splitScopeUnreadableForWave(ctx, k.DB, generic.ID)
	if err != nil || held {
		t.Fatal("unattributable product history blocked generic K", held, err)
	}
	if err := k.Put(ctx, "product_split_proposal_history", r.ProposalID, owner, original); err != nil {
		t.Fatal(err)
	}
	held, err = splitScopeUnreadableForWave(ctx, k.DB, "independent-wave")
	if err != nil || held {
		t.Fatal("exact attribution restore did not release foreign Wave", held, err)
	}
}
