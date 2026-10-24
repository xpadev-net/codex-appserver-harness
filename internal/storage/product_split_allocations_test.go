package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func splitAllocationFixture(t *testing.T) (*KernelStore, ProductSplitProposalRequest, ProductSplitChildPlanRequest, ProductSplitChildPlan) {
	t.Helper()
	k, parent, _, r := splitChildPlanFixture(t)
	plan, err := k.PrepareProductSplitChildPlan(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return k, parent, r, plan
}
func fillSplitLifetime(t *testing.T, k *KernelStore, r ProductSplitChildPlanRequest, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		d := ProductWorkItemDefinition{ID: fmt.Sprintf("other-%03d", i), Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}
		if _, err := k.RegisterProductWorkItem(context.Background(), r.Target, "", d); err != nil {
			t.Fatal(err)
		}
	}
}
func assertSplitAllocationRows(t *testing.T, k *KernelStore, want int) {
	t.Helper()
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_split_allocation','product_split_allocation_history','product_split_allocation_admission','product_split_work_item_seat')").Scan(&n); err != nil || n != want {
		t.Fatal("partial allocation", n, want, err)
	}
}

func TestProductSplitAllocationReservesWholeLifetimeSetAndReplays(t *testing.T) {
	ctx := context.Background()
	k, _, r, plan := splitAllocationFixture(t)
	fillSplitLifetime(t, k, r, maxProductWorkItems-3)
	before, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	beforeBudget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	allocation, err := k.ReserveProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(allocation.Value.WorkItemIDs, []string{plan.Children[0].Value.WorkItemID, plan.Children[1].Value.WorkItemID}) || allocation.Value.PlanID != plan.Set.ID || allocation.Value.PlanDigest != plan.Set.Digest {
		t.Fatal("allocation not bound to complete set", allocation)
	}
	assertSplitAllocationRows(t, k, 5)
	for i := 0; i < 2; i++ {
		replay, err := k.ReserveProductSplitChildren(ctx, r)
		if err != nil || !reflect.DeepEqual(replay, allocation) {
			t.Fatal("allocation charged twice", replay, err)
		}
	}
	var amount, entries int
	if err := k.DB.QueryRow("SELECT count(*),sum(amount) FROM kernel_budget_ledger WHERE kind='split_children' AND wave_id=?", r.Target.WaveID).Scan(&entries, &amount); err != nil || entries != 1 || amount != 2 {
		t.Fatal(entries, amount, err)
	}
	next := ProductWorkItemDefinition{ID: "overflow", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}
	if _, err := k.RegisterProductWorkItem(ctx, r.Target, "", next); err == nil {
		t.Fatal("normal registration ignored child seats")
	}
	next.ID = plan.Children[0].Value.WorkItemID
	if _, err := k.RegisterProductWorkItem(ctx, r.Target, "", next); err == nil {
		t.Fatal("normal registration stole child part identity")
	}
	after, err := k.Wave(ctx, before.ID)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("allocation changed Wave budget/state", err)
	}
	afterBudget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil || !reflect.DeepEqual(afterBudget, beforeBudget) {
		t.Fatal("allocation reset execution budget", err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-allocation", Target: r.Target, Action: "pause", Reason: "retain seats"}); err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.ReserveProductSplitChildren(ctx, r)
	if err != nil || !reflect.DeepEqual(replay, allocation) {
		t.Fatal("restart/pause reset allocation", err)
	}
}

func TestProductSplitAllocationBoundDenialAndLateLedgerFailureAreAtomic(t *testing.T) {
	ctx := context.Background()
	t.Run("capacity", func(t *testing.T) {
		k, _, r, _ := splitAllocationFixture(t)
		fillSplitLifetime(t, k, r, maxProductWorkItems-2)
		if _, err := k.ReserveProductSplitChildren(ctx, r); !errors.Is(err, ErrOperationBudget) {
			t.Fatal("overcommitted whole set", err)
		}
		assertSplitAllocationRows(t, k, 0)
	})
	t.Run("late_failure", func(t *testing.T) {
		k, _, r, _ := splitAllocationFixture(t)
		if _, err := k.DB.Exec("CREATE TRIGGER fail_split_budget BEFORE INSERT ON kernel_budget_ledger WHEN NEW.kind='split_children' BEGIN SELECT RAISE(FAIL,'late budget failure'); END"); err != nil {
			t.Fatal(err)
		}
		if _, err := k.ReserveProductSplitChildren(ctx, r); err == nil {
			t.Fatal("late failure not reached")
		}
		assertSplitAllocationRows(t, k, 0)
		if _, err := k.DB.Exec("DROP TRIGGER fail_split_budget"); err != nil {
			t.Fatal(err)
		}
		if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
			t.Fatal(err)
		}
	})
}

func TestProductSplitAllocationAndNormalRegistrationCannotOvercommitAcrossConnections(t *testing.T) {
	ctx := context.Background()
	k, _, r, _ := splitAllocationFixture(t)
	fillSplitLifetime(t, k, r, maxProductWorkItems-3)
	other, err := OpenExistingKernel(k.Root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() { <-start; _, err := k.ReserveProductSplitChildren(ctx, r); done <- err }()
	go func() {
		<-start
		_, err := other.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "concurrent-work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}})
		done <- err
	}()
	close(start)
	first, second := <-done, <-done
	if (first == nil) == (second == nil) {
		t.Fatal("expected exactly one admission", first, second)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := productWorkItemLifetimeIDsTx(ctx, tx, w)
	if err != nil || len(ids) > maxProductWorkItems || len(ids) < maxProductWorkItems-1 {
		t.Fatal("bad lifetime count", len(ids), err)
	}
}

func TestProductSplitAllocationProofLossCannotFreeOrRecreateSeats(t *testing.T) {
	ctx := context.Background()
	k, _, r, _ := splitAllocationFixture(t)
	saved, err := k.ReserveProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, loss := range []struct{ kind, id string }{
		{"product_split_allocation", r.ProposalID}, {"product_split_allocation_history", saved.ID}, {"product_split_allocation_admission", r.ProposalID},
		{"product_split_work_item_seat", saved.Value.WorkItemIDs[0]}, {"product_split_work_item_seat", saved.Value.WorkItemIDs[1]},
	} {
		t.Run(loss.kind+"/"+loss.id, func(t *testing.T) {
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("allocation reconstructed", err)
			}
			if _, err := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "must-hold", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("missing proof freed normal capacity", err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
		})
	}
	var raw []byte
	var amount, unknown int
	if err := k.DB.QueryRow("SELECT amount,unknown,payload FROM kernel_budget_ledger WHERE wave_id=? AND operation_key=? AND kind='split_children'", r.Target.WaveID, r.ProposalID).Scan(&amount, &unknown, &raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind='split_children'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("missing ledger recharged", err)
	}
	if _, err := k.DB.Exec("INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'split_children',?,?,?)", r.Target.WaveID, r.ProposalID, amount, unknown, raw); err != nil {
		t.Fatal(err)
	}
	restored, err := k.ReadProductSplitAllocation(ctx, r)
	if err != nil || !reflect.DeepEqual(restored, saved) {
		t.Fatal("restore rewrote allocation", err)
	}
}

func TestProductSplitAllocationSurvivingWitnessHoldsAllNewAdmissions(t *testing.T) {
	for _, witness := range []string{"product_split_allocation", "product_split_allocation_history", "product_split_allocation_admission", "product_split_work_item_seat", "ledger"} {
		t.Run(witness, func(t *testing.T) {
			ctx := context.Background()
			k, parent, r, _ := splitAllocationFixture(t)
			if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", witness); err != nil {
				t.Fatal(err)
			}
			if witness != "ledger" {
				if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind='split_children'"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost allocation reconstructed", err)
			}
			parent.Key = "new-key"
			if _, err := k.PrepareProductSplitProposal(ctx, parent); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("proposal forgot reserved children", err)
			}
			if _, err := k.PrepareProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("child plan forgot reservations", err)
			}
			if _, err := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "new-work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost evidence granted normal registration", err)
			}
		})
	}
}

func TestProductSplitAllocationRequiresCurrentNewTarget(t *testing.T) {
	ctx := context.Background()
	k, _, r, _ := splitAllocationFixture(t)
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-before-reservation", Target: r.Target, Action: "pause", Reason: "hold split"}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("pause admitted allocation", err)
	}
	assertSplitAllocationRows(t, k, 0)
}

func TestProductWorkItemLifetimeVersionsPreventCapacityReset(t *testing.T) {
	ctx := context.Background()
	k, w, d := productRegistrationFixture(t, "direct")
	for i := 0; i < maxProductWorkItems; i++ {
		next := d
		next.ID = fmt.Sprintf("lifetime-%03d", i)
		if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", next); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration'"); err != nil {
		t.Fatal(err)
	}
	d.ID = "new-after-head-loss"
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", d); err == nil {
		t.Fatal("head loss reset lifetime count")
	}
	d.ID = "lifetime-000"
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", d); err == nil {
		t.Fatal("missing registration head recreated")
	}
}

func TestProductSplitAllocationRetainsSeatsAcrossDirectionAndRejectsOldNewAllocation(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_reservation", true: "after_reservation"}[reserved], func(t *testing.T) {
			ctx := context.Background()
			k, _, r, _ := splitAllocationFixture(t)
			var saved ProductSplitAllocation
			if reserved {
				var err error
				saved, err = k.ReserveProductSplitChildren(ctx, r)
				if err != nil {
					t.Fatal(err)
				}
			}
			direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "direction-allocation", Target: r.Target, Instruction: "new requirement scope"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
				t.Fatal(err)
			}
			got, err := k.ReserveProductSplitChildren(ctx, r)
			if reserved {
				if err != nil || !reflect.DeepEqual(got, saved) {
					t.Fatal("direction reset immutable allocation", err)
				}
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				w, err := loadWave(ctx, tx, r.Target.WaveID)
				if err != nil {
					t.Fatal(err)
				}
				ids, err := productWorkItemLifetimeIDsTx(ctx, tx, w)
				if err != nil || len(ids) != 3 {
					t.Fatal("new scope lost lifetime identities", ids, err)
				}
			} else if !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("old scope created allocation", err)
			}
		})
	}
}

func TestProductSplitAllocationRejectsChangedOrMalformedBudgetProof(t *testing.T) {
	ctx := context.Background()
	k, _, r, _ := splitAllocationFixture(t)
	if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{"{", "{}"} {
		if _, err := k.DB.Exec("UPDATE kernel_budget_ledger SET payload=? WHERE kind='split_children'", payload); err != nil {
			t.Fatal(err)
		}
		if _, err := k.ReadProductSplitAllocation(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("invalid ledger accepted", err)
		}
	}
}

func TestProductSplitReservedIdentityCannotBeRegisteredByAnotherWave(t *testing.T) {
	ctx := context.Background()
	k, _, r, plan := splitAllocationFixture(t)
	if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	original, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: original.OrchestratorSessionID, Source: "direct", SourceID: "unrelated", OriginalRequest: original.OriginalRequest, Criteria: original.Criteria, Policy: original.Policy})
	if err != nil {
		t.Fatal(err)
	}
	child := ProductWorkItemDefinition{ID: plan.Children[0].Value.WorkItemID, Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", child); err == nil {
		t.Fatal("other Wave stole child identity")
	}
	if _, err := k.ReadProductSplitAllocation(ctx, r); err != nil {
		t.Fatal("collision attempt changed allocation", err)
	}
}
