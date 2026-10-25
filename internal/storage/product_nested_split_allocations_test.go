package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func nestedAllocationFixture(t *testing.T) (*KernelStore, ProductSplitChildPlanRequest, ProductNestedSplitAllocation) {
	t.Helper()
	k, r, _ := nestedChildPlanFixture(t)
	if _, err := k.ReserveProductSplitChildren(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	var saved ProductNestedSplitAllocation
	if err := k.Record(context.Background(), "product_nested_split_allocation", r.ProposalID, &saved); err != nil {
		t.Fatal(err)
	}
	return k, r, saved
}

func TestProductNestedSplitAllocationReservesWholeSetAndRetainsLifetime(t *testing.T) {
	ctx := context.Background()
	k, r, plan := nestedChildPlanFixture(t)
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	fillSplitLifetime(t, k, r, maxProductWorkItems-5)
	saved, err := k.ReserveProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saved.Value.WorkItemIDs, []string{plan.Value.Plan.Children[0].Value.WorkItemID, plan.Value.Plan.Children[1].Value.WorkItemID}) {
		t.Fatal("partial seats", saved)
	}
	var envelope ProductNestedSplitAllocation
	if err := k.Record(ctx, "product_nested_split_allocation", r.ProposalID, &envelope); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(envelope.Value.ProductNestedSplitChildPlanValue, plan.Value) || envelope.Value.ChildPlanID != plan.ID || envelope.Value.ChildPlanDigest != plan.Digest {
		t.Fatal("ancestor/own plan not pinned")
	}
	for _, id := range saved.Value.WorkItemIDs {
		var seat ProductNestedSplitAllocation
		if err := k.Record(ctx, "product_nested_split_work_item_seat", id, &seat); err != nil || !reflect.DeepEqual(seat, envelope) {
			t.Fatal("seat lost whole ancestor", err)
		}
	}
	var amount, entries int
	if err := k.DB.QueryRow("SELECT count(*),sum(amount) FROM kernel_budget_ledger WHERE kind='split_children'").Scan(&entries, &amount); err != nil || entries != 2 || amount != 4 {
		t.Fatal("ancestor or own budget lost", entries, amount, err)
	}
	if _, err := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "overflow-nested", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); err == nil {
		t.Fatal("nested seats refunded")
	}
	if _, err := k.PrepareProductSplitCreate(ctx, r, plan.Value.Plan.Children[0].ID, "nested-owner"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("allocation granted Issue send", err)
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("execution budget changed", err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-nested-allocation", Target: r.Target, Action: "pause", Reason: "retain seats"}); err != nil {
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
	if err != nil || !reflect.DeepEqual(replay, saved) {
		t.Fatal("pause/reopen reallocated", err)
	}
	direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "nested-allocation-direction", Target: r.Target, Instruction: "new scope", NextPolicyRevision: r.Target.PolicyRevision + 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	historical, err := k.ReadProductSplitAllocation(ctx, r)
	if err != nil || !reflect.DeepEqual(historical, saved) {
		t.Fatal("history changed", err)
	}
	w, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	r.Target = w.Target()
	if _, err := k.ReserveProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("old allocation retargeted", err)
	}
}

func TestProductNestedSplitAllocationRejectsMissingProofWithoutRepair(t *testing.T) {
	k, r, saved := nestedAllocationFixture(t)
	ctx := context.Background()
	records := []struct{ kind, id string }{{"product_nested_split_allocation", r.ProposalID}, {"product_nested_split_allocation_history", saved.ID}, {"product_nested_split_allocation_admission", r.ProposalID}, {"product_nested_split_work_item_seat", saved.Value.Allocation.Value.WorkItemIDs[0]}, {"product_nested_split_children", r.ProposalID}, {"product_nested_split_child_intent_history", saved.Value.Plan.Children[0].ID}, {"product_nested_split_proposal", saved.Value.ProposalHeadID}, {"product_split_parent_scope", r.ParentWorkItemID}}
	for _, record := range records {
		t.Run(record.kind, func(t *testing.T) {
			var raw []byte
			var owner string
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", record.kind, record.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			for _, read := range []func() (ProductSplitAllocation, error){func() (ProductSplitAllocation, error) { return k.ReadProductSplitAllocation(ctx, r) }, func() (ProductSplitAllocation, error) { return k.ReserveProductSplitChildren(ctx, r) }} {
				if _, err := read(); !readinessHoldError(err) {
					t.Fatal("lost proof repaired", err)
				}
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", record.kind, record.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductSplitAllocation(ctx, r); err != nil {
				t.Fatal("exact restoration denied", err)
			}
		})
	}
	var raw []byte
	if err := k.DB.QueryRow("SELECT payload FROM kernel_budget_ledger WHERE operation_key=? AND kind='split_children'", r.ProposalID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE operation_key=? AND kind='split_children'", r.ProposalID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, r); !readinessHoldError(err) {
		t.Fatal("lost ledger repaired", err)
	}
	if _, err := k.DB.Exec("INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'split_children',2,0,?)", r.Target.WaveID, r.ProposalID, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductSplitAllocation(ctx, r); err != nil {
		t.Fatal(err)
	}
}

func TestProductNestedSplitAllocationAdmissionIsAtomicAndFresh(t *testing.T) {
	for _, mode := range []string{"capacity", "late_failure", "stale", "pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, r, _ := nestedChildPlanFixture(t)
			switch mode {
			case "capacity":
				fillSplitLifetime(t, k, r, maxProductWorkItems-4)
			case "late_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_nested_allocation BEFORE INSERT ON kernel_budget_ledger WHEN NEW.operation_key='" + r.ProposalID + "' BEGIN SELECT RAISE(ABORT,'late nested allocation failure'); END"); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock()
				k.Now = func() time.Time { return now.Add(2 * time.Minute) }
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-before-nested-seats", Target: r.Target, Action: "pause", Reason: "stop"}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); err == nil {
				t.Fatal("invalid admission succeeded")
			} else if mode == "capacity" && !errors.Is(err, ErrOperationBudget) {
				t.Fatal(err)
			}
			var n int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_nested_split_allocation','product_nested_split_allocation_history','product_nested_split_allocation_admission','product_nested_split_work_item_seat')").Scan(&n); err != nil || n != 0 {
				t.Fatal("partial allocation", n, err)
			}
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE kind='split_children' AND operation_key=?", r.ProposalID).Scan(&n); err != nil || n != 0 {
				t.Fatal("partial ledger", n, err)
			}
		})
	}
}

func TestProductNestedSplitAllocationEachWitnessAloneRetainsOwnAndAncestorProhibitions(t *testing.T) {
	for _, kind := range []string{"product_nested_split_allocation", "product_nested_split_allocation_history", "product_nested_split_allocation_admission", "product_nested_split_work_item_seat", "product_nested_split_allocation_ledger"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, r, saved := nestedAllocationFixture(t)
			original, err := k.ReadProductSplitProposal(ctx, saved.Value.Request.Target, saved.Value.Request.ParentWorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			keepID := r.ProposalID
			if kind == "product_nested_split_allocation_history" {
				keepID = saved.ID
			}
			if kind == "product_nested_split_work_item_seat" {
				keepID = saved.Value.Allocation.Value.WorkItemIDs[0]
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE (kind LIKE 'product_split_%' OR kind LIKE 'product_nested_split_%') AND NOT(kind=? AND id=?)", kind, keepID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND (json_type(payload,'$.value.split_scope') IS NOT NULL OR json_type(payload,'$.split_scope') IS NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			if kind == "product_nested_split_allocation_ledger" {
				if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry') AND NOT(kind='split_children' AND operation_key=?)", r.ProposalID); err != nil {
					t.Fatal(err)
				}
			} else if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			for _, ancestor := range saved.Value.Children {
				if _, err := k.CreateProductWave(ctx, splitRootRequest(ancestor.Value.Issue.RepositoryID, ancestor.Value.Issue.Number, "github", "root-after-nested-allocation")); !errors.Is(err, ErrProductSplitChildSourceHeld) {
					t.Fatal("ancestor sibling became root", err)
				}
				if _, err := k.ReadProductSplitCreate(ctx, ancestor.Value.CreationID); !readinessHoldError(err) {
					t.Fatal("ancestor journal reconstructed", err)
				}
			}
			if _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); !readinessHoldError(err) {
				t.Fatal("ancestor retry/budget refunded", err)
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !readinessHoldError(err) {
				t.Fatal("own seats refunded", err)
			}
			if _, err := k.PrepareProductSplitChildPlan(ctx, r); !readinessHoldError(err) {
				t.Fatal("missing own plan repaired", err)
			}
			original.Value.Request.Key = "root-after-allocation-only"
			if _, err := k.PrepareProductSplitProposal(ctx, original.Value.Request); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("ancestor resumed", err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, target := range page.Targets {
				found = found || target.ID == saved.Value.Children[0].Value.CreationID
			}
			if !found {
				t.Fatal("ancestor recovery hidden", page)
			}
		})
	}
}
