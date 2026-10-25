package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSplitCreationReconciliationDiscoveryRetainsOpaqueWitnessesAndSkipsBadResource(t *testing.T) {
	ctx := context.Background()
	k, _, r, job := splitCreationFixture(t)
	resource := splitCreatedResource{job.ID, r.ProposalID, job.Value.ParentHeadID, job.Value.Child.ID, 101}
	if err := k.Put(ctx, "product_split_created_resource", "github:17:issue:11", r.Target.WaveID, resource); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_created_resource','github:17:issue:bad',?,'{')", r.Target.WaveID); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	cursor := ""
	healthy, unavailable := 0, 0
	for pages := 0; pages < 8; pages++ {
		page, err := k.SplitCreationReconciliationTargets(ctx, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, target := range page.Targets {
			if seen[target.Cursor] {
				t.Fatal("duplicate cursor", target)
			}
			seen[target.Cursor] = true
			if target.Unavailable {
				unavailable++
			} else if target.ID == job.ID {
				healthy++
			} else {
				t.Fatal("wrong job identity", target)
			}
		}
		if page.NextAfter == "" {
			break
		}
		cursor = page.NextAfter
	}
	if healthy != 2 || unavailable != 1 || len(seen) != 3 {
		t.Fatal("discovery lost witness or poisoned page", seen, healthy, unavailable)
	}
	// Even with all creation indexes lost, its event/lease keep it discoverable.
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_split_create','product_split_create_history','product_split_create_admission')"); err != nil {
		t.Fatal(err)
	}
	page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, target := range page.Targets {
		found = found || target.ID == job.ID
	}
	if !found {
		t.Fatal("missing head disappeared from work list", page)
	}
	if _, err := k.ReadProductSplitCreate(ctx, job.ID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("work list reconstructed authority", err)
	}
	for _, limit := range []int{0, 129} {
		if _, err := k.SplitCreationReconciliationTargets(ctx, "", limit); err == nil {
			t.Fatal("unbounded page accepted", limit)
		}
	}
	if _, err := k.SplitCreationReconciliationTargets(ctx, strings.Repeat("x", 513), 1); err == nil {
		t.Fatal("unbounded cursor accepted")
	}
	var runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("discovery created model run", runs, err)
	}
}
