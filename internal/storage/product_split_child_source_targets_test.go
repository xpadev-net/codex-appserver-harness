package storage

import (
	"context"
	"strings"
	"testing"
)

func TestProductSplitChildSourceTargetsPageOpaqueRowsAndReopen(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"{", "{}"} {
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_scope',?,'foreign-wave',?)", strings.Repeat(raw, 200), raw); err != nil {
			t.Fatal(err)
		}
	}
	after := ""
	seen := map[string]int{}
	bad := 0
	pages := 0
	for {
		page, err := k.ProductSplitChildSourceTargets(ctx, 17, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if pages > 20 {
			t.Fatal("cursor failed to advance")
		}
		for _, target := range page.Targets {
			if len(target.Cursor) > 64 || target.Cursor <= after {
				t.Fatal(target)
			}
			if target.Unavailable {
				bad++
			} else {
				if target.Target != r.Target {
					t.Fatal(target)
				}
				seen[target.WorkItemID]++
			}
		}
		if page.NextAfter == "" {
			break
		}
		after = page.NextAfter
		if pages == 1 {
			path := k.Root + "/state.db"
			k.Close()
			k, err = OpenExistingKernel(path)
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
		}
	}
	if bad != 2 || len(seen) != 2 {
		t.Fatal("malformed row starved healthy targets", bad, seen)
	}
	for _, binding := range scopes.Value.Children {
		if seen[binding.Value.Child.Value.WorkItemID] != 3 {
			t.Fatal("scope/history/admission discovery lost", seen)
		}
	}
	foreign, err := k.ProductSplitChildSourceTargets(ctx, 18, "", 32)
	if err != nil || len(foreign.Targets) != 2 {
		t.Fatal("repository isolation lost", foreign, err)
	}
	for _, target := range foreign.Targets {
		if !target.Unavailable {
			t.Fatal("foreign repository granted target", target)
		}
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "source-target-pause", Target: r.Target, Action: "pause", Reason: "audit"}); err != nil {
		t.Fatal(err)
	}
	paused, err := k.ProductSplitChildSourceTargets(ctx, 17, "", 32)
	if err != nil || len(paused.Targets) != 8 {
		t.Fatal("pause prevented source metadata recovery", paused, err)
	}
	for _, raw := range []string{"{", "{}"} {
		if _, err := k.DB.Exec("UPDATE kernel_waves SET payload=? WHERE id=?", raw, r.Target.WaveID); err != nil {
			t.Fatal(err)
		}
		page, err := k.ProductSplitChildSourceTargets(ctx, 17, "", 32)
		if err != nil || len(page.Targets) != 8 {
			t.Fatal("bad Wave poisoned discovery", page, err)
		}
		for _, target := range page.Targets {
			if !target.Unavailable {
				t.Fatal("bad Wave granted target", target)
			}
		}
	}
}
