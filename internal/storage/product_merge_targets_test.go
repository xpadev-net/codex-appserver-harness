package storage

import (
	"context"
	"fmt"
	"testing"
)

func TestMergeReconciliationTargetsBoundedPagesAndCorruptionDoNotInferStopping(t *testing.T) {
	ctx := context.Background()
	k, job, stop := mergeResultFixture(t, true)
	page, err := k.MergeReconciliationTargets(ctx, "", 128)
	if err != nil || len(page.OperationIDs) != 0 {
		t.Fatal("possibly live transport queued", page, err)
	}
	stop.Disposition, stop.Response = "outcome_unknown", nil
	if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, stop); err != nil {
		t.Fatal(err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	page, err = k.MergeReconciliationTargets(ctx, "", 128)
	if err != nil || len(page.OperationIDs) != 1 || page.OperationIDs[0] != job.ID {
		t.Fatal("saved unknown stop lost after reopen", page, err)
	}
	// Synthetic heads isolate the work-list query. Readers must authenticate
	// every returned ID; these fixtures are deliberately not valid merge grants.
	for i := 0; i < 257; i++ {
		id := fmt.Sprintf("page-%03d", i)
		if err := k.Put(ctx, "product_merge", id, job.Value.Product.Value.Target.WaveID, map[string]any{"merge": map[string]string{"state": "stopped_unknown"}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := k.DB.Exec("UPDATE records SET payload=? WHERE kind='product_merge' AND id='page-000'", []byte("broken")); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("UPDATE records SET payload=? WHERE kind='product_merge' AND id='page-001'", []byte(`{"unexpected":"shape"}`)); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"prepared", "launching", "unknown", "canceled", "stopped_unsent", "rejected", "merged", "aborted_http"} {
		if err := k.Put(ctx, "product_merge", "excluded-"+state, job.Value.Product.Value.Target.WaveID, map[string]any{"merge": map[string]string{"state": state}}); err != nil {
			t.Fatal(err)
		}
	}
	scan := func() int {
		after := ""
		count := 0
		for {
			page, err := k.MergeReconciliationTargets(ctx, after, 128)
			if err != nil || len(page.OperationIDs) > 128 {
				t.Fatal(page, err)
			}
			count += len(page.OperationIDs)
			if page.NextAfter == "" {
				break
			}
			if page.NextAfter <= after {
				t.Fatal("nonadvancing cursor")
			}
			after = page.NextAfter
		}
		return count
	}
	if count := scan(); count != 258 {
		t.Fatal("invalid JSON poisoned discovery or terminal/live rows queued", count)
	}
	for _, limit := range []int{0, 129} {
		if _, err := k.MergeReconciliationTargets(ctx, "", limit); err == nil {
			t.Fatal("unbounded page")
		}
	}
	if _, err := k.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, mergeReconciliationFixture(k, job)); err != nil {
		t.Fatal(err)
	}
	if count := scan(); count != 257 {
		t.Fatal("reconciled request still queued", count)
	}
}
