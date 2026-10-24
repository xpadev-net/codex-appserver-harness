package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestMergeDispatchTargetsShareRequestFlightKeyAndPageOpaqueEvidence(t *testing.T) {
	ctx := context.Background()
	k, requestID := mergeJournalFixture(t)
	page, err := k.MergeDispatchTargets(ctx, "", 128)
	if err != nil || len(page.Targets) != 1 || page.Targets[0].Kind != "request" || page.Targets[0].ID != requestID {
		t.Fatal(page, err)
	}
	key := page.Targets[0].MergeID
	job, err := k.PrepareProductMergeHTTP(ctx, requestID, "queued-owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	page, err = k.MergeDispatchTargets(ctx, "", 128)
	if err != nil || len(page.Targets) != 1 || page.Targets[0].Kind != "merge" || page.Targets[0].MergeID != key || key != job.ID {
		t.Fatal("new journal changed flight key", page, err)
	}
	if _, err := k.ClaimProductMerge(ctx, job.ID, job.Value.OwnerID, job.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "queue-seal", "discovery does not reopen terminals"); err != nil {
		t.Fatal(err)
	}
	page, err = k.MergeDispatchTargets(ctx, "", 128)
	if err != nil || len(page.Targets) != 0 {
		t.Fatal("terminal request requeued", page, err)
	}
	for i := 0; i < 257; i++ {
		id := fmt.Sprintf("page-request-%03d", i)
		if _, err := k.DB.Exec(`INSERT INTO kernel_tool_operations SELECT ?,session_id,thread_id,turn_id,?,name,argument_digest,'completed',result,? FROM kernel_tool_operations WHERE id=?`, id, id, []byte("broken"), requestID); err != nil {
			t.Fatal(err)
		}
	}
	if err := k.Put(ctx, "product_merge", "000-corrupt", job.Value.Product.Value.Target.WaveID, map[string]any{"merge": map[string]string{"state": "broken"}}); err != nil {
		t.Fatal(err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for _, limit := range []int{0, 129} {
		if _, err := k.MergeDispatchTargets(ctx, "", limit); err == nil {
			t.Fatal("unbounded page accepted")
		}
	}
	seen := map[string]bool{}
	after := ""
	for {
		page, err := k.MergeDispatchTargets(ctx, after, 128)
		if err != nil || len(page.Targets) > 128 {
			t.Fatal(page, err)
		}
		for _, target := range page.Targets {
			if seen[target.Cursor] || target.Cursor <= after || target.MergeID == "" {
				t.Fatal("bad cursor or flight identity", target)
			}
			seen[target.Cursor] = true
		}
		if page.NextAfter == "" {
			break
		}
		if page.NextAfter <= after {
			t.Fatal("page did not advance")
		}
		after = page.NextAfter
	}
	if len(seen) != 258 || !seen["merge:000-corrupt"] {
		t.Fatal("corrupt evidence hid later requests", len(seen))
	}
	if _, err := k.DB.Exec("UPDATE kernel_tool_operations SET state='unknown' WHERE id='page-request-000'"); err != nil {
		t.Fatal(err)
	}
	last, err := k.MergeDispatchTargets(ctx, "request:page-request-000", 128)
	if err != nil || len(last.Targets) == 0 || last.Targets[0].ID != "page-request-001" {
		t.Fatal(last, err)
	}
	first, err := k.MergeDispatchTargets(ctx, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range first.Targets {
		if target.ID == "page-request-000" {
			t.Fatal("unknown send became a completed Cloud request", target)
		}
	}
}

func TestCancelStalePreparedMergeRequiresPositiveChangeAndNeverCancelsClaimed(t *testing.T) {
	for _, mode := range []string{"current", "pause", "merge_hold", "candidate", "cancel", "missing_current", "claimed", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, requestID := mergeJournalFixture(t)
			job, err := k.PrepareProductMergeHTTP(ctx, requestID, "stale-owner", "merge")
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "pause", "cancel":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "stale-test-" + mode, Target: job.Value.Product.Value.Target, Action: mode, Reason: "explicit operator control"}); err != nil {
					t.Fatal(err)
				}
			case "candidate", "merge_hold", "rollback":
				p := job.Value.Product.Value
				if mode == "merge_hold" {
					p.WorkItem.MergeHold = true
				} else {
					p.Candidate.ID, p.WorkItem.Candidate = "next", "next"
					p.WorkItem.Version++
				}
				if _, err := k.SetProductTarget(ctx, job.Value.Product.Digest, p); err != nil {
					t.Fatal(err)
				}
				if mode == "rollback" {
					if _, err := k.DB.Exec("CREATE TRIGGER fail_stale_cancel BEFORE INSERT ON records WHEN NEW.kind='product_merge_cancel' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
						t.Fatal(err)
					}
				}
			case "missing_current":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_target' AND id=?", productTargetID(job.Value.Product.Value.Target, job.Value.Product.Value.WorkItem.ID)); err != nil {
					t.Fatal(err)
				}
			case "claimed":
				if _, err := k.ClaimProductMerge(ctx, job.ID, job.Value.OwnerID, job.Digest); err != nil {
					t.Fatal(err)
				}
			}
			result, changed, err := k.CancelStalePreparedProductMerge(ctx, job.ID)
			cancelErr := err
			wantCancel := mode == "candidate" || mode == "cancel"
			wantErr := mode == "missing_current" || mode == "claimed" || mode == "rollback"
			if (err != nil) != wantErr || changed != wantCancel || wantCancel && (result.State != "canceled" || result.Cancellation == nil) {
				t.Fatal(result, changed, err)
			}
			var state string
			if err := k.DB.QueryRow("SELECT state FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&state); err != nil || (state == "released") != wantCancel {
				t.Fatal("incorrect lease release", state, err)
			}
			seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
			if err != nil || seat.State != "held" {
				t.Fatal("stale prepared canceled PR lifetime", seat, err)
			}
			if mode == "claimed" && !errors.Is(cancelErr, ErrProductMergeHeld) {
				t.Fatal(cancelErr)
			}
		})
	}
}
