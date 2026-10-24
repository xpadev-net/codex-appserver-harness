package storage

import (
	"context"
	"fmt"
	"testing"
)

func TestCloudCompletionTargetsBoundedOpaquePagesSurviveCorruptEvidenceAndReopen(t *testing.T) {
	ctx := context.Background()
	k, round, value := cloudCompletionFixture(t, []int64{91})
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "paused", Target: round.Product.Value.Target, Action: "cancel", Reason: "historical work remains"}); err != nil {
		t.Fatal(err)
	}
	root := k.Root
	k.Close()
	var err error
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	// Synthetic rows exercise discovery only. Authentication remains per-reader;
	// even malformed JSON must not prevent discovery of later healthy requests.
	for i := 0; i < 257; i++ {
		id := fmt.Sprintf("page-%03d", i)
		if _, err := k.DB.Exec(`INSERT INTO kernel_dispatch_intents SELECT ?,session_id,wave_id,'held',? FROM kernel_dispatch_intents WHERE run_id=?`, id, []byte("broken"), round.OperationID); err != nil {
			t.Fatal(err)
		}
		if _, err := k.DB.Exec(`INSERT INTO kernel_tool_operations SELECT ?,session_id,thread_id,turn_id,?,name,argument_digest,'completed',result,payload FROM kernel_tool_operations WHERE id=?`, id, id, round.OperationID); err != nil {
			t.Fatal(err)
		}
	}
	for _, limit := range []int{0, 129} {
		if _, err := k.CloudCompletionTargets(ctx, "", limit); err == nil {
			t.Fatal("unbounded page accepted")
		}
	}
	scan := func() []string {
		var got []string
		after := ""
		for {
			page, err := k.CloudCompletionTargets(ctx, after, 128)
			if err != nil || len(page.OperationIDs) > 128 {
				t.Fatal(page, err)
			}
			got = append(got, page.OperationIDs...)
			if page.NextAfter == "" {
				break
			}
			if page.NextAfter <= after {
				t.Fatal("cursor did not advance")
			}
			after = page.NextAfter
		}
		return got
	}
	before := scan()
	if len(before) != 258 {
		t.Fatal(len(before))
	}
	if _, err := k.RecordProductCloudReviewCompleted(ctx, value); err != nil {
		t.Fatal(err)
	}
	if after := scan(); len(after) != 257 {
		t.Fatal("completed request remained queued", len(after))
	}
	// Unknown and accepted sends have no positively completed request binding.
	if _, err := k.DB.Exec("UPDATE kernel_tool_operations SET state='unknown' WHERE id='page-000'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("UPDATE kernel_tool_operations SET state='accepted' WHERE id='page-001'"); err != nil {
		t.Fatal(err)
	}
	if after := scan(); len(after) != 255 {
		t.Fatal("unknown request queued", len(after))
	}
}
