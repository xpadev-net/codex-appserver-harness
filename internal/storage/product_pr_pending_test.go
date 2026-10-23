package storage

import (
	"context"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestPRDispatchOperationsPagesOnlyAcceptedDurableIntents(t *testing.T) {
	k, _, _, call := productCallFixture(t)
	ctx := context.Background()
	var ids []string
	for _, suffix := range []string{"a", "b", "c", "d", "e"} {
		call.CallID = "call-" + suffix
		reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, reply.OperationID)
	}
	if _, err := k.PreparePRDispatch(ctx, ids[0], "owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.MarkPRMayHaveSent(ctx, ids[0], "owner"); err != nil {
		t.Fatal(err)
	}
	if err := k.CancelUnsentPRDispatch(ctx, ids[1], "unsent", "explicitly canceled"); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	after := ""
	for {
		page, err := k.PRDispatchOperations(ctx, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range page.Operations {
			if op.Name != "pr_publish" || op.ID == ids[0] || op.ID == ids[1] || seen[op.ID] {
				t.Fatal(op)
			}
			seen[op.ID] = true
		}
		if page.NextAfter == "" {
			break
		}
		if page.NextAfter <= after {
			t.Fatal("cursor did not advance")
		}
		after = page.NextAfter
	}
	if len(seen) != 3 {
		t.Fatal(seen)
	}
	if _, err := k.PRDispatchOperations(ctx, "", 129); err == nil {
		t.Fatal("unbounded page accepted")
	}
}
