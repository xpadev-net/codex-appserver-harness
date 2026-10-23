package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestSessionToolProfilesPersistAndRejectUnprovenExistingThreads(t *testing.T) {
	ctx := context.Background()
	k, b := boundTaskFixture(t)
	if err := k.PinSessionTools(ctx, b, "product_v1"); err == nil {
		t.Fatal("old unprofiled thread upgraded")
	}
	if err := k.PinSessionTools(ctx, b, "kernel_v1"); err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	k.Close()
	reopened, err := OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.PinSessionTools(ctx, b, "kernel_v1"); err != nil {
		t.Fatal(err)
	}
	if err := reopened.PinSessionTools(ctx, b, "product_v1"); err == nil || !strings.Contains(err.Error(), "profile differs") {
		t.Fatal(err)
	}
	stale := b
	stale.Generation++
	if err := reopened.PinSessionTools(ctx, stale, "kernel_v1"); err == nil {
		t.Fatal("stale generation accepted")
	}
}

func TestSessionToolProfileCanChangeOnlyAfterExplicitMainRotation(t *testing.T) {
	ctx := context.Background()
	k, w := rotationFixture(t)
	rotationKnownRun(t, k, w, "first")
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := k.PinSessionTools(ctx, b, "kernel_v1"); err != nil {
		t.Fatal(err)
	}
	if err := k.PinSessionTools(ctx, b, "product_v1"); err == nil {
		t.Fatal("existing profile changed")
	}
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.BeginMainThreadReplacement(ctx, b, snapshot, "explicit host tool configuration change"); err != nil {
		t.Fatal(err)
	}
	next, err := k.SessionBinding(ctx, b.ID)
	if err != nil || next.Generation != b.Generation+1 || next.ThreadID != "" {
		t.Fatal(next, err)
	}
	if err := k.PinSessionTools(ctx, next, "product_v1"); err != nil {
		t.Fatal(err)
	}
	if err := k.PinSessionTools(ctx, next, "kernel_v1"); err == nil {
		t.Fatal("product thread silently downgraded")
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='session_tool_profile'").Scan(&count); err != nil || count != 2 {
		t.Fatal("profile history lost", count, err)
	}
}

func TestSessionToolProfileSaveFailureRollsBackAndStartsNoWork(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec(`CREATE TRIGGER fail_tools BEFORE INSERT ON records WHEN NEW.kind='session_tool_profile' BEGIN SELECT RAISE(FAIL,'tool profile rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if err := k.PinSessionTools(ctx, b, "product_v1"); err == nil || !strings.Contains(err.Error(), "tool profile rollback") {
		t.Fatal("injection not reached", err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='session_tool_profile'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_tools"); err != nil {
		t.Fatal(err)
	}
	if err := k.PinSessionTools(ctx, b, "product_v1"); err != nil {
		t.Fatal(err)
	}
	if err := k.PinSessionTools(ctx, b, "bogus"); err == nil {
		t.Fatal("unknown profile accepted")
	}
}
