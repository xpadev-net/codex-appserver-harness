package storage

import (
	"context"
	"testing"
)

func TestUnusedThreadNeedsCreationProofAndNoReservedRun(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	b := prepareWakeSession(t, k, "main")
	unused, err := k.SessionThreadUnused(ctx, b)
	if err != nil || !unused {
		t.Fatal(unused, err)
	}
	if _, err = k.DB.ExecContext(ctx, "DELETE FROM records WHERE kind='session_new_thread' AND id=?", b.ThreadID); err != nil {
		t.Fatal(err)
	}
	unused, err = k.SessionThreadUnused(ctx, b)
	if err != nil || unused {
		t.Fatal("empty imported history guessed zero", unused, err)
	}
	b.Generation++
	if _, err = k.SessionThreadUnused(ctx, b); err == nil {
		t.Fatal("stale binding accepted")
	}
}
