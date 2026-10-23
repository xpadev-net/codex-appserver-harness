package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestSessionRecoveryInspectionPagesHeadersAndTurnCandidates(t *testing.T) {
	ctx := context.Background()
	k, b, run, owner := historyObservationFixture(t)
	for i := range 3 {
		turns := []map[string]any{{"id": fmt.Sprint("candidate-", i), "status": "completed", "itemsView": "full"}, {"id": "other", "status": "inProgress", "itemsView": "summary"}}
		if _, err := k.RecordHistoryRead(ctx, owner, b.ThreadID, historyResponse(t, b, turns), ""); err != nil {
			t.Fatal(err)
		}
	}
	after := ""
	seen := map[string]bool{}
	for range 3 {
		page, err := k.InspectSessionRecovery(ctx, b.ID, after, 1)
		if err != nil || len(page.Observations) != 1 || seen[page.Observations[0].ID] || page.Session.ActiveRunID != run.ID || page.Observations[0].CandidateTurns != 2 {
			t.Fatal(page, err)
		}
		seen[page.Observations[0].ID] = true
		after = page.NextAfter
	}
	if after != "" {
		t.Fatal("last header page has continuation", after)
	}
	for _, limit := range []int{0, 129} {
		if _, err := k.InspectSessionRecovery(ctx, b.ID, "", limit); err == nil {
			t.Fatal("invalid header limit accepted")
		}
		if _, err := k.InspectHistoryObservation(ctx, "missing", "", limit); err == nil {
			t.Fatal("invalid turn limit accepted")
		}
	}
}
func TestSavedInputSnapshotBoundsAndVerifiesPersistedBytes(t *testing.T) {
	ctx := context.Background()
	k, b, waves, _, _ := mainResultFixture(t)
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{waves[0].Target(), waves[1].Target()})
	if err != nil {
		t.Fatal(err)
	}
	got, err := k.SavedInputSnapshot(ctx, snapshot.ID, snapshot.Digest)
	if err != nil || string(got.Input) != string(snapshot.Input) {
		t.Fatal(got, err)
	}
	if _, err = k.SavedInputSnapshot(ctx, snapshot.ID, "wrong"); err == nil {
		t.Fatal("wrong digest accepted")
	}
	if _, err = k.SavedInputSnapshot(ctx, "", snapshot.Digest); err == nil {
		t.Fatal("empty reference accepted")
	}
	if _, err = k.DB.ExecContext(ctx, "UPDATE kernel_snapshots SET data=? WHERE id=?", []byte(`{"tampered":true}`), snapshot.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.SavedInputSnapshot(ctx, snapshot.ID, snapshot.Digest); err == nil {
		t.Fatal("changed bytes accepted")
	}
	if _, err = k.DB.ExecContext(ctx, "UPDATE kernel_snapshots SET data=? WHERE id=?", []byte(strings.Repeat("x", 2*contract.MaxSessionInput)), snapshot.ID); err != nil {
		t.Fatal(err)
	}
	got, err = k.SavedInputSnapshot(ctx, snapshot.ID, snapshot.Digest)
	if err == nil || len(got.Input) > contract.MaxSessionInput+1 {
		t.Fatal("snapshot read unbounded", len(got.Input), err)
	}
	k.Close()
	if _, err = k.SavedInputSnapshot(ctx, snapshot.ID, snapshot.Digest); err == nil || errors.Is(err, ErrMainRejectionHeld) {
		t.Fatal("database error hidden", err)
	}
}
