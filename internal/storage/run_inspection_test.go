package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestRunInspectionPagesStoredMetadataWithoutOutputOrSideEffects(t *testing.T) {
	ctx := context.Background()
	k, b, waves, run, snapshot := mainResultFixture(t)
	run.State, run.TurnID, run.Final = "completed", "known-turn", strings.Repeat("private output ", 10000)
	run.Usage = json.RawMessage(`{"last":{"totalTokens":42},"total":{"totalTokens":42}}`)
	run.ObservedUsage = &contract.RunUsageObservation{Version: 1, Source: "thread_total_delta", HasObservation: true, TotalTokens: 42}
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	other := run
	other.ID = "orphan-metadata"
	other.Final = "orphan output"
	other.ObservedUsage = nil
	// Inject missing binding/budget facts to ensure inspection marks them unavailable.
	if err := k.Store.Put(ctx, "agent_run", other.ID, b.ID, other); err != nil {
		t.Fatal(err)
	}
	before, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	page, err := k.InspectRuns(ctx, b.ID, 0, 1)
	if err != nil || len(page.Entries) != 1 || !page.More || page.Entries[0].Run.ID != run.ID || !page.Entries[0].Active || !page.Entries[0].ProcessStopped || page.Entries[0].Run.Budget.SnapshotID != snapshot.ID || page.Entries[0].ObservedUsage.TotalTokens != 42 || !page.Entries[0].Run.Budget.HasObservation || page.Entries[0].Run.Budget.ObservedTokens != 42 || page.Entries[0].Run.Budget.FinalUsage {
		t.Fatal(page, err)
	}
	raw, err := json.Marshal(page)
	if err != nil || bytes.Contains(raw, []byte("private output")) {
		t.Fatal("history copied raw output", err)
	}
	next, err := k.InspectRuns(ctx, b.ID, page.Cursor, 1)
	if err != nil || len(next.Entries) != 1 || next.More || next.Cursor <= page.Cursor || next.Entries[0].Run.ID != other.ID || next.Entries[0].BindingPresent || !next.Entries[0].BudgetUnavailable {
		t.Fatal(next, err)
	}
	end, err := k.InspectRuns(ctx, b.ID, next.Cursor, 1)
	if err != nil || len(end.Entries) != 0 || end.Cursor != next.Cursor {
		t.Fatal(end, err)
	}
	after, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("history changed session", err)
	}
	w, err := k.Wave(ctx, waves[0].ID)
	if err != nil || w.MainTurns != 1 {
		t.Fatal("history spent budget", w, err)
	}
}
func TestRunOutputInspectionPreservesUnknownAndEnforcesByteBounds(t *testing.T) {
	ctx := context.Background()
	k, b, _, run, _ := mainResultFixture(t)
	run.State, run.Final = "unknown", "結果ABC"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	entry, data, err := k.InspectRunOutput(ctx, run.ID, 1, 2)
	if err != nil || !bytes.Equal(data, []byte(run.Final)[1:3]) || entry.Run.State != "unknown" || entry.Disposed || entry.Run.FinalBytes != int64(len(run.Final)) {
		t.Fatal(entry, data, err)
	}
	for _, bounds := range []struct {
		offset int64
		limit  int
	}{{-1, 1}, {0, 0}, {0, contract.MaxInline + 1}, {int64(len(run.Final)) + 1, 1}} {
		if _, _, err = k.InspectRunOutput(ctx, run.ID, bounds.offset, bounds.limit); err == nil {
			t.Fatal("invalid output bounds accepted", bounds)
		}
	}
	_, data, err = k.InspectRunOutput(ctx, run.ID, int64(len(run.Final)), 1)
	if err != nil || len(data) != 0 {
		t.Fatal(data, err)
	}
	k.Close()
	if _, err = k.InspectRuns(ctx, b.ID, 0, 1); err == nil {
		t.Fatal("DB error converted to empty history")
	}
}

func TestRunInspectionLinksAppliedTaskResultDisposition(t *testing.T) {
	ctx := context.Background()
	k, b, run, _ := recordedTaskResultFixture(t, nil)
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry, _, err := k.InspectRunOutput(ctx, run.ID, 0, 64)
	if err != nil || !entry.Disposed || entry.Active || entry.Disposition == nil || entry.Disposition.Kind != "task_result_application" || entry.Disposition.ResultID != receipt.ResultID || entry.Disposition.State != string(receipt.State) || entry.Disposition.Archived {
		t.Fatal(entry, err)
	}
}

func TestRunInspectionMarksArchivedTaskFailuresWithoutChangingPhysicalState(t *testing.T) {
	for _, state := range []string{"failed", "interrupted", "blocked"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			k, b := boundTaskFixture(t)
			run := contract.SessionRun{ID: "archived-failure", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
			if err := k.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err != nil {
				t.Fatal(err)
			}
			run.State = state
			if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
				t.Fatal(err)
			}
			if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			if err := k.ArchiveSessionRun(ctx, b.ID, run.ID, "known stopped failure requires explicit recovery"); err != nil {
				t.Fatal(err)
			}
			page, err := k.InspectRuns(ctx, b.ID, 0, 1)
			if err != nil || len(page.Entries) != 1 {
				t.Fatal(page, err)
			}
			output, _, err := k.InspectRunOutput(ctx, run.ID, 0, 64)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range []RunHistoryEntry{page.Entries[0], output} {
				if entry.Run.State != state || !entry.Disposed || entry.Active || !entry.ProcessStopped || entry.Disposition == nil || entry.Disposition.Kind != "session_run_disposition" || !entry.Disposition.Archived || entry.Disposition.State != "archived" {
					t.Fatal("incorrect archival observation", entry)
				}
			}
		})
	}
}
