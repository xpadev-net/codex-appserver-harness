package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func historyObservationFixture(t *testing.T) (*KernelStore, contract.SessionBinding, contract.SessionRun, string) {
	t.Helper()
	ctx := context.Background()
	k, _, b, run := interruptionFixture(t)
	run.State = "unknown"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "owner", Disposition: "group_stopped", Identity: managedIdentity(42)}); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	recovery := managedReserve(t, k, b, "history-owner", run.ID, "recovery")
	managedStart(t, k, recovery, 88)
	return k, recovery.Binding, run, "history-owner"
}
func historyResponse(t *testing.T, b contract.SessionBinding, turns []map[string]any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"thread": map[string]any{"id": b.ThreadID, "cwd": b.CWD, "turns": turns}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestReadOnlyHistoryObservationsClassifyWithoutChangingRunFacts(t *testing.T) {
	for _, mode := range []string{"read_error", "missing_turn", "ambiguous_turn", "partial_history", "observed_turn", "identity_mismatch", "invalid_response"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, b, run, owner := historyObservationFixture(t)
			turns := []map[string]any{}
			var response json.RawMessage
			errorDigest := ""
			switch mode {
			case "read_error":
				errorDigest = strings.Repeat("a", 64)
			case "ambiguous_turn":
				turns = []map[string]any{{"id": "one", "status": "completed", "itemsView": "full"}, {"id": "two", "status": "completed", "itemsView": "full"}}
			case "partial_history":
				turns = []map[string]any{{"id": "one", "status": "completed", "itemsView": "summary"}}
			case "observed_turn":
				turns = []map[string]any{{"id": "one", "status": "completed", "itemsView": "full", "items": []any{map[string]any{"type": "agentMessage", "text": "private model body"}}}}
			}
			if mode != "read_error" {
				response = historyResponse(t, b, turns)
			}
			if mode == "identity_mismatch" {
				wrong := b
				wrong.ThreadID = "foreign-thread"
				response = historyResponse(t, wrong, turns)
			}
			if mode == "invalid_response" {
				response = json.RawMessage(`{"thread":`)
			}
			observed, err := k.RecordHistoryRead(ctx, owner, b.ThreadID, response, errorDigest)
			if err != nil || observed.Status != mode || observed.SessionID != b.ID || observed.RunID != run.ID || observed.PreparationID != b.PreparationID {
				t.Fatal(observed, err)
			}
			replay, err := k.RecordHistoryRead(ctx, owner, b.ThreadID, response, errorDigest)
			if err != nil || !reflect.DeepEqual(replay, observed) {
				t.Fatal(replay, err)
			}
			var retained contract.SessionRun
			if err = k.Record(ctx, "agent_run", run.ID, &retained); err != nil || !reflect.DeepEqual(retained, run) {
				t.Fatal("observation fabricated run outcome", retained, err)
			}
			raw, err := json.Marshal(observed)
			if err != nil || strings.Contains(string(raw), "private model body") {
				t.Fatal("raw body copied to observation", err)
			}
		})
	}
}
func TestHistoryObservationsRejectExecuteOwnerWrongThreadAndClosedOwner(t *testing.T) {
	ctx := context.Background()
	k, b, _, owner := historyObservationFixture(t)
	response := historyResponse(t, b, []map[string]any{})
	if _, err := k.RecordHistoryRead(ctx, owner, "foreign", response, ""); err == nil {
		t.Fatal("cross-thread observation accepted")
	}
	if err := k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: owner, Disposition: "group_stopped", Identity: managedIdentity(88)}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RecordHistoryRead(ctx, owner, b.ThreadID, response, ""); err == nil {
		t.Fatal("closed owner reported a read")
	}
	k2, _, b2, _ := interruptionFixture(t)
	if _, err := k2.RecordHistoryRead(ctx, "owner", b2.ThreadID, historyResponse(t, b2, []map[string]any{}), ""); err == nil {
		t.Fatal("execute mode supplied recovery observation")
	}
}
func TestRepeatedHistoryErrorDigestsDoNotGrowPollingJournal(t *testing.T) {
	ctx := context.Background()
	k, b, _, owner := historyObservationFixture(t)
	var first HistoryObservation
	for i := 0; i < 20; i++ {
		observation, err := k.RecordHistoryRead(ctx, owner, b.ThreadID, nil, fmt.Sprintf("%064x", i))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = observation
		} else if !reflect.DeepEqual(first, observation) {
			t.Fatal("polling churn created another observation")
		}
	}
	var count int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='session_history_observation'").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}
