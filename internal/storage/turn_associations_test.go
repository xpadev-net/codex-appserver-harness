package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func turnAssociationFixture(t *testing.T, stop bool) (*KernelStore, contract.SessionRun, TurnAssociationRequest) {
	t.Helper()
	ctx := context.Background()
	k, b, run, owner := historyObservationFixture(t)
	observation, err := k.RecordHistoryRead(ctx, owner, b.ThreadID, historyResponse(t, b, []map[string]any{{"id": "one", "status": "completed", "itemsView": "full"}, {"id": "two", "status": "completed", "itemsView": "full"}}), "")
	if err != nil || observation.Status != "ambiguous_turn" {
		t.Fatal(observation, err)
	}
	if stop {
		if err = k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: owner, Disposition: "group_stopped", Identity: managedIdentity(88)}); err != nil {
			t.Fatal(err)
		}
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	return k, run, TurnAssociationRequest{Key: "choose-observed", Expected: b, RunID: run.ID, ObservationID: observation.ID, TurnID: "one", Reason: "operator identified the saved run from observed candidates"}
}
func TestObservedTurnAssociationChangesOnlyIdentityAndReplaysAfterRestart(t *testing.T) {
	ctx := context.Background()
	k, run, request := turnAssociationFixture(t, true)
	before, err := k.SessionDispatchState(ctx, request.Expected.ID)
	if err != nil {
		t.Fatal(err)
	}
	association, err := k.AssociateObservedTurn(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var actual contract.SessionRun
	run.TurnID = request.TurnID
	if err = k.Record(ctx, "agent_run", run.ID, &actual); err != nil || !reflect.DeepEqual(actual, run) {
		t.Fatal(actual, run, err)
	}
	after, err := k.SessionDispatchState(ctx, request.Expected.ID)
	if err != nil {
		t.Fatal(err)
	}
	before.Run.TurnID = request.TurnID
	if !reflect.DeepEqual(before, after) {
		t.Fatal("association changed dispatch/outcome facts", before, after)
	}
	path := filepath.Join(k.Root, "state.db")
	if err = k.Close(); err != nil {
		t.Fatal(err)
	}
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.AssociateObservedTurn(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, association) {
		t.Fatal(replay, err)
	}
	request.Reason = "different reason"
	if _, err = k.AssociateObservedTurn(ctx, request); err == nil {
		t.Fatal("changed request replay accepted")
	}
}
func TestObservedTurnAssociationRejectsUnprovenOrStaleSelection(t *testing.T) {
	for _, mode := range []string{"live_observer", "unobserved", "wrong_run", "stale_binding", "not_ambiguous", "missing_stop_proof", "already_claimed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, run, r := turnAssociationFixture(t, mode != "live_observer")
			switch mode {
			case "unobserved":
				r.TurnID = "invented"
			case "wrong_run":
				r.RunID = "foreign"
			case "stale_binding":
				r.Expected.Generation++
			case "not_ambiguous":
				var o HistoryObservation
				if err := k.Record(ctx, "session_history_observation", r.ObservationID, &o); err != nil {
					t.Fatal(err)
				}
				o.Status = "missing_turn"
				if err := k.Put(ctx, "session_history_observation", o.ID, r.Expected.ID, o); err != nil {
					t.Fatal(err)
				}
			case "missing_stop_proof":
				if _, err := k.DB.ExecContext(ctx, "DELETE FROM records WHERE kind='managed_stop_proof' AND id='history-owner'"); err != nil {
					t.Fatal(err)
				}
			case "already_claimed":
				run.ID = "other"
				run.TurnID = r.TurnID
				raw, err := json.Marshal(run)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := k.DB.ExecContext(ctx, "INSERT INTO records(kind,id,work_item_id,payload) VALUES('agent_run',?,?,?)", run.ID, r.Expected.ID, raw); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.AssociateObservedTurn(ctx, r); err == nil {
				t.Fatal("unsafe selection accepted")
			}
			var turn string
			if err := k.DB.QueryRowContext(ctx, "SELECT json_extract(payload,'$.turn_id') FROM records WHERE kind='agent_run' AND id=?", r.RunID).Scan(&turn); err == nil && turn != "" {
				t.Fatal("held association changed turn", turn)
			}
		})
	}
}
func TestObservedTurnAssociationAuditFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	k, run, r := turnAssociationFixture(t, true)
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_turn_association BEFORE INSERT ON records WHEN NEW.kind='session_turn_association' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AssociateObservedTurn(ctx, r); err == nil {
		t.Fatal("audit failure accepted")
	}
	var actual contract.SessionRun
	if err := k.Record(ctx, "agent_run", run.ID, &actual); err != nil || !reflect.DeepEqual(actual, run) {
		t.Fatal(actual, err)
	}
}
