package kernelcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func recoveryMainFixture(t *testing.T, state string) (string, *storage.KernelStore, contract.SessionBinding, contract.SessionRun, contract.InputSnapshot) {
	t.Helper()
	ctx := context.Background()
	root, w := controlWave(t)
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.Close() })
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	r, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "recovery-test-original", RunID: "recovery-test-run", Mode: "execute", Binding: b})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID); err != nil {
		t.Fatal(err)
	}
	identity := process.Identity{PID: 600142, BootID: "recovery-controlled-fixture", StartTicks: 600142}
	if err = k.RecordManagedProcessStarted(ctx, r.Process.Request.OwnerID, identity); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkManagedThreadRPC(ctx, r.Process.Request.OwnerID, "thread/start"); err != nil {
		t.Fatal(err)
	}
	if err = k.BindSessionThread(ctx, r.Binding, "recovery-test-thread"); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: r.Process.Request.RunID, ThreadID: b.ThreadID, State: "starting", Started: time.Now().UTC(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	run.State = state
	if state == "completed" {
		run.TurnID = "known-turn"
		run.Final = "invalid private model output"
	}
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = k.ConfirmManagedProcessStopped(ctx, storage.ManagedStopProof{OwnerID: r.Process.Request.OwnerID, Disposition: "group_stopped", Identity: identity}); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	return root, k, b, run, snapshot
}
func TestKernelCLIRejectMainUsesSavedSnapshotAndReplaysWithoutModel(t *testing.T) {
	ctx := context.Background()
	root, k, b, run, snapshot := recoveryMainFixture(t, "completed")
	request := storage.MainRunRejectionRequest{Key: "reject-known", Expected: b, RunID: run.ID, Snapshot: snapshot, Reason: "known output rejected after inspection"}
	file := operatorFile(t, request)
	raw, err := cliCall(t, "reject-main", "--root", root, "--file", file)
	if err != nil {
		t.Fatal(err)
	}
	var receipt storage.MainRunRejection
	if err = json.Unmarshal(raw, &receipt); err != nil || receipt.Rotation.State != "pending" {
		t.Fatal(receipt, err)
	}
	replay, err := cliCall(t, "reject-main", "--root", root, "--file", file)
	if err != nil || !bytes.Equal(raw, replay) {
		t.Fatal(string(replay), err)
	}
	var actual contract.SessionRun
	if err = k.Record(ctx, "agent_run", run.ID, &actual); err != nil || !reflect.DeepEqual(actual, run) {
		t.Fatal("operator rewrote physical outcome", actual, err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil || current.ThreadID != "" || current.Generation != b.Generation+1 {
		t.Fatal(current, err)
	}
	request.Reason = "conflicting replay"
	if _, err = cliCall(t, "reject-main", "--root", root, "--file", operatorFile(t, request)); err == nil {
		t.Fatal("changed decision accepted")
	}
}
func TestKernelCLIRejectMainHoldsUnknownStaleAndForgedSnapshot(t *testing.T) {
	for _, mode := range []string{"unknown", "stale", "digest", "missing"} {
		t.Run(mode, func(t *testing.T) {
			state := "completed"
			if mode == "unknown" {
				state = "unknown"
			}
			root, k, b, run, snapshot := recoveryMainFixture(t, state)
			request := storage.MainRunRejectionRequest{Key: "reject", Expected: b, RunID: run.ID, Snapshot: snapshot, Reason: "explicit operator choice"}
			switch mode {
			case "stale":
				request.Expected.Generation++
			case "digest":
				request.Snapshot.Digest = "wrong"
			case "missing":
				request.Snapshot.ID = "missing"
			}
			if _, err := cliCall(t, "reject-main", "--root", root, "--file", operatorFile(t, request)); err == nil {
				t.Fatal("unsafe rejection accepted")
			}
			actual, err := k.SessionBinding(context.Background(), b.ID)
			if err != nil || !reflect.DeepEqual(actual, b) {
				t.Fatal("held rejection changed binding", actual, err)
			}
		})
	}
}
func TestKernelCLIObservedTurnAssociationAndReadOnlyPagedHistory(t *testing.T) {
	ctx := context.Background()
	root, k, b, run, _ := recoveryMainFixture(t, "unknown")
	owner := "history-controlled-owner"
	r, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: owner, RunID: run.ID, Mode: "recovery", Binding: b})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.BeginManagedProcessLaunch(ctx, owner); err != nil {
		t.Fatal(err)
	}
	identity := process.Identity{PID: 600188, BootID: "recovery-controlled-fixture", StartTicks: 600188}
	if err = k.RecordManagedProcessStarted(ctx, owner, identity); err != nil {
		t.Fatal(err)
	}
	response, _ := json.Marshal(map[string]any{"thread": map[string]any{"id": b.ThreadID, "cwd": b.CWD, "turns": []any{map[string]any{"id": "two", "status": "completed", "itemsView": "full", "items": []any{map[string]any{"text": "private body never displayed"}}}, map[string]any{"id": "one", "status": "completed", "itemsView": "full"}}}})
	observation, err := k.RecordHistoryRead(ctx, owner, b.ThreadID, response, "")
	if err != nil {
		t.Fatal(err)
	}
	b = r.Binding
	request := storage.TurnAssociationRequest{Key: "associate", Expected: b, RunID: run.ID, ObservationID: observation.ID, TurnID: "one", Reason: "operator selected actual observed candidate"}
	if _, err = cliCall(t, "associate-turn", "--root", root, "--file", operatorFile(t, request)); !errors.Is(err, storage.ErrTurnAssociationHeld) {
		t.Fatal("live observer accepted", err)
	}
	if err = k.ConfirmManagedProcessStopped(ctx, storage.ManagedStopProof{OwnerID: owner, Disposition: "group_stopped", Identity: identity}); err != nil {
		t.Fatal(err)
	}
	request.Expected, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := cliCall(t, "session", "--root", root, "--session", b.ID, "--limit", "1")
	if err != nil {
		t.Fatal(err)
	}
	var view storage.SessionRecoveryInspection
	if err = json.Unmarshal(raw, &view); err != nil || len(view.Observations) != 1 || view.Observations[0].CandidateTurns != 2 || view.Session.LastRun == nil || view.Session.LastRun.State != "unknown" || bytes.Contains(raw, []byte("private body")) {
		t.Fatal(string(raw), err)
	}
	first, err := cliCall(t, "history", "--root", root, "--id", observation.ID, "--limit", "1")
	if err != nil {
		t.Fatal(err)
	}
	var page storage.HistoryInspection
	if err = json.Unmarshal(first, &page); err != nil || len(page.Turns) != 1 || page.Turns[0].ID != "one" || !page.Turns[0].Candidate || page.NextAfter != "one" {
		t.Fatal(string(first), err)
	}
	next, err := cliCall(t, "history", "--root", root, "--id", observation.ID, "--after", page.NextAfter, "--limit", "1")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(next, &page); err != nil || len(page.Turns) != 1 || page.Turns[0].ID != "two" || page.NextAfter != "" {
		t.Fatal(string(next), err)
	}
	after, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("inspection changed run", err)
	}
	request.TurnID = "unobserved"
	if _, err = cliCall(t, "associate-turn", "--root", root, "--file", operatorFile(t, request)); err == nil {
		t.Fatal("invented candidate accepted")
	}
	request.TurnID = "one"
	file := operatorFile(t, request)
	receipt, err := cliCall(t, "associate-turn", "--root", root, "--file", file)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := cliCall(t, "associate-turn", "--root", root, "--file", file)
	if err != nil || !bytes.Equal(receipt, replay) {
		t.Fatal(string(replay), err)
	}
	var actual contract.SessionRun
	if err = k.Record(ctx, "agent_run", run.ID, &actual); err != nil || actual.State != "unknown" || actual.TurnID != "one" || actual.Final != "" {
		t.Fatal("association fabricated outcome", actual, err)
	}
}

func TestKernelCLIRecoveryInputPreparesSavedReferenceWithoutDispatch(t *testing.T) {
	root, k, b, run, _ := recoveryMainFixture(t, "completed")
	ctx := context.Background()
	budget := storage.RunBudgetBinding{}
	if err := k.Record(ctx, "run_budget", run.ID, &budget); err != nil {
		t.Fatal(err)
	}
	before, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	file := operatorFile(t, map[string]any{"targets": budget.Targets})
	raw, err := cliCall(t, "recovery-input", "--root", root, "--session", b.ID, "--file", file)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot contract.InputSnapshot
	if err = json.Unmarshal(raw, &snapshot); err != nil || snapshot.ID == "" || snapshot.Digest == "" || bytes.Contains(raw, []byte("context")) {
		t.Fatal(string(raw), err)
	}
	if _, err = k.SavedInputSnapshot(ctx, snapshot.ID, snapshot.Digest); err != nil {
		t.Fatal(err)
	}
	after, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("preparing input changed execution", err)
	}
}
