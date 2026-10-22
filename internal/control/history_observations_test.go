package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestRecoveryObservationHashesRawResponseBeforeTypedHistoryDecode(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	initial := fake(b)
	initial.turnFail = true
	unknown, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return initial, nil })}).Execute(ctx, request(b))
	if err == nil || !unknown.Stopped {
		t.Fatal(unknown, err)
	}
	recovery := fake(b)
	raw, err := json.Marshal(map[string]any{"thread": map[string]any{"id": "thread", "cwd": b.CWD, "turns": []any{map[string]any{"id": "turn", "status": "completed", "itemsView": "full", "items": []any{map[string]any{"type": "agentMessage", "text": `{"ok":true}`}, map[string]any{"type": "commandExecution", "aggregatedOutput": "body absent from worker typed Turn items", "exitCode": 0}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	recovery.threadReadReply = raw
	recovered, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return recovery, nil })}).Reconcile(ctx, b.ID, unknown.Run, request(b).Validate)
	if err != nil || !recovered.Stopped {
		t.Fatal(recovered, err)
	}
	var encoded []byte
	if err = k.DB.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind='session_history_observation' AND work_item_id=?", b.ID).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var observed storage.HistoryObservation
	if err = json.Unmarshal(encoded, &observed); err != nil || observed.ResponseDigest != fmt.Sprintf("%x", sha256.Sum256(raw)) {
		t.Fatal("lossy reconstructed response hash", observed, err)
	}
}
func TestCanceledReadPersistsObservationWithDetachedBoundedContext(t *testing.T) {
	k, b := taskFixture(t)
	initial := fake(b)
	initial.turnFail = true
	unknown, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return initial, nil })}).Execute(context.Background(), request(b))
	if err == nil || !unknown.Stopped {
		t.Fatal(unknown, err)
	}
	recovery := fake(b)
	recovery.holdThreadRead = true
	recovery.threadReadStarted = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return recovery, nil })}).Reconcile(ctx, b.ID, unknown.Run, request(b).Validate)
		done <- err
	}()
	await(t, recovery.threadReadStarted)
	cancel()
	if err = await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var encoded []byte
	if err = k.DB.QueryRowContext(context.Background(), "SELECT payload FROM records WHERE kind='session_history_observation' AND work_item_id=?", b.ID).Scan(&encoded); err != nil {
		t.Fatal("cancellation lost read observation", err)
	}
	var observed storage.HistoryObservation
	if err = json.Unmarshal(encoded, &observed); err != nil || observed.Status != "read_error" || observed.ErrorDigest == "" || observed.ResponseDigest != "" {
		t.Fatal(observed, err)
	}
	assertCapacity(t, k, 0)
}

func TestExplicitObservedAssociationStillRequiresReadOnlyOutcomeReconciliation(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	initial := fake(b)
	initial.turnFail = true
	unknown, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return initial, nil })}).Execute(ctx, request(b))
	if err == nil || !unknown.Stopped {
		t.Fatal(unknown, err)
	}
	before, err := k.Task(ctx, b.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	raw := json.RawMessage(fmt.Sprintf(`{"thread":{"id":"thread","cwd":%q,"turns":[{"id":"one","status":"completed","itemsView":"full","items":[{"type":"agentMessage","text":"{\"ok\":true}"}]},{"id":"two","status":"completed","itemsView":"full","items":[]}]}}`, b.CWD))
	recovery := fake(b)
	recovery.threadReadReply = raw
	held, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return recovery, nil })}).Reconcile(ctx, b.ID, unknown.Run, request(b).Validate)
	if err == nil || !held.Stopped || held.Run.State != "unknown" {
		t.Fatal(held, err)
	}
	var encoded []byte
	if err = k.DB.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind='session_history_observation' AND work_item_id=?", b.ID).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var observation storage.HistoryObservation
	if err = json.Unmarshal(encoded, &observation); err != nil || observation.Status != "ambiguous_turn" {
		t.Fatal(observation, err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.AssociateObservedTurn(ctx, storage.TurnAssociationRequest{Key: "choose", Expected: current, RunID: unknown.Run.ID, ObservationID: observation.ID, TurnID: "one", Reason: "operator identified the existing turn"}); err != nil {
		t.Fatal(err)
	}
	var associated contract.SessionRun
	if err = k.Record(ctx, "agent_run", unknown.Run.ID, &associated); err != nil || associated.State != "unknown" {
		t.Fatal(associated, err)
	}
	finalReader := fake(b)
	finalReader.threadReadReply = raw
	out, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return finalReader, nil })}).Reconcile(ctx, b.ID, associated, request(b).Validate)
	if err != nil || !out.Stopped || out.Run.State != "completed" || out.Run.ID != unknown.Run.ID || out.Run.TurnID != "one" {
		t.Fatal(out, err)
	}
	for _, p := range []*fakeProcess{recovery, finalReader} {
		if !reflect.DeepEqual(p.methods, []string{"initialize", "initialized", "thread/read"}) {
			t.Fatal(p.methods)
		}
	}
	after, err := k.Task(ctx, b.TaskID)
	if err != nil || after.Attempts != before.Attempts {
		t.Fatal(before, after, err)
	}
	assertCapacity(t, k, 0)
}
