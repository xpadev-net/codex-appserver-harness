package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"path/filepath"
	"testing"
)

type fakeRPC struct {
	calls  []string
	status string
	fail   bool
	events []codex.Message
}

func (f *fakeRPC) Call(_ context.Context, m string, _ any, v any) error {
	f.calls = append(f.calls, m)
	if f.fail {
		return errors.New("lost response")
	}
	var b string
	switch m {
	case "thread/start", "thread/resume":
		b = `{"thread":{"id":"thread","cwd":"/work"}}`
	case "turn/start":
		b = `{"turn":{"id":"turn"}}`
	case "thread/read":
		b = `{"thread":{"id":"thread","cwd":"/work","turns":[{"id":"turn","status":"completed","items":[{"type":"agentMessage","text":"done"}]}]}}`
	}
	return json.Unmarshal([]byte(b), v)
}
func (f *fakeRPC) Next(context.Context) (codex.Message, error) {
	if len(f.events) > 0 {
		m := f.events[0]
		f.events = f.events[1:]
		return m, nil
	}
	p, _ := json.Marshal(map[string]any{"threadId": "thread", "turn": map[string]string{"id": "turn", "status": f.status}})
	return codex.Message{Method: "turn/completed", Params: p}, nil
}
func TestLifecycleAndUnknownStart(t *testing.T) {
	ctx := context.Background()
	s, err := storage.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rpc := &fakeRPC{status: "completed"}
	w := Worker{rpc, s}
	session, err := w.Prepare(ctx, Session{WorkItemID: "w", Role: "implementation", CWD: "/work", Model: "configured"})
	if err != nil {
		t.Fatal(err)
	}
	run, err := w.Execute(ctx, session, "r1", "implement", "task", nil)
	if err != nil || run.State != "completed" {
		t.Fatal(run, err)
	}
	if _, err = w.Prepare(ctx, session); err != nil || rpc.calls[len(rpc.calls)-1] != "thread/resume" {
		t.Fatal("did not resume", err)
	}
	rpc.fail = true
	r, err := w.Execute(ctx, session, "r2", "repair", "task", nil)
	if err == nil || r.State != "unknown" {
		t.Fatal("lost response succeeded")
	}
	n := len(rpc.calls)
	if _, err = w.Execute(ctx, session, "r2", "repair", "task", nil); err == nil || len(rpc.calls) != n {
		t.Fatal("unknown start resent")
	}
	rpc.fail = false
	run, err = w.Reconcile(ctx, session, run)
	if err != nil || run.Final != "done" {
		t.Fatal(run, err)
	}
}
func TestFailedAndInputTurns(t *testing.T) {
	for _, status := range []string{"failed", "interrupted"} {
		t.Run(status, func(t *testing.T) {
			s, _ := storage.Open(filepath.Join(t.TempDir(), "db"))
			defer s.Close()
			w := Worker{&fakeRPC{status: status}, s}
			r, err := w.Execute(context.Background(), Session{WorkItemID: "w", ThreadID: "thread"}, "run", "implement", "task", nil)
			if err == nil || r.State != status {
				t.Fatal(r, err)
			}
		})
	}
	s, _ := storage.Open(filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	w := Worker{&fakeRPC{events: []codex.Message{{ID: []byte("1"), Method: "item/tool/requestUserInput"}}}, s}
	r, err := w.Execute(context.Background(), Session{WorkItemID: "w", ThreadID: "thread"}, "run", "implement", "task", nil)
	if err == nil || r.State != "blocked" || r.Request == nil {
		t.Fatal(r, err)
	}
}
func TestStrictHandoff(t *testing.T) {
	b := []byte(`{"schema_version":1,"work_item_id":"w","scope_revision":2,"input_candidate_id":"c","outcome":"candidate","summary":"done","covered_criteria":["AC1"],"remaining_work":[],"requested_profiles":[],"blockers":[]}`)
	if _, err := Decode(b, "w", 2, "c", []string{"AC1"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(b, "w", 3, "c", []string{"AC1"}, nil); err == nil {
		t.Fatal("stale scope accepted")
	}
	b = append(b, []byte(` {}`)...)
	if _, err := Decode(b, "w", 2, "c", []string{"AC1"}, nil); err == nil {
		t.Fatal("trailing data accepted")
	}
}
func TestNullHandoffElementsRejected(t *testing.T) {
	for _, key := range []string{"covered_criteria", "remaining_work", "requested_profiles", "blockers"} {
		v := map[string]any{"schema_version": 1, "work_item_id": "w", "scope_revision": 1, "input_candidate_id": "", "outcome": "blocked", "summary": "blocked", "covered_criteria": []any{}, "remaining_work": []any{}, "requested_profiles": []any{}, "blockers": []any{"reason"}}
		v[key] = []any{nil}
		b, _ := json.Marshal(v)
		if _, err := Decode(b, "w", 1, "", nil, nil); err == nil {
			t.Fatal("accepted null", key)
		}
	}
}
func TestCaseVariantHandoffKeyRejected(t *testing.T) {
	b := []byte(`{"schema_version":1,"work_item_id":"w","scope_revision":1,"input_candidate_id":"","outcome":"blocked","summary":"blocked","covered_criteria":[],"remaining_work":[],"Remaining_Work":[null],"requested_profiles":[],"blockers":["reason"]}`)
	if _, err := Decode(b, "w", 1, "", nil, nil); err == nil {
		t.Fatal("case variant accepted")
	}
}
