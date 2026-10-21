package codex

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type sessionStreamFixture struct {
	incoming chan Message
	sent     chan Message
}

func newSessionStream() *sessionStreamFixture {
	return &sessionStreamFixture{make(chan Message, 256), make(chan Message, 256)}
}
func (f *sessionStreamFixture) Next(ctx context.Context) (Message, error) {
	select {
	case m := <-f.incoming:
		return m, nil
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}
func (f *sessionStreamFixture) Send(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var m Message
	if err = json.Unmarshal(b, &m); err != nil {
		return err
	}
	f.sent <- m
	return nil
}
func sessionTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func TestSessionRPCHandlesToolsWithoutBlockingReader(t *testing.T) {
	ctx := sessionTestContext(t)
	stream := newSessionStream()
	var rpc *SessionRPC
	handled := make(chan error, 1)
	rpc = NewSessionRPC(ctx, stream, func(ctx context.Context, call DynamicToolCall) (DynamicToolResponse, error) {
		err := rpc.Call(ctx, "thread/read", map[string]string{"threadId": call.ThreadID}, nil)
		handled <- err
		return DynamicToolResponse{Success: err == nil, ContentItems: []ToolContent{{Type: "inputText", Text: "accepted"}}}, err
	})
	t.Cleanup(rpc.Close)
	started := make(chan error, 1)
	go func() { started <- rpc.Call(ctx, "turn/start", nil, nil) }()
	initial := <-stream.sent
	stream.incoming <- Message{ID: json.RawMessage("0"), Method: "item/tool/call", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","callId":"call","tool":"state_get","arguments":{}}`)}
	nested := <-stream.sent
	if nested.Method != "thread/read" {
		t.Fatal(nested)
	}
	// More notifications than the underlying Client's channel capacity. The tool
	// handler is still blocked on its nested RPC, so a synchronous reader deadlocks.
	for range 100 {
		stream.incoming <- Message{Method: "item/started", Params: json.RawMessage(`{"threadId":"thread"}`)}
	}
	stream.incoming <- Message{ID: nested.ID, Result: json.RawMessage(`{}`)}
	stream.incoming <- Message{ID: initial.ID, Result: json.RawMessage(`{}`)}
	select {
	case err := <-handled:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("tool/RPC deadlock")
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("turn RPC lost")
	}
	reply := <-stream.sent
	if string(reply.ID) != "0" || len(reply.Result) == 0 {
		t.Fatal("tool response lost", reply)
	}
	for range 100 {
		m, err := rpc.Next(ctx)
		if err != nil || m.Method != "item/started" {
			t.Fatal("notification lost", m, err)
		}
	}
}
func TestSessionRPCExperimentalOptInAndUnsupportedInput(t *testing.T) {
	ctx := sessionTestContext(t)
	stream := newSessionStream()
	rpc := NewSessionRPC(ctx, stream, nil)
	t.Cleanup(rpc.Close)
	result := make(chan error, 1)
	go func() { result <- rpc.Initialize(ctx) }()
	init := <-stream.sent
	var p struct {
		Capabilities struct {
			Experimental bool `json:"experimentalApi"`
		} `json:"capabilities"`
	}
	if json.Unmarshal(init.Params, &p) != nil || !p.Capabilities.Experimental {
		t.Fatal("experimental opt-in missing")
	}
	stream.incoming <- Message{ID: init.ID, Result: json.RawMessage(`{}`)}
	initialized := <-stream.sent
	if initialized.Method != "initialized" {
		t.Fatal(initialized)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	go func() { result <- rpc.Call(ctx, "turn/start", nil, nil) }()
	<-stream.sent
	request := Message{ID: json.RawMessage("9"), Method: "item/commandExecution/requestApproval", Params: json.RawMessage(`{}`)}
	stream.incoming <- request
	var required *RequestError
	if err := <-result; !errors.As(err, &required) {
		t.Fatal("input request not held", err)
	}
	if msg, err := rpc.Next(ctx); err != nil || msg.Method != request.Method {
		t.Fatal("input request lost", msg, err)
	}
}
func TestSessionRPCLateResponseDoesNotReplayCanceledCall(t *testing.T) {
	ctx := sessionTestContext(t)
	stream := newSessionStream()
	rpc := NewSessionRPC(ctx, stream, nil)
	t.Cleanup(rpc.Close)
	canceled, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- rpc.Call(canceled, "turn/start", nil, nil) }()
	old := <-stream.sent
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stream.incoming <- Message{ID: old.ID, Result: json.RawMessage(`{}`)}
	go func() { done <- rpc.Call(ctx, "thread/read", nil, nil) }()
	read := <-stream.sent
	if read.Method != "thread/read" {
		t.Fatal("canceled request replayed")
	}
	stream.incoming <- Message{ID: read.ID, Result: json.RawMessage(`{}`)}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
