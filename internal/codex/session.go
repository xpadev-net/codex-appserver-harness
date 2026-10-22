package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// Stream has a single reader; RPCs and handlers share its serialized Send.
// SessionRPC owns Next for its lifetime. Never also call Client.Call on it.
type Stream interface {
	Next(context.Context) (Message, error)
	Send(any) error
}
type DynamicToolCall struct {
	ThreadID  string          `json:"threadId"`
	TurnID    string          `json:"turnId"`
	CallID    string          `json:"callId"`
	Tool      string          `json:"tool"`
	Namespace *string         `json:"namespace,omitempty"`
	Arguments json.RawMessage `json:"arguments"`
}
type ToolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type DynamicToolResponse struct {
	Success      bool          `json:"success"`
	ContentItems []ToolContent `json:"contentItems"`
}
type ToolHandler func(context.Context, DynamicToolCall) (DynamicToolResponse, error)
type rpcReply struct {
	message Message
	err     error
}

// SessionRPC separates response delivery from notifications and tool execution.
// A handler can make an RPC while the reader continues receiving events.
type SessionRPC struct {
	stream     Stream
	ctx        context.Context
	cancel     context.CancelFunc
	handler    ToolHandler
	auth       *sessionAuthentication
	mu         sync.Mutex
	sequence   int
	calls      map[string]chan rpcReply
	events     []Message
	eventBytes int
	wake       chan struct{}
	done       chan struct{}
	terminal   error
	once       sync.Once
	requests   map[string]bool
	handlers   chan struct{}
}

func NewSessionRPC(ctx context.Context, stream Stream, handler ToolHandler) *SessionRPC {
	return newSessionRPC(ctx, stream, handler, nil)
}
func newSessionRPC(ctx context.Context, stream Stream, handler ToolHandler, auth *sessionAuthentication) *SessionRPC {
	ctx, cancel := context.WithCancel(ctx)
	s := &SessionRPC{auth: auth, stream: stream, ctx: ctx, cancel: cancel, handler: handler, calls: map[string]chan rpcReply{}, wake: make(chan struct{}, 1), done: make(chan struct{}), requests: map[string]bool{}, handlers: make(chan struct{}, 32)}
	go s.pump()
	return s
}
func (s *SessionRPC) finish(err error) {
	s.once.Do(func() { s.mu.Lock(); s.terminal = err; s.mu.Unlock(); close(s.done); s.cancel() })
}

// Close stops transport work. The process owner must separately close and wait
// for the underlying Client before releasing a process/turn reservation.
func (s *SessionRPC) Close() { s.finish(context.Canceled) }
func (s *SessionRPC) publish(m Message) bool {
	s.mu.Lock()
	size := len(m.ID) + len(m.Method) + len(m.Params) + len(m.Result) + len(m.Error)
	if len(s.events) >= 4096 || s.eventBytes+size > 8<<20 {
		s.mu.Unlock()
		s.finish(errors.New("session notification capacity exceeded"))
		return false
	}
	s.events = append(s.events, m)
	s.eventBytes += size
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return true
}
func (s *SessionRPC) pump() {
	for {
		m, err := s.stream.Next(s.ctx)
		if err != nil {
			s.finish(err)
			return
		}
		if m.Method == "" {
			s.mu.Lock()
			ch := s.calls[string(m.ID)]
			seq := s.sequence
			s.mu.Unlock()
			if ch != nil {
				select {
				case ch <- rpcReply{message: m}:
				default:
					s.finish(errors.New("duplicate session RPC response"))
					return
				}
				continue
			}
			id, err := strconv.Atoi(string(m.ID))
			if err != nil || id < 1 || id > seq {
				s.finish(errors.New("unrecognized session RPC response"))
				return
			}
			continue // Late response to a canceled RPC. Never replay its request.
		}
		if len(m.ID) == 0 {
			if !s.publish(m) {
				return
			}
			continue
		}
		authRequest := m.Method == "account/chatgptAuthTokens/refresh" && s.auth != nil
		if !authRequest && (m.Method != "item/tool/call" || s.handler == nil) {
			s.publish(m)
			s.finish(&RequestError{Request: m})
			return
		}
		var call DynamicToolCall
		if !authRequest && (len(m.Params) > 64<<10 || json.Unmarshal(m.Params, &call) != nil || call.ThreadID == "" || call.TurnID == "" || call.CallID == "" || call.Tool == "" || !json.Valid(call.Arguments)) {
			s.finish(&RequestError{Request: m})
			return
		}
		requestID := string(m.ID)
		if s.requests[requestID] || len(s.requests) >= 4096 {
			s.finish(errors.New("duplicate/excessive session server request"))
			return
		}
		s.requests[requestID] = true
		select {
		case s.handlers <- struct{}{}:
		default:
			s.finish(errors.New("session tool concurrency exceeded"))
			return
		}
		go func(m Message, call DynamicToolCall) {
			defer func() { <-s.handlers }()
			if authRequest {
				s.respondAuthentication(m)
				return
			}
			result, err := s.handler(s.ctx, call)
			if err != nil {
				result = DynamicToolResponse{Success: false, ContentItems: []ToolContent{{Type: "inputText", Text: "Controller rejected the tool request; inspect the saved operation."}}}
			}
			data, err := json.Marshal(result)
			if err != nil || len(data) > 64<<10 {
				s.finish(errors.New("invalid/oversized tool response"))
				return
			}
			if err = s.stream.Send(map[string]any{"id": m.ID, "result": result}); err != nil {
				s.finish(err)
			}
		}(m, call)
	}
}
func (s *SessionRPC) Next(ctx context.Context) (Message, error) {
	for {
		s.mu.Lock()
		if len(s.events) > 0 {
			m := s.events[0]
			s.eventBytes -= len(m.ID) + len(m.Method) + len(m.Params) + len(m.Result) + len(m.Error)
			s.events[0] = Message{}
			s.events = s.events[1:]
			s.mu.Unlock()
			return m, nil
		}
		err := s.terminal
		s.mu.Unlock()
		if err != nil {
			return Message{}, err
		}
		select {
		case <-s.wake:
		case <-s.done:
		case <-ctx.Done():
			return Message{}, ctx.Err()
		}
	}
}
func (s *SessionRPC) Call(ctx context.Context, method string, params, result any) error {
	s.mu.Lock()
	if s.terminal != nil {
		err := s.terminal
		s.mu.Unlock()
		return err
	}
	if len(s.calls) >= 32 {
		s.mu.Unlock()
		return errors.New("session RPC concurrency exceeded")
	}
	s.sequence++
	id := s.sequence
	key := strconv.Itoa(id)
	ch := make(chan rpcReply, 1)
	s.calls[key] = ch
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.calls, key); s.mu.Unlock() }()
	if err := s.stream.Send(map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return err
	}
	decode := func(reply rpcReply) error {
		if len(reply.message.Error) > 0 {
			return fmt.Errorf("app-server RPC %s failed", method)
		}
		if result == nil {
			return nil
		}
		return json.Unmarshal(reply.message.Result, result)
	}
	select {
	case reply := <-ch:
		return decode(reply)
	case <-s.done:
		// The reader may have delivered this response just before EOF. Keep
		// confirmed IDs/results even when shutdown wins the outer select.
		select {
		case reply := <-ch:
			return decode(reply)
		default:
		}
		s.mu.Lock()
		err := s.terminal
		s.mu.Unlock()
		return err
	case <-ctx.Done():
		select {
		case reply := <-ch:
			return decode(reply)
		default:
		}
		return ctx.Err()
	}
}
func (s *SessionRPC) Initialize(ctx context.Context) error {
	if err := s.Call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "background_wave_harness", "version": "1"}, "capabilities": map[string]bool{"experimentalApi": true}}, nil); err != nil {
		return err
	}
	if err := s.stream.Send(map[string]any{"method": "initialized"}); err != nil {
		return err
	}
	return s.initializeAuthentication(ctx)
}
