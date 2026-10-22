package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"
)

// ChatGPTAuthTokens is the pinned v0.163.0 external-auth DTO. Only short-lived
// access tokens cross this boundary. Refresh tokens stay with the source owner.
type ChatGPTAuthTokens struct {
	AccessToken string  `json:"accessToken"`
	AccountID   string  `json:"chatgptAccountId"`
	PlanType    *string `json:"chatgptPlanType"`
}

// Providers must honor context cancellation and return immutable token values.
type ChatGPTTokenProvider interface {
	Tokens(context.Context) (ChatGPTAuthTokens, error)
}
type sessionAuthentication struct {
	provider ChatGPTTokenProvider
	mu       sync.Mutex
	current  ChatGPTAuthTokens
}

func validAuthTokens(t ChatGPTAuthTokens) bool {
	return t.AccessToken != "" && len(t.AccessToken) <= 32<<10 && t.AccountID != "" && len(t.AccountID) <= 1024 && (t.PlanType == nil || len(*t.PlanType) <= 1024)
}

// NewAuthenticatedSessionRPC freezes authentication before starting the reader.
// Unsupported server requests retain the normal fail-closed behavior. The
// unstable external-auth API is pinned alongside the experimental tool contract.
func NewAuthenticatedSessionRPC(ctx context.Context, stream Stream, handler ToolHandler, provider ChatGPTTokenProvider) (*SessionRPC, error) {
	if provider == nil {
		return nil, errors.New("authentication provider required")
	}
	tokens, err := provider.Tokens(ctx)
	if err != nil || !validAuthTokens(tokens) {
		return nil, errors.New("session authentication unavailable")
	}
	return newSessionRPC(ctx, stream, handler, &sessionAuthentication{provider: provider, current: tokens}), nil
}
func (s *SessionRPC) initializeAuthentication(ctx context.Context) error {
	if s.auth == nil {
		return nil
	}
	s.auth.mu.Lock()
	tokens := s.auth.current
	s.auth.mu.Unlock()
	params := map[string]any{"type": "chatgptAuthTokens", "accessToken": tokens.AccessToken, "chatgptAccountId": tokens.AccountID, "chatgptPlanType": tokens.PlanType}
	var reply struct {
		Type string `json:"type"`
	}
	if err := s.Call(ctx, "account/login/start", params, &reply); err != nil || reply.Type != "chatgptAuthTokens" {
		return errors.New("session access-token login failed")
	}
	return nil
}
func (s *SessionRPC) respondAuthentication(m Message) {
	// Native requests have a 10s deadline. Bound the source and do not let a bad
	// request switch accounts or turn this interface into a general RPC gateway.
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	var params struct {
		Reason            string  `json:"reason"`
		PreviousAccountID *string `json:"previousAccountId"`
	}
	valid := len(m.Params) > 0 && len(m.Params) <= 4096
	if valid {
		decoder := json.NewDecoder(bytes.NewReader(m.Params))
		decoder.DisallowUnknownFields()
		valid = decoder.Decode(&params) == nil && decoder.Decode(new(any)) == io.EOF && params.Reason == "unauthorized"
	}
	s.auth.mu.Lock()
	current := s.auth.current
	if params.PreviousAccountID != nil && *params.PreviousAccountID != current.AccountID {
		valid = false
	}
	var tokens ChatGPTAuthTokens
	var err error
	if valid {
		tokens, err = s.auth.provider.Tokens(ctx)
	}
	if !valid || err != nil || !validAuthTokens(tokens) || tokens.AccountID != current.AccountID || tokens.AccessToken == current.AccessToken {
		s.auth.mu.Unlock()
		if err = s.stream.Send(map[string]any{"id": m.ID, "error": map[string]any{"code": -32000, "message": "Controller authentication source has no renewed access token for this account."}}); err != nil {
			s.finish(err)
		}
		return
	}
	s.auth.current = tokens
	s.auth.mu.Unlock()
	if err = s.stream.Send(map[string]any{"id": m.ID, "result": tokens}); err != nil {
		s.finish(err)
	}
}
