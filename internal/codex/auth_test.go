package codex

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

type tokenSourceFixture struct {
	mu             sync.Mutex
	token, account string
	failure        bool
}

func (f *tokenSourceFixture) Tokens(context.Context) (ChatGPTAuthTokens, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failure {
		return ChatGPTAuthTokens{}, errors.New("fixture-secret-source-error")
	}
	return ChatGPTAuthTokens{AccessToken: f.token, AccountID: f.account}, nil
}
func TestSessionAuthenticationUsesAccessOnlyAndKeepsReaderLive(t *testing.T) {
	ctx := sessionTestContext(t)
	stream := newSessionStream()
	provider := &tokenSourceFixture{token: "access-A", account: "account"}
	rpc, err := NewAuthenticatedSessionRPC(ctx, stream, nil, provider)
	if err != nil {
		t.Fatal(err)
	}
	defer rpc.Close()
	done := make(chan error, 1)
	go func() { done <- rpc.Initialize(ctx) }()
	init := <-stream.sent
	stream.incoming <- Message{ID: init.ID, Result: json.RawMessage(`{}`)}
	if m := <-stream.sent; m.Method != "initialized" {
		t.Fatal("missing initialized")
	}
	login := <-stream.sent
	if login.Method != "account/login/start" || strings.Contains(string(login.Params), "refresh") {
		t.Fatal("unexpected login contract")
	}
	var params struct{ Type, AccessToken, ChatgptAccountID string }
	if json.Unmarshal(login.Params, &params) != nil || params.Type != "chatgptAuthTokens" || params.AccessToken != "access-A" || params.ChatgptAccountID != "account" {
		t.Fatal("login did not use source access token")
	}
	stream.incoming <- Message{ID: login.ID, Result: json.RawMessage(`{"type":"chatgptAuthTokens"}`)}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	provider.token = "access-B"
	provider.mu.Unlock()
	stream.incoming <- Message{ID: json.RawMessage(`"refresh-1"`), Method: "account/chatgptAuthTokens/refresh", Params: json.RawMessage(`{"reason":"unauthorized","previousAccountId":"account"}`)}
	stream.incoming <- Message{Method: "item/started", Params: json.RawMessage(`{}`)}
	if m, err := rpc.Next(ctx); err != nil || m.Method != "item/started" {
		t.Fatal("auth blocked notifications", err)
	}
	reply := <-stream.sent
	var tokens ChatGPTAuthTokens
	if json.Unmarshal(reply.Result, &tokens) != nil || tokens.AccessToken != "access-B" || len(reply.Error) != 0 {
		t.Fatal("renewed access token was not delivered")
	}
	for i, body := range []string{`{"reason":"unauthorized","previousAccountId":"foreign"}`, `{"reason":"unauthorized","previousAccountId":"account"}`, `{"reason":"arbitrary"}`, `{"reason":"unauthorized","extra":"fixture-secret"}`} {
		stream.incoming <- Message{ID: json.RawMessage([]byte{byte('0' + i)}), Method: "account/chatgptAuthTokens/refresh", Params: json.RawMessage(body)}
		reply = <-stream.sent
		if len(reply.Error) == 0 || strings.Contains(string(reply.Error), "access-") || strings.Contains(string(reply.Error), "fixture-secret") {
			t.Fatal("unsafe auth refresh response")
		}
	}
	provider.mu.Lock()
	provider.token = "access-C"
	provider.account = "foreign"
	provider.mu.Unlock()
	stream.incoming <- Message{ID: json.RawMessage(`"switch"`), Method: "account/chatgptAuthTokens/refresh", Params: json.RawMessage(`{"reason":"unauthorized","previousAccountId":"account"}`)}
	if reply = <-stream.sent; len(reply.Error) == 0 {
		t.Fatal("source switched account")
	}
}
func TestSessionAuthenticationRedactsSourceFailures(t *testing.T) {
	ctx := sessionTestContext(t)
	p := &tokenSourceFixture{failure: true}
	if _, err := NewAuthenticatedSessionRPC(ctx, newSessionStream(), nil, p); err == nil || strings.Contains(err.Error(), "fixture-secret") {
		t.Fatal("source error was not redacted")
	}
}
