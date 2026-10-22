package control

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
)

var errSessionAuthentication = errors.New("session access-token source unavailable or changed account")

// sessionTokenSource reads the currently authorized session owner's tokens.
// It NEVER uses, copies, rotates or persists a refresh token. The source owner
// remains responsible for renewal, including when the harness runs unattended.
// Every process is bound to its first account and can only refresh to a changed
// access token published by that owner. Separate worker thread stores remain.
type sessionTokenSource struct{ path, account string }

func (s sessionTokenSource) Tokens(ctx context.Context) (codex.ChatGPTAuthTokens, error) {
	var result codex.ChatGPTAuthTokens
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	f, err := os.OpenFile(s.path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return result, errSessionAuthentication
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
		return result, errSessionAuthentication
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return result, errSessionAuthentication
	}
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return result, errSessionAuthentication
	}
	var auth struct {
		Mode   *string `json:"auth_mode"`
		Tokens struct {
			Access  string `json:"access_token"`
			Account string `json:"account_id"`
		} `json:"tokens"`
	}
	if json.Unmarshal(raw, &auth) != nil || s.account != "" && auth.Tokens.Account != s.account || auth.Mode != nil && *auth.Mode != "chatgpt" && *auth.Mode != "chatgptAuthTokens" {
		return result, errSessionAuthentication
	}
	// JWT metadata is not a new authentication authority: the native backend
	// validates the supplied access token. Checking expiry avoids sending stale
	// source tokens and extracting plan metadata adds no new account grants.
	pieces := strings.Split(auth.Tokens.Access, ".")
	if len(pieces) != 3 || len(auth.Tokens.Access) > 32<<10 || auth.Tokens.Account == "" || len(auth.Tokens.Account) > 1024 {
		return result, errSessionAuthentication
	}
	claims, err := base64.RawURLEncoding.DecodeString(pieces[1])
	if err != nil {
		return result, errSessionAuthentication
	}
	var payload struct {
		Expiry int64 `json:"exp"`
		Auth   struct {
			Account string  `json:"chatgpt_account_id"`
			Plan    *string `json:"chatgpt_plan_type"`
		} `json:"https://api.openai.com/auth"`
	}
	if json.Unmarshal(claims, &payload) != nil || payload.Expiry <= time.Now().Add(10*time.Second).Unix() || payload.Auth.Account != "" && payload.Auth.Account != auth.Tokens.Account || payload.Auth.Plan != nil && len(*payload.Auth.Plan) > 1024 {
		return result, errSessionAuthentication
	}
	result = codex.ChatGPTAuthTokens{AccessToken: auth.Tokens.Access, AccountID: auth.Tokens.Account, PlanType: payload.Auth.Plan}
	return result, nil
}

type authenticatedProcess struct {
	Process
	source codex.ChatGPTTokenProvider
}

func (p authenticatedProcess) TokenProvider() codex.ChatGPTTokenProvider { return p.source }
