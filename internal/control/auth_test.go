package control

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func sessionSourceFixture(t *testing.T, account string, expiry int64) string {
	t.Helper()
	claims, _ := json.Marshal(map[string]any{"exp": expiry, "https://api.openai.com/auth": map[string]any{"chatgpt_account_id": account, "chatgpt_plan_type": "pro"}})
	return "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"
}
func TestSessionTokenSourceReadsOnlyCurrentAccessTokens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	access := sessionSourceFixture(t, "account", time.Now().Add(time.Hour).Unix())
	write := func(token string) {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"auth_mode": "chatgpt", "tokens": map[string]any{"access_token": token, "account_id": "account", "refresh_token": "fixture-never-share"}})
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(access)
	source := sessionTokenSource{path: path}
	tokens, err := source.Tokens(context.Background())
	if err != nil || tokens.AccessToken != access || tokens.AccountID != "account" {
		t.Fatal("source token not loaded", err)
	}
	exported, _ := json.Marshal(tokens)
	if strings.Contains(string(exported), "fixture-never-share") || strings.Contains(string(exported), "refresh_token") || string(exported) == "" {
		t.Fatal("empty DTO")
	}
	// Updating the source does not require copying credentials into worker homes.
	next := sessionSourceFixture(t, "account", time.Now().Add(2*time.Hour).Unix())
	write(next)
	tokens, err = source.Tokens(context.Background())
	if err != nil || tokens.AccessToken != next {
		t.Fatal("source renewal was not observed", err)
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = source.Tokens(context.Background()); err == nil {
		t.Fatal("public source accepted")
	}
}
func TestSessionTokenSourceRejectsExpiredMismatchedAndRedirectedFiles(t *testing.T) {
	for _, kind := range []string{"expired", "account", "bound-account", "symlink", "fifo", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auth.json")
			token := sessionSourceFixture(t, "account", time.Now().Add(time.Hour).Unix())
			account := "account"
			if kind == "expired" {
				token = sessionSourceFixture(t, "account", time.Now().Add(-time.Minute).Unix())
			}
			if kind == "account" {
				account = "foreign"
			}
			if kind == "bound-account" {
				account = "foreign"
				token = sessionSourceFixture(t, "foreign", time.Now().Add(time.Hour).Unix())
			}
			raw, _ := json.Marshal(map[string]any{"tokens": map[string]any{"access_token": token, "account_id": account}})
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if kind == "symlink" {
				target := path + ".target"
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "oversize" {
				if err := os.WriteFile(path, make([]byte, (1<<20)+1), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "fifo" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			}
			source := sessionTokenSource{path: path}
			if kind == "bound-account" {
				source.account = "account"
			}
			if _, err := source.Tokens(context.Background()); err == nil {
				t.Fatal("unsafe source accepted")
			}
		})
	}
}
