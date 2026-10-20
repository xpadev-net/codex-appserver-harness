package github

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestWebhookSignatureAndDedup(t *testing.T) {
	s, _ := storage.Open(filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	secret := []byte("test-secret")
	handler := Webhook(secret, s)
	body := []byte(`{"action":"synchronize"}`)
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	for _, sig := range []string{"invalid", signature, signature} {
		r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
		r.Header.Set("X-Hub-Signature-256", sig)
		r.Header.Set("X-GitHub-Delivery", "delivery")
		r.Header.Set("X-GitHub-Event", "pull_request")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if sig == "invalid" && w.Code != 401 || sig == signature && w.Code != 202 {
			t.Fatal(w.Code)
		}
	}
	var count int
	s.DB.QueryRow("SELECT count(*) FROM raw_deliveries").Scan(&count)
	if count != 1 {
		t.Fatal("duplicate accepted", count)
	}
	pending, err := s.DeliveriesPending(context.Background())
	if err != nil || !pending {
		t.Fatal(pending, err)
	}
}
