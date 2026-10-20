package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
)

type Inbox interface {
	Receive(context.Context, string, string, []byte) error
}

// Webhook persists verified deliveries before responding; processing is detached.
func Webhook(secret []byte, inbox Inbox) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if len(secret) == 0 || inbox == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		signature := strings.TrimPrefix(r.Header.Get("X-Hub-Signature-256"), "sha256=")
		supplied, err := hex.DecodeString(signature)
		mac := hmac.New(sha256.New, secret)
		mac.Write(body)
		if err != nil || !hmac.Equal(mac.Sum(nil), supplied) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		id := r.Header.Get("X-GitHub-Delivery")
		event := r.Header.Get("X-GitHub-Event")
		if id == "" || event == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err = inbox.Receive(r.Context(), id, event, body); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
}
