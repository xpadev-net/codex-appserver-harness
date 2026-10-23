package kernelcli

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func webhookRequest(t *testing.T, client *http.Client, address, path, id string, body, secret []byte) int {
	t.Helper()
	r, err := http.NewRequest("POST", address+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-GitHub-Delivery", id)
	r.Header.Set("X-GitHub-Event", "issue_comment")
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	r.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func TestPRWebhookHTTPPersistsBeforeHintRejectsInvalidAndRetainsReplay(t *testing.T) {
	root := cliRoot(t)
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	secret := []byte("private fixture secret\n")
	wakes := atomic.Int64{}
	proof := make(chan error, 8)
	wake := func() {
		page, err := k.PRRawDeliveries(context.Background(), 17, 0, 1)
		if err == nil && len(page.Deliveries) != 1 {
			err = errors.New("wake before delivery commit")
		}
		proof <- err
		wakes.Add(1)
	}
	done := make(chan error, 1)
	go func() {
		done <- servePRWebhook(ctx, listener, storage.PRInbox{Store: k, RepositoryID: 17}, secret, wake)
	}()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	address := "http://" + listener.Addr().String()
	body := []byte(`{"repository":{"id":17},"issue":{"number":42,"pull_request":{}},"comment":{"id":7,"body":"change policy"}}`)
	for _, test := range []struct {
		path, id     string
		body, secret []byte
		status       int
	}{
		{"/wrong", "wrong-route", body, secret, 404},
		{"/github", "wrong-signature", body, []byte("wrong"), 401},
		{"/github", "foreign", []byte(`{"repository":{"id":18}}`), secret, 503},
		{"/github", "saved", body, secret, 202},
		{"/github", "saved", body, secret, 202},
		{"/github", "saved", append(append([]byte{}, body...), ' '), secret, 503},
	} {
		if status := webhookRequest(t, client, address, test.path, test.id, test.body, test.secret); status != test.status {
			t.Fatal(test.id, status)
		}
	}
	if wakes.Load() != 2 {
		t.Fatal("uncommitted input emitted wake", wakes.Load())
	}
	for i := 0; i < 2; i++ {
		if err := <-proof; err != nil {
			t.Fatal(err)
		}
	}
	page, err := k.PRRawDeliveries(context.Background(), 17, 0, 10)
	if err != nil || len(page.Deliveries) != 1 || !bytes.Equal(page.Deliveries[0].Payload, body) {
		t.Fatal(page, err)
	}
	var runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("ingress created operations", runs, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP shutdown hung")
	}
	if conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second); err == nil {
		conn.Close()
		t.Fatal("listener retained after shutdown")
	}
}

type stoppingPRInbox struct{ started, canceled, release chan struct{} }

func (in stoppingPRInbox) Receive(ctx context.Context, _, _ string, _ []byte) error {
	close(in.started)
	<-ctx.Done()
	close(in.canceled)
	<-in.release
	return ctx.Err()
}
func TestPRWebhookHTTPJoinsCanceledActiveHandler(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := stoppingPRInbox{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	done := make(chan error, 1)
	secret := []byte("test secret")
	go func() {
		done <- servePRWebhook(ctx, listener, in, secret, func() { t.Error("failed save emitted wake") })
	}()
	// Use a raw client request so the test's assertions stay on the test goroutine.
	request, _ := http.NewRequest("POST", "http://"+listener.Addr().String()+"/github", bytes.NewReader([]byte(`{}`)))
	request.Header.Set("X-GitHub-Delivery", "blocked")
	request.Header.Set("X-GitHub-Event", "pull_request")
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(`{}`))
	request.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, _ := client.Do(request)
		if response != nil {
			response.Body.Close()
		}
	}()
	select {
	case <-in.started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not started")
	}
	cancel()
	select {
	case <-in.canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("request context not canceled")
	}
	select {
	case err := <-done:
		t.Fatal("HTTP worker returned before handler", err)
	default:
	}
	close(in.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler not joined")
	}
	<-requestDone
}

func TestPRWebhookConfigBoundaryAndBindFailure(t *testing.T) {
	if worker, err := loadPRWebhook(""); worker != nil || err != nil {
		t.Fatal("implicit webhook", err)
	}
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("fixture secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, listen := range []string{"0.0.0.0:1234", "localhost:1234", "127.0.0.1:0", "[::]:1234", "garbage"} {
		if _, err := loadPRWebhook(operatorFile(t, PRWebhookConfig{1, 17, listen, secret})); err == nil {
			t.Fatal("unsafe ingress", listen)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	c := PRWebhookConfig{1, 17, listener.Addr().String(), secret}
	worker, err := loadPRWebhook(operatorFile(t, c))
	if err != nil {
		t.Fatal(err)
	}
	root := cliRoot(t)
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runDaemon(ctx, k, control.Runtime{Store: k, Factory: neverStartFactory{make(chan control.Launch, 1)}}, io.Discard, worker); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("bind failure not propagated", err)
	}
	if err := os.Chmod(secret, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPRWebhook(operatorFile(t, c)); err == nil {
		t.Fatal("public secret accepted")
	}
	if _, err := cliCall(t, "list", "--root", root, "--pr-webhook", "unused"); err == nil {
		t.Fatal("reader accepted webhook config")
	}
}
