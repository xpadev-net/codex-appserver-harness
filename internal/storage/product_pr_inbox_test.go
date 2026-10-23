package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
)

func prInboxFixture(t *testing.T) *KernelStore {
	t.Helper()
	k, err := OpenKernel(filepath.Join(t.TempDir(), "private", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.Close() })
	return k
}
func signedPRDelivery(handler http.Handler, secret []byte, id, event string, body []byte) int {
	r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	r.Header.Set("X-GitHub-Delivery", id)
	r.Header.Set("X-GitHub-Event", event)
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	r.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w.Code
}
func TestPRInboxSignatureAndRepositoryBoundaryCaptureBeforeAnyWaveOrBinding(t *testing.T) {
	k := prInboxFixture(t)
	ctx := context.Background()
	secret := []byte("fixture-secret")
	handler := github.Webhook(secret, PRInbox{Store: k, RepositoryID: 17})
	body := []byte("{ \n \"repository\":{\"id\":17},\"action\":\"deleted\",\"issue\":{\"number\":42,\"pull_request\":{}},\"comment\":{\"id\":11,\"body\":\"<change policy>\"} }")
	if status := signedPRDelivery(handler, []byte("wrong"), "delivery", "issue_comment", body); status != http.StatusUnauthorized {
		t.Fatal(status)
	}
	if page, err := k.PRRawDeliveries(ctx, 17, 0, 10); err != nil || len(page.Deliveries) != 0 {
		t.Fatal(page, err)
	}
	for i := 0; i < 2; i++ {
		if status := signedPRDelivery(handler, secret, "delivery", "issue_comment", body); status != http.StatusAccepted {
			t.Fatal(status)
		}
	}
	page, err := k.PRRawDeliveries(ctx, 17, 0, 10)
	if err != nil || len(page.Deliveries) != 1 || page.Deliveries[0].Sequence != 1 || !bytes.Equal(page.Deliveries[0].Payload, body) {
		t.Fatal(page, err)
	}
	if status := signedPRDelivery(handler, secret, "delivery", "issue_comment", append(append([]byte{}, body...), ' ')); status != http.StatusServiceUnavailable {
		t.Fatal("conflicting delivery ID accepted", status)
	}
	foreign := bytes.Replace(body, []byte(`"id":17`), []byte(`"id":18`), 1)
	if status := signedPRDelivery(handler, secret, "foreign", "issue_comment", foreign); status != http.StatusServiceUnavailable {
		t.Fatal(status)
	}
	for _, table := range []string{"kernel_waves", "kernel_notifications", "kernel_tool_operations"} {
		var count int
		if err = k.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("ingress created work or authority", table, count, err)
		}
	}
	coverage, err := k.PRIngressCoverage(ctx, 17)
	if err != nil || coverage.FirstSequence != 1 || coverage.HistoryBefore != "unavailable" || coverage.Coverage != "captured_deliveries_only" {
		t.Fatal(coverage, err)
	}
}
func TestPRInboxSequenceReplayPaginationAndExactBytesSurviveVacuumAndReopen(t *testing.T) {
	k := prInboxFixture(t)
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		repo := int64(17)
		if i == 2 {
			repo = 18
		}
		body := []byte(fmt.Sprintf(`{"repository":{"id":%d},"comment":{"body":"%s"}}`, repo, strings.Repeat("<", 100)))
		if err := (PRInbox{Store: k, RepositoryID: repo}).Receive(ctx, fmt.Sprint(i), "issue_comment", body); err != nil {
			t.Fatal(err)
		}
	}
	first, err := k.PRRawDeliveries(ctx, 17, 0, 2)
	if err != nil || len(first.Deliveries) != 2 || first.Deliveries[0].Sequence != 1 || first.Deliveries[1].Sequence != 3 || first.NextAfter != 3 || first.LastSequence != 3 {
		t.Fatal(first, err)
	}
	if _, err = k.DB.Exec("VACUUM"); err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	second, err := k.PRRawDeliveries(ctx, 17, first.NextAfter, 2)
	if err != nil || len(second.Deliveries) != 2 || second.LastSequence != 5 || second.NextAfter != 0 {
		t.Fatal(second, err)
	}
	if err = (PRInbox{Store: k, RepositoryID: 17}).Receive(ctx, "later", "reaction", []byte(`{"repository":{"id":17}}`)); err != nil {
		t.Fatal(err)
	}
	tail, err := k.PRRawDeliveries(ctx, 17, second.LastSequence, 2)
	if err != nil || len(tail.Deliveries) != 1 || tail.Deliveries[0].Sequence != 6 {
		t.Fatal(tail, err)
	}
	coverage, err := k.PRIngressCoverage(ctx, 18)
	if err != nil || coverage.FirstSequence != 2 {
		t.Fatal(coverage, err)
	}
}
func TestPRInboxSaveFailureRollsBackSequenceCoverageAndDelivery(t *testing.T) {
	k := prInboxFixture(t)
	ctx := context.Background()
	if _, err := k.DB.Exec(`CREATE TRIGGER fail_coverage BEFORE INSERT ON records WHEN NEW.kind='product_pr_ingress_coverage' BEGIN SELECT RAISE(FAIL,'coverage rollback'); END`); err != nil {
		t.Fatal(err)
	}
	in := PRInbox{Store: k, RepositoryID: 17}
	body := []byte(`{"repository":{"id":17}}`)
	if err := in.Receive(ctx, "first", "pull_request", body); err == nil {
		t.Fatal("partial raw ingress committed")
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_pr_%'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_coverage"); err != nil {
		t.Fatal(err)
	}
	if err := in.Receive(ctx, "first", "pull_request", body); err != nil {
		t.Fatal(err)
	}
	page, err := k.PRRawDeliveries(ctx, 17, 0, 2)
	if err != nil || page.Deliveries[0].Sequence != 1 {
		t.Fatal(page, err)
	}
}
func TestPRInboxPageByteBoundPreservesAllLargeDeliveries(t *testing.T) {
	k := prInboxFixture(t)
	ctx := context.Background()
	in := PRInbox{Store: k, RepositoryID: 17}
	body := []byte(`{"repository":{"id":17},"body":"` + strings.Repeat("<", 800<<10) + `"}`)
	for _, id := range []string{"a", "b", "c"} {
		if err := in.Receive(ctx, id, "issue_comment", body); err != nil {
			t.Fatal(err)
		}
	}
	var after int64
	seen := 0
	for {
		page, err := k.PRRawDeliveries(ctx, 17, after, 128)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Deliveries) != 1 {
			t.Fatal("page byte cap not enforced", len(page.Deliveries))
		}
		if !bytes.Equal(page.Deliveries[0].Payload, body) {
			t.Fatal("signed bytes changed")
		}
		seen++
		after = page.LastSequence
		if page.NextAfter == 0 {
			break
		}
	}
	if seen != 3 {
		t.Fatal(seen)
	}
}
