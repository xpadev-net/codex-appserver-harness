package github

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckRunPaginationShape(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf '%s' '[{\"check_runs\":[{\"id\":1,\"name\":\"unit\",\"head_sha\":\"head\",\"status\":\"completed\",\"conclusion\":\"success\",\"app\":{\"id\":9}}]},{\"check_runs\":[]}]'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	checks, err := (Client{Repository: "owner/repo", RepositoryID: 1}).Checks(context.Background(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil || len(checks) != 1 || checks[0].App.ID != 9 {
		t.Fatal(checks, err)
	}
}
func TestClosingText(t *testing.T) {
	for _, text := range []string{"fixes #123", "Fixes: #123", "Fixes:#123", "Closes owner/repo#9", "resolve https://github.com/a/b/issues/1"} {
		if CheckClosingText(text) == nil {
			t.Fatal(text)
		}
	}
	if CheckClosingText("Parent issue #123; remaining work persists") != nil {
		t.Fatal("nonclosing text rejected")
	}
}
func TestFindChildSearchesAllPages(t *testing.T) {
	dir := t.TempDir()
	script := `#!/bin/sh
case " $* " in
 *" --paginate --slurp "*) ;;
 *) exit 9 ;;
esac
printf '%s' '[[{"number":1,"body":"unrelated"}],[{"number":102,"body":"<!-- harness-child:parent:child -->"}]]'
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	number, err := (Client{Repository: "owner/repo", RepositoryID: 1}).FindChild(context.Background(), "harness-child:parent:child")
	if err != nil || number != 102 {
		t.Fatal("later-page child missing", number, err)
	}
}
func TestRequestReviewReconcilesLostCommentResponse(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HARNESS_TEST_SENT_FILE", filepath.Join(dir, "sent"))
	t.Setenv("HARNESS_TEST_POST_LOG", filepath.Join(dir, "posts"))
	script := `#!/bin/sh
case "$*" in
 *"--paginate --slurp"*)
  if test -f "$HARNESS_TEST_SENT_FILE"; then
   printf '%s' '[[{"id":501,"user":{"id":7},"body":"@codex review\n<!-- harness-review:round -->"}]]'
  else printf '%s' '[[]]'; fi ;;
 *"--method POST"*)
  printf 'post\n' >> "$HARNESS_TEST_POST_LOG"
  : > "$HARNESS_TEST_SENT_FILE"
  exit 1 ;;
 *) printf '%s' '{"id":1}' ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	client := Client{Repository: "owner/repo", RepositoryID: 1, BeforeWrite: func(context.Context, string, string) error { return nil }}
	if _, err := client.RequestReview(context.Background(), 10, "round", "@codex review", 7); err == nil {
		t.Fatal("lost response reported success")
	}
	id, err := client.RequestReview(context.Background(), 10, "round", "@codex review", 7)
	if err != nil || id != 501 {
		t.Fatal(id, err)
	}
	posts, err := os.ReadFile(filepath.Join(dir, "posts"))
	if err != nil || string(posts) != "post\n" {
		t.Fatal("duplicate review request", string(posts), err)
	}
}

func TestPrivateCheckDiagnosticsFetchesRedactedJobLogs(t *testing.T) {
	dir := t.TempDir()
	script := `#!/bin/sh
case "$*" in
 *"annotations?per_page=100"*) printf '%s' '[[]]' ;;
 *"actions/jobs/456/logs"*) printf 'assertion expected true, got false; credential=%s\n' "$GH_TOKEN" ;;
 *) exit 12 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("GH_TOKEN", "private-controller-test-token")
	client := Client{Repository: "Owner/Repo", RepositoryID: 1}
	text, err := client.CheckDiagnostics(context.Background(), Check{ID: 123, DetailsURL: "https://github.com/owner/repo/actions/runs/789/job/456"})
	if err != nil || !strings.Contains(text, "expected true") || strings.Contains(text, "private-controller-test-token") || !strings.Contains(text, "[REDACTED]") {
		t.Fatal("missing or unredacted diagnostic", err)
	}
}

func TestDiagnosticTailIsBounded(t *testing.T) {
	tail := &diagnosticTail{limit: 5}
	tail.Write([]byte("123"))
	tail.Write([]byte("4567"))
	if string(tail.data) != "34567" {
		t.Fatal(string(tail.data))
	}
	tail.Write([]byte("abcdefgh"))
	if string(tail.data) != "defgh" {
		t.Fatal(string(tail.data))
	}
}

func TestFailedLogResponseBodyIsNotDiagnosticEvidence(t *testing.T) {
	dir := t.TempDir()
	script := `#!/bin/sh
case "$*" in
 *"annotations?per_page=100"*) printf '%s' '[[]]' ;;
 *"actions/jobs/456/logs"*) printf '%s' '{"message":"Resource not accessible by integration","status":"403"}'; exit 1 ;;
 *) exit 12 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	text, err := (Client{Repository: "owner/repo", RepositoryID: 1}).CheckDiagnostics(context.Background(), Check{ID: 123, DetailsURL: "https://github.com/owner/repo/actions/runs/789/job/456"})
	if err == nil || text != "" {
		t.Fatal("failed HTTP response accepted as evidence", text, err)
	}
}
