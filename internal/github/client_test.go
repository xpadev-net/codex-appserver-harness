package github

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckRunPaginationShape(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf '%s' '[{\"check_runs\":[{\"id\":1,\"name\":\"unit\",\"head_sha\":\"head\",\"status\":\"completed\",\"conclusion\":\"success\",\"app\":{\"id\":9}}]},{\"check_runs\":[]}]'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	checks, err := (Client{"owner/repo", 1}).Checks(context.Background(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
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
