package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
)

func issuePoolConfigFile(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(IssuePoolConfig{SchemaVersion: 1, SourcePolicy: product.IssueSourcePolicy{Repository: "owner/repo", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}, PriorityLabels: []string{"P0"}, Attempts: 1, EverySeconds: 60})
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "issue-pool.json")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestIssuePoolConfigRequiresPrivateExplicitBoundedSelection(t *testing.T) {
	root := cliRoot(t)
	k, main, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	worker, err := loadIssuePool("", k, main)
	if err != nil || worker != nil {
		t.Fatal("pool enabled without opt-in", err)
	}
	for _, mode := range []string{"valid", "public", "version", "attempts", "interval", "repository", "labels", "priorities", "session"} {
		t.Run(mode, func(t *testing.T) {
			file := issuePoolConfigFile(t)
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var input map[string]any
			if err := json.Unmarshal(raw, &input); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "version":
				input["schema_version"] = 2
			case "attempts":
				input["attempts"] = 33
			case "interval":
				input["every_seconds"] = 0
			case "repository":
				input["source_policy"].(map[string]any)["repository_id"] = 0
			case "labels":
				input["source_policy"].(map[string]any)["hold_label"] = "ready"
			case "priorities":
				input["priority_labels"] = []string{"P0", "P0"}
			case "session":
				input["orchestrator_session_id"] = "injected"
			}
			raw, err = json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "public" {
				if err := os.Chmod(file, 0644); err != nil {
					t.Fatal(err)
				}
			}
			worker, err := loadIssuePool(file, k, main)
			if (err == nil) != (mode == "valid") || (worker != nil) != (mode == "valid") {
				t.Fatal(mode, err)
			}
		})
	}
}

func TestIssuePoolFlagIsRunOnlyAndInvalidConfigFailsBeforeRuntime(t *testing.T) {
	root := cliRoot(t)
	file := issuePoolConfigFile(t)
	if _, err := cliCall(t, "list", "--root", root, "--issue-pool", file); err == nil || !strings.Contains(err.Error(), "does not apply") {
		t.Fatal("pool flag accepted on read command", err)
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), `"attempts":1`, `"attempts":0`, 1))
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cliCall(t, "run", "--root", root, "--issue-pool", file, "--runtime", "/missing-runtime.json"); err == nil || !strings.Contains(err.Error(), "attempts") {
		t.Fatal("runtime loaded before rejecting Issue pool config", err)
	}
}

func TestIssuePoolConfiguredWorkerCommitsBeforeWakeWithoutModelOrWrites(t *testing.T) {
	root := cliRoot(t)
	k, main, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	file := issuePoolConfigFile(t)
	bin := t.TempDir()
	issue := github.Issue{Number: 42, Title: "pooled product request", State: "open", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Body: "```harness-scope\n" + `{"revision":1,"original_request":"acceptance","criteria":[{"id":"AC","requirement":"expected behavior"}],"included":["AC"],"remaining":[],"previously_merged":[]}` + "\n```"}
	issue.Labels = append(issue.Labels, struct {
		Name string `json:"name"`
	}{Name: "ready"})
	raw, err := json.Marshal(issue)
	if err != nil {
		t.Fatal(err)
	}
	meta, item, pages, calls := filepath.Join(bin, "meta"), filepath.Join(bin, "issue"), filepath.Join(bin, "pages"), filepath.Join(bin, "calls")
	for path, data := range map[string][]byte{meta: []byte(`{"id":17,"full_name":"owner/repo"}`), item: raw, pages: []byte("[[" + string(raw) + "]]")} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$ISSUE_POOL_TEST_CALLS"
case "$*" in
 'api --method GET repos/owner/repo') /bin/cat "$ISSUE_POOL_TEST_META" ;;
 'api --method GET repos/owner/repo/issues/42') /bin/cat "$ISSUE_POOL_TEST_ISSUE" ;;
 'api --method GET --paginate --slurp repos/owner/repo/issues?state=open&per_page=100') /bin/cat "$ISSUE_POOL_TEST_PAGES" ;;
 *) exit 49 ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("ISSUE_POOL_TEST_CALLS", calls)
	t.Setenv("ISSUE_POOL_TEST_META", meta)
	t.Setenv("ISSUE_POOL_TEST_ISSUE", item)
	t.Setenv("ISSUE_POOL_TEST_PAGES", pages)
	worker, err := loadIssuePool(file, k, main)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	wakes := 0
	err = worker(ctx, k, func() {
		waves, err := k.Waves(context.Background())
		if err != nil || len(waves) != 1 || waves[0].SourceID != "r17-i42" {
			t.Fatal("wake preceded source commit", waves, err)
		}
		wakes++
		cancel()
	}, func(err error) { t.Errorf("unexpected pool failure: %v", err) })
	if !errors.Is(err, context.Canceled) || wakes != 1 {
		t.Fatal(err, wakes)
	}
	observed, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(observed), "\n") != 6 || strings.Contains(string(observed), "POST") || strings.Contains(string(observed), "PATCH") {
		t.Fatal("unexpected transport", string(observed), err)
	}
}
