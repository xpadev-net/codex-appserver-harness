package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func issueSourceConfigFile(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(IssueSourceObserverConfig{SchemaVersion: 1, SourcePolicy: product.IssueSourcePolicy{Repository: "owner/repo", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}, Attempts: 1, EverySeconds: 60})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "issue-source.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestIssueSourceObserverConfigRequiresPrivateBoundedOptIn(t *testing.T) {
	root := cliRoot(t)
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	worker, err := loadIssueSourceObserver("", k)
	if err != nil || worker != nil {
		t.Fatal("source observer enabled without opt-in", err)
	}
	for _, mode := range []string{"valid", "public", "version", "attempts", "interval", "repository", "labels", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			path := issueSourceConfigFile(t)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var value map[string]any
			if err := json.Unmarshal(raw, &value); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "version":
				value["schema_version"] = 2
			case "attempts":
				value["attempts"] = 33
			case "interval":
				value["every_seconds"] = 0
			case "repository":
				value["source_policy"].(map[string]any)["repository_id"] = 0
			case "labels":
				value["source_policy"].(map[string]any)["hold_label"] = "ready"
			case "unknown":
				value["allow_write"] = true
			}
			raw, err = json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "public" {
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			}
			worker, err := loadIssueSourceObserver(path, k)
			if (err == nil) != (mode == "valid") || (worker != nil) != (mode == "valid") {
				t.Fatal(mode, err)
			}
		})
	}
	path := issueSourceConfigFile(t)
	if _, err := cliCall(t, "list", "--root", root, "--issue-source-observer", path); err == nil || !strings.Contains(err.Error(), "does not apply") {
		t.Fatal("observer flag applied outside run", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), `"attempts":1`, `"attempts":0`, 1))
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cliCall(t, "run", "--root", root, "--issue-source-observer", path, "--runtime", "/missing-runtime.json"); err == nil || !strings.Contains(err.Error(), "attempts") {
		t.Fatal("runtime loaded before invalid observer config", err)
	}
}
func TestIssueSourceObserverConfiguredGETWorkerCommitsBeforeHint(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		t.Run(fmt.Sprint("pipeline=", pipeline), func(t *testing.T) {
			root := cliRoot(t)
			k, main, err := openKernel(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			path := issueSourceConfigFile(t)
			policy := product.IssueSourcePolicy{Repository: "owner/repo", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}
			w, err := k.CreateProductWave(context.Background(), storage.ProductWaveRequest{SourcePolicyDigest: policy.Digest(), Wave: storage.WaveRequest{OrchestratorSessionID: main.SessionID, Source: "github", SourceID: "r17-i42", OriginalRequest: "source title\n\nsource body", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "preserve expected behavior"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: main.Limits}}, WorkItems: []storage.ProductWorkItemDefinition{{ID: "r17-i42", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}})
			if err != nil {
				t.Fatal(err)
			}
			bin := t.TempDir()
			meta, item, calls := filepath.Join(bin, "meta"), filepath.Join(bin, "issue"), filepath.Join(bin, "calls")
			if err := os.WriteFile(meta, []byte(`{"id":17,"full_name":"owner/repo"}`), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(item, []byte(`{"number":42,"title":"source title","body":"source body","state":"open","labels":[{"name":"ready"},{"name":"hold"}],"updated_at":"2026-10-24T00:00:00Z"}`), 0600); err != nil {
				t.Fatal(err)
			}
			script := `#!/bin/sh
printf '%s\n' "$*" >> "$SOURCE_OBSERVER_TEST_CALLS"
case "$*" in
 'api --method GET repos/owner/repo') /bin/cat "$SOURCE_OBSERVER_TEST_META" ;;
 'api --method GET repos/owner/repo/issues/42') /bin/cat "$SOURCE_OBSERVER_TEST_ISSUE" ;;
 *) exit 49 ;;
esac
`
			if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin)
			t.Setenv("SOURCE_OBSERVER_TEST_CALLS", calls)
			t.Setenv("SOURCE_OBSERVER_TEST_META", meta)
			t.Setenv("SOURCE_OBSERVER_TEST_ISSUE", item)
			worker, err := loadIssueSourceObserver(path, k)
			if pipeline {
				config := publicationConfigFixture(t)
				config.Publication.SourcePolicies = []product.IssueSourcePolicy{policy}
				worker, _, err = loadProductExecutor(operatorFile(t, config), k, control.IsolatedConfig{})
			}
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			hints := 0
			err = worker(ctx, k, func() {
				current, err := k.Wave(context.Background(), w.ID)
				if err != nil || current.SourceHold == nil || current.SourceHold.Reason != "hold" {
					t.Fatal("hint before source commit", current, err)
				}
				hints++
				cancel()
			}, func(err error) { t.Errorf("unexpected source worker failure: %v", err) })
			if !errors.Is(err, context.Canceled) || hints != 1 {
				t.Fatal(err, hints)
			}
			raw, err := os.ReadFile(calls)
			if err != nil || strings.Count(string(raw), "\n") != 4 || strings.Contains(string(raw), "POST") {
				t.Fatal("unexpected source transport", string(raw), err)
			}
		})
	}
}
