package kernelcli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
)

func issueImportFile(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(IssueImportRequest{SchemaVersion: 1, IssueNumber: 42, SourcePolicy: product.IssueSourcePolicy{Repository: "owner/repo", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}})
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "issue-import.json")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestIssueImportCLIUsesOnlyAuthenticatedReadTransportAndCommonWave(t *testing.T) {
	root := cliRoot(t)
	file := issueImportFile(t)
	bin := t.TempDir()
	meta := filepath.Join(bin, "metadata.json")
	issueFile := filepath.Join(bin, "issue.json")
	calls := filepath.Join(bin, "calls")
	if err := os.WriteFile(meta, []byte(`{"id":17,"full_name":"owner/repo"}`), 0600); err != nil {
		t.Fatal(err)
	}
	issue := github.Issue{Number: 42, Title: "product from Issue", State: "open", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Body: "implement request\n```harness-scope\n" + `{"revision":1,"original_request":"acceptance source","criteria":[{"id":"AC","requirement":"expected behavior"}],"included":["AC"],"remaining":[],"previously_merged":[]}` + "\n```"}
	issue.Labels = append(issue.Labels, struct {
		Name string `json:"name"`
	}{Name: "ready"})
	raw, err := json.Marshal(issue)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(issueFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$ISSUE_IMPORT_TEST_CALLS"
case "$*" in
  'api --method GET repos/owner/repo') /bin/cat "$ISSUE_IMPORT_TEST_META" ;;
  'api --method GET repos/owner/repo/issues/42') /bin/cat "$ISSUE_IMPORT_TEST_ISSUE" ;;
  *) exit 49 ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("ISSUE_IMPORT_TEST_CALLS", calls)
	t.Setenv("ISSUE_IMPORT_TEST_META", meta)
	t.Setenv("ISSUE_IMPORT_TEST_ISSUE", issueFile)
	first, err := cliCall(t, "import-issue", "--root", root, "--file", file)
	if err != nil {
		t.Fatal(err)
	}
	var wave domain.Wave
	if err := json.Unmarshal(first, &wave); err != nil || wave.Source != "github" || wave.SourceID != "r17-i42" || wave.Policy.Stage != "P" || !wave.Policy.Product || wave.Policy.Limits != domain.DefaultKernelLimits() {
		t.Fatal(wave, err)
	}
	replay, err := cliCall(t, "import-issue", "--root", root, "--file", file)
	if err != nil || !bytes.Equal(first, replay) {
		t.Fatal(string(replay), err)
	}
	observed, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(observed), "api --method GET repos/owner/repo\n") != 4 || strings.Count(string(observed), "api --method GET repos/owner/repo/issues/42\n") != 4 || len(strings.Split(strings.TrimSpace(string(observed)), "\n")) != 8 {
		t.Fatal("unexpected source transport", string(observed), err)
	}
	issue.Body += "\nchanged source"
	issue.UpdatedAt = issue.UpdatedAt.Add(time.Second)
	raw, err = json.Marshal(issue)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(issueFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cliCall(t, "import-issue", "--root", root, "--file", file); err == nil {
		t.Fatal("Issue edit silently revised saved scope")
	}
}

func TestIssueImportCLIRejectsUntrustedPolicyAndMissingTransport(t *testing.T) {
	for _, mode := range []string{"public", "policy_injection", "invalid_labels", "bad_version", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			root := cliRoot(t)
			file := issueImportFile(t)
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var request map[string]any
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "policy_injection":
				request["policy"] = map[string]any{"tokens": 999999999}
			case "invalid_labels":
				request["source_policy"].(map[string]any)["hold_label"] = "ready"
			case "bad_version":
				request["schema_version"] = 2
			}
			raw, err = json.Marshal(request)
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
			t.Setenv("PATH", t.TempDir())
			_, err = cliCall(t, "import-issue", "--root", root, "--file", file)
			if err == nil {
				t.Fatal("unsafe or unavailable source accepted")
			}
			reason := map[string]string{"public": "private same-user", "policy_injection": "unknown field", "invalid_labels": "distinct", "bad_version": "version 1", "unavailable": "gh"}[mode]
			if !strings.Contains(err.Error(), reason) {
				t.Fatal("expected source rejection was not reached", mode, err)
			}
		})
	}
}
