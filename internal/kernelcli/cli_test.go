package kernelcli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func cliCall(t *testing.T, args ...string) ([]byte, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := Run(context.Background(), args, &out, &errOut)
	return out.Bytes(), err
}
func cliRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "kernel")
	if _, err := cliCall(t, "init", "--root", root, "--model", "unavailable-model"); err != nil {
		t.Fatal(err)
	}
	return root
}
func requestFile(t *testing.T, id, body string) string {
	t.Helper()
	r := DirectRequest{SchemaVersion: 1, RequestID: id, Request: body, Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "explain the evidence"}}}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "request.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestKernelCLIInitializesAndCreatesDirectWaveWithoutModelOrGitHub(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	root := cliRoot(t)
	file := requestFile(t, "direct-one", "依頼を調査して説明する")
	first, err := cliCall(t, "create", "--root", root, "--file", file)
	if err != nil {
		t.Fatal(err)
	}
	var wave domain.Wave
	if err = json.Unmarshal(first, &wave); err != nil || wave.Source != "direct" || wave.Policy.Stage != "K" || wave.Policy.Product || wave.OriginalRequest != "依頼を調査して説明する" {
		t.Fatal(wave, err)
	}
	replay, err := cliCall(t, "create", "--root", root, "--file", file)
	if err != nil || !bytes.Equal(first, replay) {
		t.Fatal(string(replay), err)
	}
	raw, err := cliCall(t, "inspect", "--root", root, "--id", wave.ID)
	if err != nil {
		t.Fatal(err)
	}
	var inspection storage.WaveInspection
	if err = json.Unmarshal(raw, &inspection); err != nil || inspection.Wave.ID != wave.ID || inspection.Main.Binding.Model != "unavailable-model" || inspection.Budget.MainTurns != 0 || len(inspection.Tasks) != 0 {
		t.Fatal(inspection, err)
	}
	if _, err = cliCall(t, "init", "--root", root, "--model", "unavailable-model"); err != nil {
		t.Fatal("idempotent init failed", err)
	}
	if _, err = cliCall(t, "init", "--root", root, "--model", "changed-model"); err == nil {
		t.Fatal("init overwrote configured model")
	}
	info, err := os.Stat(filepath.Join(root, configName))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
}
func TestKernelCLIListPagesOmitLargeRequestsAndPreserveControllerState(t *testing.T) {
	root := cliRoot(t)
	body := strings.Repeat("large original request ", 60000)
	for _, id := range []string{"first", "second", "third"} {
		file := requestFile(t, id, body)
		if _, err := cliCall(t, "create", "--root", root, "--file", file); err != nil {
			t.Fatal(err)
		}
	}
	after := ""
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		raw, err := cliCall(t, "list", "--root", root, "--limit", "1", "--after", after)
		if err != nil {
			t.Fatal(err)
		}
		var page storage.WaveSummaryPage
		if err = json.Unmarshal(raw, &page); err != nil || len(page.Waves) != 1 || seen[page.Waves[0].ID] || page.Waves[0].OriginalRequestBytes != len(body) || page.Waves[0].MainTurns != 0 {
			t.Fatal(page, err)
		}
		if bytes.Contains(raw, []byte("large original request")) {
			t.Fatal("list copied original body")
		}
		seen[page.Waves[0].ID] = true
		after = page.NextAfter
	}
	if after != "" || len(seen) != 3 {
		t.Fatal(after, seen)
	}
}
func TestKernelCLIRejectsPolicyInjectionUnsafeConfigurationAndAccidentalInitialization(t *testing.T) {
	root := cliRoot(t)
	injected := filepath.Join(t.TempDir(), "injected.json")
	if err := os.WriteFile(injected, []byte(`{"schema_version":1,"request_id":"injected","request":"work","criteria":[{"id":"AC","requirement":"evidence"}],"policy":{"stage":"P"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cliCall(t, "create", "--root", root, "--file", injected); err == nil {
		t.Fatal("request file changed trusted policy")
	}
	for _, mode := range []string{"public", "symlink", "unknown_version"} {
		t.Run(mode, func(t *testing.T) {
			r := cliRoot(t)
			path := filepath.Join(r, configName)
			switch mode {
			case "public":
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				held := path + "-held"
				if err := os.Rename(path, held); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(held, path); err != nil {
					t.Fatal(err)
				}
			case "unknown_version":
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				raw = bytes.Replace(raw, []byte(`"schema_version": 2`), []byte(`"schema_version": 1`), 1)
				if err = os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := cliCall(t, "list", "--root", r); err == nil {
				t.Fatal("unsafe/foreign config accepted")
			}
		})
	}
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := cliCall(t, "list", "--root", missing); err == nil {
		t.Fatal("read initialized missing root")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("read created state", err)
	}
	existing := filepath.Join(t.TempDir(), "existing")
	k, err := storage.OpenKernel(filepath.Join(existing, databaseName))
	if err != nil {
		t.Fatal(err)
	}
	k.Close()
	if _, err = cliCall(t, "init", "--root", existing, "--model", "unavailable-model"); err == nil {
		t.Fatal("init adopted an unconfigured database")
	}
}
