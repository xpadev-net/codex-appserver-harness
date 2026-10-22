package kernelcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

func TestKernelCLIConcurrentInitializationPreservesSuccessfulConfiguration(t *testing.T) {
	root := filepath.Join(t.TempDir(), "kernel")
	errorsCh := make(chan error, 12)
	for i := 0; i < 12; i++ {
		go func() {
			var out, errOut bytes.Buffer
			errorsCh <- Run(context.Background(), []string{"init", "--root", root, "--model", "unavailable-model"}, &out, &errOut)
		}()
	}
	for i := 0; i < 12; i++ {
		if err := <-errorsCh; err != nil {
			t.Fatal("concurrent init failed", err)
		}
	}
	if _, err := cliCall(t, "list", "--root", root); err != nil {
		t.Fatal("successful init lost configuration", err)
	}
	if _, err := loadConfig(root); err != nil {
		t.Fatal(err)
	}
}
func TestKernelCLIOpenWaitsForInitializerAndReadNeverCreatesSchema(t *testing.T) {
	root := cliRoot(t)
	unlock, err := lockRoot(context.Background(), root, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = openKernel(ctx, root); !errors.Is(err, context.Canceled) {
		unlock()
		t.Fatal("opener ignored init lock/cancellation", err)
	}
	unlock()
	if err = os.Remove(filepath.Join(root, databaseName)); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, databaseName), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = cliCall(t, "list", "--root", root); err == nil {
		t.Fatal("read initialized an incomplete database")
	}
	info, err := os.Stat(filepath.Join(root, databaseName))
	if err != nil || info.Size() != 0 {
		t.Fatal("read changed empty database", info, err)
	}
	if _, err = cliCall(t, "init", "--root", root, "--model", "unavailable-model"); err == nil {
		t.Fatal("init silently reset previously initialized missing state")
	}
	partial := filepath.Join(t.TempDir(), "partial")
	if err = os.Mkdir(partial, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{SchemaVersion: ConfigVersion, SessionID: "main", Model: "unavailable-model", Limits: domain.DefaultKernelLimits()}
	if _, err = writeConfigExclusive(partial, cfg); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(partial, databaseName), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = cliCall(t, "list", "--root", partial); err == nil {
		t.Fatal("read initialized partial setup")
	}
	if _, err = cliCall(t, "init", "--root", partial, "--model", "unavailable-model"); err != nil {
		t.Fatal("explicit init could not resume unfinished first setup", err)
	}
	if _, err = cliCall(t, "list", "--root", partial); err != nil {
		t.Fatal(err)
	}
}

func TestKernelCLIConcurrentCreateDeduplicatesAndPreservesDistinctRequests(t *testing.T) {
	for _, same := range []bool{true, false} {
		t.Run(map[bool]string{true: "same_request", false: "distinct_requests"}[same], func(t *testing.T) {
			root := cliRoot(t)
			body := strings.Repeat("research evidence ", 120000)
			count := 6
			if !same {
				count = 2
			}
			files := make([]string, count)
			for i := range files {
				id := "shared"
				if !same {
					id = fmt.Sprint("request-", i)
				}
				files[i] = requestFile(t, id, body)
			}
			type result struct {
				raw []byte
				err error
			}
			results := make(chan result, 6)
			for _, file := range files {
				go func(file string) {
					var out, errOut bytes.Buffer
					err := Run(context.Background(), []string{"create", "--root", root, "--file", file}, &out, &errOut)
					results <- result{out.Bytes(), err}
				}(file)
			}
			ids := map[string]bool{}
			for range files {
				r := <-results
				if r.err != nil {
					t.Fatal("valid concurrent submission failed", r.err)
				}
				var w domain.Wave
				if err := json.Unmarshal(r.raw, &w); err != nil {
					t.Fatal(err)
				}
				ids[w.ID] = true
			}
			want := len(files)
			if same {
				want = 1
			}
			if len(ids) != want {
				t.Fatal("concurrent requests lost or duplicated", ids)
			}
			raw, err := cliCall(t, "list", "--root", root)
			if err != nil {
				t.Fatal(err)
			}
			var page storage.WaveSummaryPage
			if err = json.Unmarshal(raw, &page); err != nil || len(page.Waves) != want {
				t.Fatal(page, err)
			}
		})
	}
}

func TestKernelCLIRecoversOnlyKnownInterruptedConfigurationLinks(t *testing.T) {
	for _, known := range []bool{true, false} {
		t.Run(map[bool]string{true: "initializer_temporary", false: "unknown_link"}[known], func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "kernel")
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			cfg := Config{SchemaVersion: ConfigVersion, SessionID: "main", Model: "unavailable-model", Limits: domain.DefaultKernelLimits()}
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			name := ".kernel-config-interrupted"
			if !known {
				name = "unrecognized-copy"
			}
			held := filepath.Join(root, name)
			if err = os.WriteFile(held, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Link(held, filepath.Join(root, configName)); err != nil {
				t.Fatal(err)
			}
			_, err = cliCall(t, "init", "--root", root, "--model", "unavailable-model")
			if !known {
				if err == nil {
					t.Fatal("unknown hardlink accepted")
				}
				if _, err = os.Stat(held); err != nil {
					t.Fatal("unknown link removed", err)
				}
				return
			}
			if err != nil {
				t.Fatal("interrupted publication could not resume", err)
			}
			if _, err = os.Stat(held); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("initializer link not recovered", err)
			}
			if _, err = cliCall(t, "list", "--root", root); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestKernelCLIConfigurationPublicationHasOneLinkAndNeverOverwrites(t *testing.T) {
	root := filepath.Join(t.TempDir(), "kernel")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{SchemaVersion: ConfigVersion, SessionID: "main", Model: "unavailable-model", Limits: domain.DefaultKernelLimits()}
	if created, err := writeConfigExclusive(root, cfg); err != nil || !created {
		t.Fatal(created, err)
	}
	info, err := os.Stat(filepath.Join(root, configName))
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		t.Fatal("publication left hard links")
	}
	changed := cfg
	changed.Model = "changed"
	if _, err = writeConfigExclusive(root, changed); err == nil {
		t.Fatal("publication replaced existing config")
	}
	actual, err := loadConfig(root)
	if err != nil || actual.Model != cfg.Model {
		t.Fatal(actual, err)
	}
}
