package kernelcli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func productRequestFile(t *testing.T) string {
	t.Helper()
	request := ProductDirectRequest{SchemaVersion: 1, RequestID: "direct-product", Request: "製品の動作を変更する", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "固定候補の検証が通る"}}, WorkItems: []storage.ProductWorkItemDefinition{{ID: "product-work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "product.json")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestProductCreateCLIAdmitsDirectSourceWithoutModelGitHubOrLegacyTables(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	root := cliRoot(t)
	file := productRequestFile(t)
	first, err := cliCall(t, "create-product", "--root", root, "--file", file)
	if err != nil {
		t.Fatal(err)
	}
	var wave domain.Wave
	if err := json.Unmarshal(first, &wave); err != nil || wave.Source != "direct" || wave.SourceID != "direct-product" || wave.Policy.Stage != "P" || !wave.Policy.Product || wave.Policy.Revision != 1 || wave.Policy.Limits != domain.DefaultKernelLimits() {
		t.Fatal(wave, err)
	}
	replayed, err := cliCall(t, "create-product", "--root", root, "--file", file)
	if err != nil || !bytes.Equal(first, replayed) {
		t.Fatal(string(replayed), err)
	}
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	var registration storage.ProductWorkItemRegistrationSnapshot
	if err := k.Record(context.Background(), "product_work_item_registration", "product-work", &registration); err != nil || registration.Value.WaveID != wave.ID || registration.Value.SourceID != wave.SourceID || registration.Value.Definition.RepositoryID != 17 {
		t.Fatal(registration, err)
	}
	for _, query := range []string{
		"SELECT count(*) FROM kernel_run_bindings",
		"SELECT count(*) FROM kernel_tool_operations",
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('work_items','issue_claims','operation_outbox')",
	} {
		var count int
		if err := k.DB.QueryRow(query).Scan(&count); err != nil || count != 0 {
			t.Fatal("source admission manufactured execution or legacy state", query, count, err)
		}
	}
	view, err := k.InspectWave(context.Background(), wave.ID, "", "", 64)
	if err != nil || view.Budget.MainTurns != 0 || len(view.Tasks) != 0 {
		t.Fatal(view, err)
	}
}

func TestProductCreateCLIRejectsUntrustedFilesAndPolicyInjection(t *testing.T) {
	for _, mode := range []string{"public", "symlink", "hardlink", "version", "policy", "source", "session", "candidate", "unknown_criterion", "empty_items", "kernel_command"} {
		t.Run(mode, func(t *testing.T) {
			root := cliRoot(t)
			file := productRequestFile(t)
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
			case "policy":
				input["policy"] = map[string]any{"tokens": 999999999}
			case "source":
				input["source"] = "github"
			case "session":
				input["orchestrator_session_id"] = "other-main"
			case "candidate":
				input["work_items"].([]any)[0].(map[string]any)["candidate"] = "claimed-success"
			case "unknown_criterion":
				input["work_items"].([]any)[0].(map[string]any)["criteria"] = []string{"unknown"}
			case "empty_items":
				input["work_items"] = []any{}
			}
			raw, err = json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, raw, 0600); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "public":
				if err := os.Chmod(file, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink", "hardlink":
				link := file + ".link"
				linkFn := os.Symlink
				if mode == "hardlink" {
					linkFn = os.Link
				}
				if err := linkFn(file, link); err != nil {
					t.Fatal(err)
				}
				file = link
			}
			command := "create-product"
			if mode == "kernel_command" {
				command = "create"
			}
			if _, err := cliCall(t, command, "--root", root, "--file", file); err == nil {
				t.Fatal("untrusted or injected request accepted")
			}
			k, _, err := openKernel(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			waves, err := k.Waves(context.Background())
			if err != nil || len(waves) != 0 {
				t.Fatal("rejected input left a Wave", waves, err)
			}
		})
	}
}

func TestProductCreateCLIRollsBackInvalidLaterWorkItemAndRetries(t *testing.T) {
	root := cliRoot(t)
	file := productRequestFile(t)
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var request ProductDirectRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	request.WorkItems = append(request.WorkItems, storage.ProductWorkItemDefinition{ID: "second-work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"unknown"}})
	changed, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cliCall(t, "create-product", "--root", root, "--file", file); err == nil || !strings.Contains(err.Error(), "criteria") {
		t.Fatal("invalid later definition was accepted", err)
	}
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	waves, err := k.Waves(context.Background())
	if err != nil || len(waves) != 0 {
		t.Fatal("failed CLI left visible Wave", waves, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('wave_work_item','product_work_item_registration','product_wave_admission')").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed CLI left ownership", count, err)
	}
	k.Close()
	request.WorkItems[1].Criteria = []string{"AC"}
	changed, err = json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cliCall(t, "create-product", "--root", root, "--file", file); err != nil {
		t.Fatal("rolled back request could not be retried", err)
	}
}

func TestProductCreateCLIConcurrentRetriesConverge(t *testing.T) {
	root := cliRoot(t)
	file := productRequestFile(t)
	var group sync.WaitGroup
	type result struct {
		data []byte
		err  error
	}
	results := make([]result, 4)
	for i := range results {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			var out, errOut bytes.Buffer
			results[i].err = Run(context.Background(), []string{"create-product", "--root", root, "--file", file}, &out, &errOut)
			results[i].data = out.Bytes()
		}(i)
	}
	group.Wait()
	for _, result := range results {
		if result.err != nil || !bytes.Equal(result.data, results[0].data) {
			t.Fatal("concurrent source retry diverged", string(result.data), result.err)
		}
	}
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='wave_created'").Scan(&count); err != nil || count != 1 {
		t.Fatal("concurrent retries duplicated initial event", count, err)
	}
}
