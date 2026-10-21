package service

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// This opt-in probe runs only trusted commands, never a model turn. It checks the
// actual production worker environment and launcher without supplying a usable
// GitHub credential. Standard tests and CI do not access external services.
func TestLiveWorkerConnectionBoundary(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_CONNECTION") != "1" {
		t.Skip("explicit live connection probe only")
	}
	binary := os.Getenv("HARNESS_LIVE_CODEX_BINARY")
	output := os.Getenv("HARNESS_LIVE_EVIDENCE")
	if !filepath.IsAbs(binary) || !filepath.IsAbs(output) {
		t.Fatal("absolute binary and evidence paths are required")
	}
	c, _, _, _ := setup(t)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	c.Config.Launcher = []string{filepath.Clean(filepath.Join(cwd, "../../scripts/isolate-linux"))}
	c.Config.CodexBinary = binary
	c.Config.WorkerCodexHome = t.TempDir() // No controller Codex home is mounted.
	home := t.TempDir()
	w := domain.WorkItem{Worktree: c.Config.RepositoryPath}
	gitdir, err := workspace.Git(context.Background(), w.Worktree, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		t.Fatal(err)
	}
	env := (LiveAgents{Config: c.Config}).environment(w, home, strings.TrimSpace(gitdir))
	obs := map[string]any{"observed_at": time.Now().UTC(), "launcher": "scripts/isolate-linux", "model_turn": "not_attempted", "thread_resume": "not_attempted", "dedicated_codex_home": "empty probe home", "credential_values_recorded": false}
	defer func() {
		data, err := json.MarshalIndent(obs, "", "  ")
		if err != nil {
			t.Error("encode evidence")
			return
		}
		if err = os.WriteFile(output, append(data, '\n'), 0600); err != nil {
			t.Error("write evidence")
		}
	}()
	obs["proxy_variables_present"] = os.Getenv("HTTPS_PROXY") != "" || os.Getenv("HTTP_PROXY") != ""
	_, err = os.Stat("/etc/codex/network-policy.json")
	obs["network_policy_snapshot_present"] = err == nil
	if err == nil {
		data, err := os.ReadFile("/etc/codex/network-policy.json")
		if err != nil {
			t.Fatal("read network policy")
		}
		var policy struct {
			Version int `json:"version"`
		}
		if err = json.Unmarshal(data, &policy); err != nil {
			t.Fatal("decode network policy")
		}
		obs["network_policy_version"] = policy.Version
	}
	directAuth := false
	for _, entry := range env {
		for _, name := range []string{"GH_TOKEN=", "GITHUB_TOKEN=", "GH_ENTERPRISE_TOKEN=", "GITHUB_ENTERPRISE_TOKEN="} {
			if strings.HasPrefix(entry, name) {
				directAuth = true
			}
		}
	}
	obs["github_credential_environment_present"] = directAuth
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	run := func(environment []string, args ...string) (string, string) {
		command := exec.CommandContext(ctx, c.Config.Launcher[0], append(c.Config.Launcher[1:], args...)...)
		command.Env = environment
		command.Dir = w.Worktree
		var stderr strings.Builder
		command.Stderr = &stderr
		stdout, err := command.Output()
		if err == nil {
			return strings.TrimSpace(string(stdout)), "success"
		}
		status := "command_failed"
		if match := regexp.MustCompile(`HTTP ([0-9]{3})`).FindStringSubmatch(stderr.String()); len(match) > 1 {
			status = "http_" + match[1]
		}
		return "", status // Never publish raw HTTP bodies, diagnostics or credential values.
	}
	hidden := true
	for _, path := range forbiddenPaths(c.Config) {
		if _, status := run(env, "sh", "-c", `test ! -e "$1"`, "probe", path); status != "success" {
			hidden = false
		}
	}
	obs["controller_paths_hidden"] = hidden
	version, status := run(env, binary, "--version")
	obs["codex_launch_status"] = status
	obs["codex_version"] = version
	// gh requires a token to send API calls. This public noncredential only passes
	// that client preflight; an explicit header prevents relying on controller auth.
	// Any authenticated success exposes ambient/injected authority to the worker.
	ghEnv := append(append([]string{}, env...), "GH_TOKEN=unauthenticated-boundary-probe")
	user, userStatus := run(ghEnv, "gh", "api", "user", "-H", "Authorization:", "--jq", "true")
	repo, repoStatus := run(ghEnv, "gh", "api", "repos/xpadev-net/codex-appserver-harness", "-H", "Authorization:", "--jq", `has("permissions")`)
	authenticated := userStatus == "success" && user == "true"
	permissions := repoStatus == "success" && repo == "true"
	obs["user_api_status"] = userStatus
	obs["user_api_authenticated"] = authenticated
	obs["repository_api_status"] = repoStatus
	obs["repository_permissions_exposed"] = permissions
	args := append(append([]string{}, c.Config.Launcher[1:]...), binary, "app-server", "--listen", "stdio://", "--disable", "goals", "--disable", "multi_agent", "--disable", "multi_agent_v2")
	client, err := codex.StartCommand(ctx, c.Config.Launcher[0], args, env)
	initialized := false
	if err == nil {
		initialized = client.Initialize(ctx) == nil
		client.Close()
	}
	obs["isolated_app_server_initialized"] = initialized
	if !hidden || directAuth || authenticated || permissions {
		obs["credential_isolation"] = "unsupported"
		obs["model_turn_reason"] = "worker has controller paths or authenticated GitHub authority; no untrusted turn started"
		t.Fatal("worker credential isolation failed; sanitized evidence saved")
	}
	obs["credential_isolation"] = "unknown"
	obs["model_turn_reason"] = "read-only negative probes alone do not attest write-path isolation or dedicated model authentication"
	if !initialized || version != "codex-cli 0.163.0" {
		t.Fatal("isolated pinned app-server was not established; sanitized evidence saved")
	}
}
