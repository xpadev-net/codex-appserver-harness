package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
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
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Root: t.TempDir(), RepositoryPath: filepath.Clean(filepath.Join(cwd, "../..")), Launcher: []string{filepath.Clean(filepath.Join(cwd, "../../scripts/isolate-linux"))}, CodexBinary: binary, WorkerCodexHome: t.TempDir()}
	workspaceKind := "current repository checkout"
	if checkout := os.Getenv("HARNESS_LIVE_CHECKOUT"); checkout != "" {
		if !filepath.IsAbs(checkout) {
			t.Fatal("absolute probe checkout required")
		}
		cfg.RepositoryPath = filepath.Clean(checkout)
		workspaceKind = "operator-selected probe checkout"
	}
	if err = os.WriteFile(filepath.Join(cfg.Root, "state.db"), []byte("private controller probe sentinel"), 0600); err != nil {
		t.Fatal("create controller sentinel")
	} // No controller Codex home is mounted.
	home := t.TempDir()
	w := domain.WorkItem{Worktree: cfg.RepositoryPath}
	gitdir, err := workspace.Git(context.Background(), w.Worktree, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		t.Fatal(err)
	}
	env := (LiveAgents{Config: cfg}).environment(w, home, strings.TrimSpace(gitdir))
	obs := map[string]any{"observed_at": time.Now().UTC(), "launcher": "scripts/isolate-linux", "model_turn": "not_attempted", "thread_resume": "not_attempted", "dedicated_codex_home": "empty probe home", "credential_values_recorded": false, "workspace": workspaceKind, "credential_isolation": "unknown", "probe_completed": false, "model_turn_reason": "probe incomplete"}
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
		command := exec.CommandContext(ctx, cfg.Launcher[0], append(cfg.Launcher[1:], args...)...)
		command.Env = environment
		command.Dir = w.Worktree
		var stderr strings.Builder
		command.Stderr = &stderr
		stdout, err := command.Output()
		if err == nil {
			return strings.TrimSpace(string(stdout)), "success"
		}
		status := "command_failed"
		if exit, ok := err.(*exec.ExitError); ok {
			status = "exit_" + strconv.Itoa(exit.ExitCode())
		}
		if match := regexp.MustCompile(`HTTP ([0-9]{3})`).FindStringSubmatch(stderr.String()); len(match) > 1 {
			status = "http_" + match[1]
		}
		return "", status // Never publish raw HTTP bodies, diagnostics or credential values.
	}
	hidden := true
	for _, path := range forbiddenPaths(cfg) {
		if _, status := run(env, "sh", "-c", `test ! -e "$1"`, "probe", path); status != "success" {
			hidden = false
		}
	}
	obs["controller_paths_hidden"] = hidden
	// Query names only, without logging configuration keys or values. The real
	// common Git directory is mounted as in production; no synthetic clone is used.
	_, gitStatus := run(env, "git", "rev-parse", "--is-inside-work-tree")
	authNames, authStatus := run(env, "git", "config", "--includes", "--name-only", "--get-regexp", `(extraheader|^credential\.|^core\.(sshcommand|askpass|gitproxy)$|^http\..*(proxy|proxyauthmethod|cookiefile|sslkey|sslcert)$|^remote\..*\.proxy$|https?://[^/]*@)`)
	obs["git_repository_probe_status"] = gitStatus
	obs["git_auth_config_query_status"] = authStatus
	gitAuth := authStatus == "success" && authNames != ""
	obs["git_auth_configuration_present"] = gitAuth
	if gitStatus != "success" || (authStatus != "success" && authStatus != "exit_1") {
		obs["credential_isolation"] = "unknown"
		obs["model_turn_reason"] = "mounted Git metadata could not be inspected"
		t.Fatal("Git credential reachability is unresolved; sanitized evidence saved")
	}
	remoteConfig, remoteStatus := run(env, "git", "config", "--includes", "--null", "--get-regexp", `^(remote\..*\.(url|pushurl)|url\..*\.(insteadof|pushinsteadof))$`)
	obs["git_remote_config_query_status"] = remoteStatus
	remoteAuth, parseErr := gitURLCredentials(remoteConfig)
	if (remoteStatus != "success" && remoteStatus != "exit_1") || parseErr != nil {
		obs["model_turn_reason"] = "effective Git URL metadata could not be inspected"
		t.Fatal("Git URL credential reachability is unresolved; sanitized evidence saved")
	}
	obs["git_remote_credentials_present"] = remoteAuth
	gitAuth = gitAuth || remoteAuth
	obs["git_auth_configuration_present"] = gitAuth
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
	writeExposed := false
	writeStatus := "not_attempted"
	obs["write_api_status"] = writeStatus
	if canary := os.Getenv("HARNESS_LIVE_WRITE_CANARY"); canary != "" {
		number, err := strconv.Atoi(canary)
		if err != nil || number < 1 {
			t.Fatal("positive canary issue/PR number required")
		}
		title := os.Getenv("HARNESS_LIVE_CANARY_TITLE")
		if title == "" {
			t.Fatal("existing canary title required")
		}
		endpoint := "repos/xpadev-net/codex-appserver-harness/issues/" + strconv.Itoa(number)
		current, err := exec.CommandContext(ctx, "gh", "api", endpoint, "--jq", ".title").Output()
		if err != nil || strings.TrimSpace(string(current)) != title {
			t.Fatal("canary title must match existing title; no request sent")
		}
		_, writeStatus = run(ghEnv, "gh", "api", endpoint, "--method", "PATCH", "-H", "Authorization:", "-f", "title="+title, "--jq", "true")
		writeExposed = writeStatus == "success"
		obs["write_api_status"] = writeStatus
		obs["write_access_exposed"] = writeExposed
		obs["write_canary_number"] = number
	}
	args := append(append([]string{}, cfg.Launcher[1:]...), binary, "app-server", "--listen", "stdio://", "--disable", "goals", "--disable", "multi_agent", "--disable", "multi_agent_v2")
	client, err := codex.StartCommand(ctx, cfg.Launcher[0], args, env)
	initialized := false
	if err == nil {
		initialized = client.Initialize(ctx) == nil
		client.Close()
	}
	obs["isolated_app_server_initialized"] = initialized
	if !hidden || directAuth || gitAuth || authenticated || permissions || writeExposed {
		obs["credential_isolation"] = "unsupported"
		obs["probe_completed"] = true
		obs["model_turn_reason"] = "worker has controller paths, Git authentication configuration or authenticated GitHub authority; no untrusted turn started"
		t.Fatal("worker credential isolation failed; sanitized evidence saved")
	}
	repoDenied := repoStatus == "http_401" || (repoStatus == "success" && repo == "false")
	if userStatus != "http_401" || !repoDenied || (writeStatus != "not_attempted" && writeStatus != "http_401") {
		obs["model_turn_reason"] = "GitHub API boundary probes incomplete; command or transport failure is not absence of authority"
		t.Fatal("GitHub API boundary probes are inconclusive; sanitized evidence saved")
	}
	obs["credential_isolation"] = "unknown"
	obs["model_turn_reason"] = "negative probes do not attest the unconfigured authenticated worker/model deployment"
	if !initialized || version != "codex-cli 0.163.0" {
		t.Fatal("isolated pinned app-server was not established; sanitized evidence saved")
	}
	obs["probe_completed"] = true
}

// git config --null --get-regexp separates each key/value with a newline and
// terminates each record with NUL. Values (and URL-bearing keys) remain private.
func gitURLCredentials(data string) (bool, error) {
	for _, record := range strings.Split(data, "\x00") {
		if record == "" {
			continue
		}
		key, value, ok := strings.Cut(record, "\n")
		if !ok {
			return false, errors.New("invalid Git URL record")
		}
		lower := strings.ToLower(key)
		values := []string{value}
		if strings.HasPrefix(lower, "url.") {
			suffix := ".insteadof"
			if strings.HasSuffix(lower, ".pushinsteadof") {
				suffix = ".pushinsteadof"
			}
			if !strings.HasSuffix(lower, suffix) {
				return false, errors.New("invalid Git rewrite record")
			}
			values = append(values, key[len("url."):len(key)-len(suffix)])
		} else if !strings.HasPrefix(lower, "remote.") || !(strings.HasSuffix(lower, ".url") || strings.HasSuffix(lower, ".pushurl")) {
			return false, errors.New("invalid Git remote record")
		}
		for _, raw := range values {
			raw = strings.TrimSpace(raw)
			helper := strings.Index(raw, "::")
			scheme := strings.Index(raw, "://")
			if helper >= 0 && (scheme < 0 || helper < scheme) {
				return false, errors.New("unsupported explicit Git URL helper")
			}
			if !strings.Contains(raw, "://") {
				if strings.Contains(raw, "::") {
					return false, errors.New("unsupported Git URL helper")
				}
				if at := strings.LastIndexByte(raw, '@'); at >= 0 && !strings.HasPrefix(raw, "/") && raw[:at] != "git" {
					return true, nil
				}
				continue
			}
			parsed, err := url.Parse(raw)
			if err != nil {
				return false, errors.New("unparseable Git URL")
			}
			if parsed.Opaque != "" {
				return false, errors.New("unsupported opaque Git URL")
			}
			if parsed.RawQuery != "" || parsed.Fragment != "" {
				return true, nil
			}
			if parsed.User != nil {
				_, password := parsed.User.Password()
				if parsed.Scheme != "ssh" || parsed.User.Username() != "git" || password {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func TestGitURLCredentialDetection(t *testing.T) {
	cases := []struct {
		name, data          string
		credential, invalid bool
	}{
		{"https public", "remote.origin.url\nhttps://github.com/owner/repo.git\x00", false, false},
		{"local path", "remote.origin.url\n/private/probe/repo\x00", false, false},
		{"ssh identity", "remote.origin.url\ngit@github.com:owner/repo.git\x00", false, false},
		{"ssh URI identity", "remote.origin.url\nssh://git@github.com/owner/repo.git\x00", false, false},
		{"remote userinfo", "remote.origin.url\nhttps://inert-canary@github.com/owner/repo.git\x00", true, false},
		{"push userinfo", "remote.origin.pushurl\nhttps://user:inert-canary@github.com/owner/repo.git\x00", true, false},
		{"rewrite key", "url.https://inert-canary@github.com/.insteadof\nhttps://github.com/\x00", true, false},
		{"push rewrite key", "url.https://inert-canary@github.com/.pushinsteadof\nhttps://github.com/\x00", true, false},
		{"query", "remote.origin.url\nhttps://github.com/owner/repo.git?token=inert-canary\x00", true, false},
		{"malformed", "remote.origin.url", false, true},
		{"custom helper", "remote.origin.url\next::opaque-helper\x00", false, true},
		{"explicit HTTPS helper", "remote.origin.url\nhttps::https://user:inert-canary@github.com/owner/repo.git\x00", false, true},
		{"opaque URI", "remote.origin.url\nopaque:https://user:inert-canary@github.com/owner/repo.git\x00", false, true},
		{"IPv6 URI", "remote.origin.url\nhttps://[::1]/public/repo.git\x00", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := gitURLCredentials(tc.data)
			if got != tc.credential || (err != nil) != tc.invalid {
				t.Fatal("credential classification mismatch")
			}
		})
	}
}
