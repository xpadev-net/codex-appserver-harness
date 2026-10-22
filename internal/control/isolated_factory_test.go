package control

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

// This launcher is deliberately a stub. It tests runtime configuration and
// protocol ownership, and supplies no evidence of actual OS containment.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "_isolated_fixture" {
		isolatedFixture(os.Args[2:])
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type isolatedObservation struct {
	Arguments            []string `json:"arguments"`
	Home                 string   `json:"home"`
	CodexHome            string   `json:"codex_home"`
	Workspace            string   `json:"workspace"`
	GitDirectory         string   `json:"git_directory"`
	Path                 string   `json:"path"`
	ForbiddenEnvironment bool     `json:"forbidden_environment"`
	Sandbox              string   `json:"sandbox"`
	Model                string   `json:"model"`
	Tools                []string `json:"tools"`
}

func isolatedFixture(args []string) {
	if len(args) < 2 {
		os.Exit(20)
	}
	mode := args[0]
	args = args[1:]
	if args[0] == "sh" {
		if mode == "expose" {
			os.Exit(21)
		}
		return
	}
	if len(args) == 2 && args[1] == "--version" {
		if mode == "wrong-version" {
			fmt.Println("codex-cli unsupported")
			return
		}
		fmt.Println("codex-cli 0.163.0")
		return
	}
	if len(args) < 4 || args[1] != "app-server" {
		os.Exit(22)
	}
	cwd, err := os.Getwd()
	if err != nil {
		os.Exit(23)
	}
	home := os.Getenv("HOME")
	obs := isolatedObservation{Arguments: args[1:], Home: home, CodexHome: os.Getenv("CODEX_HOME"), Workspace: os.Getenv("HARNESS_WORKSPACE"), GitDirectory: os.Getenv("HARNESS_GIT_DIR"), Path: os.Getenv("PATH")}
	for _, key := range []string{"GH_TOKEN", "GITHUB_TOKEN", "GH_CONFIG_DIR", "XDG_CONFIG_HOME", "HARNESS_CONTROLLER_SOCKET", "HARNESS_WEBHOOK_SECRET", "GIT_CONFIG_COUNT"} {
		if _, present := os.LookupEnv(key); present {
			obs.ForbiddenEnvironment = true
		}
	}
	threadID := fmt.Sprintf("fixture-%x", sha256.Sum256([]byte(cwd)))
	out := json.NewEncoder(os.Stdout)
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		var msg codex.Message
		if json.Unmarshal(scan.Bytes(), &msg) != nil {
			os.Exit(24)
		}
		if len(msg.ID) == 0 {
			continue
		}
		var reply any
		switch msg.Method {
		case "initialize":
			reply = map[string]any{}
		case "thread/start":
			var p struct {
				Sandbox      string `json:"sandbox"`
				Model        string `json:"model"`
				DynamicTools []struct {
					Name string `json:"name"`
				} `json:"dynamicTools"`
			}
			if json.Unmarshal(msg.Params, &p) != nil {
				os.Exit(25)
			}
			obs.Sandbox, obs.Model = p.Sandbox, p.Model
			for _, tool := range p.DynamicTools {
				obs.Tools = append(obs.Tools, tool.Name)
			}
			data, err := json.Marshal(obs)
			if err != nil {
				os.Exit(26)
			}
			if os.WriteFile(filepath.Join(home, "observed.json"), data, 0600) != nil {
				os.Exit(27)
			}
			reply = map[string]any{"thread": map[string]any{"id": threadID, "cwd": cwd, "turns": []any{}}, "model": p.Model}
		case "turn/start":
			reply = map[string]any{"turn": map[string]any{"id": "turn"}}
		default:
			os.Exit(28)
		}
		if out.Encode(map[string]any{"id": msg.ID, "result": reply}) != nil {
			os.Exit(29)
		}
		if msg.Method == "turn/start" {
			if out.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": threadID, "turn": map[string]any{"id": "turn", "status": "completed", "items": []any{map[string]string{"type": "agentMessage", "text": `{"ok":true}`}}}}}) != nil {
				os.Exit(30)
			}
		}
	}
}

func isolatedConfig(t *testing.T, root string) IsolatedConfig {
	t.Helper()
	program, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	auth := t.TempDir()
	if err = os.Chmod(auth, 0700); err != nil {
		t.Fatal(err)
	}
	return IsolatedConfig{PrivateRoot: root, Model: "configured", CodexBinary: program, WorkerCodexHome: auth, Launcher: []string{program, "_isolated_fixture", "accept"}, LauncherContract: "argv_passthrough_v1", IsolationVerified: true}
}
func isolatedHome(c IsolatedConfig, id string) string {
	key := sha256.Sum256([]byte(id))
	return filepath.Join(c.PrivateRoot, "worker-homes", fmt.Sprintf("session-%x", key[:16]))
}

func TestIsolatedFactoryCommonParentTaskConfigurationAndNoGitWorkspace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	k, task := taskFixture(t)
	main, err := k.SessionBinding(ctx, task.ParentSessionID)
	if err != nil {
		t.Fatal(err)
	}
	config := isolatedConfig(t, k.Root)
	for _, name := range []string{"GH_TOKEN", "GITHUB_TOKEN", "GH_CONFIG_DIR", "XDG_CONFIG_HOME", "HARNESS_CONTROLLER_SOCKET", "HARNESS_WEBHOOK_SECRET", "GIT_CONFIG_COUNT"} {
		t.Setenv(name, "fixture-private-value")
	}
	runtime, err := NewIsolatedRuntime(k, config)
	if err != nil {
		t.Fatal(err)
	}
	config.Launcher[2] = "expose" // Constructor must freeze the approved arguments.
	var observations []isolatedObservation
	for _, binding := range []contract.SessionBinding{main, task} {
		r := request(binding)
		r.RunID = "run-" + binding.ID
		r.Snapshot.ID = "snapshot-" + binding.ID
		if binding.Kind == "orchestrator" {
			raw, err := json.Marshal(map[string]any{"schema_version": 1, "session_id": binding.ID, "targets": []contract.Target{task.Target}})
			if err != nil {
				t.Fatal(err)
			}
			r.Snapshot.Input, r.Snapshot.Digest = raw, fmt.Sprintf("%x", sha256.Sum256(raw))
		}
		out, err := runtime.Execute(ctx, r)
		if err != nil || !out.Stopped || out.Run.State != "completed" {
			t.Fatal(out, err)
		}
		data, err := os.ReadFile(filepath.Join(isolatedHome(config, binding.ID), "observed.json"))
		if err != nil {
			t.Fatal(err)
		}
		var obs isolatedObservation
		if err = json.Unmarshal(data, &obs); err != nil {
			t.Fatal(err)
		}
		if obs.ForbiddenEnvironment || obs.Path != isolatedPath || obs.Model != config.Model || obs.Sandbox != binding.Sandbox || obs.Workspace != binding.CWD || obs.CodexHome == config.WorkerCodexHome || !pathContains(obs.Home, obs.CodexHome) || !pathContains(obs.Home, obs.GitDirectory) || !slices.Contains(obs.Tools, "state_get") {
			t.Fatal(obs)
		}
		observations = append(observations, obs)
	}
	if !reflect.DeepEqual(observations[0].Arguments, observations[1].Arguments) || observations[0].Home == observations[1].Home || observations[0].CodexHome == observations[1].CodexHome {
		t.Fatal("role changed native runtime flags or shared its scratch home", observations)
	}
	assertCapacity(t, k, 0)
}

func TestIsolatedFactoryFailsClosedBeforeAppServerOnBadConfigurationOrProbe(t *testing.T) {
	for _, kind := range []string{"unverified", "model", "contract", "expose", "wrong-version", "auth-overlap", "workspace-alias"} {
		t.Run(kind, func(t *testing.T) {
			k, b := taskFixture(t)
			config := isolatedConfig(t, k.Root)
			switch kind {
			case "unverified":
				config.IsolationVerified = false
			case "model":
				config.Model = "other"
			case "contract":
				config.LauncherContract = "unknown"
			case "expose", "wrong-version":
				config.Launcher[2] = kind
			case "auth-overlap":
				config.WorkerCodexHome = b.CWD
			case "workspace-alias":
				alias := filepath.Join(t.TempDir(), "alias")
				if err := os.Symlink(b.CWD, alias); err != nil {
					t.Fatal(err)
				}
				b.CWD = alias
			}
			// The direct factory test has no reservation; it never initializes a
			// model. Successful admission is covered through Runtime above.
			p, err := (IsolatedFactory{Config: config}).Open(context.Background(), Launch{Binding: b})
			if p != nil || !errors.Is(err, ErrNotStarted) {
				t.Fatal(p, err)
			}
			if _, err = os.Stat(filepath.Join(isolatedHome(config, b.ID), "observed.json")); !os.IsNotExist(err) {
				t.Fatal("app-server started after a failed probe", err)
			}
		})
	}
}

func TestIsolatedFactoryRejectsReadableGitAuthenticationMetadata(t *testing.T) {
	for _, setting := range []string{"remote.origin.url", "remote.backup\u00a0mirror.url", "submodule.private.url", "lfs.url", "remote.origin.lfsurl", "remote.origin.lfspushurl", "multi-remote", "remote.origin.proxy", "core.askpass", "core.gitproxy", "credential.helper", "http.https://github.com/.extraheader", "http.https://fixture-private-token@github.com/.sslverify", "worktree-header"} {
		t.Run(setting, func(t *testing.T) {
			k, b := taskFixture(t)
			key := setting
			if setting == "multi-remote" {
				key = "remote.origin.url"
			}
			if setting == "worktree-header" {
				key = "http.https://github.com/.extraheader"
			}
			value := "https://fixture-private-token@github.com/owner/repository"
			if strings.HasSuffix(setting, ".sslverify") {
				value = "false"
			}
			commands := [][]string{{"init", "--quiet", b.CWD}}
			if setting == "worktree-header" {
				commands = append(commands, []string{"-C", b.CWD, "config", "extensions.worktreeConfig", "true"}, []string{"-C", b.CWD, "config", "--worktree", key, "fixture-private-token"})
			} else {
				commands = append(commands, []string{"-C", b.CWD, "config", key, value})
			}
			for _, args := range commands {
				if err := exec.Command("git", args...).Run(); err != nil {
					t.Fatal(err)
				}
			}
			if setting == "multi-remote" {
				if err := exec.Command("git", "-C", b.CWD, "config", "--add", key, "https://github.com/owner/repository").Run(); err != nil {
					t.Fatal(err)
				}
			}
			config := isolatedConfig(t, k.Root)
			p, err := (IsolatedFactory{Config: config}).Open(context.Background(), Launch{Binding: b})
			if p != nil || !errors.Is(err, ErrNotStarted) {
				t.Fatal(p, err)
			}
			if strings.Contains(err.Error(), "fixture-private-token") {
				t.Fatal("configuration error exposed a credential")
			}
		})
	}
}

func TestIsolatedFactoryRequiresApprovedExternalWorktreeMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	k, b := taskFixture(t)
	repo := t.TempDir()
	for _, args := range [][]string{{"init", "--quiet", repo}, {"-C", repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "commit", "--quiet", "--allow-empty", "-m", "fixture"}, {"-C", repo, "worktree", "add", "--quiet", "--detach", b.CWD}} {
		if err := exec.CommandContext(ctx, "git", args...).Run(); err != nil {
			t.Fatal("prepare linked worktree fixture", err)
		}
	}
	config := isolatedConfig(t, k.Root)
	if p, err := (IsolatedFactory{Config: config}).Open(ctx, Launch{Binding: b}); p != nil || !errors.Is(err, ErrNotStarted) {
		t.Fatal("unapproved external Git metadata was mounted", p, err)
	}
	approved := filepath.Join(repo, ".git")
	config.GitDirectories = []string{approved}
	runtime, err := NewIsolatedRuntime(k, config)
	if err != nil {
		t.Fatal(err)
	}
	config.GitDirectories[0] = t.TempDir() // Freeze approval as well as argv.
	out, err := runtime.Execute(ctx, request(b))
	if err != nil || !out.Stopped {
		t.Fatal(out, err)
	}
	data, err := os.ReadFile(filepath.Join(isolatedHome(config, b.ID), "observed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var observed isolatedObservation
	if err = json.Unmarshal(data, &observed); err != nil || observed.GitDirectory != approved {
		t.Fatal(observed, err)
	}
	// Other worktree configs are physically readable in the same common mount,
	// even when they are not effective settings for the current checkout.
	other := filepath.Join(approved, "worktrees", "other", "config.worktree")
	if err = os.MkdirAll(filepath.Dir(other), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(other, []byte("[http \"https://github.com/\"]\nextraHeader = fixture-private-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = isolatedGitDirectory(ctx, b.CWD, isolatedHome(config, b.ID), config.PrivateRoot, config.WorkerCodexHome, []string{approved}); err == nil || !strings.Contains(err.Error(), "authentication") {
		t.Fatal("another worktree's credential was exposed", err)
	}
}

func TestIsolatedGitValidatesNestedSubmoduleConfigurations(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprint(nested), func(t *testing.T) {
			k, b := taskFixture(t)
			if err := exec.Command("git", "init", "--quiet", b.CWD).Run(); err != nil {
				t.Fatal(err)
			}
			module := filepath.Join(b.CWD, ".git", "modules", "group", "module")
			if err := os.MkdirAll(module, 0700); err != nil {
				t.Fatal(err)
			}
			if nested {
				if err := os.WriteFile(filepath.Join(module, "config"), []byte("[core]\nbare=false\n"), 0600); err != nil {
					t.Fatal(err)
				}
				module = filepath.Join(module, "modules", "child")
				if err := os.MkdirAll(module, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(module, "config"), []byte("[remote \"origin\"]\nurl=https://fixture-private-token@github.com/owner/repository\n"), 0600); err != nil {
				t.Fatal(err)
			}
			c := isolatedConfig(t, k.Root)
			home := isolatedHome(c, b.ID)
			if err := os.MkdirAll(home, 0700); err != nil {
				t.Fatal(err)
			}
			if _, err := isolatedGitDirectory(context.Background(), b.CWD, home, c.PrivateRoot, c.WorkerCodexHome, nil); err == nil {
				t.Fatal("submodule credential was mounted")
			}
		})
	}
}

func TestIsolatedRuntimeBindsTheActualPrivateRoot(t *testing.T) {
	k, _ := taskFixture(t)
	c := isolatedConfig(t, t.TempDir())
	if _, err := NewIsolatedRuntime(k, c); err == nil {
		t.Fatal("runtime used another controller's probe root")
	}
}

func TestIsolatedGitProbeDoesNotExecuteInheritedPathPrograms(t *testing.T) {
	k, b := taskFixture(t)
	poison := t.TempDir()
	marker := filepath.Join(poison, "executed")
	if err := os.WriteFile(filepath.Join(poison, "git"), []byte("#!/bin/sh\n: > \""+marker+"\"\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", poison+string(os.PathListSeparator)+os.Getenv("PATH"))
	config := isolatedConfig(t, k.Root)
	home := isolatedHome(config, b.ID)
	if err := os.MkdirAll(home, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := isolatedGitDirectory(context.Background(), b.CWD, home, config.PrivateRoot, config.WorkerCodexHome, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("controller executed a mutable PATH Git helper", err)
	}
}

func TestIsolatedProbeStopsWrapperChildrenBeforeReaping(t *testing.T) {
	for _, ending := range []string{"wait", "exit 0"} {
		dir := t.TempDir()
		childPath := filepath.Join(dir, "child")
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		before := time.Now()
		_, err := isolatedProbe(ctx, "/bin/sh", []string{"-c", `sleep 30 & printf '%s' "$!" > "$1"; ` + ending, "probe", childPath}, []string{"PATH=" + isolatedPath}, dir)
		cancel()
		if ending == "wait" && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if time.Since(before) > 2*time.Second {
			t.Fatal("probe cancellation waited for descendants", time.Since(before), err)
		}
		raw, readErr := os.ReadFile(childPath)
		if readErr != nil {
			t.Fatal(readErr)
		}
		pid, parseErr := strconv.Atoi(string(raw))
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		if process.Alive(process.Identity{PID: pid}) {
			t.Fatal("probe child survived", pid, err)
		}
	}
}
func TestIsolatedGitRejectsInactiveConditionalInclude(t *testing.T) {
	k, b := taskFixture(t)
	if err := exec.Command("git", "init", "--quiet", b.CWD).Run(); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(b.CWD, ".git", "config")
	file, err := os.OpenFile(config, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteString("\n[includeIf \"onbranch:never-active-reference-branch\"]\npath = auth.inc\n")
	if closeErr := file.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if err = os.WriteFile(filepath.Join(b.CWD, ".git", "auth.inc"), []byte("[http]\nextraHeader = fixture-private-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = isolatedGitDirectory(context.Background(), b.CWD, t.TempDir(), k.Root, t.TempDir(), nil); err == nil {
		t.Fatal("inactive include exposed unvalidated credentials")
	}
}
