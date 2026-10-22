package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// The helper executes the real CLI entrypoint with the exact detached runner
// argv. It needs no separately built executable or model connection.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && (os.Args[1] == "_runner" || os.Args[1] == "wave") {
		if err := run(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
func TestDetachedRunnerPublishesThenHintsWithoutWorkerCredentials(t *testing.T) {
	ctx := context.Background()
	root, err := os.MkdirTemp("/tmp", "harness-main-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	repo := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"config", "commit.gpgsign", "false"}, {"commit", "--allow-empty", "-m", "base"}} {
		if _, err = workspace.Git(ctx, repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	sha, err := workspace.Resolve(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	s := runner.Spec{LauncherContract: "argv_passthrough_v1", JobID: "real-wrapper", Repository: repo, Root: filepath.Join(root, "jobs"), Candidate: runner.Candidate{ID: "c", SHA: sha, BaseSHA: sha, ScopeRevision: 1, PolicyRevision: 1}, ProfileID: "unit", Profile: policy.Profile{Revision: 1, Argv: []string{"sh", "-c", `test -z "$GH_TOKEN" && test -z "$HARNESS_CONTROLLER_SOCKET" && printf PASS`}, TimeoutSeconds: 2, SuccessPattern: "PASS"}, Launcher: []string{"env"}}
	if err = os.MkdirAll(s.Root, 0700); err != nil {
		t.Fatal(err)
	}
	specPath := filepath.Join(s.Root, "job.json")
	if err = runner.WriteJSON(specPath, s); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, ipc.SocketName)
	t.Setenv("GH_TOKEN", "fixture-controller-only")
	t.Setenv("HARNESS_CONTROLLER_SOCKET", socket)
	observed := make(chan runner.Result, 1)
	readErr := make(chan error, 1)
	server, err := ipc.Listen(root, nil, func() {
		result, err := runner.LoadResult(s.Root, s.JobID)
		if err != nil {
			readErr <- err
			return
		}
		observed <- result
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serving, cancel := context.WithCancel(ctx)
	defer cancel()
	go server.Serve(serving)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child, err := runner.LaunchWithWake(ctx, executable, specPath, socket)
	if err != nil {
		t.Fatal(err)
	}
	id := process.Identify(child.Pid)
	defer func() { _ = process.RequestGroupStop(id) }()
	select {
	case result := <-observed:
		if result.Status != "passed" || result.JobID != s.JobID || result.TestedSHA != sha || result.Finished.IsZero() {
			t.Fatal(result)
		}
	case err := <-readErr:
		t.Fatal("hint preceded saved result", err)
	case <-time.After(10 * time.Second):
		t.Fatal("detached runner did not publish a hint")
	}
	stopped, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if err = process.WaitStopped(stopped, id); err != nil {
		t.Fatal(err)
	}
}

func TestWaveCLIEntrypointCreatesAndInspectsDirectRequestWithoutExternalPrograms(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "kernel")
	invoke := func(args ...string) []byte {
		t.Helper()
		command := exec.Command(executable, append([]string{"wave"}, args...)...)
		command.Env = []string{"PATH=/nonexistent"}
		raw, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("wave command: %v: %s", err, raw)
		}
		return raw
	}
	invoke("init", "--root", root, "--model", "unavailable-model")
	request := filepath.Join(t.TempDir(), "request.json")
	if err = os.WriteFile(request, []byte(`{"schema_version":1,"request_id":"entrypoint","request":"調査結果を説明する","criteria":[{"id":"AC","requirement":"根拠がある"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	var wave domain.Wave
	if err = json.Unmarshal(invoke("create", "--root", root, "--file", request), &wave); err != nil || wave.ID == "" {
		t.Fatal(wave, err)
	}
	var inspection storage.WaveInspection
	if err = json.Unmarshal(invoke("inspect", "--root", root, "--id", wave.ID), &inspection); err != nil || inspection.Wave.ID != wave.ID || inspection.Budget.MainTurns != 0 {
		t.Fatal(inspection, err)
	}
	var page storage.WaveSummaryPage
	if err = json.Unmarshal(invoke("list", "--root", root), &page); err != nil || len(page.Waves) != 1 {
		t.Fatal(page, err)
	}
	var events contract.StateEventPage
	if err = json.Unmarshal(invoke("watch", "--root", root, "--once", "--cursor", "0"), &events); err != nil || len(events.Events) != 1 {
		t.Fatal(events, err)
	}
}

func TestWaveCLIConcurrentProcessesShareOneInitialization(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "kernel")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	jobs := make([]*exec.Cmd, 6)
	outputs := make([]bytes.Buffer, 6)
	for i := range jobs {
		jobs[i] = exec.CommandContext(ctx, executable, "wave", "init", "--root", root, "--model", "unavailable-model")
		jobs[i].Env = []string{"PATH=/nonexistent"}
		jobs[i].Stdout = &outputs[i]
		jobs[i].Stderr = &outputs[i]
		if err = jobs[i].Start(); err != nil {
			t.Fatal(err)
		}
	}
	for i, job := range jobs {
		if err = job.Wait(); err != nil {
			t.Fatal("concurrent CLI init", i, err, outputs[i].String())
		}
	}
	command := exec.CommandContext(ctx, executable, "wave", "list", "--root", root)
	command.Env = []string{"PATH=/nonexistent"}
	if raw, err := command.CombinedOutput(); err != nil {
		t.Fatal("successful init left unusable root", err, string(raw))
	}
}

func TestWaveCLIConcurrentCreateProcessesCommitOneStableRequest(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "kernel")
	command := exec.Command(executable, "wave", "init", "--root", root, "--model", "unavailable-model")
	command.Env = []string{"PATH=/nonexistent"}
	if raw, err := command.CombinedOutput(); err != nil {
		t.Fatal(err, string(raw))
	}
	request := filepath.Join(t.TempDir(), "request.json")
	raw, err := json.Marshal(map[string]any{"schema_version": 1, "request_id": "concurrent", "request": strings.Repeat("observed evidence ", 120000), "criteria": []map[string]string{{"id": "AC", "requirement": "traceable output"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(request, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	jobs := make([]*exec.Cmd, 4)
	outputs := make([]bytes.Buffer, 4)
	for i := range jobs {
		jobs[i] = exec.CommandContext(ctx, executable, "wave", "create", "--root", root, "--file", request)
		jobs[i].Env = []string{"PATH=/nonexistent"}
		jobs[i].Stdout = &outputs[i]
		jobs[i].Stderr = &outputs[i]
		if err = jobs[i].Start(); err != nil {
			t.Fatal(err)
		}
	}
	id := ""
	for i, job := range jobs {
		if err = job.Wait(); err != nil {
			t.Fatal("concurrent create", i, err, outputs[i].String())
		}
		var w domain.Wave
		if err = json.Unmarshal(outputs[i].Bytes(), &w); err != nil {
			t.Fatal(err)
		}
		if id != "" && id != w.ID {
			t.Fatal("same request created two Waves")
		}
		id = w.ID
	}
}
