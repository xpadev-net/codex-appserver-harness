package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	if len(os.Args) > 1 && os.Args[1] == "_runner" {
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
