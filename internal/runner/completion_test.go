package runner

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func shortRoot(t *testing.T) string {
	t.Helper()
	path, err := os.MkdirTemp("/tmp", "runner-wake-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(path) })
	return path
}
func TestCompletionHintFollowsExactSavedResultAndHidesControllerEnvironment(t *testing.T) {
	s := spec(t)
	socket := filepath.Join(shortRoot(t), ipc.SocketName)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err = os.Chmod(socket, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_TOKEN", "fixture-secret")
	t.Setenv("HARNESS_CONTROLLER_SOCKET", socket)
	t.Setenv("HARNESS_WEBHOOK_SECRET", "fixture-webhook")
	s.Profile.Argv = []string{"sh", "-c", `test -z "$GH_TOKEN" && test -z "$HARNESS_CONTROLLER_SOCKET" && test -z "$HARNESS_WEBHOOK_SECRET" && printf PASS`}
	type observation struct {
		request map[string]any
		saved   Result
		err     error
	}
	seen := make(chan observation, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			seen <- observation{err: err}
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		var request map[string]any
		err = json.NewDecoder(conn).Decode(&request)
		saved, loadErr := LoadResult(s.Root, s.JobID)
		seen <- observation{request: request, saved: saved, err: errors.Join(err, loadErr)}
		_ = json.NewEncoder(conn).Encode(ipc.Response{Version: ipc.Version, Accepted: true})
	}()
	result, err := ExecuteWithWake(context.Background(), s, socket)
	if err != nil || result.Status != "passed" {
		t.Fatal(result, err)
	}
	select {
	case got := <-seen:
		if got.err != nil || got.saved.Status != "passed" || !got.saved.Finished.Equal(result.Finished) || len(got.request) != 3 || got.request["method"] != "runner" || got.request["job_id"] != s.JobID {
			t.Fatal(got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("saved result produced no hint")
	}
}
func TestNotificationFailureDoesNotInvalidateSavedRunnerEvidence(t *testing.T) {
	s := spec(t)
	result, err := ExecuteWithWake(context.Background(), s, filepath.Join(shortRoot(t), ipc.SocketName))
	if err != nil || result.Status != "passed" {
		t.Fatal(result, err)
	}
	saved, err := LoadResult(s.Root, s.JobID)
	if err != nil || saved.Status != "passed" {
		t.Fatal(saved, err)
	}
}
func TestResultSaveFailureDoesNotSendCompletionHint(t *testing.T) {
	s := spec(t)
	s.Profile.Argv = []string{"sh", "-c", "mkdir ../result.json; printf PASS"}
	socket := filepath.Join(shortRoot(t), ipc.SocketName)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err = os.Chmod(socket, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = ExecuteWithWake(context.Background(), s, socket); err == nil {
		t.Fatal("result directory accepted as saved file")
	}
	listener.SetDeadline(time.Now().Add(30 * time.Millisecond))
	if conn, err := listener.AcceptUnix(); err == nil {
		conn.Close()
		t.Fatal("hint was sent without published result")
	}
}

func TestResultPublicationWaitsForProfileChildrenToStop(t *testing.T) {
	s := spec(t)
	s.Profile.Argv = []string{"sh", "-c", "sleep 30 & printf PASS"}
	result, err := Execute(context.Background(), s)
	if err != nil || result.Status != "passed" {
		t.Fatal(result, err)
	}
	saved, err := LoadResult(s.Root, s.JobID)
	if err != nil || saved.Finished.IsZero() {
		t.Fatal(saved, err)
	}
	life, err := readLifecycle(s)
	if err != nil {
		t.Fatal(err)
	}
	if alive, err := process.GroupAlive(life.Profile); err != nil || alive {
		t.Fatal("published result preceded group stopping", life, alive, err)
	}
}
