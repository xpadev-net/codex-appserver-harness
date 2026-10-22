package ipc_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func root(t *testing.T) string {
	t.Helper()
	p, err := os.MkdirTemp("/tmp", "ipc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(p) })
	return p
}
func fixture(t *testing.T) (*storage.KernelStore, string) {
	t.Helper()
	ctx := context.Background()
	dir := root(t)
	k, err := storage.OpenKernel(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.Close() })
	if _, err = k.CreateOrchestrator(ctx, "main", "configured", t.TempDir(), "read-only"); err != nil {
		t.Fatal(err)
	}
	if _, err = k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "request", OriginalRequest: "research", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "report"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: domain.DefaultKernelLimits()}}); err != nil {
		t.Fatal(err)
	}
	return k, dir
}
func serve(t *testing.T, k *storage.KernelStore, dir string, wake func()) *ipc.Server {
	t.Helper()
	s, err := ipc.Listen(dir, k, wake)
	if err != nil {
		t.Fatal(err)
	}
	s.PollEvery = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		s.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("IPC server did not stop")
		}
	})
	return s
}
func TestIPCNotifyHelper(t *testing.T) {
	if path := os.Getenv("HARNESS_IPC_TEST_SOCKET"); path != "" {
		if err := ipc.Notify(context.Background(), path, "job-123"); err != nil {
			t.Fatal(err)
		}
	}
}
func TestPrivateHintFromSeparateProcessHasNoStateAuthority(t *testing.T) {
	k, dir := fixture(t)
	wake := make(chan struct{}, 1)
	serve(t, k, dir, func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	path := filepath.Join(dir, ipc.SocketName)
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	program, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(program, "-test.run=^TestIPCNotifyHelper$")
	cmd.Env = []string{"HARNESS_IPC_TEST_SOCKET=" + path}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("separate-process hint: %v: %s", err, output)
	}
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("separate-process hint was lost")
	}
	page, err := k.StateEvents(context.Background(), 0, 32)
	if err != nil || len(page.Events) != 1 || page.Events[0].Kind != "wave_created" {
		t.Fatal("hint forged a state transition", page, err)
	}
}
func TestStreamIsReadOnlyAndReconnectsFromAcceptedCursor(t *testing.T) {
	k, dir := fixture(t)
	var wakes atomic.Int64
	s := serve(t, k, dir, func() { wakes.Add(1) })
	path := filepath.Join(dir, ipc.SocketName)
	done := errors.New("accepted page")
	var cursor int64
	err := ipc.Stream(context.Background(), path, 0, 1, func(p contract.StateEventPage) error {
		if len(p.Events) != 1 {
			t.Fatal(p)
		}
		cursor = p.Cursor
		return done
	})
	if !errors.Is(err, done) {
		t.Fatal(err)
	}
	if wakes.Load() != 0 {
		t.Fatal("viewing state caused a wake")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// Another daemon uses the same persisted database and accepted cursor.
	s2, err := ipc.Listen(dir, k, func() { wakes.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	s2.PollEvery = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer s2.Close()
	go s2.Serve(ctx)
	err = ipc.Stream(ctx, path, cursor, 1, func(p contract.StateEventPage) error {
		if len(p.Events) != 0 || p.Cursor != cursor {
			t.Fatal("reconnection replayed accepted events", p)
		}
		return done
	})
	if !errors.Is(err, done) || wakes.Load() != 0 {
		t.Fatal(err, wakes.Load())
	}
}
func TestServerRejectsCommandsAndMalformedHintsWithoutWake(t *testing.T) {
	k, dir := fixture(t)
	var wakes atomic.Int64
	serve(t, k, dir, func() { wakes.Add(1) })
	path := filepath.Join(dir, ipc.SocketName)
	for _, request := range []string{`{"version":1,"method":"turn/start"}`, `{"version":1,"method":"wake","prompt":"run model"}`, `{"version":1,"method":"runner","job_id":"../fake"}`, `{"version":2,"method":"wake"}`, `{"version":1,"version":1,"method":"wake"}`} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(time.Second))
		if _, err = conn.Write([]byte(request + "\n")); err != nil {
			t.Fatal(err)
		}
		var response ipc.Response
		if err = json.NewDecoder(conn).Decode(&response); err != nil {
			t.Fatal(err)
		}
		conn.Close()
		if response.Accepted || response.Error == "" {
			t.Fatal(response)
		}
	}
	if wakes.Load() != 0 {
		t.Fatal("invalid IPC request caused wake", wakes.Load())
	}
}
func TestSocketLifetimeLockAndStaleRecovery(t *testing.T) {
	dir := root(t)
	s, err := ipc.Listen(dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := ipc.Listen(dir, nil, nil); err == nil {
		other.Close()
		t.Fatal("second listener stole active socket")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ipc.SocketName)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	listener.Close()
	recovered, err := ipc.Listen(dir, nil, nil)
	if err != nil {
		t.Fatal("stale socket was not recoverable", err)
	}
	recovered.Close()
	if err = os.WriteFile(path, []byte("not a socket"), 0600); err != nil {
		t.Fatal(err)
	}
	if bad, err := ipc.Listen(dir, nil, nil); err == nil {
		bad.Close()
		t.Fatal("ordinary file replaced by socket")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "not a socket" {
		t.Fatal(err, string(data))
	}
}
func TestUnsafeRootAndSocketSymlinksAreRejected(t *testing.T) {
	dir := root(t)
	alias := filepath.Join(root(t), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if s, err := ipc.Listen(alias, nil, nil); err == nil {
		s.Close()
		t.Fatal("symlink root admitted")
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if s, err := ipc.Listen(dir, nil, nil); err == nil {
		s.Close()
		t.Fatal("public IPC root admitted")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, ipc.SocketName)); err != nil {
		t.Fatal(err)
	}
	if s, err := ipc.Listen(dir, nil, nil); err == nil {
		s.Close()
		t.Fatal("symlink socket admitted")
	}
}

func TestPrivateSocketSupportsLongConfiguredRoot(t *testing.T) {
	dir := filepath.Join(root(t), strings.Repeat("long-private-root-", 8))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	wake := make(chan struct{}, 1)
	server, err := ipc.Listen(dir, nil, func() { wake <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(ctx)
	path := filepath.Join(dir, ipc.SocketName)
	if len(path) <= 108 {
		t.Fatal("fixture must exceed Unix sockaddr path bound")
	}
	if err = ipc.Notify(ctx, path, ""); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("long private root lost wake")
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("socket was not stored in configured private root", err)
	}
}
