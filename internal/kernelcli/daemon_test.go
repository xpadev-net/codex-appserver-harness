package kernelcli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

type neverStartFactory struct{ launches chan control.Launch }

func (f neverStartFactory) Open(ctx context.Context, l control.Launch) (control.Process, error) {
	select {
	case f.launches <- l:
	case <-ctx.Done():
	}
	return nil, control.ErrNotStarted
}
func startTestDaemon(t *testing.T, root string, launches chan control.Launch) func() {
	t.Helper()
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runDaemon(ctx, k, control.Runtime{Store: k, Factory: neverStartFactory{launches}}, io.Discard)
	}()
	stop := func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("daemon did not stop")
		}
		k.Close()
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err = ipc.Notify(context.Background(), filepath.Join(root, ipc.SocketName), ""); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon IPC not ready", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Cleanup owns the resources; explicit calls must be idempotent.
	called := false
	return func() {
		if !called {
			called = true
			cancel()
		}
	}
}
func TestKernelDaemonEmptyIdleAndExclusiveController(t *testing.T) {
	root := cliRoot(t)
	launches := make(chan control.Launch, 4)
	stop := startTestDaemon(t, root, launches)
	if unlock, err := controllerLock(root); err == nil {
		unlock()
		t.Fatal("second controller admitted")
	}
	raw, err := cliCall(t, "watch", "--root", root, "--once")
	if err != nil || len(raw) == 0 {
		t.Fatal(string(raw), err)
	}
	select {
	case <-launches:
		t.Fatal("empty daemon launched model")
	case <-time.After(40 * time.Millisecond):
	}
	stop()
}
func TestKernelDaemonCommittedRequestHintLaunchesFixedMainWorkspace(t *testing.T) {
	root := cliRoot(t)
	launches := make(chan control.Launch, 4)
	startTestDaemon(t, root, launches)
	if _, err := cliCall(t, "create", "--root", root, "--file", requestFile(t, "daemon-request", "research the evidence")); err != nil {
		t.Fatal(err)
	}
	select {
	case launch := <-launches:
		if launch.Binding.Kind != "orchestrator" || launch.Binding.CWD != filepath.Join(root, "workspaces", "main") {
			t.Fatal(launch.Binding)
		}
		info, err := os.Stat(launch.Binding.CWD)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
			t.Fatal(info, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("committed request did not wake dispatcher")
	}
}
func TestKernelDaemonScansCommittedRequestOnRestartWithoutHint(t *testing.T) {
	root, _ := controlWave(t)
	launches := make(chan control.Launch, 4)
	startTestDaemon(t, root, launches)
	select {
	case <-launches:
	case <-time.After(3 * time.Second):
		t.Fatal("startup did not reconstruct work")
	}
}
func TestKernelDaemonWorkspaceRejectsRedirectAndForeignBinding(t *testing.T) {
	root := cliRoot(t)
	base := filepath.Join(root, "workspaces")
	if err := os.Symlink(t.TempDir(), filepath.Join(base, "redirect")); err != nil {
		t.Fatal(err)
	}
	f := workspaceFactory{Factory: neverStartFactory{make(chan control.Launch, 4)}, root: root}
	for _, b := range []contract.SessionBinding{
		{ID: "redirect", CWD: filepath.Join(base, "redirect")},
		{ID: "../outside", CWD: filepath.Join(root, "outside")},
		{ID: "legit", CWD: t.TempDir()},
	} {
		if _, err := f.Open(context.Background(), control.Launch{Binding: b}); !errors.Is(err, control.ErrNotStarted) {
			t.Fatal("unsafe workspace accepted", err)
		}
	}
	// Fresh Task workspaces are created lazily using exactly the saved binding.
	cwd := filepath.Join(base, "session-new-task")
	launches := make(chan control.Launch, 1)
	f.Factory = neverStartFactory{launches}
	_, err := f.Open(context.Background(), control.Launch{Binding: contract.SessionBinding{ID: "session-new-task", CWD: cwd}})
	if !errors.Is(err, control.ErrNotStarted) {
		t.Fatal(err)
	}
	select {
	case <-launches:
	default:
		t.Fatal("valid Task workspace did not reach factory")
	}
	if info, err := os.Stat(cwd); err != nil || info.Mode().Perm() != 0700 {
		t.Fatal(info, err)
	}
}
func TestKernelDaemonLockRejectsUnsafeFilesAndRetainsStopErrors(t *testing.T) {
	root := cliRoot(t)
	path := filepath.Join(root, "controller.lock")
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if unlock, err := controllerLock(root); err == nil {
		unlock()
		t.Fatal("symlink lock accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(target, path); err != nil {
		t.Fatal(err)
	}
	if unlock, err := controllerLock(root); err == nil {
		unlock()
		t.Fatal("hardlink lock accepted")
	}
	if err := withoutCancellation(errors.Join(context.Canceled, errors.Join(process.ErrUnconfirmed, context.Canceled))); !errors.Is(err, process.ErrUnconfirmed) || errors.Is(err, context.Canceled) {
		t.Fatal("lost stop failure", err)
	}
	if err := withoutCancellation(errors.Join(context.Canceled, context.Canceled)); err != nil {
		t.Fatal(err)
	}
}
func TestKernelCLIRunRejectsUnversionedOrUnverifiedRuntime(t *testing.T) {
	root := cliRoot(t)
	for _, c := range []RuntimeConfig{{}, {SchemaVersion: 1, LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/sh"}}} {
		if _, err := cliCall(t, "run", "--root", root, "--runtime", operatorFile(t, c)); err == nil {
			t.Fatal("unsafe runtime accepted")
		}
	}
	if _, err := cliCall(t, "list", "--root", root, "--runtime", "unused"); err == nil {
		t.Fatal("runtime flag accepted by reader")
	}
}

func TestKernelCLIRunUsesPrivateRuntimeConfigAndStopsCleanly(t *testing.T) {
	root := cliRoot(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		t.Fatal(err)
	}
	auth := t.TempDir()
	if err = os.Chmod(auth, 0700); err != nil {
		t.Fatal(err)
	}
	c := RuntimeConfig{SchemaVersion: 1, CodexBinary: binary, WorkerCodexHome: auth, Launcher: []string{binary}, LauncherContract: "argv_passthrough_v1", IsolationVerified: true, GitDirectories: []string{}, ProductTools: true}
	file := operatorFile(t, c)
	loaded, err := loadRuntime(file, root, "configured")
	if err != nil || !loaded.ProductTools {
		t.Fatal("host product opt-in not loaded", err)
	}
	// Empty state runs the complete production constructor and daemon, without
	// starting the deliberately non-Codex binary or requiring an external model.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	observer := operatorFile(t, PRObserverConfig{SchemaVersion: 1, Workers: 2, EverySeconds: 60})
	go func() {
		done <- Run(ctx, []string{"run", "--root", root, "--runtime", file, "--pr-observer", observer}, io.Discard, io.Discard)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err = ipc.Notify(context.Background(), filepath.Join(root, ipc.SocketName), ""); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("CLI daemon did not expose IPC", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CLI shutdown hung")
	}
	unlock, err := controllerLock(root)
	if err != nil {
		t.Fatal("controller lock retained", err)
	}
	unlock()
	if _, err = os.Lstat(filepath.Join(root, ipc.SocketName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("socket retained", err)
	}
	if err = os.Chmod(file, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = cliCall(t, "run", "--root", root, "--runtime", file); err == nil {
		t.Fatal("public runtime config accepted")
	}
	c.CodexBinary = "relative-codex"
	if _, err = cliCall(t, "run", "--root", root, "--runtime", operatorFile(t, c)); err == nil {
		t.Fatal("relative runtime path accepted")
	}
}
