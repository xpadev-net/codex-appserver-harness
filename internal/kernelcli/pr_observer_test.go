package kernelcli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestPRObserverDaemonRetainsOwnershipUntilExternalWorkerJoins(t *testing.T) {
	root := cliRoot(t)
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	worker := func(ctx context.Context, store *storage.KernelStore, wake func(), report func(error)) error {
		if store != k {
			return errors.New("wrong worker store")
		}
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		_, err := store.StateEvents(context.Background(), 0, 1)
		return errors.Join(ctx.Err(), err)
	}
	done := make(chan error, 1)
	go func() {
		done <- runDaemon(ctx, k, control.Runtime{Store: k, Factory: neverStartFactory{make(chan control.Launch, 1)}}, io.Discard, worker)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("worker not started")
	}
	// A worker waiting on external I/O does not own the IPC serving goroutine.
	readCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := ipc.Notify(readCtx, filepath.Join(root, ipc.SocketName), ""); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("worker not canceled")
	}
	select {
	case err := <-done:
		t.Fatal("daemon returned before worker joined", err)
	default:
	}
	if unlock, err := controllerLock(root); err == nil {
		unlock()
		t.Fatal("owner released before worker joined")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker shutdown not joined")
	}
	unlock, err := controllerLock(root)
	if err != nil {
		t.Fatal("owner retained after worker joined", err)
	}
	unlock()
}

func TestPRObserverDaemonWorkerFailureStopsOtherLoops(t *testing.T) {
	for _, failure := range []error{errors.New("controlled observer failure"), nil} {
		root := cliRoot(t)
		k, _, err := openKernel(context.Background(), root)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = runDaemon(ctx, k, control.Runtime{Store: k, Factory: neverStartFactory{make(chan control.Launch, 1)}}, io.Discard,
			func(context.Context, *storage.KernelStore, func(), func(error)) error { return failure })
		cancel()
		k.Close()
		if err == nil || errors.Is(err, context.DeadlineExceeded) || failure != nil && !errors.Is(err, failure) {
			t.Fatal("background exit not propagated", err)
		}
		if _, err := os.Lstat(filepath.Join(root, ipc.SocketName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("IPC left after background failure", err)
		}
	}
}

func TestPRObserverConfigRejectsUnsafeOrUnboundedOptIn(t *testing.T) {
	if worker, err := loadPRObserver(""); worker != nil || err != nil {
		t.Fatal("observer enabled implicitly", err)
	}
	for _, c := range []PRObserverConfig{{}, {1, 0, 60}, {1, 9, 60}, {1, 2, 0}, {1, 2, 3601}, {2, 2, 60}} {
		if _, err := loadPRObserver(operatorFile(t, c)); err == nil {
			t.Fatal("invalid observer configuration accepted", c)
		}
	}
	p := operatorFile(t, PRObserverConfig{1, 2, 60})
	if worker, err := loadPRObserver(p); worker == nil || err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPRObserver(p); err == nil {
		t.Fatal("public observer config accepted")
	}
	if _, err := cliCall(t, "list", "--root", cliRoot(t), "--pr-observer", p); err == nil {
		t.Fatal("observer flag accepted by reader")
	}
}
