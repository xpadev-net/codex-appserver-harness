package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func wakeRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "service-wake-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
func wakeLoopFixture(t *testing.T, every time.Duration, tick func(context.Context) error) (*storage.Store, string, context.CancelFunc, <-chan error) {
	t.Helper()
	root := wakeRoot(t)
	store, err := storage.Open(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWakeLoop(ctx, root, every, tick, func(err error) {}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("wake loop failed to stop")
		}
	})
	return store, root, cancel, done
}
func TestWakeLoopReconcilesStoredResultButHintCannotForgeOne(t *testing.T) {
	ticks := make(chan struct{}, 8)
	var stored atomic.Pointer[storage.Store]
	var ready atomic.Bool
	tick := func(ctx context.Context) error {
		defer func() { ticks <- struct{}{} }()
		s := stored.Load()
		if s == nil {
			return nil
		}
		var result map[string]bool
		if err := s.Record(ctx, "test_result", "job-1", &result); err == nil {
			ready.Store(result["saved"])
		}
		return nil
	}
	store, root, _, _ := wakeLoopFixture(t, time.Hour, tick)
	stored.Store(store)
	select {
	case <-ticks:
	case <-time.After(5 * time.Second):
		t.Fatal("no startup reconciliation")
	}
	if err := ipc.Notify(context.Background(), filepath.Join(root, ipc.SocketName), "job-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ticks:
	case <-time.After(time.Second):
		t.Fatal("hint did not reconcile")
	}
	if ready.Load() {
		t.Fatal("hint manufactured saved result")
	}
	if err := store.Put(context.Background(), "test_result", "job-1", "", map[string]bool{"saved": true}); err != nil {
		t.Fatal(err)
	}
	if err := ipc.Notify(context.Background(), filepath.Join(root, ipc.SocketName), "job-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ticks:
	case <-time.After(time.Second):
		t.Fatal("stored completion did not wake loop")
	}
	if !ready.Load() {
		t.Fatal("stored completion not collected")
	}
}
func TestWakeLoopLostHintFallsBackToTimerAndRejectsSecondOwner(t *testing.T) {
	ticks := make(chan struct{}, 16)
	_, root, _, _ := wakeLoopFixture(t, 15*time.Millisecond, func(context.Context) error {
		select {
		case ticks <- struct{}{}:
		default:
		}
		return nil
	})
	select {
	case <-ticks:
	case <-time.After(5 * time.Second):
		t.Fatal("no startup scan")
	}
	select {
	case <-ticks:
	case <-time.After(time.Second):
		t.Fatal("lost hint stopped timer reconciliation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runWakeLoop(ctx, root, time.Hour, func(context.Context) error { return nil }, nil); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("second owner was not refused", err)
	}
}

type inboxFunc func(context.Context, string, string, []byte) error

func (f inboxFunc) Receive(ctx context.Context, id, event string, body []byte) error {
	return f(ctx, id, event, body)
}
func TestWebhookWakeFollowsSuccessfulPersistence(t *testing.T) {
	root := wakeRoot(t)
	var saved atomic.Bool
	seen := make(chan bool, 4)
	server, err := ipc.Listen(root, nil, func() { seen <- saved.Load() })
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(ctx)
	wrapper := wakingInbox{Socket: filepath.Join(root, ipc.SocketName), Inbox: inboxFunc(func(context.Context, string, string, []byte) error { saved.Store(true); return nil })}
	if err = wrapper.Receive(ctx, "delivery", "issues", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case persisted := <-seen:
		if !persisted {
			t.Fatal("hint preceded durable receive")
		}
	case <-time.After(time.Second):
		t.Fatal("no webhook wake")
	}
	wrapper.Inbox = inboxFunc(func(context.Context, string, string, []byte) error { return errors.New("save failed") })
	if err = wrapper.Receive(ctx, "other", "issues", []byte(`{}`)); err == nil {
		t.Fatal("failed receive accepted")
	}
	select {
	case <-seen:
		t.Fatal("failed receive caused wake")
	case <-time.After(30 * time.Millisecond):
	}
}
