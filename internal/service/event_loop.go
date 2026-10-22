package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
)

// The legacy Tick remains serial under its existing admission policy. This
// loop connects durable CLI/webhook/runner hints to reconciliation; the Kernel
// session loop uses the same transport with its own read-only event reader.
func runWakeLoop(ctx context.Context, root string, every time.Duration, tick func(context.Context) error, report func(error)) error {
	if every <= 0 || tick == nil {
		return errors.New("positive reconciliation period and Tick required")
	}
	lock, err := os.OpenFile(filepath.Join(root, "controller.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("another controller owns this state root")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	hints := make(chan struct{}, 1)
	server, err := ipc.Listen(root, nil, func() {
		select {
		case hints <- struct{}{}:
		default:
		}
	})
	if err != nil {
		return err
	}
	defer server.Close()
	serving, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(serving) }()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err = tick(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if report != nil {
				report(err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-done:
			if err == nil {
				err = errors.New("IPC server stopped")
			}
			return err
		case <-ticker.C:
		case <-hints:
		}
	}
}

// Only a successfully persisted, authenticated webhook may emit this hint.
type wakingInbox struct {
	Inbox interface {
		Receive(context.Context, string, string, []byte) error
	}
	Socket string
}

func (w wakingInbox) Receive(ctx context.Context, id, event string, body []byte) error {
	if err := w.Inbox.Receive(ctx, id, event, body); err != nil {
		return err
	}
	_ = ipc.Notify(context.WithoutCancel(ctx), w.Socket, "")
	return nil
}
