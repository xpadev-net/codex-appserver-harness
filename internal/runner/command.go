package runner

import (
	"context"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"golang.org/x/sys/unix"
)

// The caller owns cmd.Wait and has not invoked it yet. Keep the original leader
// unreaped while observing its exit, so its numeric group ID cannot be reused
// before the synchronous descendant stop. stdout held by children is not an
// exit signal; waiting for those pipes first could stall until profile timeout.
func waitLeaderExit(ctx context.Context, p process.Identity) error {
	if !p.Valid() {
		return process.ErrUnconfirmed
	}
	for {
		_, _, ticks, err := process.ReadStat(p.PID)
		if err != nil || ticks != p.StartTicks {
			return errors.Join(process.ErrUnconfirmed, err)
		}
		// The leader can become a zombie while other threads still run (e.g.
		// pthread_exit from main). Only waitid reports a waitable thread group.
		// WNOWAIT keeps its PID reserved for the caller's stop-before-reap.
		var info unix.Siginfo
		err = unix.Waitid(unix.P_PID, p.PID, &info, unix.WEXITED|unix.WNOWAIT|unix.WNOHANG, nil)
		if err != nil && !errors.Is(err, unix.EINTR) {
			return errors.Join(process.ErrUnconfirmed, err)
		}
		if err == nil && info.Signo != 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}
