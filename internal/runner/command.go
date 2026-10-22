package runner

import (
	"context"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
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
		state, _, ticks, err := process.ReadStat(p.PID)
		if err != nil || ticks != p.StartTicks {
			return errors.Join(process.ErrUnconfirmed, err)
		}
		if state == "Z" || state == "X" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}
