package runner

import (
	"context"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func waitLeaderExit(ctx context.Context, p process.Identity) error {
	return process.WaitLeaderExit(ctx, p)
}
