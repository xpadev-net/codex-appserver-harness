package control

import (
	"context"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

// RSSSampleInterval describes the sampling resolution, not an exact peak. The
// first sample precedes initialize; cancellation joins the sampler before stop
// metrics are persisted. Unknown samples leave the last observed peak intact.
const RSSSampleInterval = 500 * time.Millisecond

func sampleRSS(ctx context.Context, identity process.Identity) <-chan *int64 {
	done := make(chan *int64, 1)
	var peak *int64
	observe := func() {
		if value, err := process.TreeRSS(ctx, identity); err == nil && (peak == nil || value > *peak) {
			peak = &value
		}
	}
	observe()
	go func() {
		ticker := time.NewTicker(RSSSampleInterval)
		defer ticker.Stop()
		defer func() { done <- peak; close(done) }()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				observe()
			}
		}
	}()
	return done
}
