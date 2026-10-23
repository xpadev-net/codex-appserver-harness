package kernelcli

import (
	"context"
	"fmt"
	"io"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// Diagnostics are best effort: durable state remains the authority. A slow
// stderr consumer may drop diagnostics but cannot stop reconciliation. An
// arbitrary Writer cannot be canceled; at most one output worker may remain
// blocked until that writer returns (or the CLI process exits). Shutdown never
// waits for that worker and never closes the caller's writer.
func diagnosticReporter(ctx context.Context, out io.Writer) func(error) {
	queue := make(chan string, 64)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-queue:
				if ctx.Err() != nil {
					return
				}
				_, _ = fmt.Fprintln(out, message)
			}
		}
	}()
	return func(err error) {
		if err == nil || ctx.Err() != nil {
			return
		}
		message := err.Error()
		if len(message) > contract.MaxInline {
			message = message[:contract.MaxInline]
		}
		select {
		case queue <- message:
		default:
		}
	}
}
