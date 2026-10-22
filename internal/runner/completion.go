package runner

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
)

// ExecuteWithWake is a trusted wrapper entrypoint. The hint is sent only after
// this invocation publishes its exact result. IPC failure never changes the
// durable result; ordinary reconciliation remains authoritative.
func ExecuteWithWake(ctx context.Context, s Spec, socket string) (Result, error) {
	result, runErr := Execute(ctx, s)
	if socket == "" || result.Finished.IsZero() {
		return result, runErr
	}
	saved, err := LoadResult(s.Root, s.JobID)
	if err != nil {
		return result, runErr
	}
	expected, err := json.Marshal(result)
	if err != nil {
		return result, runErr
	}
	actual, err := json.Marshal(saved)
	if err != nil || !bytes.Equal(actual, expected) {
		return result, runErr
	}
	_ = ipc.Notify(context.WithoutCancel(ctx), socket, s.JobID)
	return result, runErr
}
