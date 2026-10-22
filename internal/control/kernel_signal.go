package control

import (
	"context"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// A lost or duplicate hint is harmless: the commit and exact call replay remain
// authoritative. Read tools do not cause a scan; a mutation wakes a recipient
// without waiting for the sending model's long-running turn to finish.
type signalingKernelBackend struct {
	contract.KernelToolBackend
	afterCommit func()
}

func (b signalingKernelBackend) Apply(ctx context.Context, call contract.ToolInvocation, command any, guard func(contract.SessionAuthority) error) (contract.ToolReply, error) {
	reply, err := b.KernelToolBackend.Apply(ctx, call, command, guard)
	if err == nil && b.afterCommit != nil {
		switch call.Name {
		case "send_message", "task_instruct", "artifact_publish", "wave_create", "wave_plan":
			b.afterCommit()
		}
	}
	return reply, err
}
