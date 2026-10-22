package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type kernelHandlerBackend struct {
	calls   []contract.ToolInvocation
	target  contract.Target
	content string
}

func (b *kernelHandlerBackend) Apply(_ context.Context, call contract.ToolInvocation, _ any, guard func(contract.SessionAuthority) error) (contract.ToolReply, error) {
	b.calls = append(b.calls, call)
	err := guard(contract.SessionAuthority{SessionID: call.SessionID, ThreadID: call.ThreadID, ActiveTurnID: call.TurnID, Kind: "task", TaskID: "task", ParentSessionID: "parent", TaskState: contract.TaskRunning, Target: b.target})
	return contract.ToolReply{SchemaVersion: 1, State: "completed", OperationID: "operation", ResourceID: "resource", Content: b.content}, err
}
func TestKernelToolHandlerCapturesTrustedSessionAndRun(t *testing.T) {
	target := contract.Target{WaveID: "wave", ScopeRevision: 1, PlanRevision: 1, PolicyRevision: 1}
	backend := &kernelHandlerBackend{target: target, content: "state"}
	handler, err := KernelToolHandler("trusted-session", "trusted-run", contract.KernelGateway{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	arguments, err := json.Marshal(contract.StateRequest{SchemaVersion: 1, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	call := codex.DynamicToolCall{ThreadID: "thread", TurnID: "turn", CallID: "call", Tool: "state_get", Arguments: arguments}
	response, err := handler(context.Background(), call)
	if err != nil || !response.Success || len(response.ContentItems) != 1 || response.ContentItems[0].Type != "inputText" {
		t.Fatal(response, err)
	}
	if len(backend.calls) != 1 || backend.calls[0].SessionID != "trusted-session" || backend.calls[0].RunID != "trusted-run" || backend.calls[0].ThreadID != call.ThreadID || backend.calls[0].TurnID != call.TurnID {
		t.Fatal(backend.calls)
	}
	var reply contract.ToolReply
	if err = json.Unmarshal([]byte(response.ContentItems[0].Text), &reply); err != nil || reply.ResourceID != "resource" {
		t.Fatal(reply, err)
	}
	namespace := "other"
	call.Namespace = &namespace
	if _, err = handler(context.Background(), call); err == nil {
		t.Fatal("handler accepted another namespace")
	}
	call.Namespace = nil
	call.Arguments = json.RawMessage(`{"schema_version":1,"target":{"wave_id":"wave","scope_revision":1,"plan_revision":1,"policy_revision":1},"session_id":"forged"}`)
	if _, err = handler(context.Background(), call); err == nil || len(backend.calls) != 1 {
		t.Fatal("model arguments replaced trusted identity", err)
	}
}
func TestKernelToolHandlerRequiresRuntimeAndBoundsWireResponse(t *testing.T) {
	for _, identity := range [][2]string{{"", "run"}, {"session", ""}} {
		if _, err := KernelToolHandler(identity[0], identity[1], contract.KernelGateway{}); err == nil {
			t.Fatal("unbound handler created")
		}
	}
	target := contract.Target{WaveID: "wave", ScopeRevision: 1, PlanRevision: 1, PolicyRevision: 1}
	backend := &kernelHandlerBackend{target: target, content: strings.Repeat("<", contract.MaxInline)}
	handler, err := KernelToolHandler("session", "run", contract.KernelGateway{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(contract.StateRequest{SchemaVersion: 1, Target: target})
	if _, err = handler(context.Background(), codex.DynamicToolCall{ThreadID: "thread", TurnID: "turn", CallID: "call", Tool: "state_get", Arguments: raw}); err == nil {
		t.Fatal("oversized encoded response escaped the wire limit")
	}
}
