package worker

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// KernelToolHandler captures the process owner's logical session and reserved
// run. Server parameters and model arguments cannot replace these identities.
// It is supplied to codex.NewSessionRPC before a turn is started. The database
// backend rechecks the current thread/turn and binding on every invocation.
func KernelToolHandler(sessionID, runID string, gateway contract.KernelGateway) (codex.ToolHandler, error) {
	if sessionID == "" || runID == "" || gateway.Backend == nil {
		return nil, errors.New("kernel tool handler requires a trusted session/run/backend")
	}
	return func(ctx context.Context, call codex.DynamicToolCall) (codex.DynamicToolResponse, error) {
		namespace := ""
		if call.Namespace != nil {
			namespace = *call.Namespace
		}
		reply, err := gateway.Handle(ctx, contract.ToolInvocation{SessionID: sessionID, RunID: runID, ThreadID: call.ThreadID, TurnID: call.TurnID, CallID: call.CallID, Name: call.Tool, Namespace: namespace, Arguments: call.Arguments})
		if err != nil {
			return codex.DynamicToolResponse{}, err
		}
		raw, err := json.Marshal(reply)
		if err != nil {
			return codex.DynamicToolResponse{}, err
		}
		response := codex.DynamicToolResponse{Success: true, ContentItems: []codex.ToolContent{{Type: "inputText", Text: string(raw)}}}
		wire, err := json.Marshal(response)
		if err != nil || len(wire) > contract.MaxSessionInput {
			return codex.DynamicToolResponse{}, errors.New("kernel tool response exceeds wire limit")
		}
		return response, nil
	}, nil
}
