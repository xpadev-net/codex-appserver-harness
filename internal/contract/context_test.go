package contract

import (
	"context"
	"encoding/json"
	"testing"
)

func TestContextReadIsCommonBoundedTool(t *testing.T) {
	target := Target{"wave", 1, 1, 1}
	for _, kind := range []string{"task", "orchestrator"} {
		found := false
		for _, tool := range KernelTools(kind) {
			if tool.Name == "context_read" {
				found = true
			}
		}
		if !found {
			t.Fatal("missing common context tool", kind)
		}
		for _, mode := range []string{"valid", "schema", "section", "offset", "limit", "maximum", "target"} {
			r := ContextReadRequest{SchemaVersion: 1, Target: target, Section: "request", Limit: MaxContextRead}
			switch mode {
			case "schema":
				r.SchemaVersion = 2
			case "section":
				r.Section = "path"
			case "offset":
				r.Offset = -1
			case "limit":
				r.Limit = 0
			case "maximum":
				r.Limit++
			case "target":
				r.Target.ScopeRevision = 0
			}
			raw, _ := json.Marshal(r)
			backend := &fakeKernelBackend{authority: SessionAuthority{SessionID: "session", ThreadID: "thread", ActiveTurnID: "turn", Kind: kind, TaskID: "task", ParentSessionID: "main", TaskState: "running", Target: target}}
			_, err := (KernelGateway{Backend: backend}).Handle(context.Background(), ToolInvocation{SessionID: "session", ThreadID: "thread", TurnID: "turn", CallID: "read", Name: "context_read", Arguments: raw})
			if (err == nil) != (mode == "valid") {
				t.Fatal(kind, mode, err)
			}
			if mode != "valid" && backend.effects != 0 {
				t.Fatal("invalid read reached backend", mode)
			}
		}
	}
}

func TestManifestReadRequiresBoundReference(t *testing.T) {
	r := ContextReadRequest{SchemaVersion: 1, Target: Target{"wave", 1, 1, 1}, Section: "manifest", Limit: MaxContextRead}
	if r.Valid() {
		t.Fatal("missing manifest reference")
	}
	r.ReferenceID = "input-manifest-reference"
	if !r.Valid() {
		t.Fatal("valid manifest reference rejected")
	}
	r.Section = "request"
	if r.Valid() {
		t.Fatal("reference accepted for a live context section")
	}
}
