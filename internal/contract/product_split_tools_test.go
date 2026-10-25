package contract

import (
	"context"
	"encoding/json"
	"testing"
)

func TestProductSplitToolOnlyInExplicitV3ParentProfile(t *testing.T) {
	for _, kind := range []string{"task", "orchestrator"} {
		old := ProductToolsForProfile(kind, "product_v2")
		next := ProductToolsForProfile(kind, "product_v3")
		if kind == "orchestrator" {
			if len(next) != len(old)+1 || next[len(next)-1].Name != "product_split" {
				t.Fatal("missing parent split tool", next)
			}
			next = next[:len(next)-1]
		}
		oldJSON, _ := json.Marshal(old)
		inheritedJSON, _ := json.Marshal(next)
		if string(oldJSON) != string(inheritedJSON) {
			t.Fatal("inherited dynamic schema changed", kind)
		}
		for _, profile := range []string{"product_v1", "product_v2"} {
			for _, tool := range ProductToolsForProfile(kind, profile) {
				if tool.Name == "product_split" {
					t.Fatal("old thread schema gained structural tool", profile, kind)
				}
			}
		}
		for _, tool := range KernelTools(kind) {
			if tool.Name == "product_split" {
				t.Fatal("K gained product structural tool")
			}
		}
	}
}

func TestProductSplitToolUsesCommonStrictParentAuthority(t *testing.T) {
	target := Target{"wave", 1, 1, 1}
	for _, mode := range []string{"valid", "task", "namespace", "alias", "bad_version", "missing_artifact", "spoof_owner", "stale_target"} {
		t.Run(mode, func(t *testing.T) {
			backend := &fakeKernelBackend{authority: SessionAuthority{SessionID: "main", ThreadID: "thread", ActiveTurnID: "turn", Kind: "orchestrator", Target: target}}
			request := SplitProductRequest{SchemaVersion: 1, Target: target, ProposalArtifactID: "artifact"}
			if mode == "bad_version" {
				request.SchemaVersion = 2
			}
			if mode == "missing_artifact" {
				request.ProposalArtifactID = ""
			}
			raw, _ := json.Marshal(request)
			call := ToolInvocation{SessionID: "main", RunID: "run", ThreadID: "thread", TurnID: "turn", CallID: "split", Name: "product_split", Arguments: raw}
			switch mode {
			case "task":
				backend.authority.Kind = "task"
				backend.authority.TaskID = "task"
				backend.authority.ParentSessionID = "main"
				backend.authority.TaskState = TaskRunning
			case "namespace":
				call.Namespace = "relay"
			case "alias":
				call.Name = "split"
			case "spoof_owner":
				call.Arguments = append(raw[:len(raw)-1], []byte(`,"owner_id":"main"}`)...)
			case "stale_target":
				backend.authority.Target.ScopeRevision++
			}
			_, err := (KernelGateway{Backend: backend}).Handle(context.Background(), call)
			if (err == nil) != (mode == "valid") {
				t.Fatal(mode, err)
			}
			if mode != "valid" && backend.effects != 0 {
				t.Fatal("rejected call reached effects", backend.effects)
			}
		})
	}
}

func TestProductSplitParentScopeV4PreservesEarlierTools(t *testing.T) {
	for _, kind := range []string{"task", "orchestrator"} {
		old := ProductToolsForProfile(kind, "product_v3")
		next := ProductToolsForProfile(kind, "product_v4")
		if kind == "orchestrator" {
			if len(next) != len(old)+1 || next[len(next)-1].Name != "product_split_parent_scope" {
				t.Fatal("missing explicit parent scope tool", next)
			}
			next = next[:len(next)-1]
		}
		oldJSON, _ := json.Marshal(old)
		nextJSON, _ := json.Marshal(next)
		if string(oldJSON) != string(nextJSON) {
			t.Fatal("inherited schema changed", kind)
		}
	}
	for _, kind := range []string{"task", "orchestrator"} {
		for _, profile := range []string{"product_v1", "product_v2", "product_v3"} {
			for _, tool := range ProductToolsForProfile(kind, profile) {
				if tool.Name == "product_split_parent_scope" {
					t.Fatal("old thread schema gained new tool", profile, kind)
				}
			}
		}
	}
}

func TestProductSplitParentScopeStrictParentAuthority(t *testing.T) {
	target := Target{"wave", 1, 1, 1}
	for _, mode := range []string{"valid", "task", "stale", "bad_version", "empty", "oversized", "spoofed"} {
		t.Run(mode, func(t *testing.T) {
			backend := &fakeKernelBackend{authority: SessionAuthority{SessionID: "main", ThreadID: "thread", ActiveTurnID: "turn", Kind: "orchestrator", Target: target}}
			request := SplitProductParentScopeRequest{SchemaVersion: 1, Target: target, ParentWorkItemID: "child"}
			switch mode {
			case "task":
				backend.authority.Kind = "task"
			case "stale":
				backend.authority.Target.ScopeRevision++
			case "bad_version":
				request.SchemaVersion++
			case "empty":
				request.ParentWorkItemID = ""
			case "oversized":
				request.ParentWorkItemID = string(make([]byte, 129))
			}
			raw, _ := json.Marshal(request)
			if mode == "spoofed" {
				raw = append(raw[:len(raw)-1], []byte(`,"parent_scope_digest":"fake"}`)...)
			}
			call := ToolInvocation{SessionID: "main", RunID: "run", ThreadID: "thread", TurnID: "turn", CallID: "scope", Name: "product_split_parent_scope", Arguments: raw}
			_, err := (KernelGateway{Backend: backend}).Handle(context.Background(), call)
			if (err == nil) != (mode == "valid") || mode != "valid" && backend.effects != 0 {
				t.Fatal(mode, err, backend.effects)
			}
		})
	}
}

func TestProductNestedSplitV5PreservesEarlierToolsAndRestrictsStructure(t *testing.T) {
	for _, kind := range []string{"task", "orchestrator"} {
		old := ProductToolsForProfile(kind, "product_v4")
		next := ProductToolsForProfile(kind, "product_v5")
		if kind == "orchestrator" {
			if len(next) != len(old)+1 || next[len(next)-1].Name != "product_split_nested" {
				t.Fatal("missing nested adoption tool")
			}
			next = next[:len(next)-1]
		}
		a, _ := json.Marshal(old)
		b, _ := json.Marshal(next)
		if string(a) != string(b) {
			t.Fatal("inherited native schemas changed", kind)
		}
	}
	for _, mode := range []string{"valid", "task", "bad_digest", "bad_version", "empty_parent", "stale_target", "spoof_repository"} {
		t.Run(mode, func(t *testing.T) {
			target := Target{"wave", 1, 1, 1}
			backend := &fakeKernelBackend{authority: SessionAuthority{SessionID: "main", ThreadID: "thread", ActiveTurnID: "turn", Kind: "orchestrator", Target: target}}
			request := NestedSplitProductRequest{SchemaVersion: 1, Target: target, ProposalArtifactID: "proposal", ParentScopeID: "scope", ParentScopeDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
			switch mode {
			case "task":
				backend.authority.Kind = "task"
			case "bad_digest":
				request.ParentScopeDigest = "bad"
			case "bad_version":
				request.SchemaVersion++
			case "empty_parent":
				request.ParentScopeID = ""
			case "stale_target":
				backend.authority.Target.ScopeRevision++
			}
			raw, _ := json.Marshal(request)
			if mode == "spoof_repository" {
				raw = append(raw[:len(raw)-1], []byte(`,"repository":"foreign/repo"}`)...)
			}
			call := ToolInvocation{SessionID: "main", RunID: "run", ThreadID: "thread", TurnID: "turn", CallID: "nested", Name: "product_split_nested", Arguments: raw}
			_, err := (KernelGateway{Backend: backend}).Handle(context.Background(), call)
			if (err == nil) != (mode == "valid") || mode != "valid" && backend.effects != 0 {
				t.Fatal(mode, err, backend.effects)
			}
		})
	}
}
