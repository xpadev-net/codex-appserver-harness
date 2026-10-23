package contract

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestProductToolSetRequiresExplicitOptInAndRetainsTaskWaveRestriction(t *testing.T) {
	for _, kind := range []string{"orchestrator", "task"} {
		for _, tool := range KernelTools(kind) {
			if strings.HasPrefix(tool.Name, "pr_") {
				t.Fatal("product tool published in K")
			}
		}
		seen := map[string]bool{}
		for _, tool := range ProductTools(kind) {
			seen[tool.Name] = true
			if kind == "task" && parentOnly(tool.Name) {
				t.Fatal("product Task sees Wave management")
			}
		}
		if !seen["pr_publish"] || !seen["pr_review_request"] {
			t.Fatal(seen)
		}
	}
	if ProductTools("unknown") != nil {
		t.Fatal("unknown role has product tools")
	}
}

func TestExternalContextSchemaIsExplicitProductV2WithoutChangingExistingProfiles(t *testing.T) {
	for _, kind := range []string{"task", "orchestrator"} {
		for _, profile := range []string{"kernel_v1", "product_v1", "product_v2", "kernel_v1", "product_v1"} {
			tools := KernelTools(kind)
			if profile != "kernel_v1" {
				tools = ProductToolsForProfile(kind, profile)
			}
			found := false
			for _, tool := range tools {
				if kind == "task" && parentOnly(tool.Name) {
					t.Fatal("Task Wave tool exposed")
				}
				if tool.Name != "context_read" {
					continue
				}
				sections := tool.InputSchema["properties"].(map[string]any)["section"].(map[string]any)["enum"].([]string)
				for _, section := range sections {
					if section == "external" {
						found = true
					}
				}
			}
			if found != (profile == "product_v2") {
				t.Fatal("thread schema changed outside explicit profile", kind, profile)
			}
		}
	}
	if ProductToolsForProfile("task", "unknown") != nil {
		t.Fatal("unknown product profile accepted")
	}
	r := ContextReadRequest{SchemaVersion: 1, Target: Target{"wave", 1, 1, 1}, Section: "external", ReferenceID: "notification", Limit: 2048}
	if !r.Valid() {
		t.Fatal("external slice rejected")
	}
	r.ReferenceID = ""
	if r.Valid() {
		t.Fatal("unscoped external read accepted")
	}
}

func TestProductToolRequestsUseCommonAuthorityAndStrictCandidateContract(t *testing.T) {
	target := Target{"wave", 1, 1, 1}
	for _, mode := range []string{"valid", "review", "bad_sha", "spoof_repository", "wrong_scope", "namespace", "task_wave", "guard_omitted"} {
		t.Run(mode, func(t *testing.T) {
			backend := &fakeKernelBackend{authority: SessionAuthority{SessionID: "session", ThreadID: "thread", ActiveTurnID: "turn", Kind: "task", TaskID: "task", ParentSessionID: "main", TaskState: TaskRunning, Target: target}}
			request := PublishPRRequest{SchemaVersion: 1, Target: target, Candidate: CandidateReference{WorkItemID: "work", CandidateID: "candidate", SHA: strings.Repeat("a", 40)}}
			if mode == "bad_sha" {
				request.Candidate.SHA = "unfixed"
			}
			raw, _ := json.Marshal(request)
			call := ToolInvocation{SessionID: "session", RunID: "run", ThreadID: "thread", TurnID: "turn", CallID: "call", Name: "pr_publish", Arguments: raw}
			switch mode {
			case "review":
				call.Name = "pr_review_request"
			case "spoof_repository":
				call.Arguments = append(raw[:len(raw)-1], []byte(`,"repository":"foreign/repo"}`)...)
			case "wrong_scope":
				backend.authority.Target.ScopeRevision++
			case "namespace":
				call.Namespace = "alias"
			case "task_wave":
				call.Name = "wave_create"
				call.Arguments = []byte(`{"schema_version":1,"request_id":"request"}`)
			case "guard_omitted":
				backend.omitGuard = true
			}
			_, err := (KernelGateway{Backend: backend}).Handle(context.Background(), call)
			if (err == nil) != (mode == "valid" || mode == "review") {
				t.Fatal(mode, err)
			}
		})
	}
}
