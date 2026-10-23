package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestRuntimeProductToolsUseCommonDriverAndPinnedThreadProfile(t *testing.T) {
	for _, mode := range []struct {
		enabled bool
		profile string
	}{{false, ""}, {true, ""}, {true, "product_v2"}} {
		enabled := mode.enabled
		ctx := context.Background()
		k, b := taskFixture(t)
		p := fake(b)
		calls := 0
		r := Runtime{Store: k, ProductTools: enabled, ProductToolProfile: mode.profile, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { calls++; return p, nil })}
		if _, err := r.Execute(ctx, request(b)); err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, tool := range p.dynamicTools {
			names[tool.Name] = true
			if tool.Name == "context_read" {
				sections := tool.InputSchema["properties"].(map[string]any)["section"].(map[string]any)["enum"].([]any)
				external := false
				for _, section := range sections {
					if section == "external" {
						external = true
					}
				}
				if external != (mode.profile == "product_v2") {
					t.Fatal("native tool schema differs from pinned profile")
				}
			}
			if strings.HasPrefix(tool.Name, "wave_") {
				t.Fatal("Task acquired Wave tool", tool.Name)
			}
		}
		if names["pr_publish"] != enabled || names["pr_review_request"] != enabled || !names["send_message"] {
			t.Fatal("wrong native tool set", enabled, names)
		}
		current, err := k.SessionBinding(ctx, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		profile := "kernel_v1"
		if enabled {
			profile = "product_v1"
			if mode.profile != "" {
				profile = mode.profile
			}
		}
		if err := k.PinSessionTools(ctx, current, profile); err != nil {
			t.Fatal("profile not persisted", err)
		}
		r.ProductTools = !enabled
		r.ProductToolProfile = ""
		next := request(b)
		next.RunID = "second-run"
		if _, err := r.Execute(ctx, next); err == nil || !strings.Contains(err.Error(), "profile differs") {
			t.Fatal("existing thread tool change not held", err)
		}
		if calls != 1 {
			t.Fatal("profile mismatch spawned a process", calls)
		}
	}
}

func TestRuntimeNativeProductToolUsesCommonGatewayAndCannotAuthorizeK(t *testing.T) {
	for _, stage := range []string{"K", "P"} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		k, _ := taskFixture(t)
		w, err := k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "native-product", OriginalRequest: "publish the verified registered candidate", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "verified candidate"}}, Policy: domain.WavePolicy{Revision: 1, Stage: stage, Product: stage == "P", Limits: domain.DefaultKernelLimits()}})
		if err != nil {
			t.Fatal(err)
		}
		candidate := runner.Candidate{ID: "candidate", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: 1, PolicyRevision: 1}
		if stage == "P" {
			_, err = k.SetProductTarget(ctx, "", storage.ProductTarget{Target: w.Target(), Repository: "owner/repo", BaseBranch: "trunk", WorkItem: domain.WorkItem{ID: "work", RepositoryID: 17, Issue: 1, Version: 1, ScopeRevision: 1, PolicyRevision: 1, Phase: domain.Reviewing, Candidate: candidate.ID, Branch: "harness/work"}, Candidate: candidate})
			if err != nil {
				t.Fatal(err)
			}
		}
		b, err := k.SessionBinding(ctx, "main")
		if err != nil {
			t.Fatal(err)
		}
		p := &sourceToolProcess{fakeProcess: fake(b), replies: make(chan codex.Message, 2)}
		p.manualCompletion = true
		p.turnStarted = make(chan struct{})
		r := Runtime{Store: k, ProductTools: true, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return p, nil })}
		req := request(b)
		req.RunID = "native-product-run"
		input, _ := json.Marshal(map[string]any{"schema_version": 1, "session_id": b.ID, "targets": []contract.Target{w.Target()}})
		req.Snapshot = contract.InputSnapshot{ID: "native-product-input", Input: input, Digest: fmt.Sprintf("%x", sha256.Sum256(input))}
		done := make(chan error, 1)
		go func() {
			out, err := r.Execute(ctx, req)
			if err == nil && !out.Stopped {
				err = fmt.Errorf("native tool owner not stopped")
			}
			done <- err
		}()
		select {
		case <-p.turnStarted:
		case err := <-done:
			t.Fatal("runtime stopped before tool", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		args, _ := json.Marshal(contract.PublishPRRequest{SchemaVersion: 1, Target: w.Target(), Candidate: contract.CandidateReference{WorkItemID: "work", CandidateID: candidate.ID, SHA: candidate.SHA}})
		params, _ := json.Marshal(codex.DynamicToolCall{ThreadID: p.threadID, TurnID: p.resultTurnID(), CallID: "publish", Tool: "pr_publish", Arguments: args})
		var first contract.ToolReply
		for i := 0; i < 2; i++ {
			p.queue <- codex.Message{ID: json.RawMessage(fmt.Sprintf("%q", fmt.Sprint("tool-", i))), Method: "item/tool/call", Params: params}
			reply := await(t, p.replies)
			var result codex.DynamicToolResponse
			if err := json.Unmarshal(reply.Result, &result); err != nil || result.Success != (stage == "P") {
				t.Fatal(stage, result, err)
			}
			if stage == "P" {
				var tool contract.ToolReply
				if len(result.ContentItems) != 1 {
					t.Fatal(result)
				}
				if err := json.Unmarshal([]byte(result.ContentItems[0].Text), &tool); err != nil || tool.State != "accepted" || len(tool.SubscriptionIDs) != 1 {
					t.Fatal(tool, err)
				}
				if i == 0 {
					first = tool
				} else if tool.OperationID != first.OperationID || tool.SubscriptionIDs[0] != first.SubscriptionIDs[0] {
					t.Fatal("native retry duplicated operation/subscription", first, tool)
				}
			}
		}
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations WHERE name='pr_publish'").Scan(&count); err != nil || count != map[string]int{"K": 0, "P": 1}[stage] {
			t.Fatal(stage, count, err)
		}
		if stage == "P" {
			var intent storage.PROperationIntent
			if err := k.Record(ctx, "product_pr_intent", first.OperationID, &intent); err != nil || intent.Source.SessionID != b.ID || intent.Source.RunID != req.RunID || intent.Source.CallID != "publish" || intent.State != "accepted" {
				t.Fatal(intent, err)
			}
		}
		completed, _ := json.Marshal(map[string]any{"threadId": p.threadID, "turn": map[string]any{"id": p.resultTurnID(), "status": "completed", "items": []any{map[string]string{"type": "agentMessage", "text": p.resultText()}}}})
		p.queue <- codex.Message{Method: "turn/completed", Params: completed}
		if err := await(t, done); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
}
