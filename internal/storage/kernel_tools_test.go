package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
)

func kernelToolFixture(t *testing.T, kind string) (*KernelStore, domain.Wave, []domain.Task, contract.SessionBinding, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	k, wave, tasks := mailboxFixture(t)
	session := tasks[0].SessionID
	if kind == "orchestrator" {
		session = "main"
	}
	b := prepareWakeSession(t, k, session)
	snapshot := sessionSnapshot()
	if kind == "orchestrator" {
		raw, err := json.Marshal(map[string]any{"schema_version": 1, "session_id": session, "targets": []contract.Target{wave.Target()}})
		if err != nil {
			t.Fatal(err)
		}
		snapshot = contract.InputSnapshot{ID: "scoped-main", Input: raw, Digest: fmt.Sprintf("%x", sha256.Sum256(raw))}
	}
	run := contract.SessionRun{ID: "tool-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{"old-turn"}}
	if err := k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	return k, wave, tasks, b, run
}
func kernelCall(t *testing.T, b contract.SessionBinding, run contract.SessionRun, name string, arguments any) contract.ToolInvocation {
	t.Helper()
	raw, err := json.Marshal(arguments)
	if err != nil {
		t.Fatal(err)
	}
	return contract.ToolInvocation{SessionID: b.ID, RunID: run.ID, ThreadID: b.ThreadID, TurnID: "tool-turn", CallID: "call", Name: name, Arguments: raw}
}
func TestKernelToolSourceRejectsForeignOrOldRuntimeIdentities(t *testing.T) {
	for _, mutation := range []string{"session", "run", "thread", "baseline-turn", "namespace", "paused", "stopped", "superseded"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			k, wave, _, b, run := kernelToolFixture(t, "task")
			call := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: wave.Target()})
			switch mutation {
			case "session":
				call.SessionID = "main"
			case "run":
				call.RunID = "another"
			case "thread":
				call.ThreadID = "another"
			case "baseline-turn":
				call.TurnID = "old-turn"
			case "namespace":
				call.Namespace = "functions"
			case "stopped":
				if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
					t.Fatal(err)
				}
			case "paused", "superseded":
				wave, err := k.Wave(ctx, wave.ID)
				if err != nil {
					t.Fatal(err)
				}
				wave.Paused = mutation == "paused"
				if mutation == "superseded" {
					wave.ScopeRevision++
					call.Arguments, _ = json.Marshal(contract.StateRequest{SchemaVersion: 1, Target: wave.Target()})
				}
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err = saveWave(ctx, tx, wave); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
				t.Fatal("invalid runtime identity was authorized")
			}
			var operations int
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations").Scan(&operations); err != nil || operations != 0 {
				t.Fatal(operations, err)
			}
			var saved contract.SessionRun
			if err := k.Record(ctx, "agent_run", run.ID, &saved); err != nil || saved.TurnID != "" {
				t.Fatal("rejected call bound a turn", saved, err)
			}
		})
	}
}
func TestKernelToolEarlyTurnAndReplayHaveOneAtomicMessageEffect(t *testing.T) {
	ctx := context.Background()
	k, wave, _, b, run := kernelToolFixture(t, "task")
	gateway := contract.KernelGateway{Backend: k}
	request := contract.SendMessageRequest{SchemaVersion: 1, Target: wave.Target(), Recipient: "parent", Body: "please clarify the requirement"}
	call := kernelCall(t, b, run, "send_message", request)
	var group sync.WaitGroup
	group.Add(2)
	replies := make([]contract.ToolReply, 2)
	errs := make([]error, 2)
	for i := range replies {
		go func(i int) { defer group.Done(); replies[i], errs[i] = gateway.Handle(ctx, call) }(i)
	}
	group.Wait()
	if errs[0] != nil || errs[1] != nil || !reflect.DeepEqual(replies[0], replies[1]) || replies[0].ResourceID == "" {
		t.Fatal(replies, errs)
	}
	var saved contract.SessionRun
	if err := k.Record(ctx, "agent_run", run.ID, &saved); err != nil || saved.TurnID != call.TurnID || saved.State != "running" {
		t.Fatal(saved, err)
	}
	entries, err := k.Mailbox(ctx, "main", 0, 10)
	if err != nil || len(entries) != 1 || entries[0].Message.SenderSessionID != b.ID || entries[0].Message.Request.SourceRunID != run.ID {
		t.Fatal(entries, err)
	}
	// JSON object order/whitespace are not a different semantic request.
	call.Arguments = json.RawMessage(fmt.Sprintf(`{ "body": "please clarify the requirement", "reply_to":"", "recipient":"parent", "target":%s, "schema_version":1 }`, mustJSON(t, wave.Target())))
	if reply, err := gateway.Handle(ctx, call); err != nil || !reflect.DeepEqual(reply, replies[0]) {
		t.Fatal(reply, err)
	}
	request.Body = "different effect"
	call.Arguments, _ = json.Marshal(request)
	if _, err = gateway.Handle(ctx, call); err == nil {
		t.Fatal("call ID reused with changed arguments")
	}
	// Process restart reuses the same source facts and response, not a message.
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	call.Arguments, _ = json.Marshal(contract.SendMessageRequest{SchemaVersion: 1, Target: wave.Target(), Recipient: "parent", Body: "please clarify the requirement"})
	if reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err != nil || !reflect.DeepEqual(reply, replies[0]) {
		t.Fatal(reply, err)
	}
	entries, err = k.Mailbox(ctx, "main", 0, 10)
	if err != nil || len(entries) != 1 {
		t.Fatal(entries, err)
	}
}
func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
func TestKernelToolOperationFailureRollsBackTurnAndSideEffects(t *testing.T) {
	ctx := context.Background()
	k, wave, _, b, run := kernelToolFixture(t, "task")
	call := kernelCall(t, b, run, "send_message", contract.SendMessageRequest{SchemaVersion: 1, Target: wave.Target(), Recipient: "parent", Body: "question"})
	if _, err := k.DB.Exec("CREATE TRIGGER inject_tool_operation BEFORE INSERT ON kernel_tool_operations BEGIN SELECT RAISE(ABORT,'tool operation failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("unrecorded side effect was accepted")
	}
	var messages, notifications int
	k.DB.QueryRow("SELECT count(*) FROM kernel_messages").Scan(&messages)
	k.DB.QueryRow("SELECT count(*) FROM kernel_notifications").Scan(&notifications)
	if messages != 0 || notifications != 0 {
		t.Fatal(messages, notifications)
	}
	var saved contract.SessionRun
	if err := k.Record(ctx, "agent_run", run.ID, &saved); err != nil || saved.TurnID != "" {
		t.Fatal(saved, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER inject_tool_operation"); err != nil {
		t.Fatal(err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err != nil {
		t.Fatal(err)
	}
}
func TestKernelToolWaitOwnsItsSubscriptionsAndTaskCannotManageWave(t *testing.T) {
	ctx := context.Background()
	k, wave, tasks, b, run := kernelToolFixture(t, "task")
	gateway := contract.KernelGateway{Backend: k}
	own := mailboxSubscriptionID(b.ID, wave.Target(), b.TaskID)
	call := kernelCall(t, b, run, "session_wait", contract.WaitRequest{SchemaVersion: 1, Target: wave.Target(), SubscriptionIDs: []string{own}})
	reply, err := gateway.Handle(ctx, call)
	if err != nil || len(reply.SubscriptionIDs) != 1 || reply.SubscriptionIDs[0] != own {
		t.Fatal(reply, err)
	}
	for _, id := range []string{mailboxSubscriptionID("main", wave.Target(), b.TaskID), mailboxSubscriptionID(tasks[1].SessionID, wave.Target(), tasks[1].ID)} {
		call.CallID = id
		call.Arguments, _ = json.Marshal(contract.WaitRequest{SchemaVersion: 1, Target: wave.Target(), SubscriptionIDs: []string{id}})
		if _, err = gateway.Handle(ctx, call); err == nil {
			t.Fatal("Task waited on another mailbox")
		}
	}
	call = kernelCall(t, b, run, "wave_create", contract.CreateWaveRequest{SchemaVersion: 1, RequestID: "request"})
	call.CallID = "wave"
	if _, err = gateway.Handle(ctx, call); err == nil {
		t.Fatal("Task created a Wave")
	}
	call.Name = "functions.wave_create"
	if _, err = gateway.Handle(ctx, call); err == nil {
		t.Fatal("alias created a Wave")
	}
	call = kernelCall(t, b, run, "task_instruct", contract.InstructRequest{SchemaVersion: 1, Target: wave.Target(), TaskID: tasks[1].ID, Body: "sibling instruction"})
	call.CallID = "instruction"
	if _, err = gateway.Handle(ctx, call); err == nil {
		t.Fatal("Task instructed a sibling")
	}
}
func TestKernelToolMainCannotBorrowUnchargedWaveScope(t *testing.T) {
	ctx := context.Background()
	k, wave, tasks, b, run := kernelToolFixture(t, "orchestrator")
	other, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: b.ID, Source: "direct", SourceID: "other", OriginalRequest: "other request", Criteria: wave.Criteria, Policy: wave.Policy})
	if err != nil {
		t.Fatal(err)
	}
	gateway := contract.KernelGateway{Backend: k}
	call := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: other.Target()})
	if _, err = gateway.Handle(ctx, call); err == nil {
		t.Fatal("main borrowed a Wave outside its run snapshot")
	}
	call = kernelCall(t, b, run, "task_instruct", contract.InstructRequest{SchemaVersion: 1, Target: wave.Target(), TaskID: tasks[0].ID, Body: "check the original criterion"})
	call.CallID = "instruction"
	if _, err = gateway.Handle(ctx, call); err != nil {
		t.Fatal(err)
	}
	entries, err := k.Mailbox(ctx, tasks[0].SessionID, 0, 10)
	if err != nil || len(entries) != 1 || entries[0].Message.SenderSessionID != b.ID {
		t.Fatal(entries, err)
	}
}
func TestKernelToolStateAndArtifactReadsRemainScopedAndBounded(t *testing.T) {
	ctx := context.Background()
	k, wave, _, b, run := kernelToolFixture(t, "task")
	gateway := contract.KernelGateway{Backend: k}
	call := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: wave.Target()})
	reply, err := gateway.Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		Tasks []struct {
			ID string `json:"id"`
		} `json:"tasks"`
		SubscriptionIDs []string `json:"subscription_ids"`
	}
	if err = json.Unmarshal([]byte(reply.Content), &state); err != nil || len(state.Tasks) != 1 || state.Tasks[0].ID != b.TaskID || len(state.SubscriptionIDs) != 1 {
		t.Fatal(state, err)
	}
	private := mustImport(t, k, ArtifactImport{SessionID: "main", Target: wave.Target(), Key: "private", Kind: "report", ContentType: "application/octet-stream", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "private source"}}, "not granted")
	call = kernelCall(t, b, run, "artifact_read", contract.ReadArtifactRequest{SchemaVersion: 1, Target: wave.Target(), ArtifactID: private.ID, Offset: 0, Limit: contract.MaxInline})
	call.CallID = "private"
	if _, err = gateway.Handle(ctx, call); err == nil {
		t.Fatal("Artifact read crossed its grant")
	}
	owned := mustImport(t, k, ArtifactImport{SessionID: b.ID, Target: b.Target, Key: "owned", Kind: "report", ContentType: "application/octet-stream", Origin: domain.ArtifactOrigin{Kind: "task", Reference: b.TaskID}}, string(make([]byte, 8000)))
	call.Arguments, _ = json.Marshal(contract.ReadArtifactRequest{SchemaVersion: 1, Target: wave.Target(), ArtifactID: owned.ID, Offset: 0, Limit: contract.MaxInline})
	call.CallID = "owned"
	reply, err = gateway.Handle(ctx, call)
	if err != nil || len(reply.Content) > contract.MaxInline {
		t.Fatal(reply, err)
	}
	var chunk struct {
		Bytes    int    `json:"bytes"`
		Data     string `json:"data"`
		Encoding string `json:"encoding"`
		Next     int64  `json:"next_offset"`
		EOF      bool   `json:"eof"`
	}
	if err = json.Unmarshal([]byte(reply.Content), &chunk); err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(chunk.Data)
	if err != nil || len(decoded) != 4096 || chunk.Bytes != len(decoded) || chunk.Next != 4096 || chunk.EOF || chunk.Encoding != "base64" {
		t.Fatal(chunk, err)
	}
}

type earlyToolStartRPC struct {
	gateway   contract.KernelGateway
	call      contract.ToolInvocation
	missingID bool
	toolError error
}

func (r *earlyToolStartRPC) Call(ctx context.Context, method string, _ any, result any) error {
	if method != "turn/start" {
		return errors.New("unexpected RPC")
	}
	_, r.toolError = r.gateway.Handle(ctx, r.call)
	if r.toolError != nil {
		return r.toolError
	}
	if r.missingID {
		return json.Unmarshal([]byte(`{"turn":{}}`), result)
	}
	return errors.New("turn/start reply lost")
}
func (*earlyToolStartRPC) Next(context.Context) (codex.Message, error) {
	return codex.Message{}, errors.New("unexpected notification wait")
}
func TestKernelEarlyToolProofSurvivesUnknownStartWithoutAuthorizingMoreTools(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			ctx := context.Background()
			k, wave, tasks := mailboxFixture(t)
			b := prepareWakeSession(t, k, tasks[0].SessionID)
			rpc := &earlyToolStartRPC{gateway: contract.KernelGateway{Backend: k}, missingID: missing}
			rpc.call = kernelCall(t, b, contract.SessionRun{ID: "early-run"}, "state_get", contract.StateRequest{SchemaVersion: 1, Target: wave.Target()})
			driver := worker.SessionDriver{RPC: rpc, Store: k}
			prepared := worker.PreparedSession{Binding: b, BaselineKnown: true, BaselineTurnIDs: []string{"old-turn"}}
			run, err := driver.Execute(ctx, prepared, "early-run", sessionSnapshot(), contract.TaskResultSchema(), worker.ValidateTaskOutput)
			if err == nil || rpc.toolError != nil || run.State != "unknown" || run.TurnID != rpc.call.TurnID {
				t.Fatal("early proof or transport outcome was lost", run, err, rpc.toolError)
			}
			var saved contract.SessionRun
			if err = k.Record(ctx, "agent_run", run.ID, &saved); err != nil || saved.State != "unknown" || saved.TurnID != rpc.call.TurnID {
				t.Fatal(saved, err)
			}
			rpc.call.CallID = "after-unknown"
			if _, err = rpc.gateway.Handle(ctx, rpc.call); err == nil {
				t.Fatal("unresolved start still authorized a tool")
			}
			if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			if err = k.ArchiveSessionRun(ctx, b.ID, run.ID, "unresolved"); err == nil {
				t.Fatal("unknown start was discarded")
			}
		})
	}
}
func TestKernelStatePaginationKeepsLargeSupportedPlanRetrievable(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.Policy.Limits.Tasks = 128
	wave, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	phase := contract.PhaseSpec{ID: "large", FailurePolicy: "drain_and_block", Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{}}, Tasks: []contract.TaskSpec{}}
	for i := range 128 {
		id := fmt.Sprintf("task-%03d", i)
		phase.Tasks = append(phase.Tasks, contract.TaskSpec{ID: id, Kind: "work", Instruction: "produce evidence", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}})
		phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, id)
	}
	adopted, err := k.AdoptPlan(ctx, wave.Target(), "large", contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{phase}, Templates: []contract.FixReviewTemplate{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range adopted.CreatedTaskIDs {
		if _, err = k.Subscribe(ctx, SubscriptionRequest{SessionID: "main", Target: wave.Target(), Key: id, Kind: "task_terminal", TaskID: id}); err != nil {
			t.Fatal(err)
		}
	}
	b := prepareWakeSession(t, k, "main")
	raw, _ := json.Marshal(map[string]any{"schema_version": 1, "session_id": b.ID, "targets": []contract.Target{wave.Target()}})
	snapshot := contract.InputSnapshot{ID: "large-main", Input: raw, Digest: fmt.Sprintf("%x", sha256.Sum256(raw))}
	run := contract.SessionRun{ID: "large-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	gateway := contract.KernelGateway{Backend: k}
	for _, collection := range []string{"tasks", "subscriptions"} {
		after := ""
		seen := map[string]bool{}
		for page := 0; page < 100; page++ {
			args := contract.StateRequest{SchemaVersion: 1, Target: wave.Target(), Page: &contract.StatePage{Collection: collection, After: after, Limit: 16}}
			call := kernelCall(t, b, run, "state_get", args)
			call.CallID = fmt.Sprintf("page-%s-%d", collection, page)
			reply, err := gateway.Handle(ctx, call)
			if err != nil || len(reply.Content) > contract.MaxInline {
				t.Fatal("state collection became unreadable", err)
			}
			var state struct {
				Tasks []struct {
					ID string `json:"id"`
				} `json:"tasks"`
				SubscriptionIDs   []string `json:"subscription_ids"`
				NextTasks         string   `json:"next_tasks_after"`
				NextSubscriptions string   `json:"next_subscriptions_after"`
			}
			if err = json.Unmarshal([]byte(reply.Content), &state); err != nil {
				t.Fatal(err)
			}
			ids, next := state.SubscriptionIDs, state.NextSubscriptions
			if collection == "tasks" {
				if len(state.SubscriptionIDs) != 0 {
					t.Fatal("another collection leaked into page")
				}
				ids, next = nil, state.NextTasks
				for _, task := range state.Tasks {
					ids = append(ids, task.ID)
				}
			} else if len(state.Tasks) != 0 {
				t.Fatal("another collection leaked into page")
			}
			if len(ids) > 16 {
				t.Fatal("page exceeded row limit")
			}
			for _, id := range ids {
				if seen[id] {
					t.Fatal("page repeated an item", id)
				}
				seen[id] = true
			}
			if next == "" {
				break
			}
			if next <= after {
				t.Fatal("page cursor did not advance")
			}
			after = next
		}
		want := 128
		if collection == "subscriptions" {
			want = 256
		}
		if len(seen) != want {
			t.Fatal("pagination lost an authorized item", collection, len(seen), want)
		}
	}
	call := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: wave.Target()})
	call.CallID = "legacy-overview"
	if reply, err := gateway.Handle(ctx, call); err != nil || len(reply.Content) > contract.MaxInline {
		t.Fatal("old request shape could not retrieve overview", err)
	}
}
