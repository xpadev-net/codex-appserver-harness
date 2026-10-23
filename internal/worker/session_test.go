package worker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// The production implementation of this port belongs to the new-DB Task_2.
type sessionStoreFixture struct {
	binding    SessionBinding
	records    map[string][]byte
	active     string
	stopped    bool
	rebindings int
	creating   bool
}

func (s *sessionStoreFixture) ReserveSessionThread(_ context.Context, b SessionBinding) error {
	if s.creating || !reflect.DeepEqual(s.binding, b) {
		return errors.New("unknown thread creation pending")
	}
	s.creating = true
	s.binding.PreparationID = "preparation"
	return nil
}

func (s *sessionStoreFixture) SessionThreadUnused(context.Context, SessionBinding) (bool, error) {
	return false, nil
}
func (s *sessionStoreFixture) SessionBinding(context.Context, string) (SessionBinding, error) {
	return s.binding, nil
}
func (s *sessionStoreFixture) BindSessionThread(_ context.Context, b SessionBinding, thread string) error {
	if !reflect.DeepEqual(s.binding, b) || (b.ThreadID != "" && b.ThreadID != thread) {
		return errors.New("binding changed")
	}
	s.binding.ThreadID = thread
	s.creating = false
	return nil
}
func (s *sessionStoreFixture) ReserveSessionRun(ctx context.Context, b SessionBinding, snapshot InputSnapshot, r Run) error {
	if !reflect.DeepEqual(s.binding, b) || s.active != "" || snapshot.ID == "" {
		return errors.New("active/changed session")
	}
	s.active = r.ID
	return s.Put(ctx, "agent_run", r.ID, b.ID, r)
}
func (s *sessionStoreFixture) RebindMainThread(_ context.Context, b SessionBinding, next, snapshot, reason string) error {
	if b.Kind != "orchestrator" || s.active != "" || !s.stopped || !reflect.DeepEqual(b, s.binding) {
		return errors.New("rebind requires stopped process and reconciled run")
	}
	s.binding.ThreadID = next
	s.binding.Generation++
	s.rebindings++
	return nil
}
func (s *sessionStoreFixture) Put(_ context.Context, kind, id, _ string, v any) error {
	b, err := json.Marshal(v)
	if err == nil {
		s.records[kind+":"+id] = b
	}
	return err
}
func (s *sessionStoreFixture) Record(_ context.Context, kind, id string, v any) error {
	b, ok := s.records[kind+":"+id]
	if !ok {
		return sql.ErrNoRows
	}
	return json.Unmarshal(b, v)
}

type driverRPC struct {
	fakeRPC
	preparedTools []contract.ToolDefinition
	thread        string
	output        string
	status        string
	failStart     bool
	failPrepare   bool
}

func (r *driverRPC) Call(ctx context.Context, method string, params, result any) error {
	r.calls = append(r.calls, method)
	switch method {
	case "thread/start", "thread/resume":
		if r.failPrepare {
			return errors.New("lost thread response")
		}
		p := params.(map[string]any)
		if tools, ok := p["dynamicTools"].([]contract.ToolDefinition); ok {
			r.preparedTools = tools
		}
		if r.thread == "" {
			r.thread = "thread"
		}
		b, _ := json.Marshal(map[string]any{"model": "configured", "thread": map[string]any{"id": r.thread, "cwd": "/work", "turns": []any{}}})
		return json.Unmarshal(b, result)
	case "turn/start":
		if r.failStart {
			return errors.New("unknown start")
		}
		return json.Unmarshal([]byte(`{"turn":{"id":"turn"}}`), result)
	case "thread/read":
		b, _ := json.Marshal(map[string]any{"thread": map[string]any{"id": r.thread, "cwd": "/work", "turns": []any{map[string]any{"id": "turn", "status": "completed", "items": []any{map[string]string{"type": "agentMessage", "text": r.output}}}}}})
		return json.Unmarshal(b, result)
	}
	return errors.New("unexpected RPC")
}
func (r *driverRPC) Next(context.Context) (codex.Message, error) {
	b, _ := json.Marshal(map[string]any{"threadId": r.thread, "turn": map[string]any{"id": "turn", "status": r.status, "items": []any{map[string]string{"type": "agentMessage", "text": r.output}}}})
	return codex.Message{Method: "turn/completed", Params: b}, nil
}
func newSessionDriverFixture(kind string) (SessionDriver, *sessionStoreFixture, *driverRPC) {
	b := SessionBinding{ID: "session", Kind: kind, CWD: "/work", Model: "configured", Sandbox: "read-only", Generation: 1}
	if kind == "task" {
		b.TaskID = "task"
		b.PhaseID = "phase"
		b.ParentSessionID = "main"
		b.TaskState = "running"
		b.Target = contract.Target{WaveID: "wave", ScopeRevision: 1, PlanRevision: 1, PolicyRevision: 1}
	}
	s := &sessionStoreFixture{binding: b, records: map[string][]byte{}}
	output := contract.TaskResult{SchemaVersion: 1, Target: b.Target, PhaseID: "phase", TaskID: "task", Outcome: "result", Artifacts: []string{"artifact"}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}}
	raw, _ := json.Marshal(output)
	rpc := &driverRPC{output: string(raw), status: "completed"}
	return SessionDriver{RPC: rpc, Store: s}, s, rpc
}
func snapshotFixture() InputSnapshot {
	data := []byte(`{"waves":[{"wave_id":"wave","scope_revision":1,"plan_revision":1}],"notifications":[]}`)
	return InputSnapshot{ID: "snapshot", Input: data, Digest: fmt.Sprintf("%x", sha256.Sum256(data))}
}
func TestCommonDriverTaskSessionLifecycle(t *testing.T) {
	ctx := context.Background()
	driver, store, rpc := newSessionDriverFixture("task")
	prepared, err := driver.Prepare(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range rpc.preparedTools {
		if strings.HasPrefix(tool.Name, "wave_") {
			t.Fatal("Task Wave tool exposed")
		}
	}
	run, err := driver.Execute(ctx, prepared, "run", snapshotFixture(), contract.Schema(contract.TaskResult{}), ValidateTaskOutput)
	if err != nil || run.State != "completed" {
		t.Fatal(run, err)
	}
	// Wait/recovery retains the thread, but completion is controller-applied only.
	store.binding.TaskState = "waiting"
	store.active = ""
	if _, err = driver.Prepare(ctx, "session"); err != nil || rpc.calls[len(rpc.calls)-1] != "thread/resume" {
		t.Fatal("incomplete Task cannot resume", err)
	}
	store.binding.TaskState = "completed"
	count := len(rpc.calls)
	if _, err = driver.Prepare(ctx, "session"); err == nil || len(rpc.calls) != count {
		t.Fatal("completed Task reused a thread")
	}
	store.binding.ID = "fresh-session"
	store.binding.TaskID = "fresh-task"
	store.binding.TaskState = "ready"
	store.binding.ThreadID = ""
	rpc.thread = "fresh-thread"
	if _, err = driver.Prepare(ctx, "fresh-session"); err != nil || rpc.calls[len(rpc.calls)-1] != "thread/start" {
		t.Fatal("new Task did not create a new session", err)
	}
}
func TestCommonDriverUnknownStartReconcilesWithoutReplay(t *testing.T) {
	ctx := context.Background()
	driver, store, rpc := newSessionDriverFixture("task")
	prepared, err := driver.Prepare(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	rpc.failStart = true
	run, err := driver.Execute(ctx, prepared, "unknown", snapshotFixture(), contract.Schema(contract.TaskResult{}), ValidateTaskOutput)
	if err == nil || run.State != "unknown" {
		t.Fatal(run, err)
	}
	count := len(rpc.calls)
	if _, err = driver.Execute(ctx, prepared, "unknown", snapshotFixture(), contract.Schema(contract.TaskResult{}), ValidateTaskOutput); err == nil || len(rpc.calls) != count {
		t.Fatal("unknown start replayed")
	}
	rpc.failStart = false
	store.binding.TaskState = "canceled"
	store.binding.Closed = true
	recovered, err := driver.Reconcile(ctx, prepared, run, ValidateTaskOutput)
	if err != nil || recovered.TurnID != "turn" || rpc.calls[len(rpc.calls)-1] != "thread/read" {
		t.Fatal(recovered, err)
	}
}

func TestUnknownThreadCreationCannotAllocateAnotherTaskThread(t *testing.T) {
	driver, _, rpc := newSessionDriverFixture("task")
	rpc.failPrepare = true
	if _, err := driver.Prepare(context.Background(), "session"); err == nil {
		t.Fatal("lost thread response accepted")
	}
	count := len(rpc.calls)
	rpc.failPrepare = false
	if _, err := driver.Prepare(context.Background(), "session"); err == nil || len(rpc.calls) != count {
		t.Fatal("unknown thread creation retried")
	}
}
func TestCommonDriverRejectsStaleSnapshotAndOutput(t *testing.T) {
	for _, mode := range []string{"snapshot_digest", "input_size", "stale_binding", "invalid_result", "wrong_task"} {
		t.Run(mode, func(t *testing.T) {
			driver, store, rpc := newSessionDriverFixture("task")
			ctx := context.Background()
			prepared, err := driver.Prepare(ctx, "session")
			if err != nil {
				t.Fatal(err)
			}
			snapshot := snapshotFixture()
			switch mode {
			case "snapshot_digest":
				snapshot.Digest = "wrong"
			case "input_size":
				snapshot.Input = []byte(`"` + strings.Repeat("x", contract.MaxSessionInput) + `"`)
				snapshot.Digest = fmt.Sprintf("%x", sha256.Sum256(snapshot.Input))
			case "stale_binding":
				store.binding.Target.ScopeRevision++
			case "invalid_result":
				rpc.output = "done"
			case "wrong_task":
				rpc.output = strings.Replace(rpc.output, `"task_id":"task"`, `"task_id":"other"`, 1)
			}
			if _, err = driver.Execute(ctx, prepared, "run", snapshot, contract.Schema(contract.TaskResult{}), ValidateTaskOutput); err == nil {
				t.Fatal("invalid input/output accepted")
			}
		})
	}
}
func TestMainAndTaskShareDriverWithAuditedMainRebinding(t *testing.T) {
	ctx := context.Background()
	driver, store, rpc := newSessionDriverFixture("orchestrator")
	if _, err := driver.Prepare(ctx, "session"); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tool := range rpc.preparedTools {
		if tool.Name == "wave_create" {
			found = true
		}
	}
	if !found {
		t.Fatal("main missing Wave tools")
	}
	if err := driver.RebindMain(ctx, "session", "new-thread", "snapshot", "context maintenance"); err == nil {
		t.Fatal("unconfirmed process stop accepted")
	}
	store.stopped = true
	if err := driver.RebindMain(ctx, "session", "new-thread", "snapshot", "context maintenance"); err != nil || store.rebindings != 1 {
		t.Fatal(err)
	}
	task, _, _ := newSessionDriverFixture("task")
	if err := task.RebindMain(ctx, "session", "new-thread", "snapshot", "must use new Task"); err == nil {
		t.Fatal("Task changed its thread")
	}
}

func TestCommonDriverProductOutputUsesDedicatedVersionAndPreservesRawChanges(t *testing.T) {
	ctx := context.Background()
	driver, store, rpc := newSessionDriverFixture("task")
	output := contract.TaskResult{SchemaVersion: 2, Target: store.binding.Target, TaskID: store.binding.TaskID, PhaseID: store.binding.PhaseID, Outcome: "result", Artifacts: []string{}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, ProductChanges: []contract.ProductChangeReference{{WorkItemID: "work", WorkspaceID: "workspace", WorkspaceDigest: strings.Repeat("a", 64)}}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}}
	raw, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	rpc.output = string(raw)
	if err := ValidateTaskOutput(raw, store.binding); err == nil {
		t.Fatal("generic output profile accepted product schema")
	}
	if err := ValidateProductTaskOutput(raw, store.binding); err != nil {
		t.Fatal(err)
	}
	prepared, err := driver.Prepare(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	run, err := driver.Execute(ctx, prepared, "product-change", snapshotFixture(), contract.StrictOutputSchema(contract.ProductTaskResultSchema()), ValidateProductTaskOutput)
	if err != nil || run.State != "completed" || run.Final != string(raw) {
		t.Fatal("common driver rewrote proposed changes into candidate facts", run, err)
	}
	old := output
	old.SchemaVersion = 1
	old.ProductChanges = nil
	old.Artifacts = []string{"artifact"}
	legacy, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateProductTaskOutput(legacy, store.binding); err == nil {
		t.Fatal("product output profile accepted legacy final")
	}
	var injected map[string]any
	if err := json.Unmarshal(raw, &injected); err != nil {
		t.Fatal(err)
	}
	injected["product_changes"].([]any)[0].(map[string]any)["sha"] = strings.Repeat("b", 40)
	fake, err := json.Marshal(injected)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateProductTaskOutput(fake, store.binding); err == nil {
		t.Fatal("model supplied candidate SHA in workspace output")
	}
}

func TestCommonDriverVerificationOutputIsVersionLockedAndKeepsRawPlanReference(t *testing.T) {
	ctx := context.Background()
	driver, store, rpc := newSessionDriverFixture("task")
	output := contract.TaskResult{SchemaVersion: 3, Target: store.binding.Target, TaskID: store.binding.TaskID, PhaseID: store.binding.PhaseID, Outcome: "result", Artifacts: []string{"artifact-plan"}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, ProductChanges: []contract.ProductChangeReference{{WorkItemID: "work", WorkspaceID: "workspace", WorkspaceDigest: strings.Repeat("a", 64)}}, ProductVerification: &contract.ProductVerificationReference{SubjectID: "subject", SubjectDigest: strings.Repeat("b", 64), PlanArtifactID: "artifact-plan"}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}}
	raw, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateTaskOutput(raw, store.binding); err == nil {
		t.Fatal("v1 accepted verification change output")
	}
	if err := ValidateProductTaskOutput(raw, store.binding); err == nil {
		t.Fatal("v2 accepted verification change output")
	}
	if err := ValidateProductVerificationTaskOutput(raw, store.binding); err != nil {
		t.Fatal(err)
	}
	rpc.output = string(raw)
	prepared, err := driver.Prepare(ctx, "session")
	if err != nil {
		t.Fatal(err)
	}
	run, err := driver.Execute(ctx, prepared, "verification-change", snapshotFixture(), contract.StrictOutputSchema(contract.ProductVerificationTaskResultSchema()), ValidateProductVerificationTaskOutput)
	if err != nil || run.Final != string(raw) {
		t.Fatal("driver rewrote plan or sealed SHA", run, err)
	}
}
