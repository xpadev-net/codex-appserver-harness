package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func sourceWaiting(t *testing.T, k *storage.KernelStore, b contract.SessionBinding) string {
	t.Helper()
	var subscription string
	if err := k.DB.QueryRow("SELECT id FROM kernel_subscriptions WHERE session_id=? AND resource LIKE 'kernel:mailbox:%' ORDER BY id LIMIT 1", b.ID).Scan(&subscription); err != nil {
		t.Fatal(err)
	}
	result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "waiting", Artifacts: []string{}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{subscription}}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
func sourceFactory(t *testing.T, k *storage.KernelStore, turn string) (Factory, *[]*fakeProcess) {
	t.Helper()
	processes := []*fakeProcess{}
	return factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		p := fake(l.Binding)
		p.threadID = l.Binding.ThreadID
		if p.threadID == "" {
			p.threadID = "thread-" + l.Binding.ID
		}
		p.turnID = turn
		p.final = sourceWaiting(t, k, l.Binding)
		processes = append(processes, p)
		return p, nil
	}), &processes
}
func sourceTaskWork(t *testing.T, s *KernelSource, sid string) SessionWork {
	t.Helper()
	page, err := s.Ready(context.Background(), "", 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, work := range page.Work {
		id, _, _ := work.identity()
		if id == sid {
			return work
		}
	}
	t.Fatalf("no work for %s: %+v", sid, page)
	return SessionWork{}
}
func ensureSourceTaskDirectories(t *testing.T, k *storage.KernelStore, waveID string) {
	t.Helper()
	tasks, err := k.Tasks(context.Background(), waveID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		b, err := k.SessionBinding(context.Background(), task.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.MkdirAll(b.CWD, 0700); err != nil {
			t.Fatal(err)
		}
	}
}
func TestKernelSourceInitialTaskAndNotificationResumeUseSavedInputs(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	ensureSourceTaskDirectories(t, k, b.Target.WaveID)
	s := NewKernelSource(k)
	page, err := s.Ready(ctx, "", 2)
	if err != nil || len(page.Work) != 2 {
		t.Fatal(page, err)
	}
	assertCapacity(t, k, 0)
	first := sourceTaskWork(t, s, b.ID)
	if first.Execute == nil || first.Execute.OutputSchema == nil || first.Execute.Validate == nil || len(first.Execute.Snapshot.Input) == 0 {
		t.Fatal(first)
	}
	factory, _ := sourceFactory(t, k, "first-turn")
	runtime := Runtime{Store: k, Factory: factory}
	out, err := runtime.Execute(ctx, *first.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, Completion{SessionID: b.ID, RunID: first.Execute.RunID, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	state, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || state.Binding.TaskState != contract.TaskWaiting || state.Run != nil {
		t.Fatal(state, err)
	}
	page, err = s.Ready(ctx, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, work := range page.Work {
		sid, _, _ := work.identity()
		if sid == b.ID {
			t.Fatal("waiting without notification restarted a model")
		}
	}
	m, err := k.SendMessage(ctx, storage.MessageRequest{SenderSessionID: b.ParentSessionID, Target: b.Target, Key: "resume", Recipient: b.TaskID, Body: "please continue the research"})
	if err != nil {
		t.Fatal(err)
	}
	next := sourceTaskWork(t, s, b.ID)
	if next.Execute == nil || next.Execute.RunID == first.Execute.RunID || !strings.Contains(string(next.Execute.Snapshot.Input), m.ID) {
		t.Fatal(next)
	}
	state, err = k.SessionDispatchState(ctx, b.ID)
	if err != nil || state.Wake == nil || state.Wake.Intent.State != "reserved" || len(next.Execute.Snapshot.Input) > contract.MaxSessionInput {
		t.Fatal(state, err)
	}
	factory, _ = sourceFactory(t, k, "second-turn")
	runtime.Factory = factory
	out, err = runtime.Execute(ctx, *next.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, Completion{SessionID: b.ID, RunID: next.Execute.RunID, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	after, err := k.SessionBinding(ctx, b.ID)
	if err != nil || after.ThreadID != state.Binding.ThreadID {
		t.Fatal("waiting changed logical thread", after, err)
	}
	// A duplicate completion carrying an earlier transport/validation error cannot
	// create a dispatch hold after the saved outcome was already applied.
	if err = s.Complete(ctx, Completion{SessionID: b.ID, RunID: next.Execute.RunID, Err: errors.New("old error")}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='kernel_dispatch_hold' AND id=?", b.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	assertCapacity(t, k, 0)
}
func TestKernelSourceRestartAppliesCompletedRunWithoutAnotherModel(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	ensureSourceTaskDirectories(t, k, b.Target.WaveID)
	source := NewKernelSource(k)
	work := sourceTaskWork(t, source, b.ID)
	factory, _ := sourceFactory(t, k, "saved-turn")
	if _, err := (Runtime{Store: k, Factory: factory}).Execute(ctx, *work.Execute); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	var err error
	k, err = storage.OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	source = NewKernelSource(k)
	page, err := source.Ready(ctx, "", 128)
	if err != nil || !page.Recheck {
		t.Fatal("startup application did not request an immediate recheck", page, err)
	}
	for _, candidate := range page.Work {
		sid, _, _ := candidate.identity()
		if sid == b.ID {
			t.Fatal("completed saved run started another model")
		}
	}
	state, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || state.Run != nil || state.Binding.TaskState != contract.TaskWaiting {
		t.Fatal(state, err)
	}
}
func TestKernelSourceUnknownRunOnlyReadsHistoryAndHasCooldown(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	ensureSourceTaskDirectories(t, k, b.Target.WaveID)
	s := NewKernelSource(k)
	work := sourceTaskWork(t, s, b.ID)
	failed := fake(b)
	failed.threadID = "thread-unknown"
	failed.turnFail = true
	out, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return failed, nil })}).Execute(ctx, *work.Execute)
	if err == nil || out.Run.State != "unknown" || !out.Stopped {
		t.Fatal(out, err)
	}
	forged := out
	forged.Run.State = "completed"
	forged.Run.Final = sourceWaiting(t, k, b)
	if err = s.Complete(ctx, Completion{SessionID: b.ID, RunID: work.Execute.RunID, Outcome: forged, Err: err}); err != nil {
		t.Fatal(err)
	}
	recovery := sourceTaskWork(t, s, b.ID)
	if recovery.Execute != nil || recovery.Recovery == nil || recovery.Recovery.Run.ID != out.Run.ID {
		t.Fatal("unknown run replayed as execute", recovery)
	}
	page, err := s.Ready(ctx, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range page.Work {
		sid, _, _ := candidate.identity()
		if sid == b.ID {
			t.Fatal("unknown history is being polled without cooldown")
		}
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	p := fake(current)
	p.threadID = current.ThreadID
	p.turnID = "recovered-turn"
	p.final = sourceWaiting(t, k, current)
	recovered, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return p, nil })}).Reconcile(ctx, b.ID, recovery.Recovery.Run, recovery.Recovery.Validate)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, Completion{SessionID: b.ID, RunID: out.Run.ID, Outcome: recovered}); err != nil {
		t.Fatal(err)
	}
	for _, method := range p.methods {
		if method == "turn/start" || method == "thread/start" || method == "thread/resume" {
			t.Fatal("recovery performed a writing RPC", p.methods)
		}
	}
	state, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || state.Run != nil || state.Binding.TaskState != contract.TaskWaiting {
		t.Fatal(state, err)
	}
}
func TestKernelSourceShrinksWakeWithoutLosingLargeMessages(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	s := NewKernelSource(k)
	work := sourceTaskWork(t, s, b.ID)
	factory, _ := sourceFactory(t, k, "wait-turn")
	out, err := (Runtime{Store: k, Factory: factory}).Execute(ctx, *work.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, Completion{SessionID: b.ID, RunID: work.Execute.RunID, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err = k.SendMessage(ctx, storage.MessageRequest{SenderSessionID: b.ParentSessionID, Target: b.Target, Key: fmt.Sprint("large-", i), Recipient: b.TaskID, Body: strings.Repeat("\x00", contract.MaxInline)}); err != nil {
			t.Fatal(err)
		}
	}
	next := sourceTaskWork(t, s, b.ID)
	if next.Execute == nil || len(next.Execute.Snapshot.Input) > contract.MaxSessionInput {
		t.Fatal(next)
	}
	state, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || state.Wake == nil || len(state.Wake.Intent.NotificationIDs) != 2 {
		t.Fatal(state, err)
	}
	pending, err := k.PendingWakeNotifications(ctx, b.ID, 16)
	if err != nil || len(pending) != 2 {
		t.Fatal("oversized batch consumed unselected messages", pending, err)
	}
}

type observingKernelSource struct {
	*KernelSource
	completed chan Completion
}

func (s *observingKernelSource) Complete(ctx context.Context, c Completion) error {
	if err := s.KernelSource.Complete(ctx, c); err != nil {
		return err
	}
	s.completed <- c
	return nil
}
func TestKernelLoopStartsTasksImmediatelyAfterInitialMainApplication(t *testing.T) {
	ctx := context.Background()
	k, err := storage.OpenKernel(filepath.Join(t.TempDir(), "private", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err = k.CreateOrchestrator(ctx, "main", "configured", t.TempDir(), "read-only"); err != nil {
		t.Fatal(err)
	}
	wave, err := k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "direct", OriginalRequest: "research independent sources", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "provide evidence"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: domain.DefaultKernelLimits()}})
	if err != nil {
		t.Fatal(err)
	}
	source := &observingKernelSource{KernelSource: NewKernelSource(k), completed: make(chan Completion, 8)}
	var mu sync.Mutex
	calls := map[string]int{}
	factory := factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		mu.Lock()
		calls[l.Binding.ID]++
		mu.Unlock()
		p := fake(l.Binding)
		p.threadID = "thread-" + l.Binding.ID
		p.turnID = "turn-" + l.Process.Request.RunID
		if l.Binding.Kind == "orchestrator" {
			first := contract.TaskSpec{ID: "first", Kind: "work", Instruction: "research first source", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
			second := first
			second.ID = "second"
			plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "research", Tasks: []contract.TaskSpec{first, second}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"first", "second"}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
			final := contract.MainResult{SchemaVersion: 1, SessionID: l.Binding.ID, Decisions: []contract.MainDecision{{Target: wave.Target(), Action: "plan", Plan: &plan, SubscriptionIDs: []string{}, Summary: "decompose research", Remaining: []string{}, Blockers: []string{}}}}
			raw, _ := json.Marshal(final)
			p.final = string(raw)
		} else {
			p.final = sourceWaiting(t, k, l.Binding)
		}
		return p, nil
	})
	loop := NewLoop(Runtime{Store: k, Factory: factory}, source)
	loop.ReconcileEvery = time.Hour
	cancel, _ := loopStart(t, loop)
	defer cancel()
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		completion := await(t, source.completed)
		if completion.Err != nil {
			t.Fatal(completion.Err)
		}
		seen[completion.SessionID] = true
	}
	if len(seen) != 3 || !seen["main"] {
		t.Fatal("main did not fan out new Tasks immediately", seen)
	}
	mu.Lock()
	defer mu.Unlock()
	for sid, count := range calls {
		if count != 1 {
			t.Fatal("unexpected extra model/process call", sid, count)
		}
	}
	tasks, err := k.Tasks(ctx, wave.ID)
	if err != nil || len(tasks) != 2 {
		t.Fatal(tasks, err)
	}
	for _, task := range tasks {
		if task.State != contract.TaskWaiting {
			t.Fatal(task)
		}
	}
	assertCapacity(t, k, 0)
}

func TestKernelSourceInvalidMainOutputDoesNotBlockOtherSessions(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	ensureSourceTaskDirectories(t, k, b.Target.WaveID)
	if _, err := k.CreateOrchestrator(ctx, "bad-main", "configured", t.TempDir(), "read-only"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: "bad-main", Source: "direct", SourceID: "bad-request", OriginalRequest: "research", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "evidence"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: domain.DefaultKernelLimits()}}); err != nil {
		t.Fatal(err)
	}
	s := NewKernelSource(k)
	work := sourceTaskWork(t, s, "bad-main")
	p := fake(contract.SessionBinding{})
	p.final = `{"invalid":true}`
	out, err := (Runtime{Store: k, Factory: factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		p.binding = l.Binding
		p.threadID = "bad-thread"
		return p, nil
	})}).Execute(ctx, *work.Execute)
	if err == nil || out.Run.State != "completed" {
		t.Fatal(out, err)
	}
	if err = s.Complete(ctx, Completion{SessionID: "bad-main", RunID: work.Execute.RunID, Outcome: out, Err: err}); err != nil {
		t.Fatal(err)
	}
	page, err := s.Ready(ctx, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Work) != 2 || page.Recheck {
		t.Fatal("held main consumed another session's progress", page)
	}
	for _, candidate := range page.Work {
		sid, _, _ := candidate.identity()
		if sid == "bad-main" {
			t.Fatal("invalid output replayed a model")
		}
	}
	state, err := k.SessionDispatchState(ctx, "bad-main")
	if err != nil || state.Run == nil || state.Run.ID != work.Execute.RunID {
		t.Fatal("invalid output/input was silently discarded", state, err)
	}
}
func TestKernelSourcePreRunFailureIsHeldAndCompletionReplayIsScoped(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	s := NewKernelSource(k)
	work := sourceTaskWork(t, s, b.ID)
	out, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) {
		return nil, fmt.Errorf("bad profile: %w", ErrNotStarted)
	})}).Execute(ctx, *work.Execute)
	if err == nil || !out.Stopped {
		t.Fatal(out, err)
	}
	if err = s.Complete(ctx, Completion{SessionID: b.ID, RunID: work.Execute.RunID, Outcome: out, Err: err}); err != nil {
		t.Fatal(err)
	}
	page, err := s.Ready(ctx, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range page.Work {
		sid, _, _ := candidate.identity()
		if sid == b.ID {
			t.Fatal("permanent factory failure retried in a busy loop")
		}
	}
	if err = s.Complete(ctx, Completion{SessionID: b.ParentSessionID, RunID: "nonexistent"}); err != nil {
		t.Fatal("empty callback changed unrelated main", err)
	}
}

type sourceToolProcess struct {
	*fakeProcess
	replies chan codex.Message
}

func (p *sourceToolProcess) Send(value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var message codex.Message
	if err = json.Unmarshal(raw, &message); err != nil {
		return err
	}
	if message.Method == "" && len(message.ID) > 0 {
		p.replies <- message
		return nil
	}
	return p.fakeProcess.Send(value)
}
func TestKernelToolMessageWakesChildWhileMainTurnContinues(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	ensureSourceTaskDirectories(t, k, b.Target.WaveID)
	source := &observingKernelSource{KernelSource: NewKernelSource(k), completed: make(chan Completion, 8)}
	first := sourceTaskWork(t, source.KernelSource, b.ID)
	initialFactory, _ := sourceFactory(t, k, "initial-wait")
	out, err := (Runtime{Store: k, Factory: initialFactory}).Execute(ctx, *first.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.KernelSource.Complete(ctx, Completion{SessionID: b.ID, RunID: first.Execute.RunID, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	tasks, err := k.Tasks(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.ID != b.TaskID {
			if _, err = k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause-sibling", Target: b.Target, TaskID: task.ID, Action: "pause", Reason: "focus dialog fixture"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err = k.SendMessage(ctx, storage.MessageRequest{SenderSessionID: b.ID, Target: b.Target, Key: "ask-parent", Recipient: "parent", Body: "please clarify"}); err != nil {
		t.Fatal(err)
	}
	parentStarted := make(chan *sourceToolProcess, 1)
	factory := factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		p := fake(l.Binding)
		p.threadID = l.Binding.ThreadID
		if p.threadID == "" {
			p.threadID = "thread-" + l.Binding.ID
		}
		p.turnID = "turn-" + l.Process.Request.RunID
		if l.Binding.Kind == "orchestrator" {
			p.manualCompletion = true
			wrapped := &sourceToolProcess{fakeProcess: p, replies: make(chan codex.Message, 4)}
			parentStarted <- wrapped
			return wrapped, nil
		}
		p.final = sourceWaiting(t, k, l.Binding)
		return p, nil
	})
	loop := NewLoop(Runtime{Store: k, Factory: factory}, source)
	loop.ReconcileEvery = time.Hour
	cancel, _ := loopStart(t, loop)
	defer cancel()
	parent := await(t, parentStarted)
	// Wait for the saved run, then inject the native tool request into the actual
	// SessionRPC/owner handler. No direct test-only wake is sent to the Loop.
	var parentState storage.SessionDispatchState
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		parentState, err = k.SessionDispatchState(ctx, b.ParentSessionID)
		if err != nil {
			t.Fatal(err)
		}
		if parentState.Run != nil && parentState.Run.TurnID != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if parentState.Run == nil || parentState.Run.TurnID == "" {
		t.Fatal("parent did not start its turn")
	}
	args, _ := json.Marshal(contract.InstructRequest{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, Body: "here is the clarification"})
	params, _ := json.Marshal(codex.DynamicToolCall{ThreadID: parentState.Binding.ThreadID, TurnID: parentState.Run.TurnID, CallID: "clarify", Tool: "task_instruct", Arguments: args})
	parent.queue <- codex.Message{ID: json.RawMessage(`"tool-clarify"`), Method: "item/tool/call", Params: params}
	reply := await(t, parent.replies)
	if reply.Error != nil {
		t.Fatal(reply.Error)
	}
	completed := await(t, source.completed)
	if completed.SessionID != b.ID || completed.Err != nil {
		t.Fatal("child did not resume while sender was active", completed)
	}
	still, err := k.SessionDispatchState(ctx, b.ParentSessionID)
	if err != nil || still.Run == nil || still.ProcessStopped || still.Run.State != "running" {
		t.Fatal("sender turn had to finish before recipient resumed", still, err)
	}
}
