package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func taskFixture(t *testing.T) (*storage.KernelStore, contract.SessionBinding) {
	t.Helper()
	return taskFixtureWithLimits(t, domain.DefaultKernelLimits())
}
func taskFixtureWithLimits(t *testing.T, limits domain.KernelLimits) (*storage.KernelStore, contract.SessionBinding) {
	t.Helper()
	ctx := context.Background()
	k, err := storage.OpenKernel(filepath.Join(t.TempDir(), "private", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.Close() })
	if _, err = k.CreateOrchestrator(ctx, "main", "configured", t.TempDir(), "read-only"); err != nil {
		t.Fatal(err)
	}
	w, err := k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "request", OriginalRequest: "research", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "report"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: limits}})
	if err != nil {
		t.Fatal(err)
	}
	task := contract.TaskSpec{ID: "work", Kind: "work", Instruction: "research", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	other := task
	other.ID = "other"
	p, err := k.AdoptPlan(ctx, w.Target(), "initial", contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "research", Tasks: []contract.TaskSpec{task, other}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"work", "other"}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := k.Task(ctx, p.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, stored.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(b.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	return k, b
}
func request(b contract.SessionBinding) ExecuteRequest {
	raw := []byte(`{"schema_version":1,"controller":"fixture"}`)
	return ExecuteRequest{SessionID: b.ID, RunID: "run", Snapshot: contract.InputSnapshot{ID: "snapshot", Input: raw, Digest: fmt.Sprintf("%x", sha256.Sum256(raw))}, OutputSchema: map[string]any{"type": "object"}, Validate: func(raw []byte, _ contract.SessionBinding) error {
		if string(raw) != `{"ok":true}` {
			return errors.New("invalid result")
		}
		return nil
	}}
}

type factoryFunc func(context.Context, Launch) (Process, error)

func (f factoryFunc) Open(ctx context.Context, l Launch) (Process, error) { return f(ctx, l) }

type fakeProcess struct {
	mu                                        sync.Mutex
	queue                                     chan codex.Message
	methods                                   []string
	binding                                   contract.SessionBinding
	threadID                                  string
	turnID                                    string
	final                                     string
	manualCompletion                          bool
	turnStarted                               chan struct{}
	threadReadReply                           json.RawMessage
	threadReadStarted                         chan struct{}
	holdThreadRead                            bool
	stopped, closed, initializeFail, turnFail bool
}

func fake(b contract.SessionBinding) *fakeProcess {
	return &fakeProcess{binding: b, queue: make(chan codex.Message, 16), stopped: true, threadID: "thread"}
}
func (p *fakeProcess) ProcessIdentity() process.Identity {
	return process.Identity{PID: 12345, BootID: "fixture", StartTicks: 12345}
}
func (p *fakeProcess) StartedAt() time.Time { return time.Now().UTC() }
func (p *fakeProcess) GroupStopped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed && p.stopped
}
func (p *fakeProcess) Close() error { p.mu.Lock(); defer p.mu.Unlock(); p.closed = true; return nil }
func (p *fakeProcess) Next(ctx context.Context) (codex.Message, error) {
	select {
	case m := <-p.queue:
		return m, nil
	case <-ctx.Done():
		return codex.Message{}, ctx.Err()
	}
}
func (p *fakeProcess) Send(value any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	raw, _ := json.Marshal(value)
	var m codex.Message
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	p.methods = append(p.methods, m.Method)
	if len(m.ID) == 0 {
		return nil
	}
	var result any
	switch m.Method {
	case "initialize":
		if p.initializeFail {
			return errors.New("initialize failed")
		}
		result = map[string]any{}
	case "thread/start", "thread/resume":
		result = map[string]any{"thread": map[string]any{"id": p.threadID, "cwd": p.binding.CWD, "turns": []any{}}, "model": p.binding.Model}
	case "turn/start":
		if p.turnFail {
			return errors.New("lost turn reply")
		}
		result = map[string]any{"turn": map[string]any{"id": p.resultTurnID(), "status": "inProgress"}}
	case "thread/read":
		if p.threadReadStarted != nil {
			close(p.threadReadStarted)
		}
		if p.holdThreadRead {
			return nil
		}
		if p.threadReadReply != nil {
			p.queue <- codex.Message{ID: m.ID, Result: p.threadReadReply}
			return nil
		}
		result = map[string]any{"thread": map[string]any{"id": p.threadID, "cwd": p.binding.CWD, "turns": []any{map[string]any{"id": p.resultTurnID(), "status": "completed", "items": []any{map[string]string{"type": "agentMessage", "text": p.resultText()}}}}}}
	default:
		return fmt.Errorf("unexpected method %s", m.Method)
	}
	reply, _ := json.Marshal(result)
	p.queue <- codex.Message{ID: m.ID, Result: reply}
	if m.Method == "turn/start" {
		if p.turnStarted != nil {
			close(p.turnStarted)
		}
		if p.manualCompletion {
			return nil
		}
		params, _ := json.Marshal(map[string]any{"threadId": p.threadID, "turn": map[string]any{"id": p.resultTurnID(), "status": "completed", "items": []any{map[string]string{"type": "agentMessage", "text": p.resultText()}}}})
		p.queue <- codex.Message{Method: "turn/completed", Params: params}
	}
	return nil
}
func assertCapacity(t *testing.T, k *storage.KernelStore, want int) {
	t.Helper()
	c, err := k.ExecutionCapacity(context.Background())
	if err != nil || c.HeldProcessSlots != want {
		t.Fatal(c, err)
	}
}
func TestRuntimeLazilyReservesBeforeSpawnAndReleasesAfterStop(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	p := fake(b)
	calls := 0
	r := Runtime{Store: k, Factory: factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		calls++
		assertCapacity(t, k, 1)
		saved, err := k.ManagedProcess(ctx, l.Process.Request.OwnerID)
		if err != nil || saved.State != "launching" || saved.PreparationID != l.Binding.PreparationID {
			t.Fatal(saved, err)
		}
		return p, nil
	})}
	out, err := r.Execute(ctx, request(b))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Stopped || out.Run.State != "completed" || calls != 1 {
		t.Fatal(out, calls)
	}
	assertCapacity(t, k, 0)
	if !reflect.DeepEqual(p.methods, []string{"initialize", "initialized", "thread/start", "turn/start"}) {
		t.Fatal(p.methods)
	}
	metrics, err := k.ProcessMetrics(ctx, out.PreparationID)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.PID == nil || metrics.SpawnedAt == nil || metrics.ReadyAt == nil || metrics.StoppedAt == nil {
		t.Fatal(metrics)
	}
	if metrics.PeakRSSBytes != nil || metrics.RSSMethod != "" || metrics.RSSIntervalNanos != 0 {
		t.Fatal("fake identity produced a memory measurement", metrics)
	}
	task, err := k.Task(ctx, b.TaskID)
	if err != nil || task.Attempts != 1 {
		t.Fatal(task, err)
	}
}
func TestRuntimeFailureProofControlsCapacity(t *testing.T) {
	for _, kind := range []string{"not_started", "unknown", "initialize", "stop_unknown"} {
		t.Run(kind, func(t *testing.T) {
			k, b := taskFixture(t)
			p := fake(b)
			p.initializeFail = kind == "initialize"
			p.stopped = kind != "stop_unknown"
			f := factoryFunc(func(context.Context, Launch) (Process, error) {
				switch kind {
				case "not_started":
					return nil, errors.Join(ErrNotStarted, errors.New("no exec"))
				case "unknown":
					return nil, errors.New("ambiguous launch")
				}
				return p, nil
			})
			out, err := (Runtime{Store: k, Factory: f}).Execute(context.Background(), request(b))
			if err == nil {
				t.Fatal("failure accepted")
			}
			want := 0
			if kind == "unknown" || kind == "stop_unknown" {
				want = 1
				if !errors.Is(err, process.ErrUnconfirmed) {
					t.Fatal(err)
				}
			}
			assertCapacity(t, k, want)
			if out.Stopped != (want == 0) {
				t.Fatal(out, err)
			}
			if kind == "initialize" && !reflect.DeepEqual(p.methods, []string{"initialize"}) {
				t.Fatal(p.methods)
			}
		})
	}
}
func TestRuntimeRejectsInvalidInputBeforeFactory(t *testing.T) {
	k, b := taskFixture(t)
	calls := 0
	r := Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { calls++; return nil, ErrNotStarted })}
	req := request(b)
	req.Snapshot.Digest = "bad"
	if _, err := r.Execute(context.Background(), req); err == nil {
		t.Fatal("bad digest admitted")
	}
	if calls != 0 {
		t.Fatal(calls)
	}
	assertCapacity(t, k, 0)
}
func TestRuntimeRecoveryReadsSavedTurnWithoutNewAttempt(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	p := fake(b)
	p.turnFail = true
	out, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return p, nil })}).Execute(ctx, request(b))
	if err == nil || !out.Stopped || out.Run.State != "unknown" {
		t.Fatal(out, err)
	}
	before, err := k.Task(ctx, b.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	recovery := fake(b)
	r := Runtime{Store: k, Factory: factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		if l.Process.Request.Mode != "recovery" {
			t.Fatal(l)
		}
		return recovery, nil
	})}
	recovered, err := r.Reconcile(ctx, b.ID, out.Run, request(b).Validate)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered.Stopped || recovered.Run.State != "completed" || recovered.Run.ID != out.Run.ID {
		t.Fatal(recovered)
	}
	after, err := k.Task(ctx, b.TaskID)
	if err != nil || after.Attempts != before.Attempts {
		t.Fatal(before, after, err)
	}
	if !reflect.DeepEqual(recovery.methods, []string{"initialize", "initialized", "thread/read"}) {
		t.Fatal(recovery.methods)
	}
	var observationRaw []byte
	if err = k.DB.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind='session_history_observation' AND work_item_id=?", b.ID).Scan(&observationRaw); err != nil {
		t.Fatal(err)
	}
	var observation storage.HistoryObservation
	if err = json.Unmarshal(observationRaw, &observation); err != nil || observation.Status != "observed_turn" || observation.RunID != out.Run.ID || observation.SessionID != b.ID || observation.ResponseDigest == "" {
		t.Fatal(observation, err)
	}
	assertCapacity(t, k, 0)
}

func TestOwnerInitializeFailureNeverRetriesAndCloseIsConcurrent(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	p := fake(b)
	p.initializeFail = true
	calls := 0
	o, err := newOwner(ctx, k, factoryFunc(func(context.Context, Launch) (Process, error) { calls++; return p, nil }), b.ID, "run", "execute", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.reserve(ctx, b); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = o.Call(ctx, "thread/start", map[string]any{}, nil); err == nil {
			t.Fatal("failed initialize reused")
		}
	}
	if calls != 1 || !reflect.DeepEqual(p.methods, []string{"initialize"}) {
		t.Fatal(calls, p.methods)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := o.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	assertCapacity(t, k, 0)
}
func TestRecoveryRejectsWritesBeforeSpawn(t *testing.T) {
	k, b := taskFixture(t)
	calls := 0
	o, err := newOwner(context.Background(), k, factoryFunc(func(context.Context, Launch) (Process, error) { calls++; return nil, ErrNotStarted }), b.ID, "run", "recovery", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	for _, method := range []string{"thread/start", "thread/resume", "turn/start", "turn/interrupt", "thread/fork"} {
		if err = o.Call(context.Background(), method, map[string]any{}, nil); err == nil {
			t.Fatal(method)
		}
	}
	if calls != 0 {
		t.Fatal(calls)
	}
}

func (p *fakeProcess) resultText() string {
	if p.final != "" {
		return p.final
	}
	return `{"ok":true}`
}

func (p *fakeProcess) resultTurnID() string {
	if p.turnID != "" {
		return p.turnID
	}
	return "turn"
}
