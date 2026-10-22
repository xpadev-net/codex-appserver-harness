package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type sourceFixture struct {
	mu        sync.Mutex
	ready     []SessionWork
	completed chan Completion
	failures  int
	scans     chan struct{}
}

func newSource(work ...SessionWork) *sourceFixture {
	return &sourceFixture{ready: work, completed: make(chan Completion, 32), scans: make(chan struct{}, 32)}
}
func (s *sourceFixture) Ready(_ context.Context, after string, limit int) (WorkPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case s.scans <- struct{}{}:
	default:
	}
	n := min(limit, len(s.ready))
	p := WorkPage{Work: append([]SessionWork{}, s.ready[:n]...)}
	s.ready = s.ready[n:]
	if len(s.ready) > 0 {
		p.NextAfter = strconv.Itoa(len(s.ready))
	}
	return p, nil
}
func (s *sourceFixture) Complete(_ context.Context, c Completion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failures > 0 {
		s.failures--
		return errors.New("temporary result write failure")
	}
	s.completed <- c
	return nil
}
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
		var zero T
		return zero
	}
}
func loopStart(t *testing.T, l *Loop) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("loop did not stop")
		}
	})
	return cancel, done
}
func completeProcess(p *fakeProcess) {
	raw, _ := json.Marshal(map[string]any{"threadId": p.threadID, "turn": map[string]any{"id": "turn", "status": "completed", "items": []any{map[string]string{"type": "agentMessage", "text": `{"ok":true}`}}}})
	p.queue <- codex.Message{Method: "turn/completed", Params: raw}
}
func workRequest(b contract.SessionBinding) SessionWork {
	r := request(b)
	r.RunID = "run-" + b.ID
	r.Snapshot.ID = "snapshot-" + b.ID
	return SessionWork{Execute: &r}
}
func TestLoopStartsIndependentTaskWhileAnotherTurnIsWaiting(t *testing.T) {
	k, first := taskFixture(t)
	tasks, err := k.Tasks(context.Background(), first.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	var second contract.SessionBinding
	for _, task := range tasks {
		if task.SessionID != first.ID {
			second, err = k.SessionBinding(context.Background(), task.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if second.ID == "" {
		t.Fatal("second independent task missing")
	}
	if err = os.MkdirAll(second.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	p1 := fake(first)
	p1.threadID = "first-thread"
	p1.manualCompletion = true
	p1.turnStarted = make(chan struct{})
	p2 := fake(second)
	p2.threadID = "second-thread"
	p2.turnStarted = make(chan struct{})
	source := newSource(workRequest(first), workRequest(second))
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(_ context.Context, launch Launch) (Process, error) {
		if launch.Binding.ID == first.ID {
			return p1, nil
		}
		return p2, nil
	})}, source)
	l.ReconcileEvery = time.Hour
	cancel, _ := loopStart(t, l)
	await(t, p1.turnStarted)
	await(t, p2.turnStarted)
	c := await(t, source.completed)
	if c.SessionID != second.ID || c.Err != nil || !c.Outcome.Stopped {
		t.Fatal(c)
	}
	assertCapacity(t, k, 1)
	completeProcess(p1)
	c = await(t, source.completed)
	if c.SessionID != first.ID || c.Err != nil {
		t.Fatal(c)
	}
	cancel()
}
func TestLoopCompletionRetryDoesNotExecuteAgain(t *testing.T) {
	k, b := taskFixture(t)
	source := newSource(workRequest(b))
	source.failures = 1
	reported := make(chan error, 8)
	p := fake(b)
	calls := 0
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { calls++; return p, nil })}, source)
	l.ReconcileEvery = time.Hour
	l.Report = func(err error) { reported <- err }
	loopStart(t, l)
	await(t, reported)
	l.Wake()
	c := await(t, source.completed)
	if c.Err != nil || calls != 1 {
		t.Fatal(c, calls)
	}
	assertCapacity(t, k, 0)
}
func TestLoopRecoversLostHintByPeriodicScan(t *testing.T) {
	k, b := taskFixture(t)
	source := newSource()
	p := fake(b)
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return p, nil })}, source)
	l.ReconcileEvery = 15 * time.Millisecond
	loopStart(t, l)
	await(t, source.scans)
	source.mu.Lock()
	source.ready = append(source.ready, workRequest(b))
	source.mu.Unlock() // durable source change, intentionally no Wake.
	c := await(t, source.completed)
	if c.Err != nil {
		t.Fatal(c.Err)
	}
}
func TestLoopShutdownWaitsForStopAndPersistsUnknownRun(t *testing.T) {
	k, b := taskFixture(t)
	p := fake(b)
	p.manualCompletion = true
	p.turnStarted = make(chan struct{})
	source := newSource(workRequest(b))
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return p, nil })}, source)
	l.ReconcileEvery = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	await(t, p.turnStarted)
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c := await(t, source.completed)
	if !c.Outcome.Stopped || c.Outcome.Run.State != "unknown" {
		t.Fatal(c)
	}
	assertCapacity(t, k, 0)
	var saved contract.SessionRun
	if err := k.Record(context.Background(), "agent_run", c.RunID, &saved); err != nil || saved.State != "unknown" {
		t.Fatal(saved, err)
	}
}
func TestLoopStartupReleasesOnlyProvenUnsentPreparation(t *testing.T) {
	for _, state := range []string{"admitted", "launching"} {
		t.Run(state, func(t *testing.T) {
			k, b := taskFixture(t)
			ctx := context.Background()
			r, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "orphan", RunID: "orphan-run", Mode: "execute", Binding: b})
			if err != nil {
				t.Fatal(err)
			}
			if state == "launching" {
				if _, err = k.BeginManagedProcessLaunch(ctx, "orphan"); err != nil {
					t.Fatal(err)
				}
			}
			source := newSource()
			l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return nil, fmt.Errorf("unexpected launch") })}, source)
			loopStart(t, l)
			await(t, source.scans)
			current, err := k.ManagedProcess(ctx, r.Process.Request.OwnerID)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if state == "admitted" {
				want = 0
				if current.State != "stopped" {
					t.Fatal(current)
				}
			} else if current.State != "launching" {
				t.Fatal(current)
			}
			assertCapacity(t, k, want)
		})
	}
}

func TestLoopDeadlinePersistsHoldThenStopsExactPreparation(t *testing.T) {
	k, b := taskFixture(t)
	var clock atomic.Int64
	k.Now = func() time.Time { return time.Now().Add(time.Duration(clock.Load())).UTC() }
	p := fake(b)
	p.manualCompletion = true
	p.turnStarted = make(chan struct{})
	source := newSource(workRequest(b))
	reported := make(chan error, 16)
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return p, nil })}, source)
	l.ReconcileEvery = time.Hour
	l.Report = func(err error) { reported <- err }
	loopStart(t, l)
	await(t, p.turnStarted)
	clock.Add(int64(49 * time.Hour))
	l.Wake()
	var c Completion
	select {
	case c = <-source.completed:
	case e := <-reported:
		t.Fatal(e)
	case <-time.After(3 * time.Second):
		w, _ := k.Wave(context.Background(), b.Target.WaveID)
		actions, _ := k.PendingDeadlineActions(context.Background(), "", 128)
		p, _ := k.PendingManagedProcesses(context.Background(), "", 128)
		t.Fatalf("deadline no stop: wave=%+v actions=%+v processes=%+v", w, actions, p)
	}
	if !c.Outcome.Stopped || c.Outcome.Run.State != "unknown" {
		t.Fatal(c)
	}
	w, err := k.Wave(context.Background(), b.Target.WaveID)
	if err != nil || w.Reason != "deadline_exceeded" {
		t.Fatal(w, err)
	}
	assertCapacity(t, k, 0)
	// Completion triggers another maintenance pass, which acknowledges only the
	// original released preparation. It cannot use cancellation as proof.
	l.Wake()
	deadline := time.After(5 * time.Second)
	for {
		page, err := k.PendingDeadlineActions(context.Background(), "", 128)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Actions) == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("stopping action not acknowledged")
		case err := <-reported:
			t.Fatal(err)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestLoopShutdownReturnsUnconfirmedStopDespiteSuccessfulResultWrite(t *testing.T) {
	k, b := taskFixture(t)
	p := fake(b)
	p.manualCompletion = true
	p.stopped = false
	p.turnStarted = make(chan struct{})
	source := newSource(workRequest(b))
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return p, nil })}, source)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	await(t, p.turnStarted)
	cancel()
	err := await(t, done)
	if !errors.Is(err, process.ErrUnconfirmed) {
		t.Fatal("shutdown concealed unresolved process", err)
	}
	c := await(t, source.completed)
	if c.Outcome.Stopped {
		t.Fatal(c)
	}
	assertCapacity(t, k, 1)
}

func TestLoopDeadlineStopsPersistedOwnerAfterRestart(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	r, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "old-daemon", RunID: "old-run", Mode: "execute", Binding: b})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.BeginManagedProcessLaunch(ctx, "old-daemon"); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	id := process.Identify(cmd.Process.Pid)
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	if err = k.RecordManagedProcessStarted(ctx, "old-daemon", id); err != nil {
		t.Fatal(err)
	}
	k.Now = func() time.Time { return time.Now().Add(49 * time.Hour) }
	source := newSource()
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return nil, errors.New("unexpected new process") })}, source)
	l.ReconcileEvery = 10 * time.Millisecond
	loopStart(t, l)
	deadline := time.After(5 * time.Second)
	for {
		p, err := k.ManagedProcess(ctx, r.Process.Request.OwnerID)
		if err != nil {
			t.Fatal(err)
		}
		actions, err := k.PendingDeadlineActions(ctx, "", 128)
		if err != nil {
			t.Fatal(err)
		}
		if p.State == "stopped" && len(actions.Actions) == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("restart left deadline process running", p, actions)
		case <-time.After(time.Millisecond):
		}
	}
	assertCapacity(t, k, 0)
	if alive, err := process.GroupAlive(id); err != nil || alive {
		t.Fatal(alive, err)
	}
}

func TestMaintenanceErrorDoesNotSuppressOtherDeadlineAction(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	work := workRequest(b)
	if _, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "live", RunID: work.Execute.RunID, Mode: "execute", Binding: b}); err != nil {
		t.Fatal(err)
	}
	if err := k.Put(ctx, "managed_process", "corrupt-unrelated", "unrelated", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	k.Now = func() time.Time { return time.Now().Add(49 * time.Hour) }
	if _, err := k.ExpireWave(ctx, b.Target.WaveID); err != nil {
		t.Fatal(err)
	}
	canceled := false
	l := NewLoop(Runtime{Store: k}, nil)
	pc, ac := "", ""
	err := l.maintain(ctx, map[string]*flight{b.ID: {work: work, cancel: func() { canceled = true }}}, &pc, &ac)
	if err == nil || !canceled {
		t.Fatal("unrelated corrupt process suppressed deadline stop", err, canceled)
	}
}
