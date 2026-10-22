package control

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestLoopExplicitInterruptStopsOnlyNamedTask(t *testing.T) {
	ctx := context.Background()
	k, first := taskFixture(t)
	tasks, err := k.Tasks(ctx, first.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	var second contract.SessionBinding
	for _, task := range tasks {
		if task.SessionID != first.ID {
			second, err = k.SessionBinding(ctx, task.SessionID)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = os.MkdirAll(second.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	p1, p2 := fake(first), fake(second)
	p1.manualCompletion, p1.turnStarted, p1.threadID = true, make(chan struct{}), "first"
	p2.manualCompletion, p2.turnStarted, p2.threadID = true, make(chan struct{}), "second"
	source := newSource(workRequest(first), workRequest(second))
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(_ context.Context, launch Launch) (Process, error) {
		if launch.Binding.ID == first.ID {
			return p1, nil
		}
		return p2, nil
	})}, source)
	l.ReconcileEvery = time.Hour
	loopStart(t, l)
	await(t, p1.turnStarted)
	await(t, p2.turnStarted)
	b, err := k.SessionBinding(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.RequestSessionInterrupt(ctx, storage.SessionInterruptRequest{Key: "interrupt", Target: first.Target, SessionID: first.ID, RunID: workRequest(first).Execute.RunID, PreparationID: b.PreparationID, Reason: "explicit operator stop"}); err != nil {
		t.Fatal(err)
	}
	l.Wake()
	c := await(t, source.completed)
	if c.SessionID != first.ID || !c.Outcome.Stopped || c.Err == nil {
		t.Fatal(c)
	}
	assertCapacity(t, k, 1)
	p2.mu.Lock()
	closed := p2.closed
	p2.mu.Unlock()
	if closed {
		t.Fatal("interruption stopped an independent Task")
	}
	task, err := k.Task(ctx, first.TaskID)
	if err != nil || task.State != contract.TaskBlocked {
		t.Fatal(task, err)
	}
	completeProcess(p2)
	c = await(t, source.completed)
	if c.SessionID != second.ID || c.Err != nil || !c.Outcome.Stopped {
		t.Fatal(c)
	}
}

func TestStaleInterruptionDoesNotCancelNewRecoveryPreparationForSameRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	k, b := taskFixture(t)
	p := fake(b)
	p.manualCompletion, p.turnStarted = true, make(chan struct{})
	r := Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return p, nil })}
	type result struct {
		out Outcome
		err error
	}
	done := make(chan result, 1)
	go func() { out, err := r.Execute(ctx, request(b)); done <- result{out, err} }()
	await(t, p.turnStarted)
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	i, err := k.RequestSessionInterrupt(ctx, storage.SessionInterruptRequest{Key: "stop", Target: b.Target, SessionID: b.ID, RunID: "run", PreparationID: current.PreparationID, Reason: "explicit stop"})
	if err != nil {
		t.Fatal(err)
	}
	// A normal completion can race the saved stop request. The intent is still
	// pending when the same physical run is later read with a new preparation.
	completeProcess(p)
	old := await(t, done)
	if old.err != nil || !old.out.Stopped {
		t.Fatal(old)
	}
	current, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	recovery, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "recovery", RunID: "run", Mode: "recovery", Binding: current})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.BeginManagedProcessLaunch(ctx, "recovery"); err != nil {
		t.Fatal(err)
	}
	identity := process.Identity{PID: 77, BootID: "recovery-fixture", StartTicks: 77}
	if err = k.RecordManagedProcessStarted(ctx, "recovery", identity); err != nil {
		t.Fatal(err)
	}
	canceled := false
	flights := map[string]*flight{b.ID: {work: SessionWork{Recovery: &RecoveryRequest{SessionID: b.ID, Run: old.out.Run}}, cancel: func() { canceled = true }}}
	l := NewLoop(Runtime{Store: k}, nil)
	cursor := ""
	if err = l.maintainInterrupts(ctx, flights, &cursor); err != nil || canceled {
		t.Fatal("historical stop canceled read-only recovery", canceled, err)
	}
	assertCapacity(t, k, 1)
	var saved storage.SessionInterruption
	if err = k.Record(ctx, "session_interrupt", i.ID, &saved); err != nil || saved.State != "stopped" {
		t.Fatal(saved, err)
	}
	if err = k.ConfirmManagedProcessStopped(ctx, storage.ManagedStopProof{OwnerID: "recovery", Disposition: "group_stopped", Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if recovery.Binding.PreparationID == i.Request.PreparationID {
		t.Fatal("recovery reused original preparation")
	}
}

func TestLoopRestartConsumesStoredInterruptWithoutStartingModel(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	reservation, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "survivor", RunID: "survivor-run", Mode: "execute", Binding: b})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.BeginManagedProcessLaunch(ctx, "survivor"); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() })
	identity := process.Identify(cmd.Process.Pid)
	if err = k.RecordManagedProcessStarted(ctx, "survivor", identity); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkManagedThreadRPC(ctx, "survivor", "thread/start"); err != nil {
		t.Fatal(err)
	}
	if err = k.BindSessionThread(ctx, reservation.Binding, "survivor-thread"); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "survivor-run", ThreadID: b.ThreadID, State: "starting", Started: time.Now().UTC(), BaselineKnown: true}
	if err = k.ReserveSessionRun(ctx, b, request(b).Snapshot, run); err != nil {
		t.Fatal(err)
	}
	i, err := k.RequestSessionInterrupt(ctx, storage.SessionInterruptRequest{Key: "saved", Target: b.Target, SessionID: b.ID, RunID: run.ID, PreparationID: b.PreparationID, Reason: "saved before restart"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = storage.OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.Close() })
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) {
		t.Error("stop reconciliation started a model")
		return nil, ErrNotStarted
	})}, newSource())
	l.ReconcileEvery = 10 * time.Millisecond
	loopStart(t, l)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var saved storage.SessionInterruption
		if err = k.Record(ctx, "session_interrupt", i.ID, &saved); err != nil {
			t.Fatal(err)
		}
		if saved.State == "stopped" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restart did not finish stored interruption", saved)
		}
		time.Sleep(time.Millisecond)
	}
	assertCapacity(t, k, 0)
	if alive, err := process.GroupAlive(identity); err != nil || alive {
		t.Fatal(alive, err)
	}
	task, err := k.Task(ctx, b.TaskID)
	if err != nil || task.State != contract.TaskBlocked {
		t.Fatal(task, err)
	}
	// Stopping is independent of the outcome: original starting/unknown history
	// remains active and must be reconciled before any retry is authorized.
	var state string
	if err = k.DB.QueryRow("SELECT state FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&state); err != nil || state != "starting" {
		t.Fatal(state, err)
	}
}
