package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Instrument the production source, without replacing its selection, saved
// context construction, admission, result application, or wake settlement.
// Only model/process/external I/O are controlled fixtures.
type measuredKernelSource struct {
	*KernelSource
	mu        sync.Mutex
	pending   map[string]time.Time
	samples   []int64
	completed chan Completion
}

func (s *measuredKernelSource) Ready(ctx context.Context, after string, limit int) (WorkPage, error) {
	page, err := s.KernelSource.Ready(ctx, after, limit)
	if err != nil {
		return page, err
	}
	for _, work := range page.Work {
		if work.Execute == nil {
			return page, errors.New("unexpected reference history recovery")
		}
		state, e := s.Store.SessionDispatchState(ctx, work.Execute.SessionID)
		if e != nil {
			return page, e
		}
		if state.Wake == nil {
			continue
		} // Initial turns are warm-up, not measured resumes.
		s.mu.Lock()
		before, ok := s.pending[work.Execute.SessionID]
		if ok {
			s.samples = append(s.samples, time.Since(before).Nanoseconds())
			delete(s.pending, work.Execute.SessionID)
		}
		s.mu.Unlock()
		if !ok {
			return page, errors.New("wake without one measured notification")
		}
		if len(state.Wake.Intent.NotificationIDs) != 1 || state.Wake.Intent.RunID != work.Execute.RunID {
			return page, errors.New("measured reservation did not bind exactly one notification")
		}
	}
	return page, nil
}
func (s *measuredKernelSource) Complete(ctx context.Context, c Completion) error {
	if c.Err != nil {
		return c.Err
	}
	tokens := int64(1) // Known stub usage only, never inferred native usage.
	if err := s.Store.ObserveRunTokens(ctx, storage.RunTokenObservation{SessionID: c.SessionID, RunID: c.RunID, ThreadID: c.Outcome.Run.ThreadID, TurnID: c.Outcome.Run.TurnID, TotalForRun: &tokens, Final: true}); err != nil {
		return err
	}
	if err := s.KernelSource.Complete(ctx, c); err != nil {
		return err
	}
	s.completed <- c
	return nil
}
func TestKernelSourceNotificationReservationReferenceLoad(t *testing.T) {
	limits := domain.DefaultKernelLimits()
	limits.TaskAttempts = 51
	k, first := taskFixtureWithLimits(t, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tasks, err := k.Tasks(ctx, first.Target.WaveID)
	if err != nil || len(tasks) != 2 {
		t.Fatal("two independent tasks required", err)
	}
	ensureSourceTaskDirectories(t, k, first.Target.WaveID)
	source := &measuredKernelSource{KernelSource: NewKernelSource(k), pending: map[string]time.Time{}, completed: make(chan Completion, 4)}
	var factoryMu sync.Mutex
	pair := make(chan struct{})
	arrivals := 0
	factory := factoryFunc(func(ctx context.Context, l Launch) (Process, error) {
		if l.Binding.Kind != "task" {
			return nil, errors.Join(ErrNotStarted, errors.New("ordinary notification woke main"))
		}
		factoryMu.Lock()
		current := pair
		arrivals++
		if arrivals == 2 {
			close(pair)
			arrivals = 0
			pair = make(chan struct{})
		}
		factoryMu.Unlock()
		select {
		case <-current:
		case <-ctx.Done():
			return nil, errors.Join(ErrNotStarted, ctx.Err())
		}
		b := l.Binding
		var sub string
		if err := k.DB.QueryRowContext(ctx, "SELECT id FROM kernel_subscriptions WHERE session_id=? AND resource LIKE 'kernel:mailbox:%' LIMIT 1", b.ID).Scan(&sub); err != nil {
			return nil, errors.Join(ErrNotStarted, err)
		}
		raw, err := json.Marshal(contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "waiting", Artifacts: []string{}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{sub}})
		if err != nil {
			return nil, errors.Join(ErrNotStarted, err)
		}
		p := fake(b)
		p.threadID = b.ThreadID
		if p.threadID == "" {
			p.threadID = "reference-" + b.ID
		}
		p.turnID = l.Process.Request.RunID
		p.final = string(raw)
		return p, nil
	})
	loop := NewLoop(Runtime{Store: k, Factory: factory}, source)
	loop.ReconcileEvery = time.Hour
	reports := make(chan error, 128)
	report := func(err error) {
		select {
		case reports <- err:
		default:
		}
	}
	source.Report = report
	loop.Report = report
	server, err := ipc.Listen(k.Root, k, loop.Wake)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(ctx) }()
	loopDone := make(chan error, 1)
	go func() { loopDone <- loop.Run(ctx) }()
	defer func() {
		cancel()
		if err := await(t, loopDone); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
		server.Close()
		if err := await(t, serverDone); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	waitPair := func() {
		t.Helper()
		seen := map[string]bool{}
		for range 2 {
			c := await(t, source.completed)
			if c.Err != nil || !c.Outcome.Stopped || seen[c.SessionID] {
				t.Fatal(c)
			}
			seen[c.SessionID] = true
		}
		assertCapacity(t, k, 0)
	}
	waitPair() // Initial turns establish the same unfinished thread and subscriptions.
	capacity, err := k.ExecutionCapacity(ctx)
	if err != nil || capacity.Policy.Limits.Turns != 2 {
		t.Fatal("two process slots required", err)
	}
	started := time.Now()
	for round := range 50 {
		for _, task := range tasks {
			source.mu.Lock()
			before := time.Now()
			_, err = k.SendMessage(ctx, storage.MessageRequest{SenderSessionID: first.ParentSessionID, Target: first.Target, Key: fmt.Sprintf("kernel-reference-%d-%s", round, task.ID), Recipient: task.ID, Body: "reference continuation"})
			if err == nil {
				source.pending[task.SessionID] = before
			}
			source.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
		}
		if err = ipc.Notify(ctx, filepath.Join(k.Root, ipc.SocketName), ""); err != nil {
			t.Fatal(err)
		}
		waitPair()
	}
	elapsed := time.Since(started)
	select {
	case err := <-reports:
		t.Fatal(err)
	default:
	}
	source.mu.Lock()
	samples := append([]int64{}, source.samples...)
	pending := len(source.pending)
	source.mu.Unlock()
	if len(samples) != 100 || pending != 0 {
		t.Fatal("exactly 100 durable wake reservations required", len(samples), pending)
	}
	sorted := append([]int64{}, samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p95 := sorted[94]
	var total, applied, mainRuns int
	if err = k.DB.QueryRowContext(ctx, "SELECT count(*),sum(state='applied') FROM kernel_notifications WHERE session_id IN (?,?)", tasks[0].SessionID, tasks[1].SessionID).Scan(&total, &applied); err != nil || total != 100 || applied != 100 {
		t.Fatal(total, applied, err)
	}
	if err = k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='agent_run' AND work_item_id=?", first.ParentSessionID).Scan(&mainRuns); err != nil || mainRuns != 0 {
		t.Fatal("ordinary waiting triggered main", mainRuns, err)
	}
	for _, task := range tasks {
		b, err := k.SessionBinding(ctx, task.SessionID)
		if err != nil || b.ThreadID != "reference-"+b.ID {
			t.Fatal("resume replaced unfinished thread", err)
		}
	}
	t.Logf("production KernelSource: 100 notifications/reservations, p95=%s max=%s elapsed=%s main_runs=%d", time.Duration(p95), time.Duration(sorted[99]), elapsed, mainRuns)
	if p95 > int64(time.Second) {
		t.Log("SHOULD p95 target missed; inspect retained samples and host load")
	}
	if output := os.Getenv("HARNESS_KERNEL_LATENCY_EVIDENCE"); output != "" {
		if !filepath.IsAbs(output) {
			t.Fatal("absolute evidence path required")
		}
		evidence := map[string]any{"observed_at": time.Now().UTC(), "scope": "production KernelSource/Loop/SQLite/IPC with stub process/model/external I/O", "go_version": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "independent_tasks": 2, "process_slots": 2, "task_attempt_limit": limits.TaskAttempts, "warmup_turns": 2, "notifications": 100, "reservations": 100, "applied_notifications": applied, "main_runs": mainRuns, "latency_boundary": "before notification save call through Source.Ready return with durable wake selection; conservative bound includes save and full bounded source scan", "latency_samples_nanos": samples, "p95_nanos": p95, "max_nanos": sorted[99], "target_nanos": int64(time.Second), "target_met": p95 <= int64(time.Second), "elapsed_nanos": elapsed.Nanoseconds(), "cold_start": "process stub; actual native metrics in runtime/isolated-launcher.json and runtime/parallel-native.json", "model_wait": "stubbed; zero model calls", "external_io": "stubbed; no external service calls", "timer_reconcile_nanos": int64(time.Hour)}
		raw, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(output, append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
