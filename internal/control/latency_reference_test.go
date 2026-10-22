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

// This is a controller/SQLite/IPC reference load, not the Task_6 production
// WorkSource or K workflow acceptance. WorkSource and process are controlled fixtures.
// One notification per reservation avoids counting a single batch as 100 runs.
type latencySource struct {
	store     *storage.KernelStore
	sessions  []string
	mu        sync.Mutex
	saved     map[string]time.Time
	latencies []int64
	completed chan Completion
}

func (s *latencySource) Ready(ctx context.Context, after string, limit int) (WorkPage, error) {
	page := WorkPage{}
	for _, sid := range s.sessions {
		if sid <= after {
			continue
		}
		notifications, err := s.store.PendingWakeNotifications(ctx, sid, 1)
		if err != nil {
			return page, err
		}
		if len(notifications) == 0 {
			continue
		}
		// Register the measurement before the hint. A completion-driven scan can
		// observe a committed message before its producer has recorded the timestamp.
		s.mu.Lock()
		start, known := s.saved[notifications[0].MessageID]
		s.mu.Unlock()
		if !known {
			continue
		}
		wake, err := s.store.ReserveWake(ctx, storage.WakeRequest{SessionID: sid, Key: notifications[0].ID, NotificationIDs: []string{notifications[0].ID}})
		if errors.Is(err, storage.ErrWakeHeld) || errors.Is(err, storage.ErrWakeBusy) {
			continue
		}
		if err != nil {
			return page, err
		}
		elapsed := time.Since(start).Nanoseconds()
		s.mu.Lock()
		s.latencies = append(s.latencies, elapsed)
		s.mu.Unlock()
		b, err := s.store.SessionBinding(ctx, sid)
		if err != nil {
			return page, err
		}
		page.Work = append(page.Work, SessionWork{Execute: &ExecuteRequest{SessionID: sid, RunID: wake.Intent.RunID, Snapshot: wake.Snapshot, OutputSchema: map[string]any{"type": "object"}, Validate: func(raw []byte, b contract.SessionBinding) error {
			var result contract.TaskResult
			if err := contract.DecodeStrict(raw, &result); err != nil {
				return err
			}
			if result.Target != b.Target || result.TaskID != b.TaskID || result.PhaseID != b.PhaseID {
				return errors.New("stub result binding mismatch")
			}
			return result.Validate()
		}}})
		if len(page.Work) == limit {
			page.NextAfter = b.ID
			break
		}
	}
	return page, nil
}
func (s *latencySource) Complete(ctx context.Context, c Completion) error {
	if c.Err != nil {
		s.completed <- c
		return c.Err
	}
	// The stub's exact per-run usage is known to this fixture. Real usage is never
	// inferred from this value or from an absent native notification.
	tokens := int64(1)
	if err := s.store.ObserveRunTokens(ctx, storage.RunTokenObservation{SessionID: c.SessionID, RunID: c.RunID, ThreadID: c.Outcome.Run.ThreadID, TurnID: c.Outcome.Run.TurnID, TotalForRun: &tokens, Final: true}); err != nil {
		return err
	}
	result, err := s.store.ApplyTaskResult(ctx, c.SessionID, c.RunID)
	if err != nil {
		return err
	}
	if result.State != contract.TaskWaiting {
		return fmt.Errorf("expected waiting, got %+v", result)
	}
	s.completed <- c
	return nil
}
func TestNotificationReservationReferenceLoad(t *testing.T) {
	limits := domain.DefaultKernelLimits()
	limits.TaskAttempts = 50 // Explicit reference workload policy; defaults are unchanged.
	k, first := taskFixtureWithLimits(t, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tasks, err := k.Tasks(ctx, first.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	source := &latencySource{store: k, saved: map[string]time.Time{}, completed: make(chan Completion, 2)}
	for _, task := range tasks {
		source.sessions = append(source.sessions, task.SessionID)
		b, err := k.SessionBinding(ctx, task.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.MkdirAll(b.CWD, 0700); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(source.sessions)
	if len(source.sessions) != 2 {
		t.Fatal("two independent Tasks required")
	}
	var factoryMu sync.Mutex
	arrivals := 0
	pair := make(chan struct{})
	l := NewLoop(Runtime{Store: k, Factory: factoryFunc(func(ctx context.Context, launch Launch) (Process, error) {
		// Require both independent Tasks to reserve/spawn before either stub finishes.
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
		b := launch.Binding
		p := fake(b)
		p.turnID = launch.Process.Request.RunID
		p.threadID = "reference-" + b.ID
		var sub string
		if err := k.DB.QueryRowContext(ctx, "SELECT id FROM kernel_subscriptions WHERE session_id=? AND resource LIKE 'kernel:mailbox:%' LIMIT 1", b.ID).Scan(&sub); err != nil {
			return nil, errors.Join(ErrNotStarted, err)
		}
		final, err := json.Marshal(contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "waiting", Artifacts: []string{}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{sub}})
		if err != nil {
			return nil, errors.Join(ErrNotStarted, err)
		}
		p.final = string(final)
		return p, nil
	})}, source)
	l.ReconcileEvery = time.Hour // Every measured reservation must follow a hint/completion.
	reports := make(chan error, 128)
	l.Report = func(err error) {
		select {
		case reports <- err:
		default:
		}
	}
	server, err := ipc.Listen(k.Root, k, l.Wake)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(ctx) }()
	loopDone := make(chan error, 1)
	go func() { loopDone <- l.Run(ctx) }()
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
	capacity, err := k.ExecutionCapacity(ctx)
	if err != nil || capacity.Policy.Limits.Turns != 2 {
		t.Fatal("reference requires exactly two process slots", err)
	}
	started := time.Now()
	for round := range 50 {
		assertCapacity(t, k, 0)
		for _, task := range tasks {
			before := time.Now() // Conservative bound includes the notification save transaction.
			message, err := k.SendMessage(ctx, storage.MessageRequest{SenderSessionID: first.ParentSessionID, Target: first.Target, Key: fmt.Sprintf("reference-%d-%s", round, task.ID), Recipient: task.ID, Body: "reference notification"})
			if err != nil {
				t.Fatal(err)
			}
			source.mu.Lock()
			source.saved[message.ID] = before
			source.mu.Unlock()
		}
		if err = ipc.Notify(ctx, filepath.Join(k.Root, ipc.SocketName), ""); err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for range 2 {
			completion := await(t, source.completed)
			if completion.Err != nil || !completion.Outcome.Stopped || seen[completion.SessionID] {
				t.Fatal(completion)
			}
			seen[completion.SessionID] = true
		}
	}
	elapsed := time.Since(started)
	select {
	case err := <-reports:
		t.Fatal(err)
	default:
	}
	source.mu.Lock()
	samples := append([]int64{}, source.latencies...)
	source.mu.Unlock()
	if len(samples) != 100 {
		t.Fatal("expected 100 distinct notification reservations", len(samples))
	}
	sorted := append([]int64{}, samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p95 := sorted[(95*len(sorted)+99)/100-1]
	t.Logf("100 notifications / 100 reservations, p95 %s, max %s, elapsed %s", time.Duration(p95), time.Duration(sorted[len(sorted)-1]), elapsed)
	// NFR-012 is SHOULD. Record a miss without converting a noisy host into a
	// functional failure; independent completion/persistence assertions stay hard.
	if p95 > int64(time.Second) {
		t.Log("SHOULD target missed; inspect host load and retained samples")
	}
	var bound, applied int
	if err = k.DB.QueryRowContext(ctx, "SELECT count(*),sum(state='applied') FROM kernel_notifications WHERE session_id IN (?,?)", source.sessions[0], source.sessions[1]).Scan(&bound, &applied); err != nil || bound != 100 || applied != 100 {
		t.Fatal(bound, applied, err)
	}
	assertCapacity(t, k, 0)
	if output := os.Getenv("HARNESS_LATENCY_EVIDENCE"); output != "" {
		if !filepath.IsAbs(output) {
			t.Fatal("absolute evidence path required")
		}
		evidence := map[string]any{"observed_at": time.Now().UTC(), "scope": "controller SQLite IPC reference with stub process and WorkSource", "k_workflow_acceptance": false, "go_version": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "independent_tasks": 2, "process_slots": 2, "task_attempt_limit": limits.TaskAttempts, "notifications": 100, "reservations": 100, "latency_boundary": "before notification save call through successful durable wake reservation; includes save transaction", "latency_samples_nanos": samples, "p95_nanos": p95, "max_nanos": sorted[len(sorted)-1], "target_nanos": int64(time.Second), "target_met": p95 <= int64(time.Second), "elapsed_nanos": elapsed.Nanoseconds(), "cold_start": "stubbed; actual native observations in isolated-launcher.json", "model_wait": "stubbed; zero model calls", "external_io": "stubbed; no external service calls", "timer_reconcile_nanos": int64(time.Hour)}
		raw, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(output, append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
