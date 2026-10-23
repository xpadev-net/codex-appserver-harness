package control

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func taskWorkIfPresent(t *testing.T, s *KernelSource, sid string) *SessionWork {
	t.Helper()
	page, err := s.Ready(context.Background(), "", 128)
	if err != nil {
		t.Fatal(err)
	}
	for _, work := range page.Work {
		id, _, _ := work.identity()
		if id == sid {
			return &work
		}
	}
	return nil
}
func TestKernelSourceDispatchesOnlyExactAuthorizedTaskRetryAfterRestart(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	source := NewKernelSource(k)
	first := sourceTaskWork(t, source, b.ID)
	factory := factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		p := fake(l.Binding)
		p.threadID = l.Binding.ThreadID
		if p.threadID == "" {
			p.threadID = "retry-thread"
		}
		p.turnID = "turn-" + l.Process.Request.RunID
		result := contract.TaskResult{SchemaVersion: 1, Target: l.Binding.Target, TaskID: l.Binding.TaskID, PhaseID: l.Binding.PhaseID, Outcome: "blocked", Artifacts: []string{}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{"operator must resolve transient input"}, SubscriptionIDs: []string{}}
		raw, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		p.final = string(raw)
		if l.Process.Request.RunID == "authorized-retry" {
			p.final = sourceWaiting(t, k, l.Binding)
		}
		return p, nil
	})
	runtime := Runtime{Store: k, Factory: factory}
	out, err := runtime.Execute(ctx, *first.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: b.ID, RunID: first.Execute.RunID, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, b.TaskID)
	if err != nil || task.State != contract.TaskBlocked || task.Attempts != 1 {
		t.Fatal(task, err)
	}
	if work := taskWorkIfPresent(t, source, b.ID); work != nil {
		t.Fatal("polling retried blocked Task", work)
	}
	bound, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{task.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if err = k.AuthorizeTaskRecovery(ctx, task, "authorized-retry", snapshot, "operator resolved transient input"); err != nil {
		t.Fatal(err)
	}
	// Reconstruct the production source from durable facts, without a local work queue.
	source = NewKernelSource(k)
	pause := storage.OperatorControl{Key: "pause-retry", Target: task.Target(), Action: "pause", Reason: "inspect authorized retry"}
	if _, err = k.ApplyOperatorControl(ctx, pause); err != nil {
		t.Fatal(err)
	}
	if work := taskWorkIfPresent(t, source, b.ID); work != nil {
		t.Fatal("permit bypassed pause", work)
	}
	pause.Key, pause.Action = "resume-retry", "resume"
	if _, err = k.ApplyOperatorControl(ctx, pause); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		work := sourceTaskWork(t, source, b.ID)
		if work.Execute == nil || work.Execute.RunID != "authorized-retry" || !reflect.DeepEqual(work.Execute.Snapshot, snapshot) {
			t.Fatal("source changed authorized retry", work)
		}
	}
	unchanged, err := k.Task(ctx, b.TaskID)
	if err != nil || unchanged.Attempts != 1 {
		t.Fatal("selection spent Attempt", unchanged, err)
	}
	work := sourceTaskWork(t, source, b.ID)
	out, err = runtime.Execute(ctx, *work.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: b.ID, RunID: work.Execute.RunID, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	task, err = k.Task(ctx, b.TaskID)
	if err != nil || task.Attempts != 2 || task.State != contract.TaskWaiting {
		t.Fatal(task, err)
	}
	finalBinding, err := k.SessionBinding(ctx, b.ID)
	if err != nil || finalBinding.ThreadID != bound.ThreadID {
		t.Fatal("retry replaced Task thread", finalBinding, err)
	}
	if pending, err := k.PendingTaskRecovery(ctx, task.ID); err != nil || pending != nil {
		t.Fatal("consumed permit remained pending", pending, err)
	}
	if work := taskWorkIfPresent(t, source, b.ID); work != nil {
		t.Fatal("consumed retry repeated", work)
	}
}

func TestKernelSourceHoldsUnstartedRetryUntilDistinctOperatorDecision(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	source := NewKernelSource(k)
	first := sourceTaskWork(t, source, b.ID)
	blocked := factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		p := fake(l.Binding)
		p.threadID = "original-retry-thread"
		p.turnID = "first-turn"
		result := contract.TaskResult{SchemaVersion: 1, Target: l.Binding.Target, TaskID: l.Binding.TaskID, PhaseID: l.Binding.PhaseID, Outcome: "blocked", Artifacts: []string{}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{"transient setup"}, SubscriptionIDs: []string{}}
		raw, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		p.final = string(raw)
		return p, nil
	})
	out, err := (Runtime{Store: k, Factory: blocked}).Execute(ctx, *first.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: b.ID, RunID: first.Execute.RunID, Outcome: out}); err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, b.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{task.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if err = k.AuthorizeTaskRecovery(ctx, task, "retry-not-started", snapshot, "operator retry"); err != nil {
		t.Fatal(err)
	}
	work := sourceTaskWork(t, source, b.ID)
	notStarted := factoryFunc(func(context.Context, Launch) (Process, error) { return nil, ErrNotStarted })
	out, err = (Runtime{Store: k, Factory: notStarted}).Execute(ctx, *work.Execute)
	if err == nil {
		t.Fatal("fixture unexpectedly launched")
	}
	if err = source.Complete(ctx, Completion{SessionID: b.ID, RunID: work.Execute.RunID, Outcome: out, Err: err}); err != nil {
		t.Fatal(err)
	}
	if work := taskWorkIfPresent(t, source, b.ID); work != nil {
		t.Fatal("launch failure automatically repeated", work)
	}
	inspection, inspectErr := k.InspectSessionRecovery(ctx, b.ID, "", 1)
	if inspectErr != nil || inspection.Session.DispatchHold == nil || inspection.Session.DispatchHold.RunID != "retry-not-started" || inspection.Session.DispatchHold.Reason == "" {
		t.Fatal("saved launch hold unavailable to operator", inspection, inspectErr)
	}
	if err = k.AuthorizeTaskRecovery(ctx, task, "retry-not-started", snapshot, "operator retry"); err != nil {
		t.Fatal(err)
	}
	if work := taskWorkIfPresent(t, source, b.ID); work != nil {
		t.Fatal("old receipt replay reactivated failed launch", work)
	}
	if err = k.AuthorizeTaskRecovery(ctx, task, "distinct-retry", snapshot, "operator corrected launch configuration"); err != nil {
		t.Fatal(err)
	}
	work = sourceTaskWork(t, source, b.ID)
	if work.Execute == nil || work.Execute.RunID != "distinct-retry" {
		t.Fatal(work)
	}
}
