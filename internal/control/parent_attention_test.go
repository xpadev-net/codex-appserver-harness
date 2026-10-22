package control

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestKernelSourceBlockedTaskWakesParentOnceWithoutAutomaticRetry(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	ensureSourceTaskDirectories(t, k, b.Target.WaveID)
	source := NewKernelSource(k)
	var reported []error
	source.Report = func(err error) { reported = append(reported, err) }
	calls := map[string]int{}
	subscription := ""
	factory := factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		calls[l.Binding.Kind]++
		p := fake(l.Binding)
		p.threadID = "thread-" + l.Binding.ID
		p.turnID = "turn-" + l.Process.Request.RunID
		var final any
		if l.Binding.Kind == "task" {
			final = contract.TaskResult{SchemaVersion: 1, Target: l.Binding.Target, TaskID: l.Binding.TaskID, PhaseID: l.Binding.PhaseID, Outcome: "blocked", Artifacts: []string{}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{"answer the question"}, Blockers: []string{"need input from parent"}, SubscriptionIDs: []string{}}
		} else {
			final = contract.MainResult{SchemaVersion: 1, SessionID: l.Binding.ID, Decisions: []contract.MainDecision{{Target: b.Target, Action: "wait", Summary: "operator input required", SubscriptionIDs: []string{subscription}, Remaining: []string{"resolve blocker"}, Blockers: []string{"need operator clarification"}}}}
		}
		raw, err := json.Marshal(final)
		if err != nil {
			return nil, err
		}
		p.final = string(raw)
		return p, nil
	})
	runtime := Runtime{Store: k, Factory: factory}
	first := sourceTaskWork(t, source, b.ID)
	outcome, err := runtime.Execute(ctx, *first.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: b.ID, RunID: first.Execute.RunID, Outcome: outcome}); err != nil {
		t.Fatal(err)
	}
	page, err := source.Ready(ctx, "", 128)
	if err != nil || len(page.Work) != 1 || page.Work[0].Execute == nil || page.Work[0].Execute.SessionID != b.ParentSessionID {
		t.Fatal(page, err, reported)
	}
	parent := page.Work[0].Execute
	var input struct {
		Notifications []struct {
			Kind, Reason   string
			SubscriptionID string `json:"subscription_id"`
		} `json:"notifications"`
	}
	if err = json.Unmarshal(parent.Snapshot.Input, &input); err != nil || len(input.Notifications) != 1 || input.Notifications[0].Kind != "parent_attention" || input.Notifications[0].Reason != "task_blocked" || !strings.Contains(string(parent.Snapshot.Input), "need input from parent") {
		t.Fatal(input, err)
	}
	subscription = input.Notifications[0].SubscriptionID
	outcome, err = runtime.Execute(ctx, *parent)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: parent.SessionID, RunID: parent.RunID, Outcome: outcome}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		page, err = source.Ready(ctx, "", 128)
		if err != nil || len(page.Work) != 0 || page.Recheck {
			t.Fatal("unchanged blocker caused retry/wake", page, err, reported)
		}
	}
	task, err := k.Task(ctx, b.TaskID)
	if err != nil || task.State != contract.TaskBlocked || task.Attempts != 1 {
		t.Fatal(task, err)
	}
	wave, err := k.Wave(ctx, b.Target.WaveID)
	if err != nil || wave.MainTurns != 1 || wave.Replans != 0 || calls["task"] != 1 || calls["orchestrator"] != 1 || len(reported) != 0 {
		t.Fatal(wave, calls, err, reported)
	}
	assertCapacity(t, k, 0)
}
