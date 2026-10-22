package control

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestKernelSourceRetriesKnownRejectedMainOnlyAfterExplicitDecision(t *testing.T) {
	ctx := context.Background()
	k, err := storage.OpenKernel(filepath.Join(t.TempDir(), "private", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err = k.CreateOrchestrator(ctx, "main", "configured", t.TempDir(), "read-only"); err != nil {
		t.Fatal(err)
	}
	wave, err := k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "rejection", OriginalRequest: "research original scope", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "report original evidence"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: domain.DefaultKernelLimits()}})
	if err != nil {
		t.Fatal(err)
	}
	source := NewKernelSource(k)
	calls := 0
	runtime := Runtime{Store: k, Factory: factoryFunc(func(_ context.Context, l Launch) (Process, error) {
		calls++
		p := fake(l.Binding)
		p.threadID = l.Binding.ThreadID
		if p.threadID == "" {
			if calls == 1 {
				p.threadID = "old-main-thread"
			} else {
				p.threadID = "new-main-thread"
			}
		}
		p.turnID = l.Process.Request.RunID + "-turn"
		decision := contract.MainDecision{Target: wave.Target(), Action: "report", Summary: "retain the original request", SubscriptionIDs: []string{}, Remaining: []string{"original research remains"}, Blockers: []string{}}
		if calls == 1 {
			task := contract.TaskSpec{ID: "outside", Kind: "work", Instruction: "invalid proposed scope", Criteria: []string{"NOT-APPROVED"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
			decision.Action = "plan"
			decision.Plan = &contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{}, Phases: []contract.PhaseSpec{{ID: "outside", Tasks: []contract.TaskSpec{task}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{task.ID}}, FailurePolicy: "drain_and_block"}}}
		}
		raw, err := json.Marshal(contract.MainResult{SchemaVersion: 1, SessionID: l.Binding.ID, Decisions: []contract.MainDecision{decision}})
		if err != nil {
			return nil, err
		}
		p.final = string(raw)
		return p, nil
	})}
	first := sourceTaskWork(t, source, "main")
	outcome, err := runtime.Execute(ctx, *first.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: "main", RunID: first.Execute.RunID, Outcome: outcome}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		page, err := source.Ready(ctx, "", 128)
		if err != nil || len(page.Work) != 0 || calls != 1 {
			t.Fatal("invalid result retried implicitly", page, calls, err)
		}
	}
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	next, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{wave.Target()})
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := k.RejectStoppedMainRun(ctx, storage.MainRunRejectionRequest{Key: "operator-reject", Expected: b, RunID: first.Execute.RunID, Snapshot: next, Reason: "proposed criteria were outside the approved request"})
	if err != nil || calls != 1 || rejected.Rotation.OldThreadID != "old-main-thread" {
		t.Fatal(rejected, calls, err)
	}
	second := sourceTaskWork(t, source, "main")
	if second.Execute.RunID == first.Execute.RunID {
		t.Fatal("old model run reused")
	}
	outcome, err = runtime.Execute(ctx, *second.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: "main", RunID: second.Execute.RunID, Outcome: outcome}); err != nil {
		t.Fatal(err)
	}
	current, err := k.SessionBinding(ctx, "main")
	if err != nil || current.Generation != b.Generation+1 || current.ThreadID != "new-main-thread" || calls != 2 {
		t.Fatal(current, calls, err)
	}
	actual, err := k.Wave(ctx, wave.ID)
	if err != nil || actual.MainTurns != 2 || actual.Replans != 0 || actual.TasksCreated != 0 {
		t.Fatal(actual, err)
	}
	page, err := source.Ready(ctx, "", 128)
	if err != nil || len(page.Work) != 0 {
		t.Fatal(page, err)
	}
	assertCapacity(t, k, 0)
}
