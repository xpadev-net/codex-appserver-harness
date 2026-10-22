package control

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestKernelSourceParentPlanHandsBlockedWorkToFreshTaskAndReview(t *testing.T) {
	ctx := context.Background()
	k, original := taskFixture(t)
	source := NewKernelSource(k)
	var sourceErrors []error
	source.Report = func(err error) { sourceErrors = append(sourceErrors, err) }
	inherited := ""
	candidate := ""
	calls := map[string]int{}
	factory := factoryFunc(func(ctx context.Context, l Launch) (Process, error) {
		calls[l.Binding.ID]++
		p := fake(l.Binding)
		p.threadID = "thread-" + l.Binding.ID
		p.turnID = "turn-" + l.Process.Request.RunID
		var output any
		if l.Binding.Kind == "orchestrator" {
			fix := contract.TaskSpec{ID: "replacement", Kind: "fix", Instruction: "address the original research blocker with a bounded report", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}, SourceResultIDs: []string{inherited}}
			review := fix
			review.ID, review.Kind, review.Instruction = "replacement-review", "review", "independently evaluate the replacement report and original blocker"
			review.DependsOn = []contract.TaskDependency{{TaskID: fix.ID}}
			plan := contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{}, Phases: []contract.PhaseSpec{}}
			for _, spec := range []contract.TaskSpec{fix, review} {
				plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: spec.ID, Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"})
			}
			output = contract.MainResult{SchemaVersion: 1, SessionID: l.Binding.ID, Decisions: []contract.MainDecision{{Target: original.Target, Action: "plan", Plan: &plan, Summary: "handoff blocked research without changing scope", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}}}}
		} else {
			result := contract.TaskResult{SchemaVersion: 1, Target: l.Binding.Target, TaskID: l.Binding.TaskID, PhaseID: l.Binding.PhaseID, Outcome: "result", Artifacts: []string{}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}}
			if l.Binding.ID == original.ID {
				result.Outcome = "blocked"
				result.Remaining = []string{"refine research task"}
				result.Blockers = []string{"parent judgement required"}
			} else {
				task, err := k.Task(ctx, l.Binding.TaskID)
				if err != nil {
					return nil, err
				}
				if len(task.Spec.SourceResultIDs) != 1 || task.Spec.SourceResultIDs[0] != inherited {
					return nil, fmt.Errorf("structured source not handed off: %+v", task.Spec)
				}
				a, err := k.ImportArtifact(ctx, storage.ArtifactImport{SessionID: l.Binding.ID, Target: l.Binding.Target, Key: "output", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: task.ID}}, []byte("report:"+task.ID))
				if err != nil {
					return nil, err
				}
				result.Artifacts = []string{a.ID}
				result.CriterionResults = []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "recorded observation", EvidenceIDs: []string{a.ID}}}
				if task.Spec.Kind == "review" {
					result.Review = &contract.ReviewResult{TargetDigest: candidate, Verdict: "clean", Findings: []contract.ReviewFinding{}}
				} else {
					candidate = a.Digest
				}
			}
			output = result
		}
		raw, err := json.Marshal(output)
		if err != nil {
			return nil, err
		}
		p.final = string(raw)
		return p, nil
	})
	runtime := Runtime{Store: k, Factory: factory}
	first := sourceTaskWork(t, source, original.ID)
	outcome, err := runtime.Execute(ctx, *first.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: original.ID, RunID: first.Execute.RunID, Outcome: outcome}); err != nil {
		t.Fatal(err)
	}
	var application storage.ResultApplication
	if err = k.Record(ctx, "task_result_application", first.Execute.RunID, &application); err != nil {
		t.Fatal(err)
	}
	inherited = application.ResultID
	if inherited == "" {
		t.Fatal("blocked structured result missing")
	}
	for i := 0; i < 3; i++ {
		ensureSourceTaskDirectories(t, k, original.Target.WaveID)
		page, err := source.Ready(ctx, "", 128)
		if err != nil || len(page.Work) != 1 || page.Work[0].Execute == nil {
			t.Fatal(i, page, err, sourceErrors)
		}
		next := page.Work[0].Execute
		outcome, err = runtime.Execute(ctx, *next)
		if err != nil {
			t.Fatal(i, err)
		}
		if err = source.Complete(ctx, Completion{SessionID: next.SessionID, RunID: next.RunID, Outcome: outcome}); err != nil {
			t.Fatal(i, err)
		}
	}
	old, err := k.Task(ctx, original.TaskID)
	if err != nil || old.State != contract.TaskCanceled {
		t.Fatal(old, err)
	}
	wave, err := k.Wave(ctx, original.Target.WaveID)
	if err != nil || wave.MainTurns != 1 || wave.Replans != 1 || wave.TasksCreated != 4 || len(calls) != 4 || len(sourceErrors) != 0 {
		t.Fatal(wave, calls, sourceErrors, err)
	}
	for sid, count := range calls {
		if count != 1 {
			t.Fatal("session reused", sid, count)
		}
	}
	page, err := source.Ready(ctx, "", 128)
	if err != nil || len(page.Work) != 1 || page.Work[0].Execute == nil || page.Work[0].Execute.SessionID != original.ParentSessionID {
		t.Fatal("fresh fix/review did not request final judgment", page, err)
	}
	finalRun := page.Work[0].Execute.RunID
	for i := 0; i < 2; i++ {
		page, err = source.Ready(ctx, "", 128)
		if err != nil || len(page.Work) != 1 || page.Work[0].Execute.RunID != finalRun || calls[original.ParentSessionID] != 1 {
			t.Fatal("final readiness retried parent planning", page, calls, err)
		}
	}
	assertCapacity(t, k, 0)
}
