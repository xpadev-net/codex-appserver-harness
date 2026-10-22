package control

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestKernelSourceMissingAcceptanceReferenceReleasesFlightAndOtherWaveProgresses(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	if _, err := k.SendMessage(ctx, storage.MessageRequest{SenderSessionID: b.ID, Target: b.Target, Key: "final-evaluation", Recipient: "parent", Body: "evaluate the completed outputs"}); err != nil {
		t.Fatal(err)
	}
	source := NewKernelSource(k)
	calls := 0
	runtime := Runtime{Store: k, Factory: factoryFunc(func(ctx context.Context, l Launch) (Process, error) {
		calls++
		p := fake(l.Binding)
		p.threadID = "thread-" + l.Binding.ID
		p.turnID = l.Process.Request.RunID + "-turn"
		var result any
		if l.Binding.Kind == "task" {
			a, err := k.ImportArtifact(ctx, storage.ArtifactImport{SessionID: l.Binding.ID, Target: l.Binding.Target, Key: "output", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: l.Binding.TaskID}}, []byte("actual output"))
			if err != nil {
				return nil, err
			}
			result = contract.TaskResult{SchemaVersion: 1, Target: l.Binding.Target, TaskID: l.Binding.TaskID, PhaseID: l.Binding.PhaseID, Outcome: "result", Artifacts: []string{a.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "observed output", EvidenceIDs: []string{a.ID}}}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}}
		} else {
			proposal := contract.ArtifactAcceptance{Outputs: []string{"nonexistent"}, Evaluations: []contract.ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: "nonexistent", ReviewTaskID: "review"}}}
			result = contract.MainResult{SchemaVersion: 1, SessionID: l.Binding.ID, Decisions: []contract.MainDecision{{Target: b.Target, Action: "accept", Acceptance: &proposal, Summary: "proposed acceptance with an invalid reference", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}}}}
		}
		raw, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		p.final = string(raw)
		return p, nil
	})}
	tasks, err := k.Tasks(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	ensureSourceTaskDirectories(t, k, b.Target.WaveID)
	for _, task := range tasks {
		work := sourceTaskWork(t, source, task.SessionID)
		out, err := runtime.Execute(ctx, *work.Execute)
		if err != nil {
			t.Fatal(err)
		}
		if err = source.Complete(ctx, Completion{SessionID: task.SessionID, RunID: work.Execute.RunID, Outcome: out}); err != nil {
			t.Fatal(err)
		}
	}
	main := sourceTaskWork(t, source, b.ParentSessionID)
	out, err := runtime.Execute(ctx, *main.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: b.ParentSessionID, RunID: main.Execute.RunID, Outcome: out}); err != nil {
		t.Fatal("reference failure kept local completion flight", err)
	}
	state, err := k.SessionDispatchState(ctx, b.ParentSessionID)
	if err != nil || state.Run == nil || state.Run.ID != out.Run.ID {
		t.Fatal("reference failure discarded physical run", state, err)
	}
	wave, err := k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: b.ParentSessionID, Source: "direct", SourceID: "unrelated", OriginalRequest: "independent research", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "unrelated output"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: domain.DefaultKernelLimits()}})
	if err != nil {
		t.Fatal(err)
	}
	spec := contract.TaskSpec{ID: "independent", Kind: "work", Instruction: "independent research", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	plan := contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{}, Phases: []contract.PhaseSpec{{ID: "independent", Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"}}}
	admitted, err := k.AdoptPlan(ctx, wave.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		page, err := source.Ready(ctx, "", 128)
		if err != nil || len(page.Work) != 1 || page.Work[0].Execute.SessionID != task.SessionID || calls != 3 {
			t.Fatal("held output blocked unrelated work or replayed main", page, calls, err)
		}
	}
	assertCapacity(t, k, 0)
}
