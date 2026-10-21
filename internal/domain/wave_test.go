package domain

import (
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"testing"
	"time"
)

func waveFixture() Wave {
	now := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	return Wave{ID: "wave", OrchestratorSessionID: "main", Source: "direct", SourceID: "request", OriginalRequest: "research and report", Criteria: []WaveCriterion{{ID: "AC", Requirement: "evidence-backed report"}}, ScopeRevision: 1, PlanRevision: 1, Policy: WavePolicy{Revision: 1, Stage: "K", Limits: DefaultKernelLimits()}, State: WavePlanning, CreatedAt: now, Deadline: now.Add(48 * time.Hour)}
}
func wavePlanFixture() contract.PlanProposal {
	return contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "p", FailurePolicy: "drain_and_block", Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"task"}}, Tasks: []contract.TaskSpec{{ID: "task", Kind: "work", Instruction: "research", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}}}}, Templates: []contract.FixReviewTemplate{}}
}
func TestGenericWavePolicyAndCoverage(t *testing.T) {
	for _, mode := range []string{"generic", "product", "issue", "missing_coverage", "invented", "unsupported_phase", "future_dependency", "structural_limit", "work_item"} {
		t.Run(mode, func(t *testing.T) {
			w, p := waveFixture(), wavePlanFixture()
			switch mode {
			case "product":
				w.Policy.Product = true
			case "issue":
				w.Source = "github"
			case "missing_coverage":
				w.Criteria = append(w.Criteria, WaveCriterion{"AC2", "second"})
			case "invented":
				p.Phases[0].Tasks[0].Criteria = []string{"invented"}
			case "unsupported_phase":
				p.Phases[0].Completion.Kind = "any_success"
			case "future_dependency":
				p.Phases[0].Tasks[0].DependsOn = []contract.TaskDependency{{TaskID: "future"}}
			case "structural_limit":
				w.Policy.Limits.Tasks = 0
			case "work_item":
				p.Phases[0].Tasks[0].WorkItemIDs = []string{"product"}
			}
			err := w.Validate()
			if err == nil {
				err = ValidateWavePlan(w, p)
			}
			if (err == nil) != (mode == "generic") {
				t.Fatal(mode, err)
			}
		})
	}
}
func TestTaskTerminalIsDistinctFromIncompleteFailure(t *testing.T) {
	for _, state := range []contract.TaskState{contract.TaskReady, contract.TaskRunning, contract.TaskWaiting, contract.TaskBlocked} {
		if state.Terminal() {
			t.Fatal("incomplete Task is terminal", state)
		}
	}
	for _, state := range []contract.TaskState{contract.TaskCompleted, contract.TaskFailed, contract.TaskCanceled} {
		if !state.Terminal() {
			t.Fatal("terminal Task reusable", state)
		}
	}
}
