package control

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestKernelSourceAutomaticallyRunsSavedFixReviewWithoutIntermediateMain(t *testing.T) {
	ctx := context.Background()
	k, err := storage.OpenKernel(filepath.Join(t.TempDir(), "private", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err = k.CreateOrchestrator(ctx, "main", "configured", t.TempDir(), "read-only"); err != nil {
		t.Fatal(err)
	}
	wave, err := k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "template", OriginalRequest: "research and independently review", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "provide an independently reviewed report"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: domain.DefaultKernelLimits()}})
	if err != nil {
		t.Fatal(err)
	}
	work := contract.TaskSpec{ID: "work", Kind: "work", Instruction: "produce report", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	review := work
	review.ID, review.Kind, review.Instruction = "review", "review", "independently review report"
	review.DependsOn = []contract.TaskDependency{{TaskID: work.ID}}
	report := work
	report.ID, report.Instruction = "report", "report reviewed output"
	report.DependsOn = []contract.TaskDependency{{TaskID: review.ID}}
	plan := contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{{ID: "artifact-fix", InputTaskID: "review", Classifications: []string{"artifact_defect"}, FixInstruction: "fix reported artifact defects", ReviewInstruction: "independently review corrected report", MaxRounds: 2}}, Phases: []contract.PhaseSpec{}}
	for _, spec := range []contract.TaskSpec{work, review, report} {
		plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: spec.ID, Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"})
	}
	if _, err = k.AdoptPlan(ctx, wave.Target(), "initial", plan); err != nil {
		t.Fatal(err)
	}
	source := NewKernelSource(k)
	var sourceErrors []error
	source.Report = func(err error) { sourceErrors = append(sourceErrors, err) }
	subject := ""
	finalArtifact, finalReviewer := "", ""
	mainLaunches := 0
	launched := []string{}
	sessions := map[string]bool{}
	factory := factoryFunc(func(ctx context.Context, l Launch) (Process, error) {
		if l.Binding.Kind == "orchestrator" {
			mainLaunches++
			current, err := k.Wave(ctx, wave.ID)
			if err != nil {
				return nil, err
			}
			proposal := contract.ArtifactAcceptance{Outputs: []string{finalArtifact}, Evaluations: []contract.ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: finalArtifact, ReviewTaskID: finalReviewer}}}
			result := contract.MainResult{SchemaVersion: 1, SessionID: l.Binding.ID, Decisions: []contract.MainDecision{{Target: current.Target(), Action: "accept", Acceptance: &proposal, Summary: "all original requirements independently evaluated", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}}}}
			raw, err := json.Marshal(result)
			if err != nil {
				return nil, err
			}
			p := fake(l.Binding)
			p.threadID = "final-main-thread"
			p.turnID = l.Process.Request.RunID + "-turn"
			p.final = string(raw)
			return p, nil
		}
		if sessions[l.Binding.ID] {
			return nil, fmt.Errorf("completed session reused: %s", l.Binding.ID)
		}
		sessions[l.Binding.ID] = true
		task, err := k.Task(ctx, l.Binding.TaskID)
		if err != nil {
			return nil, err
		}
		launched = append(launched, task.Spec.Kind)
		a, err := k.ImportArtifact(ctx, storage.ArtifactImport{SessionID: l.Binding.ID, Target: l.Binding.Target, Key: "output", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: task.ID}}, []byte("output:"+task.ID))
		if err != nil {
			return nil, err
		}
		result := contract.TaskResult{SchemaVersion: 1, Target: l.Binding.Target, TaskID: task.ID, PhaseID: task.PhaseID, Outcome: "result", WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}, Artifacts: []string{a.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "recorded observation", EvidenceIDs: []string{a.ID}}}}
		if task.Spec.Kind == "review" {
			result.Review = &contract.ReviewResult{TargetDigest: subject, Verdict: "clean", Findings: []contract.ReviewFinding{}}
			if task.LocalID == "review" {
				result.Review.Verdict = "findings"
				result.Review.Findings = []contract.ReviewFinding{{ID: "defect", Classification: "artifact_defect", TargetDigest: subject, Basis: "original requirement", Description: "fix the report"}}
			}
			if task.LocalID != "review" {
				finalReviewer = task.ID
			}
		} else {
			subject = a.Digest
			if task.Spec.Kind == "fix" {
				finalArtifact = a.ID
			}
		}
		raw, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		p := fake(l.Binding)
		p.threadID = "thread-" + l.Binding.ID
		p.turnID = "turn-" + l.Process.Request.RunID
		p.final = string(raw)
		return p, nil
	})
	runtime := Runtime{Store: k, Factory: factory}
	for i := 0; i < 5; i++ {
		ensureSourceTaskDirectories(t, k, wave.ID)
		page, err := source.Ready(ctx, "", 128)
		for recheck := 0; err == nil && len(page.Work) == 0 && page.Recheck && recheck < 3; recheck++ {
			ensureSourceTaskDirectories(t, k, wave.ID)
			page, err = source.Ready(ctx, "", 128)
		}
		if err != nil || len(page.Work) != 1 {
			t.Fatal("expected one next Task", i, page, err, sourceErrors)
		}
		next := page.Work[0]
		if next.Execute == nil {
			t.Fatal(next)
		}
		outcome, err := runtime.Execute(ctx, *next.Execute)
		if err != nil {
			t.Fatal(i, err)
		}
		if err = source.Complete(ctx, Completion{SessionID: next.Execute.SessionID, RunID: next.Execute.RunID, Outcome: outcome}); err != nil {
			t.Fatal(i, err)
		}
	}
	expected := []string{"work", "review", "fix", "review", "work"}
	if fmt.Sprint(launched) != fmt.Sprint(expected) {
		t.Fatal(launched)
	}
	if mainLaunches != 0 {
		t.Fatal("ordinary template progress launched main", mainLaunches)
	}
	var final SessionWork
	for i := 0; i < 3; i++ {
		page, err := source.Ready(ctx, "", 128)
		if err != nil || len(page.Work) != 1 || page.Work[0].Execute == nil || page.Work[0].Execute.SessionID != "main" {
			t.Fatal("final acceptance was not selected", page, err)
		}
		if i > 0 && page.Work[0].Execute.RunID != final.Execute.RunID {
			t.Fatal("polling changed final judgment run")
		}
		final = page.Work[0]
	}
	current, err := k.Wave(ctx, wave.ID)
	if err != nil || current.MainTurns != 0 || current.Replans != 0 || current.FixRounds != 1 || current.State == domain.WaveCompleted || mainLaunches != 0 || len(sourceErrors) != 0 {
		t.Fatal(current, err, sourceErrors)
	}
	outcome, err := runtime.Execute(ctx, *final.Execute)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Complete(ctx, Completion{SessionID: "main", RunID: final.Execute.RunID, Outcome: outcome}); err != nil {
		t.Fatal(err)
	}
	current, err = k.Wave(ctx, wave.ID)
	if err != nil || current.State != domain.WaveCompleted || current.MainTurns != 1 || mainLaunches != 1 || current.Replans != 0 || current.FixRounds != 1 {
		t.Fatal(current, mainLaunches, err)
	}
	for i := 0; i < 3; i++ {
		page, err := source.Ready(ctx, "", 128)
		if err != nil || len(page.Work) != 0 || mainLaunches != 1 {
			t.Fatal("final judgment repeated", page, mainLaunches, err)
		}
	}
	assertCapacity(t, k, 0)
}
