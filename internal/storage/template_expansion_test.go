package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func templateFixture(t *testing.T, future bool, templates []contract.FixReviewTemplate, findings []string, mutators ...func(*contract.PlanProposal)) (*KernelStore, domain.Wave, domain.Task, domain.Artifact) {
	t.Helper()
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	request.Policy.Limits.Replans = 0
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	plan.Templates = templates
	for _, mutate := range mutators {
		mutate(&plan)
	}
	if future {
		plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: "report", Tasks: []contract.TaskSpec{{ID: "report", Kind: "work", Instruction: "report the independently reviewed output", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: "review"}}}}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"report"}}, FailurePolicy: "drain_and_block"})
	}
	receipt, err := store.AdoptPlan(ctx, wave.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.Task(ctx, receipt.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	candidate := completeTemplateTask(t, store, work, "initial candidate", "", nil)
	review, err := store.Task(ctx, receipt.CreatedTaskIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	completeTemplateTask(t, store, review, "initial review", candidate.Digest, findings)
	return store, wave, review, candidate
}
func kernelTemplate() contract.FixReviewTemplate {
	return contract.FixReviewTemplate{ID: "artifact-owner", InputTaskID: "review", Classifications: []string{"artifact_defect"}, FixInstruction: "fix the reported artifact defects using the supplied findings and latest output", ReviewInstruction: "independently review the latest fixed output against the original criteria", MaxRounds: 3}
}
func completeTemplateTask(t *testing.T, k *KernelStore, task domain.Task, text, target string, classes []string, additional ...domain.Artifact) domain.Artifact {
	t.Helper()
	ctx := context.Background()
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, k, b)
	if err = k.BindSessionThread(ctx, b, "thread-"+task.ID); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	artifact := mustImport(t, k, ArtifactImport{SessionID: b.ID, Target: b.Target, Key: "output", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: task.ID}}, text)
	result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: task.ID, PhaseID: task.PhaseID, Outcome: "result", Artifacts: []string{artifact.ID}, CriterionResults: []contract.CriterionResult{}}
	for _, extra := range additional {
		result.Artifacts = append(result.Artifacts, extra.ID)
	}
	for _, id := range task.Spec.Criteria {
		result.CriterionResults = append(result.CriterionResults, contract.CriterionResult{CriterionID: id, Status: "claimed", Basis: "recorded observation", EvidenceIDs: []string{artifact.ID}})
	}
	if task.Spec.Kind == "review" {
		result.Review = &contract.ReviewResult{TargetDigest: target, Verdict: "clean", Findings: []contract.ReviewFinding{}}
		if len(classes) > 0 {
			result.Review.Verdict = "findings"
		}
		for i, class := range classes {
			result.Review.Findings = append(result.Review.Findings, contract.ReviewFinding{ID: fmt.Sprint("finding-", i), Classification: class, TargetDigest: target, Basis: "original requirement", Description: "observed defect"})
		}
	}
	runID := "run-" + task.ID
	recordAndStopTaskResult(t, k, b, runID, result)
	applied, err := k.ApplyTaskResult(ctx, b.ID, runID)
	if err != nil || applied.State != contract.TaskCompleted {
		t.Fatal(task.LocalID, applied, err)
	}
	return artifact
}
func TestTemplateExpansionIsAtomicReplayableAndKeepsCumulativeBudgets(t *testing.T) {
	ctx := context.Background()
	store, wave, review, _ := templateFixture(t, true, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"})
	request := TemplateExpansionRequest{Target: wave.Target(), ReviewTaskID: review.ID}
	receipt, err := store.ExpandReviewTemplates(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Round != 1 || receipt.Admission.Mode != "template" || receipt.Admission.Target.PlanRevision != 2 || len(receipt.Admission.CreatedTaskIDs) != 3 || len(receipt.Admission.CarriedTaskIDs) != 2 {
		t.Fatal(receipt)
	}
	updated, err := store.Wave(ctx, wave.ID)
	if err != nil || updated.Replans != 0 || updated.MainTurns != 0 || updated.FixRounds != 1 || updated.TasksCreated != 6 || updated.PhasesCreated != 6 {
		t.Fatal(updated, err)
	}
	oldFuture, err := store.Task(ctx, structureID("task-", wave.ID, "report"))
	if err != nil || oldFuture.State != contract.TaskCanceled {
		t.Fatal(oldFuture, err)
	}
	var future domain.Task
	var newReview domain.Task
	for _, id := range receipt.Admission.CreatedTaskIDs {
		task, err := store.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if task.Spec.Kind == "work" {
			future = task
		}
		if task.Spec.Kind == "review" {
			newReview = task
		}
	}
	if future.ID == oldFuture.ID || future.SessionID == oldFuture.SessionID || len(future.Spec.DependsOn) != 1 || future.Spec.DependsOn[0].TaskID != newReview.LocalID {
		t.Fatal(future, newReview)
	}
	replay, err := store.ExpandReviewTemplates(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, receipt) {
		t.Fatal(replay, err)
	}
	// The same immutable source batch at the latest target is still one expansion.
	currentRequest := request
	currentRequest.Target = updated.Target()
	replay, err = store.ExpandReviewTemplates(ctx, currentRequest)
	if err != nil || replay.Key != receipt.Key {
		t.Fatal(replay, err)
	}
	path := filepath.Join(store.Root, "state.db")
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replay, err = reopened.ExpandReviewTemplates(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, receipt) {
		t.Fatal(replay, err)
	}
	var reservations int
	if err = reopened.DB.QueryRow("SELECT sum(amount) FROM kernel_budget_ledger WHERE kind='fix_round'").Scan(&reservations); err != nil || reservations != 1 {
		t.Fatal(reservations, err)
	}
}
func TestTemplateExpansionCreatesFreshSessionsEveryRoundAndStopsAtLimit(t *testing.T) {
	ctx := context.Background()
	store, wave, review, _ := templateFixture(t, false, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"})
	sessions := map[string]bool{review.SessionID: true}
	for round := 1; round <= 3; round++ {
		receipt, err := store.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: wave.Target(), ReviewTaskID: review.ID})
		if err != nil || receipt.Round != round {
			t.Fatal(receipt, err)
		}
		var candidate domain.Artifact
		for _, id := range receipt.Admission.CreatedTaskIDs {
			task, err := store.Task(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if sessions[task.SessionID] {
				t.Fatal("session reused", task)
			}
			sessions[task.SessionID] = true
			if task.Spec.Kind == "fix" {
				candidate = completeTemplateTask(t, store, task, fmt.Sprint("fixed candidate ", round), "", nil)
			} else if task.Spec.Kind == "review" {
				completeTemplateTask(t, store, task, fmt.Sprint("review ", round), candidate.Digest, []string{"artifact_defect"})
				review = task
			}
		}
		wave, err = store.Wave(ctx, wave.ID)
		if err != nil {
			t.Fatal(err)
		}
		if wave.FixRounds != round || wave.Replans != 0 || wave.MainTurns != 0 {
			t.Fatal(wave)
		}
	}
	before := wave
	if _, err := store.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: wave.Target(), ReviewTaskID: review.ID}); !errors.Is(err, ErrTemplateNeedsMain) {
		t.Fatal("repeat limit ignored", err)
	}
	after, err := store.Wave(ctx, wave.ID)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal(after, err)
	}
}
func TestTemplateExpansionGroupsSavedOwnersIntoOneBatch(t *testing.T) {
	ctx := context.Background()
	a := kernelTemplate()
	b := kernelTemplate()
	b.ID = "test-owner"
	b.Classifications = []string{"test_defect"}
	b.FixInstruction = "fix the reported test defects"
	b.ReviewInstruction = "independently evaluate test fixes"
	store, wave, source, _ := templateFixture(t, false, []contract.FixReviewTemplate{a, b}, []string{"artifact_defect", "test_defect", "artifact_defect"})
	receipt, err := store.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: wave.Target(), ReviewTaskID: source.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.TemplateIDs) != 2 || len(receipt.Admission.CreatedTaskIDs) != 4 {
		t.Fatal(receipt)
	}
	fixes := []domain.Task{}
	reviews := []domain.Task{}
	for _, id := range receipt.Admission.CreatedTaskIDs {
		task, err := store.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if task.Spec.Kind == "fix" {
			fixes = append(fixes, task)
		} else if task.Spec.Kind == "review" {
			reviews = append(reviews, task)
		}
	}
	if len(fixes) != 2 || len(reviews) != 2 {
		t.Fatal(fixes, reviews)
	}
	if fixes[1].Spec.DependsOn[len(fixes[1].Spec.DependsOn)-1].TaskID != fixes[0].LocalID {
		t.Fatal("competing fixes are not sequenced", fixes)
	}
	for _, review := range reviews {
		if len(review.Spec.DependsOn) != 1 || review.Spec.DependsOn[0].TaskID != fixes[1].LocalID {
			t.Fatal("review bypasses final fix", review)
		}
	}
	wave, err = store.Wave(ctx, wave.ID)
	if err != nil || wave.FixRounds != 1 || wave.Replans != 0 {
		t.Fatal(wave, err)
	}
}

func TestTemplateExpansionRejectsUnsafeTransitionsWithoutPartialEffects(t *testing.T) {
	for _, mode := range []string{"unknown-class", "ambiguous-owner", "phase-budget", "active-process", "pause", "future-progress", "plan-event-failure", "budget-failure"} {
		t.Run(mode, func(t *testing.T) {
			templates := []contract.FixReviewTemplate{kernelTemplate()}
			classes := []string{"artifact_defect"}
			if mode == "unknown-class" {
				classes = []string{"specification_gap"}
			}
			if mode == "ambiguous-owner" {
				other := kernelTemplate()
				other.ID = "ambiguous"
				templates = append(templates, other)
			}
			store, wave, review, _ := templateFixture(t, true, templates, classes)
			ctx := context.Background()
			switch mode {
			case "phase-budget", "pause":
				tx, err := store.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "phase-budget" {
					wave.Policy.Limits.Phases = 4
				} else {
					wave.Paused = true
				}
				if err = saveWave(ctx, tx, wave); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "active-process":
				if _, err := store.DB.Exec("UPDATE kernel_sessions SET process_stopped=0 WHERE id=?", review.SessionID); err != nil {
					t.Fatal(err)
				}
			case "future-progress":
				tx, err := store.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				future, err := loadTask(ctx, tx, structureID("task-", wave.ID, "report"))
				if err != nil {
					t.Fatal(err)
				}
				future.State, future.Reason = contract.TaskBlocked, "requires parent decision"
				if err = saveTask(ctx, tx, future); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "plan-event-failure":
				if _, err := store.DB.Exec("CREATE TRIGGER fail_expansion BEFORE INSERT ON kernel_events WHEN NEW.kind='plan_adopted' AND NEW.plan_revision>1 BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
					t.Fatal(err)
				}
			case "budget-failure":
				if _, err := store.DB.Exec("CREATE TRIGGER fail_expansion_budget BEFORE INSERT ON kernel_budget_ledger WHEN NEW.kind='fix_round' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
					t.Fatal(err)
				}
			}
			before, err := store.Wave(ctx, wave.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: wave.Target(), ReviewTaskID: review.ID}); err == nil {
				t.Fatal("unsafe expansion admitted")
			}
			after, err := store.Wave(ctx, wave.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("failed transaction changed Wave", after, err)
			}
			tasks, err := store.Tasks(ctx, wave.ID)
			if err != nil || len(tasks) != 3 {
				t.Fatal(tasks, err)
			}
			for _, task := range tasks {
				if task.LocalID == "report" && task.State == contract.TaskCanceled {
					t.Fatal("unrelated future Task canceled on failed expansion")
				}
			}
			var count int
			if err = store.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('template_expansion','template_task_context','template_review_lineage')").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial expansion records", count, err)
			}
			var budgets int
			if err = store.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE kind='fix_round'").Scan(&budgets); err != nil || budgets != 0 {
				t.Fatal(budgets, err)
			}
		})
	}
}
func TestTemplateExpansionDrainsEveryRequiredSiblingBeforeBranching(t *testing.T) {
	ctx := context.Background()
	store, wave, review, _ := templateFixture(t, false, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"}, func(plan *contract.PlanProposal) {
		sibling := plan.Phases[1].Tasks[0]
		sibling.ID = "review-sibling"
		plan.Phases[1].Tasks = append(plan.Phases[1].Tasks, sibling)
		plan.Phases[1].Completion.RequiredTaskIDs = append(plan.Phases[1].Completion.RequiredTaskIDs, sibling.ID)
	})
	if _, err := store.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: wave.Target(), ReviewTaskID: review.ID}); err == nil {
		t.Fatal("unfinished required sibling skipped")
	}
	tasks, err := store.Tasks(ctx, wave.ID)
	if err != nil || len(tasks) != 3 {
		t.Fatal(tasks, err)
	}
}

func TestTemplateDownstreamReadsReplacementInsteadOfDefectiveSubject(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	request.Policy.Limits.Replans = 0
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	plan.Templates = []contract.FixReviewTemplate{kernelTemplate()}
	plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: "report", Tasks: []contract.TaskSpec{{ID: "report", Kind: "work", Instruction: "report reviewed candidate", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: "work"}, {TaskID: "review"}}}}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"report"}}, FailurePolicy: "drain_and_block"})
	initial, err := store.AdoptPlan(ctx, wave.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.Task(ctx, initial.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	supporting := mustImport(t, store, ArtifactImport{SessionID: work.SessionID, Target: work.Target(), Key: "supporting", Kind: "reference", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: work.ID}}, "supporting evidence")
	oldCandidate := completeTemplateTask(t, store, work, "old candidate", "", nil, supporting)
	source, err := store.Task(ctx, initial.CreatedTaskIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	completeTemplateTask(t, store, source, "old review", oldCandidate.Digest, []string{"artifact_defect"})
	expanded, err := store.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: wave.Target(), ReviewTaskID: source.ID})
	if err != nil {
		t.Fatal(err)
	}
	var fixed domain.Artifact
	var future domain.Task
	for _, id := range expanded.Admission.CreatedTaskIDs {
		task, err := store.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		switch task.Spec.Kind {
		case "fix":
			fixed = completeTemplateTask(t, store, task, "corrected candidate", "", nil)
		case "review":
			completeTemplateTask(t, store, task, "clean review", fixed.Digest, nil)
		case "work":
			future = task
		}
	}
	if _, data, err := store.ReadArtifact(ctx, future.SessionID, future.Target(), fixed.ID, 0, 100); err != nil || string(data) != "corrected candidate" {
		t.Fatal("downstream cannot read corrected candidate", string(data), err)
	}
	if _, _, err = store.ReadArtifact(ctx, future.SessionID, future.Target(), oldCandidate.ID, 0, 100); err == nil {
		t.Fatal("downstream retains defective subject grant")
	}
	if _, data, err := store.ReadArtifact(ctx, future.SessionID, future.Target(), supporting.ID, 0, 100); err != nil || string(data) != "supporting evidence" {
		t.Fatal("unrelated immutable input lost", string(data), err)
	}
	completeTemplateTask(t, store, future, "final corrected report", "", nil)
}

func TestTemplateClonedPendingReviewRetainsItsAuthorityAndNextRound(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	request.Policy.Limits.Replans = 0
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	workA := contract.TaskSpec{ID: "work-A", Kind: "work", Instruction: "produce A", Criteria: []string{"AC"}}
	workB := workA
	workB.ID = "work-B"
	workB.Instruction = "produce B"
	reviewA := contract.TaskSpec{ID: "review-A", Kind: "review", Instruction: "review A", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: workA.ID}}}
	reviewB := reviewA
	reviewB.ID = "review-B"
	reviewB.Instruction = "review B"
	reviewB.DependsOn = []contract.TaskDependency{{TaskID: workB.ID}}
	a, b := kernelTemplate(), kernelTemplate()
	a.ID, a.InputTaskID = "owner-A", reviewA.ID
	b.ID, b.InputTaskID = "owner-B", reviewB.ID
	plan := contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{a, b}, Phases: []contract.PhaseSpec{{ID: "work", Tasks: []contract.TaskSpec{workA, workB}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{workA.ID, workB.ID}}, FailurePolicy: "drain_and_block"}, {ID: "review", Tasks: []contract.TaskSpec{reviewA, reviewB}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{reviewA.ID, reviewB.ID}}, FailurePolicy: "drain_and_block"}}}
	initial, err := store.AdoptPlan(ctx, wave.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	tasks := []domain.Task{}
	for _, id := range initial.CreatedTaskIDs {
		task, err := store.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, task)
	}
	subjectA := completeTemplateTask(t, store, tasks[0], "candidate A", "", nil)
	subjectB := completeTemplateTask(t, store, tasks[1], "candidate B", "", nil)
	completeTemplateTask(t, store, tasks[2], "findings A", subjectA.Digest, []string{"artifact_defect"})
	completeTemplateTask(t, store, tasks[3], "findings B", subjectB.Digest, []string{"artifact_defect"})
	first, err := store.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: wave.Target(), ReviewTaskID: tasks[2].ID})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: first.Admission.Target, ReviewTaskID: tasks[3].ID})
	if err != nil {
		t.Fatal(err)
	}
	var fixedA, fixedB domain.Artifact
	var clonedReview domain.Task
	for _, id := range second.Admission.CreatedTaskIDs {
		task, err := store.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		var input map[string]any
		if err = store.Record(ctx, "template_task_context", task.ID, &input); err != nil {
			t.Fatal("generated clone lost input context", err)
		}
		if task.Spec.Kind == "fix" {
			if input["template_id"] == "owner-A" {
				fixedA = completeTemplateTask(t, store, task, "fixed A", "", nil)
			} else {
				fixedB = completeTemplateTask(t, store, task, "fixed B", "", nil)
			}
		} else if task.Spec.Kind == "review" {
			var lineage templateReviewLineage
			if err = store.Record(ctx, "template_review_lineage", task.ID, &lineage); err != nil {
				t.Fatal("review clone lost lineage", err)
			}
			if lineage.RootReviewLocalID == reviewA.ID {
				if lineage.Round != 1 || lineage.ExpansionKey != first.Key {
					t.Fatal(lineage)
				}
				if input["input_task_local_id"] == "" {
					t.Fatal(input)
				}
				completeTemplateTask(t, store, task, "A still needs fix", fixedA.Digest, []string{"artifact_defect"})
				clonedReview = task
			} else {
				completeTemplateTask(t, store, task, "B clean", fixedB.Digest, nil)
			}
		}
	}
	next, err := store.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: second.Admission.Target, ReviewTaskID: clonedReview.ID})
	if err != nil || next.Round != 2 {
		t.Fatal("valid next round lost after cloning", next, err)
	}
	wave, err = store.Wave(ctx, wave.ID)
	if err != nil || wave.FixRounds != 3 || wave.MainTurns != 0 || wave.Replans != 0 {
		t.Fatal(wave, err)
	}
}
