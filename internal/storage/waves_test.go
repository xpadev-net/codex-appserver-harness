package storage

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func waveStoreFixture(t *testing.T) (*KernelStore, WaveRequest) {
	t.Helper()
	path := kernelPath(t)
	store, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	workspace := filepath.Join(filepath.Dir(store.Root), "workspace")
	if err = os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CreateOrchestrator(context.Background(), "main", "configured", workspace, "read-only"); err != nil {
		t.Fatal(err)
	}
	return store, WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "request", OriginalRequest: "research and report", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "evidence-backed report"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: domain.DefaultKernelLimits()}}
}
func storagePlanFixture() contract.PlanProposal {
	work := contract.TaskSpec{ID: "work", Kind: "work", Instruction: "produce research", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	review := contract.TaskSpec{ID: "review", Kind: "review", Instruction: "independent review", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: "work", ArtifactID: ""}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	return contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "research", Tasks: []contract.TaskSpec{work}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"work"}}, FailurePolicy: "drain_and_block"}, {ID: "evaluation", Tasks: []contract.TaskSpec{review}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"review"}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
}
func TestDirectWavePlanAdmissionIsAtomicAndReplayable(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := store.CreateWave(ctx, request)
	if err != nil || repeated.ID != wave.ID {
		t.Fatal(repeated, err)
	}
	changed := request
	changed.OriginalRequest += " changed"
	if _, err = store.CreateWave(ctx, changed); err == nil {
		t.Fatal("changed source reused old Wave")
	}
	receipt, err := store.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture())
	if err != nil || !reflect.DeepEqual(receipt, again) {
		t.Fatal(again, err)
	}
	tasks, err := store.Tasks(ctx, wave.ID)
	if err != nil || len(tasks) != 2 {
		t.Fatal(tasks, err)
	}
	if tasks[0].SessionID == tasks[1].SessionID || tasks[0].PhaseID == tasks[1].PhaseID {
		t.Fatal("Task/session or Phase collapsed")
	}
	var sessions int
	if err = store.DB.QueryRow("SELECT count(*) FROM kernel_sessions WHERE kind='task'").Scan(&sessions); err != nil || sessions != 2 {
		t.Fatal(sessions, err)
	}
	current, err := store.Wave(ctx, wave.ID)
	if err != nil || current.TasksCreated != 2 || current.PhasesCreated != 2 || current.PlanRevision != 1 || current.Replans != 0 {
		t.Fatal(current, err)
	}
	var events, intents int
	store.DB.QueryRow("SELECT count(*) FROM kernel_events").Scan(&events)
	store.DB.QueryRow("SELECT count(*) FROM records WHERE kind='notification_intent'").Scan(&intents)
	if events != 2 || intents != events {
		t.Fatal("event/notification intent lost", events, intents)
	}
	path := filepath.Join(store.Root, "state.db")
	store.Close()
	store, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	replay, err := store.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture())
	if err != nil || !reflect.DeepEqual(replay, receipt) {
		t.Fatal("reboot duplicated plan", replay, err)
	}
}
func TestPlanAdmissionRollsBackStructureAndBudget(t *testing.T) {
	for _, failure := range []string{"coverage", "budget", "event_crash", "cross_scope_artifact", "active_process"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			store, request := waveStoreFixture(t)
			if failure == "budget" {
				request.Policy.Limits.Tasks = 1
			}
			wave, err := store.CreateWave(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			plan := storagePlanFixture()
			switch failure {
			case "coverage":
				plan.Phases[0].Tasks[0].Criteria = []string{"invented"}
			case "event_crash":
				_, err = store.DB.Exec("CREATE TRIGGER inject_plan_crash BEFORE INSERT ON kernel_events WHEN NEW.kind='plan_adopted' BEGIN SELECT RAISE(ABORT,'injected crash'); END")
				if err != nil {
					t.Fatal(err)
				}
			case "cross_scope_artifact":
				plan.Phases[0].Tasks[0].InputArtifactIDs = []string{"not-authorized"}
			case "active_process":
				base, err := store.AdoptPlan(ctx, wave.Target(), "initial", plan)
				if err != nil {
					t.Fatal(err)
				}
				_, err = store.DB.Exec("UPDATE kernel_sessions SET process_stopped=0 WHERE task_id=?", base.CreatedTaskIDs[0])
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err = store.AdoptPlan(ctx, wave.Target(), "rejected", plan); err == nil {
				t.Fatal("invalid plan accepted")
			}
			var taskCount, planCount int
			store.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&taskCount)
			store.DB.QueryRow("SELECT count(*) FROM kernel_plans").Scan(&planCount)
			expectedTasks, expectedPlans := 0, 0
			if failure == "active_process" {
				expectedTasks, expectedPlans = 2, 1
			}
			if taskCount != expectedTasks || planCount != expectedPlans {
				t.Fatal("partial plan survived", taskCount, planCount)
			}
			current, _ := store.Wave(ctx, wave.ID)
			if current.TasksCreated != expectedTasks {
				t.Fatal("budget reservation survived rollback", current)
			}
		})
	}
}
func TestReplanUsesFreshTasksAndRetainsCumulativeBudgets(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	first := storagePlanFixture()
	if _, err = store.AdoptPlan(ctx, wave.Target(), "first", first); err != nil {
		t.Fatal(err)
	}
	next := storagePlanFixture()
	next.Phases[0].ID = "research-2"
	next.Phases[0].Tasks[0].ID = "work-2"
	next.Phases[0].Completion.RequiredTaskIDs = []string{"work-2"}
	next.Phases[1].ID = "evaluation-2"
	next.Phases[1].Tasks[0].ID = "review-2"
	next.Phases[1].Tasks[0].DependsOn[0].TaskID = "work-2"
	next.Phases[1].Completion.RequiredTaskIDs = []string{"review-2"}
	receipt, err := store.AdoptPlan(ctx, wave.Target(), "replan", next)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Target.PlanRevision != 2 {
		t.Fatal(receipt)
	}
	current, _ := store.Wave(ctx, wave.ID)
	if current.TasksCreated != 4 || current.PhasesCreated != 4 || current.Replans != 1 {
		t.Fatal(current)
	}
	old, err := store.Task(ctx, structureID("task-", wave.ID, "work"))
	if err != nil || old.State != contract.TaskCanceled || old.Reason == "" {
		t.Fatal(old, err)
	}
	if _, err = store.AdoptPlan(ctx, wave.Target(), "stale", next); err == nil {
		t.Fatal("stale plan revision applied")
	}
	if _, err = store.AdoptPlan(ctx, wave.Target(), "first", next); err == nil {
		t.Fatal("same key accepted changed plan")
	}
}
func TestPlanCarriesOnlyMatchingCompletedResultBindings(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	first, err := store.AdoptPlan(ctx, wave.Target(), "first", plan)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range first.CreatedTaskIDs {
		task, err := loadTask(ctx, tx, id)
		if err != nil {
			t.Fatal(err)
		}
		task.State = contract.TaskCompleted
		task.ResultID = "result-" + id
		task.InputDigest, err = store.taskInputDigestTx(ctx, tx, wave, task.Spec)
		if err != nil {
			t.Fatal(err)
		}
		if err = saveTask(ctx, tx, task); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.AdoptPlan(ctx, wave.Target(), "carry", plan)
	if err != nil || len(receipt.CreatedTaskIDs) != 0 || len(receipt.CarriedTaskIDs) != 2 {
		t.Fatal(receipt, err)
	}
	var bindings int
	if err = store.DB.QueryRow("SELECT count(*) FROM kernel_plan_bindings WHERE revision=2 AND source_result_id<>''").Scan(&bindings); err != nil || bindings != 2 {
		t.Fatal(bindings, err)
	}
}
func TestOrchestratorCannotExposeControllerRootAsWorkspace(t *testing.T) {
	store, request := waveStoreFixture(t)
	if _, err := store.CreateOrchestrator(context.Background(), "unsafe", "configured", filepath.Dir(store.Root), "read-only"); err == nil {
		t.Fatal("controller root exposed")
	}
	request.OrchestratorSessionID = "missing"
	if _, err := store.CreateWave(context.Background(), request); err == nil {
		t.Fatal("missing parent accepted")
	}
	var count int
	if err := store.DB.QueryRow("SELECT count(*) FROM kernel_waves").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}
