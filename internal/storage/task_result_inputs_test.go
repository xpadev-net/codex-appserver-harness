package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func handoffFixture(t *testing.T) (*KernelStore, domain.Wave, domain.Task, domain.Artifact, domain.Artifact) {
	t.Helper()
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	work, err := k.Task(ctx, admission.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	subject := completeTemplateTask(t, k, work, "original candidate", "", nil)
	review, err := k.Task(ctx, admission.CreatedTaskIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	report := completeTemplateTask(t, k, review, "original findings", subject.Digest, []string{"specification_change"})
	review, err = k.Task(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, review, subject, report
}
func handoffPlan(source string, artifacts []string) contract.PlanProposal {
	fix := contract.TaskSpec{ID: "exception-fix", Kind: "fix", Instruction: "apply parent exception decision within original criteria", Criteria: []string{"AC"}, SourceResultIDs: []string{source}, InputArtifactIDs: artifacts, DependsOn: []contract.TaskDependency{}, WorkItemIDs: []string{}}
	review := contract.TaskSpec{ID: "exception-review", Kind: "review", Instruction: "independently review the new output against the original requirement and original findings", Criteria: []string{"AC"}, SourceResultIDs: []string{source}, DependsOn: []contract.TaskDependency{{TaskID: fix.ID}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	return contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{}, Phases: []contract.PhaseSpec{
		{ID: "exception-fix", Tasks: []contract.TaskSpec{fix}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{fix.ID}}, FailurePolicy: "drain_and_block"},
		{ID: "exception-review", Tasks: []contract.TaskSpec{review}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{review.ID}}, FailurePolicy: "drain_and_block"},
	}}
}
func TestTaskResultHandoffCarriesStructuredFindingsToFreshSessions(t *testing.T) {
	ctx := context.Background()
	k, w, source, subject, report := handoffFixture(t)
	plan := handoffPlan(source.ResultID, []string{subject.ID, report.ID})
	admission, err := k.AdoptPlan(ctx, w.Target(), "parent-exception", plan)
	if err != nil || len(admission.CreatedTaskIDs) != 2 || len(admission.CarriedTaskIDs) != 0 {
		t.Fatal(admission, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.Replans != 1 || current.TasksCreated != 4 || current.PhasesCreated != 4 || current.MainTurns != 0 {
		t.Fatal(current, err)
	}
	fix, err := k.Task(ctx, admission.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if fix.ID == source.ID || fix.SessionID == source.SessionID {
		t.Fatal("source Task/session reused")
	}
	snapshot, err := k.BuildSessionInput(ctx, fix.SessionID, []contract.Target{current.Target()})
	if err != nil || len(snapshot.Input) > contract.MaxSessionInput {
		t.Fatal(snapshot, err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := sessionBindingTx(ctx, tx, fix.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := k.sessionManifestTx(ctx, tx, binding, current)
	tx.Rollback()
	inherited, ok := manifest.SourceResults[source.ResultID]
	if err != nil || !ok || inherited.TaskID != source.ID || inherited.Target != source.Target() || inherited.Result.Review == nil || inherited.Result.Review.Findings[0].Classification != "specification_change" {
		t.Fatal(inherited, err)
	}
	if len(manifest.Tasks) != 1 || manifest.Tasks[0].ID != fix.ID {
		t.Fatal("historical source became current Task", manifest.Tasks)
	}
	if _, _, err = k.ReadArtifact(ctx, fix.SessionID, fix.Target(), report.ID, 0, 128); err != nil {
		t.Fatal(err)
	}
	stoppedSource, err := k.SessionBinding(ctx, source.SessionID)
	if err != nil || !stoppedSource.Closed || stoppedSource.TaskState != contract.TaskCompleted {
		t.Fatal(stoppedSource, err)
	}
	candidate := completeTemplateTask(t, k, fix, "corrected candidate", "", nil)
	next, err := k.Task(ctx, admission.CreatedTaskIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	completeTemplateTask(t, k, next, "independent clean review", candidate.Digest, nil)
	if next.SessionID == source.SessionID || next.SessionID == fix.SessionID {
		t.Fatal("review session reused")
	}
	current, err = k.Wave(ctx, w.ID)
	if err != nil || current.Replans != 1 || current.TasksCreated != 4 {
		t.Fatal(current, err)
	}
}
func TestSourceResultReferenceDoesNotGrantArtifactBodies(t *testing.T) {
	ctx := context.Background()
	k, w, source, _, report := handoffFixture(t)
	admission, err := k.AdoptPlan(ctx, w.Target(), "data-only", handoffPlan(source.ResultID, []string{}))
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admission.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = k.ReadArtifact(ctx, task.SessionID, task.Target(), report.ID, 0, 128); !errors.Is(err, ErrArtifactGrant) {
		t.Fatal("result reference expanded body permission", err)
	}
}
func TestSourceResultAdmissionRejectsUnprovenOrForeignInputsAtomically(t *testing.T) {
	for _, mode := range []string{"missing", "unapplied", "unstopped", "active", "baseline", "thread", "changed_body", "scope", "foreign"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, source, _, _ := handoffFixture(t)
			runID := "run-" + source.ID
			switch mode {
			case "missing":
				source.ResultID = "not-a-result"
			case "unapplied":
				_, _ = k.DB.ExecContext(ctx, "UPDATE kernel_run_bindings SET applied=0 WHERE run_id=?", runID)
			case "unstopped":
				_, _ = k.DB.ExecContext(ctx, "UPDATE kernel_run_bindings SET process_stopped=0 WHERE run_id=?", runID)
			case "active":
				_, _ = k.DB.ExecContext(ctx, "UPDATE kernel_sessions SET active_run_id=?,process_stopped=0 WHERE id=?", runID, source.SessionID)
			case "baseline", "thread":
				var run contract.SessionRun
				if err := k.Record(ctx, "agent_run", runID, &run); err != nil {
					t.Fatal(err)
				}
				if mode == "baseline" {
					run.BaselineTurnIDs = []string{run.TurnID}
				} else {
					run.ThreadID = "foreign-thread"
				}
				raw, err := json.Marshal(run)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := k.DB.ExecContext(ctx, "UPDATE records SET payload=? WHERE kind='agent_run' AND id=?", raw, runID); err != nil {
					t.Fatal(err)
				}
			case "changed_body":
				var result contract.TaskResult
				if err := k.Record(ctx, "task_result", source.ResultID, &result); err != nil {
					t.Fatal(err)
				}
				result.Review.Findings[0].Description = "changed after application"
				if err := k.Put(ctx, "task_result", source.ResultID, source.ID, result); err != nil {
					t.Fatal(err)
				}
			case "scope":
				w.ScopeRevision++
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err = saveWave(ctx, tx, w); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "foreign":
				other, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: w.OrchestratorSessionID, Source: "direct", SourceID: "foreign-source", OriginalRequest: w.OriginalRequest, Criteria: w.Criteria, Policy: w.Policy})
				if err != nil {
					t.Fatal(err)
				}
				w = other
			}
			before := 0
			if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_tasks").Scan(&before); err != nil {
				t.Fatal(err)
			}
			if _, err := k.AdoptPlan(ctx, w.Target(), "bad-source", handoffPlan(source.ResultID, []string{})); err == nil {
				t.Fatal("unproven source admitted")
			}
			var after int
			if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_tasks").Scan(&after); err != nil || before != after {
				t.Fatal("partial task creation", before, after, err)
			}
		})
	}
}
func TestOptionalEmptySourceListPreservesLegacyCarryAndReplay(t *testing.T) {
	ctx := context.Background()
	k, w, _, _, _ := handoffFixture(t)
	plan := storagePlanFixture()
	legacy, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	for i := range plan.Phases {
		for j := range plan.Phases[i].Tasks {
			plan.Phases[i].Tasks[j].SourceResultIDs = []string{}
		}
	}
	empty, err := json.Marshal(plan)
	if err != nil || string(empty) != string(legacy) {
		t.Fatal("optional field changed legacy digest", err)
	}
	admission, err := k.AdoptPlan(ctx, w.Target(), "legacy-carry", plan)
	if err != nil || len(admission.CarriedTaskIDs) != 2 {
		t.Fatal(admission, err)
	}
	replay, err := k.AdoptPlan(ctx, w.Target(), "legacy-carry", plan)
	if err != nil || !reflect.DeepEqual(replay, admission) {
		t.Fatal(replay, err)
	}
}

func TestBlockedResultHandoffSurvivesSourceRetirement(t *testing.T) {
	ctx := context.Background()
	k, b, run, _ := recordedTaskResultFixture(t, func(result *contract.TaskResult) {
		result.Outcome = "blocked"
		result.Blockers = []string{"parent needs to refine the work"}
		result.Remaining = []string{"uncompleted research"}
	})
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != contract.TaskBlocked || receipt.ResultID == "" {
		t.Fatal(receipt, err)
	}
	w, err := k.Wave(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := k.AdoptPlan(ctx, w.Target(), "refined-task", handoffPlan(receipt.ResultID, []string{}))
	if err != nil {
		t.Fatal(err)
	}
	old, err := k.Task(ctx, b.TaskID)
	if err != nil || old.State != contract.TaskCanceled {
		t.Fatal(old, err)
	}
	fix, err := k.Task(ctx, admission.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := k.taskInputDigestTx(ctx, tx, w, fix.Spec)
	input, inputErr := k.taskResultInputTx(ctx, tx, w, receipt.ResultID)
	tx.Rollback()
	if err != nil || inputErr != nil || digest != fix.InputDigest || input.Result.Outcome != "blocked" || input.Result.Blockers[0] != "parent needs to refine the work" {
		t.Fatal(digest, fix.InputDigest, input, err, inputErr)
	}
	if _, err = k.BuildSessionInput(ctx, fix.SessionID, []contract.Target{w.Target()}); err != nil {
		t.Fatal(err)
	}
}
func TestCorruptSourceHoldsTaskAndRemainsExplicitInParentInput(t *testing.T) {
	ctx := context.Background()
	k, w, source, _, _ := handoffFixture(t)
	admission, err := k.AdoptPlan(ctx, w.Target(), "handoff", handoffPlan(source.ResultID, []string{}))
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	var result contract.TaskResult
	if err = k.Record(ctx, "task_result", source.ResultID, &result); err != nil {
		t.Fatal(err)
	}
	result.Review.Findings[0].Description = "corrupt structured finding"
	if err = k.Put(ctx, "task_result", source.ResultID, source.ID, result); err != nil {
		t.Fatal(err)
	}
	set, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
	if err != nil || len(set.Tasks) != 0 || len(set.Held) != 1 || set.Held[0].Reason != "input_unavailable" {
		t.Fatal(set, err)
	}
	if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || !changed {
		t.Fatal(changed, err)
	}
	pending, err := k.PendingWakeNotifications(ctx, w.OrchestratorSessionID, 16)
	if err != nil || len(pending) != 1 {
		t.Fatal(pending, err)
	}
	if _, err = k.ReserveWake(ctx, WakeRequest{SessionID: w.OrchestratorSessionID, Key: "corrupt-source-parent", NotificationIDs: []string{pending[0].ID}, IncludeSessionContext: true}); err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admission.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.BuildSessionInput(ctx, task.SessionID, []contract.Target{w.Target()}); err == nil {
		t.Fatal("corrupt source Task input accepted")
	}
}
func TestHandoffTaskInsertFailureRollsBackBudgetsAndRetirement(t *testing.T) {
	ctx := context.Background()
	k, w, source, _, _ := handoffFixture(t)
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_handoff BEFORE INSERT ON kernel_tasks WHEN NEW.local_id='exception-review' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "failed-handoff", handoffPlan(source.ResultID, []string{})); err == nil {
		t.Fatal("partial handoff committed")
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.Target() != w.Target() || current.Replans != 0 || current.TasksCreated != 2 {
		t.Fatal(current, err)
	}
	var tasks, sessions int
	if err = k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if err = k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_sessions WHERE kind='task'").Scan(&sessions); err != nil || tasks != 2 || sessions != 2 {
		t.Fatal(tasks, sessions, err)
	}
}
