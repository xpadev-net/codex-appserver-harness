package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func readyFixture(t *testing.T, mutations ...func(*contract.PlanProposal)) (*KernelStore, domain.Wave, []domain.Task) {
	t.Helper()
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	wave, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	spec := func(id string) contract.TaskSpec {
		return contract.TaskSpec{ID: id, Kind: "work", Instruction: "produce independent evidence", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	}
	a, b, report := spec("a"), spec("b"), spec("report")
	report.DependsOn = []contract.TaskDependency{{TaskID: "a"}, {TaskID: "b"}}
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{
		{ID: "parallel", Tasks: []contract.TaskSpec{a, b}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"a", "b"}}, FailurePolicy: "drain_and_block"},
		{ID: "report", Tasks: []contract.TaskSpec{report}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"report"}}, FailurePolicy: "drain_and_block"},
	}, Templates: []contract.FixReviewTemplate{}}
	for _, mutate := range mutations {
		mutate(&plan)
	}
	receipt, err := k.AdoptPlan(ctx, wave.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	tasks := []domain.Task{}
	for _, id := range receipt.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, task)
	}
	return k, wave, tasks
}
func readyIDs(set ReadySet) []string {
	ids := []string{}
	for _, r := range set.Tasks {
		ids = append(ids, r.Task.ID)
	}
	return ids
}
func mustReady(t *testing.T, k *KernelStore, w domain.Wave, ids ...string) ReadySet {
	t.Helper()
	set, err := k.ReadyTasks(context.Background(), ReadyRequest{Target: w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(readyIDs(set), append([]string{}, ids...)) {
		t.Fatalf("ready=%v want=%v, state=%s reason=%s held=%v", readyIDs(set), ids, set.State, set.Reason, set.Held)
	}
	return set
}
func TestReadyPhaseSelectsIndependentTasksAndAdvancesOnlyOnAllSuccess(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := readyFixture(t)
	first := mustReady(t, k, w, tasks[0].ID, tasks[1].ID)
	if first.PhaseID != tasks[0].PhaseID || first.State != "active" {
		t.Fatal(first)
	}
	b := prepareWakeSession(t, k, tasks[0].SessionID)
	run := contract.SessionRun{ID: "a-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
	if err := k.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err != nil {
		t.Fatal(err)
	}
	mustReady(t, k, w, tasks[1].ID) // running A must not serialize independent B.
	completeTemplateTask(t, k, tasks[1], "independent B", "", nil)
	mustReady(t, k, w) // next Phase still waits for A.
	run.State, run.TurnID = "failed", "a-turn"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := k.ArchiveSessionRun(ctx, b.ID, run.ID, "classified transient failure"); err != nil {
		t.Fatal(err)
	}
	recovery, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target(), RecoveryTaskIDs: []string{tasks[0].ID}})
	if err != nil || !reflect.DeepEqual(readyIDs(recovery), []string{tasks[0].ID}) {
		t.Fatal(recovery, err)
	}
	authorizeRecoveryFixture(t, k, b.TaskID, "a-retry", sessionSnapshot())
	// Complete the authorized second Attempt on the existing unfinished thread.
	b = prepareWakeSession(t, k, b.ID)
	a := mustImport(t, k, ArtifactImport{SessionID: b.ID, Target: b.Target, Key: "retry-output", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: b.TaskID}}, "recovered observation")
	result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "result", Artifacts: []string{a.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "independent observation", EvidenceIDs: []string{a.ID}}}}
	recordAndStopTaskResult(t, k, b, "a-retry", result)
	if applied, err := k.ApplyTaskResult(ctx, b.ID, "a-retry"); err != nil || applied.State != contract.TaskCompleted {
		t.Fatal(applied, err)
	}
	mustReady(t, k, w, tasks[2].ID)
	completeTemplateTask(t, k, tasks[2], "combined report", "", nil)
	final := mustReady(t, k, w)
	if final.State != "execution_completed" {
		t.Fatal(final)
	}
	stored, err := k.Wave(ctx, w.ID)
	if err != nil || stored.State == domain.WaveCompleted {
		t.Fatal("execution completion bypassed Wave acceptance", stored, err)
	}
}
func TestReadyDrainAndBlockHoldsNewTasksButAllowsExplicitRecovery(t *testing.T) {
	for _, state := range []contract.TaskState{contract.TaskBlocked, contract.TaskFailed, contract.TaskCanceled} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			k, w, tasks := readyFixture(t)
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			tasks[0].State = state
			tasks[0].Attempts = 1
			// Aggregate progress must not prevent this Task's authorized
			// recovery; the actual Phase/Task conditions control eligibility.
			w.State = domain.WaveBlocked
			if err = saveWave(ctx, tx, w); err != nil {
				t.Fatal(err)
			}
			if err = saveTask(ctx, tx, tasks[0]); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			set := mustReady(t, k, w)
			if set.Reason != "drain_and_block" || set.PhaseID != tasks[0].PhaseID {
				t.Fatal(set)
			}
			if state == contract.TaskBlocked {
				set, err = k.ReadyTasks(ctx, ReadyRequest{Target: w.Target(), RecoveryTaskIDs: []string{tasks[0].ID}})
				if err != nil || !reflect.DeepEqual(readyIDs(set), []string{tasks[0].ID}) {
					t.Fatal(set, err)
				}
			}
			// A previously selected fresh Task must also be denied at turn reservation.
			b := prepareWakeSession(t, k, tasks[1].SessionID)
			run := contract.SessionRun{ID: "forbidden", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
			if err = k.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err == nil {
				t.Fatal("drain bypassed during dispatch")
			}
			var count int
			if err = k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings WHERE run_id='forbidden'").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("rejected dispatch changed ledger")
			}
		})
	}
}
func TestReadyUnknownRunHoldsSiblingAcrossRestartAndStoppingProof(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := readyFixture(t)
	b := prepareWakeSession(t, k, tasks[0].SessionID)
	run := contract.SessionRun{ID: "unknown", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
	if err := k.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err != nil {
		t.Fatal(err)
	}
	run.State = "unknown"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	mustReady(t, k, w)
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	mustReady(t, k, w) // process stop alone does not resolve an unknown turn.
	path := k.Root + "/state.db"
	k.Close()
	reopened, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	set := mustReady(t, reopened, w)
	if set.Reason != "drain_and_block" {
		t.Fatal(set)
	}
}
func TestReadyRechecksPhaseAndInputAtDispatchWithoutChargingRun(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := readyFixture(t)
	mustReady(t, k, w, tasks[0].ID, tasks[1].ID)
	b := prepareWakeSession(t, k, tasks[2].SessionID)
	run := contract.SessionRun{ID: "future", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
	if err := k.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err == nil {
		t.Fatal("future Phase began before all_success")
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE operation_key='future'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("future Phase charged a run")
	}
	// A new current target cannot reinterpret a previous selection.
	stale := w.Target()
	stale.PlanRevision++
	if _, err := k.ReadyTasks(ctx, ReadyRequest{Target: stale}); err == nil {
		t.Fatal("stale target accepted")
	}
	for _, request := range []ReadyRequest{{Target: w.Target(), RecoveryTaskIDs: []string{tasks[1].ID}}, {Target: w.Target(), ResumeTaskIDs: []string{tasks[1].ID}}} {
		if _, err := k.ReadyTasks(ctx, request); err == nil {
			t.Fatal("unclassified recovery accepted")
		}
	}
}
func TestReadyGatesPauseDeadlineAndCorruptCompletedEvidence(t *testing.T) {
	for _, condition := range []string{"pause", "deadline", "missing_result", "artifact_bytes", "old_binding"} {
		t.Run(condition, func(t *testing.T) {
			ctx := context.Background()
			k, w, tasks := readyFixture(t)
			if condition == "pause" || condition == "deadline" {
				if condition == "pause" {
					w.Paused = true
				} else {
					k.Now = func() time.Time { return w.Deadline }
				}
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
			} else {
				a := completeTemplateTask(t, k, tasks[0], "recorded evidence", "", nil)
				switch condition {
				case "missing_result":
					if _, err := k.DB.Exec("DELETE FROM records WHERE kind='task_result' AND id=(SELECT source_result_id FROM kernel_plan_bindings WHERE task_id=?)", tasks[0].ID); err != nil {
						t.Fatal(err)
					}
				case "artifact_bytes":
					if err := os.WriteFile(filepath.Join(k.Root, "objects", a.ID), []byte("tampered"), 0600); err != nil {
						t.Fatal(err)
					}
				case "old_binding":
					if _, err := k.DB.Exec("DELETE FROM kernel_plan_bindings WHERE task_id=?", tasks[0].ID); err != nil {
						t.Fatal(err)
					}
				}
			}
			set, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
			if condition == "old_binding" {
				if err == nil {
					t.Fatal("missing current binding accepted")
				}
				return
			}
			if err != nil || len(set.Tasks) != 0 {
				t.Fatal(set, err)
			}
		})
	}
}

func TestReadyPhaseEvidenceSurvivesCollectionUntilWaveAcceptance(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := readyFixture(t, func(p *contract.PlanProposal) { p.Phases[1].Tasks[0].DependsOn = []contract.TaskDependency{} })
	base := k.clock()
	k.Now = func() time.Time { return base }
	w.Policy.Limits.RetentionSeconds = 1
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
	a := completeTemplateTask(t, k, tasks[0], "A completion proof", "", nil)
	b := completeTemplateTask(t, k, tasks[1], "B completion proof", "", nil)
	k.Now = func() time.Time { return base.Add(2 * time.Second) }
	if err = k.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{a.ID, b.ID} {
		artifact, err := k.Artifact(ctx, id)
		if err != nil || artifact.State != "committed" {
			t.Fatal("Phase barrier evidence collected", artifact, err)
		}
	}
	mustReady(t, k, w, tasks[2].ID) // later Phase has no explicit data dependency.
	c := completeTemplateTask(t, k, tasks[2], "independent later result", "", nil)
	k.Now = func() time.Time { return base.Add(4 * time.Second) }
	if err = k.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	if set := mustReady(t, k, w); set.State != "execution_completed" {
		t.Fatal(set)
	}
	// Final acceptance still needs the current results after every Task is done.
	for _, id := range []string{a.ID, b.ID, c.ID} {
		artifact, err := k.Artifact(ctx, id)
		if err != nil || artifact.State != "committed" {
			t.Fatal(artifact, err)
		}
	}
	tx, err = k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w, err = loadWave(ctx, tx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	w.State, w.CompletedAt = domain.WaveCompleted, k.clock()
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = k.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{a.ID, b.ID, c.ID} {
		artifact, err := k.Artifact(ctx, id)
		if err != nil || artifact.State != "expired" {
			t.Fatal("terminal evidence did not follow retention", artifact, err)
		}
	}
}
func TestReadyRetainsCarriedEvidenceAndReleasesDroppedPhaseEvidence(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := readyFixture(t, func(p *contract.PlanProposal) { p.Phases[1].Tasks[0].DependsOn = []contract.TaskDependency{} })
	base := k.clock()
	k.Now = func() time.Time { return base }
	w.Policy.Limits.RetentionSeconds = 1
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
	a := completeTemplateTask(t, k, tasks[0], "carried A", "", nil)
	b := completeTemplateTask(t, k, tasks[1], "carried B", "", nil)
	var admission PlanAdmission
	var raw []byte
	if err = k.DB.QueryRow("SELECT payload FROM kernel_plans WHERE wave_id=? AND revision=1", w.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &admission); err != nil {
		t.Fatal(err)
	}
	plan := admission.Plan
	plan.Phases[1].ID = "report-2"
	plan.Phases[1].Tasks[0].ID = "report-2"
	plan.Phases[1].Completion.RequiredTaskIDs = []string{"report-2"}
	receipt, err := k.AdoptPlan(ctx, w.Target(), "carry", plan)
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	k.Now = func() time.Time { return base.Add(2 * time.Second) }
	if err = k.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	mustReady(t, k, w, receipt.CreatedTaskIDs[0])
	for _, id := range []string{a.ID, b.ID} {
		artifact, err := k.Artifact(ctx, id)
		if err != nil || artifact.State != "committed" {
			t.Fatal("carried Phase evidence collected", artifact, err)
		}
	}
	replacement := plan.Phases[1]
	replacement.ID = "replacement"
	replacement.Tasks[0].ID = "replacement"
	replacement.Completion.RequiredTaskIDs = []string{"replacement"}
	plan.Phases = []contract.PhaseSpec{replacement}
	if _, err = k.AdoptPlan(ctx, w.Target(), "drop", plan); err != nil {
		t.Fatal(err)
	}
	if err = k.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{a.ID, b.ID} {
		artifact, err := k.Artifact(ctx, id)
		if err != nil || artifact.State != "expired" {
			t.Fatal("historical dropped evidence was retained", artifact, err)
		}
	}
}
