package storage

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func recordedTaskResultFixture(t *testing.T, mutate func(*contract.TaskResult)) (*KernelStore, contract.SessionBinding, contract.SessionRun, contract.TaskResult) {
	t.Helper()
	ctx := context.Background()
	store, b := boundTaskFixture(t)
	request := ArtifactImport{SessionID: b.ID, Target: b.Target, Key: "output", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: b.TaskID}}
	a := mustImport(t, store, request, "observed output")
	result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, PhaseID: b.PhaseID, TaskID: b.TaskID, Outcome: "result", Artifacts: []string{a.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", EvidenceIDs: []string{a.ID}, Status: "claimed", Basis: "observed output"}}}
	if mutate != nil {
		mutate(&result)
	}
	run := contract.SessionRun{ID: "run", ThreadID: b.ThreadID, State: "starting", Started: time.Now().UTC(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err := store.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err != nil {
		t.Fatal(err)
	}
	raw, err := marshalTaskResultFixture(result)
	if err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID, run.Final = "completed", "turn", string(raw)
	if err = store.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	return store, b, run, result
}
func TestTaskResultApplicationRequiresStoppedNormalTurnAndIsReplayable(t *testing.T) {
	ctx := context.Background()
	store, b, run, result := recordedTaskResultFixture(t, nil)
	if _, err := store.ApplyTaskResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("live process result applied")
	}
	if err := store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != contract.TaskCompleted || receipt.ResultID == "" {
		t.Fatal(receipt, err)
	}
	task, err := store.Task(ctx, b.TaskID)
	if err != nil || task.ResultID != receipt.ResultID || task.State != contract.TaskCompleted || task.CompletedAt.IsZero() {
		t.Fatal(task, err)
	}
	var saved contract.TaskResult
	if err = store.Record(ctx, "task_result", receipt.ResultID, &saved); err != nil || saved.Target != result.Target || saved.TaskID != result.TaskID || !reflect.DeepEqual(saved.Artifacts, result.Artifacts) {
		t.Fatal(saved, err)
	}
	replay, err := store.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || !reflect.DeepEqual(replay, receipt) {
		t.Fatal(replay, err)
	}
	current, err := store.SessionBinding(ctx, b.ID)
	if err != nil || !current.Closed {
		t.Fatal(current, err)
	}
	if err = store.ReserveSessionThread(ctx, current); err == nil {
		t.Fatal("completed Task resumed")
	}
	var events int
	if err = store.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='task_result_applied'").Scan(&events); err != nil || events != 1 {
		t.Fatal(events, err)
	}
	wave, err := store.Wave(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.completedTaskResultTx(ctx, tx, wave, task); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
}
func TestTaskResultContractViolationIsTerminalAndKeepsPhysicalEvidence(t *testing.T) {
	for _, mutation := range []string{"wrong-task", "wrong-scope", "missing-criterion", "invented-criterion", "product-ref"} {
		t.Run(mutation, func(t *testing.T) {
			store, b, run, _ := recordedTaskResultFixture(t, func(r *contract.TaskResult) {
				switch mutation {
				case "wrong-task":
					r.TaskID = "other"
				case "wrong-scope":
					r.Target.ScopeRevision++
				case "missing-criterion":
					r.CriterionResults = nil
				case "invented-criterion":
					r.CriterionResults[0].CriterionID = "unknown"
				case "product-ref":
					r.WorkItemRefs = []contract.CandidateReference{{WorkItemID: "w", CandidateID: "c", SHA: "0123456789012345678901234567890123456789"}}
				}
			})
			ctx := context.Background()
			if err := store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			receipt, err := store.ApplyTaskResult(ctx, b.ID, run.ID)
			if err != nil || receipt.State != contract.TaskFailed || receipt.Reason == "" || receipt.ResultID != "" {
				t.Fatal(receipt, err)
			}
			var physical contract.SessionRun
			if err = store.Record(ctx, "agent_run", run.ID, &physical); err != nil || physical.State != "completed" || physical.Final != run.Final {
				t.Fatal(physical, err)
			}
		})
	}
}
func TestTaskResultReferenceCanReconcileWithoutAnotherModelAttempt(t *testing.T) {
	ctx := context.Background()
	store, b := boundTaskFixture(t)
	r := ArtifactImport{SessionID: b.ID, Target: b.Target, Key: "pending", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: b.TaskID}}
	a, err := store.reserveArtifact(ctx, r, []byte("late bytes"))
	if err != nil {
		t.Fatal(err)
	}
	result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "result", Artifacts: []string{a.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "late bytes", EvidenceIDs: []string{a.ID}}}}
	run := contract.SessionRun{ID: "run", ThreadID: b.ThreadID, State: "starting", Started: time.Now().UTC(), BaselineKnown: true}
	if err = store.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err != nil {
		t.Fatal(err)
	}
	raw, err := marshalTaskResultFixture(result)
	if err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID, run.Final = "completed", "turn", string(raw)
	if err = store.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.ApplyTaskResult(ctx, b.ID, run.ID)
	if !errors.Is(err, ErrTaskResultReferences) || receipt.State != contract.TaskBlocked {
		t.Fatal(receipt, err)
	}
	if _, err = store.ImportArtifact(ctx, r, []byte("late bytes")); err != nil {
		t.Fatal(err)
	}
	receipt, err = store.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal(receipt, err)
	}
	task, err := store.Task(ctx, b.TaskID)
	if err != nil || task.Attempts != 1 {
		t.Fatal(task, err)
	}
}
func TestTaskResultLateCanceledOrSupersededOutcomeIsArchived(t *testing.T) {
	for _, mode := range []string{"cancel", "steer", "pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store, b, run, _ := recordedTaskResultFixture(t, nil)
			if err := store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
				t.Fatal(err)
			}
			wave, err := store.Wave(ctx, b.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := store.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "cancel":
				wave.State = domain.WaveCanceled
			case "steer":
				wave.ScopeRevision++
			case "pause":
				wave.Paused = true
			}
			if err = saveWave(ctx, tx, wave); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			receipt, err := store.ApplyTaskResult(ctx, b.ID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "pause" {
				if receipt.State != contract.TaskCompleted {
					t.Fatal(receipt)
				}
			} else if !receipt.Archived || receipt.State == contract.TaskCompleted || receipt.ResultID != "" {
				t.Fatal(receipt)
			}
			current, err := store.SessionBinding(ctx, b.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.ReserveSessionThread(ctx, current); err == nil {
				t.Fatal("late result resumed forbidden Task")
			}
		})
	}
}
func TestTaskResultWaitingRequiresOwnedDurableSubscription(t *testing.T) {
	ctx := context.Background()
	store, b, run, _ := recordedTaskResultFixture(t, func(r *contract.TaskResult) {
		r.Outcome = "waiting"
		r.Artifacts = nil
		r.CriterionResults = nil
		r.SubscriptionIDs = []string{"subscription"}
	})
	if err := store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.ApplyTaskResult(ctx, b.ID, run.ID)
	if !errors.Is(err, ErrTaskResultReferences) || receipt.State != contract.TaskBlocked {
		t.Fatal(receipt, err)
	}
	subscription, _ := json.Marshal(map[string]any{"target": b.Target})
	if _, err = store.DB.Exec("INSERT INTO kernel_subscriptions(id,session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload) VALUES(?,?,?,?,?,'message',0,?)", "subscription", b.ID, b.Target.WaveID, b.Target.ScopeRevision, b.Target.PlanRevision, subscription); err != nil {
		t.Fatal(err)
	}
	receipt, err = store.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != contract.TaskWaiting {
		t.Fatal(receipt, err)
	}
	task, err := store.Task(ctx, b.TaskID)
	if err != nil || task.State.Terminal() || task.ResultID != "" {
		t.Fatal(task, err)
	}
}

func marshalTaskResultFixture(r contract.TaskResult) ([]byte, error) {
	r.Artifacts = append([]string{}, r.Artifacts...)
	r.CriterionResults = append([]contract.CriterionResult{}, r.CriterionResults...)
	r.WorkItemRefs = append([]contract.CandidateReference{}, r.WorkItemRefs...)
	r.Remaining = append([]string{}, r.Remaining...)
	r.Blockers = append([]string{}, r.Blockers...)
	r.SubscriptionIDs = append([]string{}, r.SubscriptionIDs...)
	if r.Review != nil {
		review := *r.Review
		review.Findings = append([]contract.ReviewFinding{}, review.Findings...)
		r.Review = &review
	}
	return json.Marshal(r)
}

func TestTaskResultApplicationEventFailureRollsBackAndReopens(t *testing.T) {
	ctx := context.Background()
	store, b, run, _ := recordedTaskResultFixture(t, nil)
	if err := store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.Exec("CREATE TRIGGER fail_result BEFORE INSERT ON kernel_events WHEN NEW.kind='task_result_applied' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyTaskResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("injected transaction failure ignored")
	}
	task, err := store.Task(ctx, b.TaskID)
	if err != nil || task.State != contract.TaskRunning || task.ResultID != "" {
		t.Fatal(task, err)
	}
	var applied int
	if err = store.DB.QueryRow("SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&applied); err != nil || applied != 0 {
		t.Fatal(applied, err)
	}
	if _, err = store.DB.Exec("DROP TRIGGER fail_result"); err != nil {
		t.Fatal(err)
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
	receipt, err := reopened.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal(receipt, err)
	}
	replay, err := reopened.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || !reflect.DeepEqual(receipt, replay) {
		t.Fatal(replay, err)
	}
}

func TestTaskResultReviewTargetMustBeInItsExplicitArtifactGrant(t *testing.T) {
	for _, target := range []string{"granted", "ungranted"} {
		t.Run(target, func(t *testing.T) {
			ctx := context.Background()
			store, b := boundTaskFixture(t)
			output := ArtifactImport{SessionID: b.ID, Target: b.Target, Key: "A", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: b.TaskID}}
			a := mustImport(t, store, output, "candidate A")
			output.Key = "B"
			other := mustImport(t, store, output, "candidate B")
			result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "result", Artifacts: []string{a.ID, other.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "observed", EvidenceIDs: []string{a.ID}}}}
			recordAndStopTaskResult(t, store, b, "work-run", result)
			if receipt, err := store.ApplyTaskResult(ctx, b.ID, "work-run"); err != nil || receipt.State != contract.TaskCompleted {
				t.Fatal(receipt, err)
			}
			// Parent explicitly grants A to a fresh reviewer after the producer completes.
			plan := storagePlanFixture()
			plan.Phases[1].ID = "review-explicit"
			plan.Phases[1].Tasks[0].ID = "review-explicit"
			plan.Phases[1].Tasks[0].DependsOn[0].ArtifactID = a.ID
			plan.Phases[1].Completion.RequiredTaskIDs = []string{"review-explicit"}
			receipt, err := store.AdoptPlan(ctx, b.Target, "review-A", plan)
			if err != nil {
				t.Fatal(err)
			}
			review, err := store.Task(ctx, receipt.CreatedTaskIDs[0])
			if err != nil {
				t.Fatal(err)
			}
			rb, err := store.SessionBinding(ctx, review.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			rb = reserveSessionFixture(t, store, rb)
			if err = store.BindSessionThread(ctx, rb, "review-thread"); err != nil {
				t.Fatal(err)
			}
			rb, err = store.SessionBinding(ctx, rb.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = store.ReadArtifact(ctx, rb.ID, rb.Target, other.ID, 0, 16); err == nil {
				t.Fatal("reviewer can read ungranted B")
			}
			report := mustImport(t, store, ArtifactImport{SessionID: rb.ID, Target: rb.Target, Key: "review-report", Kind: "review", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: rb.TaskID}}, "review report")
			digest := a.Digest
			if target == "ungranted" {
				digest = other.Digest
			}
			reviewed := contract.TaskResult{SchemaVersion: 1, Target: rb.Target, TaskID: rb.TaskID, PhaseID: rb.PhaseID, Outcome: "result", Artifacts: []string{report.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "evaluated", EvidenceIDs: []string{report.ID}}}, Review: &contract.ReviewResult{TargetDigest: digest, Verdict: "clean", Findings: []contract.ReviewFinding{}}}
			recordAndStopTaskResult(t, store, rb, "review-run", reviewed)
			applied, err := store.ApplyTaskResult(ctx, rb.ID, "review-run")
			if err != nil {
				t.Fatal(err)
			}
			expected := contract.TaskCompleted
			if target == "ungranted" {
				expected = contract.TaskFailed
			}
			if applied.State != expected {
				t.Fatal(applied)
			}
		})
	}
}
func recordAndStopTaskResult(t *testing.T, store *KernelStore, b contract.SessionBinding, id string, result contract.TaskResult) {
	t.Helper()
	ctx := context.Background()
	run := contract.SessionRun{ID: id, ThreadID: b.ThreadID, State: "starting", Started: time.Now().UTC(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err := store.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err != nil {
		t.Fatal(err)
	}
	raw, err := marshalTaskResultFixture(result)
	if err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID, run.Final = "completed", id+"-turn", string(raw)
	if err = store.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
}
