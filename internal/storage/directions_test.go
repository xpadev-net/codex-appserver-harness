package storage

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func directionRequest(w domain.Wave) WaveDirectionRequest {
	return WaveDirectionRequest{Key: "direction", Target: w.Target(), Instruction: "focus the report on reproducible observations"}
}
func mustDirection(t *testing.T, k *KernelStore, w domain.Wave) WaveDirection {
	t.Helper()
	d, err := k.RequestWaveDirection(context.Background(), directionRequest(w))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestWaveDirectionGatesDispatchAndRequiresFreshPlanWithoutResettingBudget(t *testing.T) {
	ctx := context.Background()
	k, w, tasks := readyFixture(t)
	before, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	d := mustDirection(t, k, w)
	mustReady(t, k, w)
	mustControl(t, k, operatorRequest(w, "resume", "resume", ""))
	mustReady(t, k, w) // Manual resume cannot clear the direction barrier.
	b, err := k.SessionBinding(ctx, tasks[0].SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReserveManagedProcess(ctx, ManagedProcessRequest{OwnerID: "forbidden", RunID: "forbidden", Mode: "execute", Binding: b}); err == nil {
		t.Fatal("direction allowed another old-scope process")
	}
	if _, err = k.ReserveOperationCapacity(ctx, operationRequest(t, w, "runner", "forbidden-operation")); err == nil {
		t.Fatal("direction allowed an old-scope operation")
	}
	applied, err := k.ApplyWaveDirection(ctx, d.ID)
	if err != nil || applied.State != "applied" || applied.AppliedTarget.ScopeRevision != before.ScopeRevision+1 {
		t.Fatal(applied, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.DispatchPaused() || current.DirectionID != d.ID || current.State != domain.WavePlanning || current.OriginalRequest != before.OriginalRequest || current.Policy != before.Policy || current.TasksCreated != before.TasksCreated || current.PhasesCreated != before.PhasesCreated || current.Replans != before.Replans || current.MainTurns != before.MainTurns || !current.Deadline.Equal(before.Deadline) {
		t.Fatal(current, before, err)
	}
	if set := mustReady(t, k, current); set.Reason != "plan_required" {
		t.Fatal(set)
	}
	for _, task := range tasks {
		old, err := k.Task(ctx, task.ID)
		if err != nil || old.State != contract.TaskCanceled {
			t.Fatal(old, err)
		}
	}
	if _, err = k.AdoptPlan(ctx, w.Target(), "stale", storagePlanFixture()); err == nil {
		t.Fatal("old-scope plan adopted")
	}
	if _, err = k.AdoptPlan(ctx, current.Target(), "new", storagePlanFixture()); err != nil {
		t.Fatal(err)
	}
	after, err := k.Wave(ctx, w.ID)
	if err != nil || after.Replans != before.Replans+1 || after.TasksCreated != before.TasksCreated+2 {
		t.Fatal("direction reset structural usage", after, err)
	}
	if replay, err := k.RequestWaveDirection(ctx, directionRequest(w)); err != nil || !reflect.DeepEqual(replay, applied) {
		t.Fatal(replay, err)
	}
}

func TestWaveDirectionWaitsForStopAndResultApplicationAndPreservesOldOutcome(t *testing.T) {
	ctx := context.Background()
	k, b, run, result := recordedTaskResultFixture(t, nil)
	w, err := k.Wave(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	d := mustDirection(t, k, w)
	if _, err = k.ApplyWaveDirection(ctx, d.ID); !errors.Is(err, ErrDirectionHeld) {
		t.Fatal("direction ignored live run", err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyWaveDirection(ctx, d.ID); !errors.Is(err, ErrDirectionHeld) {
		t.Fatal("direction discarded unapplied old result", err)
	}
	application, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || application.State != contract.TaskCompleted {
		t.Fatal("old outcome lost while direction pending", application, err)
	}
	if _, err = k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	var saved contract.TaskResult
	if err = k.Record(ctx, "task_result", application.ResultID, &saved); err != nil || saved.Target != b.Target || saved.TaskID != b.TaskID || !reflect.DeepEqual(saved.Artifacts, result.Artifacts) {
		t.Fatal(saved, result, err)
	}
	var physical contract.SessionRun
	if err = k.Record(ctx, "agent_run", run.ID, &physical); err != nil || physical.Final != run.Final {
		t.Fatal(physical, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = k.ReadArtifact(ctx, b.ParentSessionID, current.Target(), result.Artifacts[0], 0, 1024); !errors.Is(err, ErrArtifactScope) {
		t.Fatal("old artifact remained current-scope evidence", err)
	}
}

func TestWaveDirectionScopesMainSettlementToItsTargetSet(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(map[bool]string{false: "other-wave", true: "own-wave"}[own], func(t *testing.T) {
			ctx := context.Background()
			k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 2)
			target := waves[1].Target()
			if own {
				target = waves[0].Target()
			}
			b, run, _ := reserveBudgetMain(t, k, "main-active", target)
			d := mustDirection(t, k, waves[0])
			_, err := k.ApplyWaveDirection(ctx, d.ID)
			if own && !errors.Is(err, ErrDirectionHeld) || !own && err != nil {
				t.Fatal(own, err)
			}
			finishBudgetMain(t, k, b, run)
			if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWaveDirectionCancelPriorityManualPauseAndReopen(t *testing.T) {
	ctx := context.Background()
	k, w, _ := readyFixture(t)
	mustControl(t, k, operatorRequest(w, "pause", "pause", ""))
	d := mustDirection(t, k, w)
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	pending, err := k.PendingWaveDirections(ctx, "", 1)
	if err != nil || len(pending.Directions) != 1 || pending.Directions[0].ID != d.ID {
		t.Fatal(pending, err)
	}
	if _, err = k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || !current.Paused || current.PendingDirectionID != "" {
		t.Fatal("direction implicitly resumed manual pause", current, err)
	}
	nextRequest := directionRequest(current)
	nextRequest.Key = "second-direction"
	d2, err := k.RequestWaveDirection(ctx, nextRequest)
	if err != nil {
		t.Fatal(err)
	}
	mustControl(t, k, operatorRequest(current, "cancel", "cancel", ""))
	canceled, err := k.ApplyWaveDirection(ctx, d2.ID)
	if err != nil || canceled.State != "canceled" {
		t.Fatal("cancel did not supersede direction", canceled, err)
	}
	current, err = k.Wave(ctx, w.ID)
	if err != nil || current.DispatchPaused() || current.State != domain.WaveCanceled || current.ScopeRevision != w.ScopeRevision+1 {
		t.Fatal(current, err)
	}
}

func TestWaveDirectionRequestAndApplicationRollbackAndRejectConflicts(t *testing.T) {
	ctx := context.Background()
	k, w, _ := readyFixture(t)
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_direction BEFORE INSERT ON kernel_events WHEN NEW.kind='direction_requested' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RequestWaveDirection(ctx, directionRequest(w)); err == nil {
		t.Fatal("direction event failure accepted")
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.DispatchPaused() {
		t.Fatal(current, err)
	}
	if _, err := k.DB.ExecContext(ctx, "DROP TRIGGER fail_direction"); err != nil {
		t.Fatal(err)
	}
	d := mustDirection(t, k, w)
	conflict := directionRequest(w)
	conflict.Instruction += " changed"
	if _, err := k.RequestWaveDirection(ctx, conflict); err == nil {
		t.Fatal("conflicting direction replay accepted")
	}
	conflict = directionRequest(w)
	conflict.Key = "another"
	if _, err := k.RequestWaveDirection(ctx, conflict); !errors.Is(err, ErrDirectionHeld) {
		t.Fatal("pending direction overwritten", err)
	}
	if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_direction_apply BEFORE INSERT ON kernel_events WHEN NEW.kind='direction_applied' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err == nil {
		t.Fatal("direction application event failure accepted")
	}
	current, err = k.Wave(ctx, w.ID)
	if err != nil || current.Target() != w.Target() || current.PendingDirectionID != d.ID {
		t.Fatal("failed application did not preserve barrier", current, err)
	}
}

func TestWaveDirectionWaitsForUnknownThreadCreationDisposition(t *testing.T) {
	ctx := context.Background()
	k, w, b := managedTaskFixture(t)
	r := managedReserve(t, k, b, "owner", "planned-run", "execute")
	managedStart(t, k, r, 42)
	if err := k.MarkManagedThreadRPC(ctx, "owner", "thread/start"); err != nil {
		t.Fatal(err)
	}
	d := mustDirection(t, k, w)
	if _, err := k.ApplyWaveDirection(ctx, d.ID); !errors.Is(err, ErrDirectionHeld) {
		t.Fatal("unknown live thread creation ignored", err)
	}
	if err := k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "owner", Disposition: "group_stopped", Identity: managedIdentity(42)}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, d.ID); !errors.Is(err, ErrDirectionHeld) {
		t.Fatal("lost thread/start was silently abandoned", err)
	}
	if err := k.RetireUnboundTaskSession(ctx, b.ID, "audited lost thread/start reply"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
}

func TestWaveDirectionUpdatesOnlyExplicitCriteriaAndRejectsMainToolDispatch(t *testing.T) {
	ctx := context.Background()
	k, w, _, b, run := kernelToolFixture(t, "orchestrator")
	r := directionRequest(w)
	r.Criteria = []domain.WaveCriterion{{ID: "revised", Requirement: "operator revised acceptance"}}
	d, err := k.RequestWaveDirection(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	call := kernelCall(t, b, run, "state_get", contract.StateRequest{SchemaVersion: 1, Target: w.Target()})
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("parent tool bypassed direction barrier")
	}
	finishBudgetMain(t, k, b, run)
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(current.Criteria, r.Criteria) || current.OriginalRequest != w.OriginalRequest || current.Policy != w.Policy {
		t.Fatal("direction lost provenance or changed policy", current, err)
	}
}

func TestWaveDirectionOldPhaseEvidenceDoesNotBlockControllerArtifactCollection(t *testing.T) {
	ctx := context.Background()
	k, b, run, result := recordedTaskResultFixture(t, nil)
	base := k.clock()
	k.Now = func() time.Time { return base }
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, b.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
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
	other, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: w.OrchestratorSessionID, Source: "direct", SourceID: "other", OriginalRequest: "independent report", Criteria: w.Criteria, Policy: w.Policy})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := k.AdoptPlan(ctx, other.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, plan.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	proof := completeTemplateTask(t, k, task, "independent current Phase evidence", "", nil)
	d := mustDirection(t, k, w)
	if _, err = k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if err = k.CollectArtifacts(ctx); err != nil {
		t.Fatal("redirected Wave blocked controller collection", err)
	}
	old, err := k.Artifact(ctx, result.Artifacts[0])
	if err != nil || old.State != "committed" {
		t.Fatal("direction bypassed historical retention", old, err)
	}
	k.Now = func() time.Time { return base.Add(2 * time.Second) }
	if err = k.CollectArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	old, err = k.Artifact(ctx, result.Artifacts[0])
	if err != nil || old.State != "expired" {
		t.Fatal("superseded evidence did not follow retention", old, err)
	}
	current, err := k.Artifact(ctx, proof.ID)
	if err != nil || current.State != "committed" {
		t.Fatal("unrelated current Phase evidence was collected", current, err)
	}
}
