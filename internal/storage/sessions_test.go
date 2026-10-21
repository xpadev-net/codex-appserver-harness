package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
)

var _ worker.SessionPersistence = (*KernelStore)(nil)

func sessionSnapshot() contract.InputSnapshot {
	data := []byte(`{"schema_version":1,"wave":"bound by controller"}`)
	return contract.InputSnapshot{ID: "snapshot", Digest: fmt.Sprintf("%x", sha256.Sum256(data)), Input: data}
}
func boundTaskFixture(t *testing.T) (*KernelStore, contract.SessionBinding) {
	t.Helper()
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := store.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	task, err := store.Task(ctx, receipt.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	binding, err := store.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	binding = reserveSessionFixture(t, store, binding)
	if err = store.BindSessionThread(ctx, binding, "thread"); err != nil {
		t.Fatal(err)
	}
	binding, err = store.SessionBinding(ctx, binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	return store, binding
}
func TestKernelSessionReservesSnapshotsRunsAndPreservesUnknownStarts(t *testing.T) {
	ctx := context.Background()
	store, binding := boundTaskFixture(t)
	run := contract.SessionRun{ID: "run", ThreadID: binding.ThreadID, State: "starting", Started: time.Now().UTC(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	snapshot := sessionSnapshot()
	if err := store.ReserveSessionRun(ctx, binding, snapshot, run); err != nil {
		t.Fatal(err)
	}
	run.State = "unknown"
	if err := store.Put(ctx, "agent_run", run.ID, binding.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := store.ArchiveSessionRun(ctx, binding.ID, run.ID, "must read existing history"); err == nil {
		t.Fatal("unknown start discarded")
	}
	current, err := store.SessionBinding(ctx, binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	another := run
	another.ID = "new-run"
	another.State = "starting"
	if err = store.ReserveSessionRun(ctx, current, snapshot, another); err == nil {
		t.Fatal("active run duplicated")
	}
	run.State = "completed"
	run.TurnID = "turn"
	run.Final = `{"observed":"completed"}`
	if err = store.Put(ctx, "agent_run", run.ID, binding.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = store.ArchiveSessionRun(ctx, binding.ID, run.ID, "not stopped yet"); err == nil {
		t.Fatal("completion released live process")
	}
	if err = store.MarkSessionStopped(ctx, binding.ID, "wrong-run"); err == nil {
		t.Fatal("wrong stopping proof accepted")
	}
	if err = store.MarkSessionStopped(ctx, binding.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.ArchiveSessionRun(ctx, binding.ID, run.ID, "controller applied structured result"); err != nil {
		t.Fatal(err)
	}
	// A late retry of an old disposition must not block a newer Task attempt.
	current, err = store.SessionBinding(ctx, binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	another = run
	another.ID, another.State, another.TurnID, another.Final = "retry", "starting", "", ""
	current = reserveSessionFixture(t, store, current)
	if err = store.ReserveSessionRun(ctx, current, snapshot, another); err != nil {
		t.Fatal(err)
	}
	if err = store.ArchiveSessionRun(ctx, binding.ID, run.ID, "controller applied structured result"); err != nil {
		t.Fatal(err)
	}
	task, err := store.Task(ctx, binding.TaskID)
	if err != nil || task.State != contract.TaskRunning || task.Attempts != 2 {
		t.Fatal("old disposition changed new attempt", task, err)
	}
	if err = store.ArchiveSessionRun(ctx, binding.ID, run.ID, "different disposition"); err == nil {
		t.Fatal("conflicting disposition accepted")
	}
	altered := run
	altered.Final = "rewritten completion"
	if err = store.Put(ctx, "agent_run", run.ID, binding.ID, altered); err == nil {
		t.Fatal("confirmed result overwritten")
	}
	if err = store.MarkSessionStopped(ctx, binding.ID, run.ID); err == nil {
		t.Fatal("late stopping proof accepted")
	}
	var taskAttempts int
	if err = store.DB.QueryRow("SELECT sum(amount) FROM kernel_budget_ledger WHERE kind='task_attempt'").Scan(&taskAttempts); err != nil || taskAttempts != 2 {
		t.Fatal(taskAttempts, err)
	}
}
func TestKernelThreadCreationAndIdentityAreExclusive(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	first, err := store.SessionBinding(ctx, request.OrchestratorSessionID)
	if err != nil {
		t.Fatal(err)
	}
	first = reserveSessionFixture(t, store, first)
	if err = store.ReserveSessionThread(ctx, first); err == nil {
		t.Fatal("unknown thread creation repeated")
	}
	if err = store.BindSessionThread(ctx, first, "thread"); err != nil {
		t.Fatal(err)
	}
	current, err := store.SessionBinding(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindSessionThread(ctx, current, "replacement"); err == nil {
		t.Fatal("normal binding changed thread")
	}
	second, err := store.CreateOrchestrator(ctx, "other", first.Model, first.CWD, first.Sandbox)
	if err != nil {
		t.Fatal(err)
	}
	second = reserveSessionFixture(t, store, second)
	if err = store.BindSessionThread(ctx, second, "thread"); err == nil {
		t.Fatal("thread assigned to two sessions")
	}
}
func TestKernelMainRebindingRequiresQuiescentAppliedRunAndSavedSnapshot(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	binding, err := store.SessionBinding(ctx, request.OrchestratorSessionID)
	if err != nil {
		t.Fatal(err)
	}
	binding = reserveSessionFixture(t, store, binding)
	if err = store.BindSessionThread(ctx, binding, "old-main"); err != nil {
		t.Fatal(err)
	}
	binding, err = store.SessionBinding(ctx, binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.RebindMainThread(ctx, binding, "new-main", "missing", "history unavailable"); err == nil {
		t.Fatal("snapshotless recovery")
	}
	snapshot := sessionSnapshot()
	run := contract.SessionRun{ID: "main-run", ThreadID: binding.ThreadID, State: "starting", Started: time.Now(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = store.ReserveSessionRun(ctx, binding, snapshot, run); err != nil {
		t.Fatal(err)
	}
	if err = store.RebindMainThread(ctx, binding, "new-main", snapshot.ID, "maintenance"); err == nil {
		t.Fatal("active main rebound")
	}
	run.State = "completed"
	run.TurnID = "turn"
	if err = store.Put(ctx, "agent_run", run.ID, binding.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkSessionStopped(ctx, binding.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err = store.RebindMainThread(ctx, binding, "new-main", snapshot.ID, "maintenance"); err == nil {
		t.Fatal("unapplied inputs abandoned")
	}
	if err = store.ArchiveSessionRun(ctx, binding.ID, run.ID, "plan applied"); err != nil {
		t.Fatal(err)
	}
	if err = store.RebindMainThread(ctx, binding, "new-main", snapshot.ID, "maintenance"); err != nil {
		t.Fatal(err)
	}
	fresh, err := store.SessionBinding(ctx, binding.ID)
	if err != nil || fresh.ID != binding.ID || fresh.ThreadID != "new-main" || fresh.Generation != binding.Generation+1 {
		t.Fatal(fresh, err)
	}
	if err = store.RebindMainThread(ctx, binding, "another-main", snapshot.ID, "stale request"); err == nil {
		t.Fatal("stale binding reused")
	}
	var audits int
	if err = store.DB.QueryRow("SELECT count(*) FROM kernel_rebindings").Scan(&audits); err != nil || audits != 1 {
		t.Fatal(audits, err)
	}
}
func TestKernelSnapshotMismatchRollsBackAttempt(t *testing.T) {
	ctx := context.Background()
	store, binding := boundTaskFixture(t)
	run := contract.SessionRun{ID: "run", ThreadID: binding.ThreadID, State: "starting", Started: time.Now(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	snapshot := sessionSnapshot()
	snapshot.Digest = "wrong"
	if err := store.ReserveSessionRun(ctx, binding, snapshot, run); err == nil {
		t.Fatal("false snapshot accepted")
	}
	var saved contract.SessionRun
	if err := store.Record(ctx, "agent_run", run.ID, &saved); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("failed reservation recorded a run", err)
	}
	task, err := store.Task(ctx, binding.TaskID)
	if err != nil || task.Attempts != 0 {
		t.Fatal(task, err)
	}
}

func TestKernelPreparedSessionHoldsPlanUntilStopped(t *testing.T) {
	ctx := context.Background()
	store, binding := boundTaskFixture(t)
	wave, err := store.Wave(ctx, binding.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	for i := range plan.Phases {
		p := &plan.Phases[i]
		p.ID += "-new"
		for j := range p.Tasks {
			p.Tasks[j].ID += "-new"
			for d := range p.Tasks[j].DependsOn {
				p.Tasks[j].DependsOn[d].TaskID += "-new"
			}
		}
		for j := range p.Completion.RequiredTaskIDs {
			p.Completion.RequiredTaskIDs[j] += "-new"
		}
	}
	if _, err = store.AdoptPlan(ctx, wave.Target(), "new", plan); err == nil {
		t.Fatal("prepared app-server did not hold replan")
	}
	if err = store.ReserveSessionThread(ctx, binding); err == nil {
		t.Fatal("duplicate prepare accepted")
	}
	if err = store.MarkSessionStopped(ctx, binding.ID, binding.PreparationID); err != nil {
		t.Fatal(err)
	}
	// A stale prepared process cannot send a turn after stopping proof.
	run := contract.SessionRun{ID: "run", ThreadID: binding.ThreadID, State: "starting", Started: time.Now(), BaselineKnown: true}
	if err = store.ReserveSessionRun(ctx, binding, sessionSnapshot(), run); err == nil {
		t.Fatal("stopped preparation sent turn")
	}
	// Resuming the bound thread holds the session before the first resume RPC.
	oldPreparation := binding
	binding = reserveSessionFixture(t, store, binding)
	if binding.PreparationID == oldPreparation.PreparationID {
		t.Fatal("preparation identity reused")
	}
	if err = store.MarkSessionStopped(ctx, binding.ID, oldPreparation.PreparationID); err == nil {
		t.Fatal("old stopping proof released a new process")
	}
	if err = store.ReserveSessionRun(ctx, oldPreparation, sessionSnapshot(), run); err == nil {
		t.Fatal("stale preparation sent a new turn")
	}
	if _, err = store.AdoptPlan(ctx, wave.Target(), "new", plan); err == nil {
		t.Fatal("resume preparation did not hold replan")
	}
	if err = store.MarkSessionStopped(ctx, binding.ID, binding.PreparationID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AdoptPlan(ctx, wave.Target(), "new", plan); err != nil {
		t.Fatal(err)
	}
}

func TestKernelLostThreadCreationRequiresStopThenFreshTask(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := store.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	task, err := store.Task(ctx, receipt.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, store, b)
	if err = store.RetireUnboundTaskSession(ctx, b.ID, "lost start response"); err == nil {
		t.Fatal("live creation retired")
	}
	if err = store.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	if err = store.RetireUnboundTaskSession(ctx, b.ID, "lost start response"); err != nil {
		t.Fatal(err)
	}
	task, err = store.Task(ctx, task.ID)
	if err != nil || task.State != contract.TaskFailed {
		t.Fatal(task, err)
	}
	if err = store.ReserveSessionThread(ctx, b); err == nil {
		t.Fatal("retired thread creation reused")
	}
	plan := storagePlanFixture()
	plan.Phases[0].ID = "replacement"
	plan.Phases[0].Tasks[0].ID = "replacement"
	plan.Phases[0].Completion.RequiredTaskIDs = []string{"replacement"}
	plan.Phases[1].ID = "review-new"
	plan.Phases[1].Tasks[0].ID = "review-new"
	plan.Phases[1].Tasks[0].DependsOn[0].TaskID = "replacement"
	plan.Phases[1].Completion.RequiredTaskIDs = []string{"review-new"}
	next, err := store.AdoptPlan(ctx, receipt.Target, "recover", plan)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := store.Task(ctx, next.CreatedTaskIDs[0])
	if err != nil || replacement.SessionID == b.ID {
		t.Fatal(replacement, err)
	}
}

func TestKernelDependencyArtifactMustBeInCompletedResult(t *testing.T) {
	ctx := context.Background()
	store, request := waveStoreFixture(t)
	wave, err := store.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	plan.Phases[1].Tasks[0].DependsOn[0].ArtifactID = "missing-artifact"
	receipt, err := store.AdoptPlan(ctx, wave.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := store.Task(ctx, receipt.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prior.State, prior.ResultID = contract.TaskCompleted, "prior-result"
	if err = saveTask(ctx, tx, prior); err != nil {
		t.Fatal(err)
	}
	result := contract.TaskResult{SchemaVersion: 1, TaskID: prior.ID, PhaseID: prior.PhaseID, Target: prior.Target(), Outcome: "result", Artifacts: []string{"other-artifact"}}
	if err = kernelRecordTx(ctx, tx, "task_result", prior.ResultID, prior.ID, result); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	downstream, err := store.Task(ctx, receipt.CreatedTaskIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.SessionBinding(ctx, downstream.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, store, b)
	if err = store.BindSessionThread(ctx, b, "review-thread"); err != nil {
		t.Fatal(err)
	}
	b, err = store.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "run", ThreadID: b.ThreadID, State: "starting", Started: time.Now(), BaselineKnown: true}
	if err = store.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err == nil {
		t.Fatal("absent dependency artifact allowed dispatch")
	}
	saved, err := store.Task(ctx, downstream.ID)
	if err != nil || saved.Attempts != 0 {
		t.Fatal(saved, err)
	}
}

func reserveSessionFixture(t *testing.T, store *KernelStore, b contract.SessionBinding) contract.SessionBinding {
	t.Helper()
	if err := store.ReserveSessionThread(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	fresh, err := store.SessionBinding(context.Background(), b.ID)
	if err != nil {
		t.Fatal(err)
	}
	return fresh
}
