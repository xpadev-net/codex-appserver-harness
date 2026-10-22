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
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func managedTaskFixture(t *testing.T) (*KernelStore, domain.Wave, contract.SessionBinding) {
	t.Helper()
	ctx := context.Background()
	k, r := waveStoreFixture(t)
	w, err := k.CreateWave(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, plan.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, b
}
func managedIdentity(pid int) process.Identity {
	return process.Identity{PID: pid, BootID: "trusted-owner-fixture", StartTicks: uint64(pid)}
}
func managedReserve(t *testing.T, k *KernelStore, b contract.SessionBinding, owner, run, mode string) ManagedReservation {
	t.Helper()
	r, err := k.ReserveManagedProcess(context.Background(), ManagedProcessRequest{OwnerID: owner, RunID: run, Mode: mode, Binding: b})
	if err != nil || !r.Created {
		t.Fatal(r, err)
	}
	return r
}
func managedStart(t *testing.T, k *KernelStore, r ManagedReservation, pid int) {
	t.Helper()
	ctx := context.Background()
	if _, err := k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID); err != nil {
		t.Fatal(err)
	}
	if err := k.RecordManagedProcessStarted(ctx, r.Process.Request.OwnerID, managedIdentity(pid)); err != nil {
		t.Fatal(err)
	}
}
func managedBind(t *testing.T, k *KernelStore, r ManagedReservation) contract.SessionBinding {
	t.Helper()
	ctx := context.Background()
	method := "thread/start"
	thread := "thread-" + r.Binding.ID
	if r.Binding.ThreadID != "" {
		method = "thread/resume"
		thread = r.Binding.ThreadID
	}
	if err := k.MarkManagedThreadRPC(ctx, r.Process.Request.OwnerID, method); err != nil {
		t.Fatal(err)
	}
	if err := k.BindSessionThread(ctx, r.Binding, thread); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, r.Binding.ID)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func managedCapacity(t *testing.T, k *KernelStore, want int) {
	t.Helper()
	cap, err := k.ExecutionCapacity(context.Background())
	if err != nil || cap.HeldProcessSlots != want {
		t.Fatal(cap, err)
	}
}
func TestManagedAdmissionBindsCapacityOwnerAndPlannedRunAtomically(t *testing.T) {
	ctx := context.Background()
	k, w, task := managedTaskFixture(t)
	main, err := k.SessionBinding(ctx, w.OrchestratorSessionID)
	if err != nil {
		t.Fatal(err)
	}
	request := ManagedProcessRequest{OwnerID: "task-owner", RunID: "task-run", Mode: "execute", Binding: task}
	first, err := k.ReserveManagedProcess(ctx, request)
	if err != nil || !first.Created || first.Process.State != "admitted" {
		t.Fatal(first, err)
	}
	replay, err := k.ReserveManagedProcess(ctx, request)
	if err != nil || replay.Created || replay.Process != first.Process {
		t.Fatal(replay, err)
	}
	changed := request
	changed.RunID = "other-run"
	if _, err := k.ReserveManagedProcess(ctx, changed); err == nil {
		t.Fatal("owner changed planned run")
	}
	managedReserve(t, k, main, "main-owner", "main-run", "execute")
	managedCapacity(t, k, 2)
	other := capacityMain(t, k, "other", t.TempDir())
	if _, err := k.ReserveManagedProcess(ctx, ManagedProcessRequest{OwnerID: "overflow", RunID: "overflow", Mode: "execute", Binding: other}); !errors.Is(err, ErrExecutionCapacity) {
		t.Fatal(err)
	}
	managedStart(t, k, first, 41)
	if _, err := k.BeginManagedProcessLaunch(ctx, request.OwnerID); !errors.Is(err, ErrManagedProcessHeld) {
		t.Fatal("launch replay granted another start", err)
	}
	if err := k.BindSessionThread(ctx, first.Binding, "unadmitted-thread"); err == nil {
		t.Fatal("thread reply without send intent accepted")
	}
	bound := managedBind(t, k, first)
	if err := k.MarkManagedThreadRPC(ctx, request.OwnerID, "thread/start"); err == nil {
		t.Fatal("thread RPC replayed")
	}
	snapshot := sessionSnapshot()
	run := recoveryRun(k, bound, "wrong-run")
	if err := k.ReserveSessionRun(ctx, bound, snapshot, run); !errors.Is(err, ErrManagedProcessHeld) {
		t.Fatal("different run used owner capacity", err)
	}
	run.ID = request.RunID
	if err := k.ReserveSessionRun(ctx, bound, snapshot, run); err != nil {
		t.Fatal(err)
	}
	taskState, err := k.Task(ctx, task.TaskID)
	if err != nil || taskState.Attempts != 1 {
		t.Fatal(taskState, err)
	}
}
func TestManagedUnsentFailureClearsCreationButLostThreadRPCDoesNot(t *testing.T) {
	for _, mode := range []string{"not_started", "initialize_failure", "lost_thread"} {
		t.Run(mode, func(t *testing.T) {
			sent := mode == "lost_thread"
			ctx := context.Background()
			k, _, b := managedTaskFixture(t)
			r := managedReserve(t, k, b, "owner", "run", "execute")
			if _, err := k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID); err != nil {
				t.Fatal(err)
			}
			proof := ManagedStopProof{OwnerID: "owner", Disposition: "not_started"}
			if mode != "not_started" {
				if err := k.RecordManagedProcessStarted(ctx, "owner", managedIdentity(42)); err != nil {
					t.Fatal(err)
				}
				if sent {
					if err := k.MarkManagedThreadRPC(ctx, "owner", "thread/start"); err != nil {
						t.Fatal(err)
					}
				}
				if err := k.ConfirmManagedProcessStopped(ctx, proof); !errors.Is(err, ErrManagedProcessHeld) {
					t.Fatal("sent RPC treated as unsent", err)
				}
				proof.Disposition, proof.Identity = "group_stopped", managedIdentity(42)
			}
			if err := k.ConfirmManagedProcessStopped(ctx, proof); err != nil {
				t.Fatal(err)
			}
			if err := k.ConfirmManagedProcessStopped(ctx, proof); err != nil {
				t.Fatal(err)
			}
			changed := proof
			changed.Identity = managedIdentity(100)
			if err := k.ConfirmManagedProcessStopped(ctx, changed); err == nil {
				t.Fatal("stop proof changed")
			}
			managedCapacity(t, k, 0)
			current, err := k.SessionBinding(ctx, b.ID)
			if err != nil {
				t.Fatal(err)
			}
			next := ManagedProcessRequest{OwnerID: "next", RunID: "next", Mode: "execute", Binding: current}
			_, err = k.ReserveManagedProcess(ctx, next)
			if sent {
				if err == nil {
					t.Fatal("unknown thread/start silently retried")
				}
				if err := k.RetireUnboundTaskSession(ctx, b.ID, "audited lost thread creation"); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal("known unsent creation could not retry", err)
			}
			task, err := k.Task(ctx, b.TaskID)
			if err != nil || task.Attempts != 0 {
				t.Fatal("transport preparation charged a model Attempt", task, err)
			}
		})
	}
}
func TestManagedUnknownLaunchKeepsCapacityWithoutStoppingEvidence(t *testing.T) {
	ctx := context.Background()
	k, _, b := managedTaskFixture(t)
	r := managedReserve(t, k, b, "unknown-owner", "run", "execute")
	if _, err := k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID); err != nil {
		t.Fatal(err)
	}
	for _, proof := range []ManagedStopProof{{OwnerID: r.Process.Request.OwnerID, Disposition: "timeout"}, {OwnerID: r.Process.Request.OwnerID, Disposition: "group_stopped"}} {
		if err := k.ConfirmManagedProcessStopped(ctx, proof); err == nil {
			t.Fatal("unknown launch freed capacity", proof)
		}
	}
	managedCapacity(t, k, 1)
	fresh, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveManagedProcess(ctx, ManagedProcessRequest{OwnerID: "another", RunID: "another", Mode: "execute", Binding: fresh}); err == nil {
		t.Fatal("unknown preparation reused")
	}
	if err := k.RecordManagedProcessStarted(ctx, r.Process.Request.OwnerID, managedIdentity(45)); err != nil {
		t.Fatal(err)
	}
	if err := k.RecordManagedProcessStarted(ctx, r.Process.Request.OwnerID, managedIdentity(46)); err == nil {
		t.Fatal("observed PID changed")
	}
	managedCapacity(t, k, 1)
}
func TestManagedRecoveryReadsCanceledScopeWithoutChargingAnotherRun(t *testing.T) {
	ctx := context.Background()
	k, w, b := managedTaskFixture(t)
	original := managedReserve(t, k, b, "original", "original-run", "execute")
	managedStart(t, k, original, 51)
	bound := managedBind(t, k, original)
	run := recoveryRun(k, bound, "original-run")
	if err := k.ReserveSessionRun(ctx, bound, sessionSnapshot(), run); err != nil {
		t.Fatal(err)
	}
	run.State = "unknown"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionPreparationStopped(ctx, b.ID, original.Binding.PreparationID, run.ID); err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, b.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := k.kernelTx(ctx)
	task.State = contract.TaskCanceled
	task.Reason = "scope was superseded"
	w.ScopeRevision++
	w.Paused = true
	if err := saveTask(ctx, tx, task); err != nil {
		t.Fatal(err)
	}
	if err := saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	before, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	var ledgerBefore int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger").Scan(&ledgerBefore); err != nil {
		t.Fatal(err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil || !current.Closed {
		t.Fatal(current, err)
	}
	recovery := managedReserve(t, k, current, "recovery", run.ID, "recovery")
	managedStart(t, k, recovery, 52)
	managedCapacity(t, k, 1)
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err == nil {
		t.Fatal("run-only proof released a new recovery generation")
	}
	if err := k.MarkSessionPreparationStopped(ctx, b.ID, original.Binding.PreparationID, run.ID); err == nil {
		t.Fatal("old preparation proof released recovery")
	}
	if err := k.MarkManagedThreadRPC(ctx, "recovery", "thread/start"); err == nil {
		t.Fatal("recovery allowed a thread mutation")
	}
	if err := k.ReserveSessionRun(ctx, recovery.Binding, sessionSnapshot(), recoveryRun(k, recovery.Binding, "new-turn")); err == nil {
		t.Fatal("recovery allowed a model turn")
	}
	// The old owner's late confirmation acknowledges its original release only.
	if err := k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "original", Disposition: "group_stopped", Identity: managedIdentity(51)}); err != nil {
		t.Fatal(err)
	}
	managedCapacity(t, k, 1)
	after, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("read-only recovery charged budget", before, after, err)
	}
	var ledgerAfter int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger").Scan(&ledgerAfter); err != nil || ledgerBefore != ledgerAfter {
		t.Fatal(ledgerBefore, ledgerAfter, err)
	}
	task, err = k.Task(ctx, b.TaskID)
	if err != nil || task.Attempts != 1 || task.State != contract.TaskCanceled {
		t.Fatal(task, err)
	}
	proof := ManagedStopProof{OwnerID: "recovery", Disposition: "group_stopped", Identity: managedIdentity(52)}
	if err := k.ConfirmManagedProcessStopped(ctx, proof); err != nil {
		t.Fatal(err)
	}
	managedCapacity(t, k, 0)
	current, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	managedReserve(t, k, current, "recovery-again", run.ID, "recovery")
	if err := k.ConfirmManagedProcessStopped(ctx, proof); err != nil {
		t.Fatal("late acknowledgment confused current generation", err)
	}
	managedCapacity(t, k, 1)
}
func TestManagedAdmissionAndStopProofRollBackWithEveryResource(t *testing.T) {
	ctx := context.Background()
	k, _, b := managedTaskFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_managed_admission BEFORE INSERT ON records WHEN NEW.kind='managed_process' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	req := ManagedProcessRequest{OwnerID: "owner", RunID: "run", Mode: "execute", Binding: b}
	if _, err := k.ReserveManagedProcess(ctx, req); err == nil {
		t.Fatal("injected admission committed")
	}
	managedCapacity(t, k, 0)
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE state='held'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_managed_admission"); err != nil {
		t.Fatal(err)
	}
	r := managedReserve(t, k, b, "owner", "run", "execute")
	if _, err := k.DB.Exec("CREATE TRIGGER fail_managed_stop BEFORE INSERT ON records WHEN NEW.kind='managed_stop_proof' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	proof := ManagedStopProof{OwnerID: r.Process.Request.OwnerID, Disposition: "not_started"}
	if err := k.ConfirmManagedProcessStopped(ctx, proof); err == nil {
		t.Fatal("injected stop committed")
	}
	managedCapacity(t, k, 1)
	p, err := k.ManagedProcess(ctx, req.OwnerID)
	if err != nil || p.State != "admitted" {
		t.Fatal(p, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_managed_stop"); err != nil {
		t.Fatal(err)
	}
	if err := k.ConfirmManagedProcessStopped(ctx, proof); err != nil {
		t.Fatal(err)
	}
	managedCapacity(t, k, 0)
}

func TestManagedLaunchReconciliationSurvivesRestartAndPagesPastHistory(t *testing.T) {
	ctx := context.Background()
	k, _, b := managedTaskFixture(t)
	old := managedReserve(t, k, b, "a-stopped", "first", "execute")
	if err := k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: old.Process.Request.OwnerID, Disposition: "not_started"}); err != nil {
		t.Fatal(err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	r := managedReserve(t, k, current, "z-unknown", "next", "execute")
	if _, err := k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	first, err := k.PendingManagedProcesses(ctx, "", 1)
	if err != nil || len(first.Processes) != 0 || first.NextAfter != "a-stopped" {
		t.Fatal(first, err)
	}
	next, err := k.PendingManagedProcesses(ctx, first.NextAfter, 1)
	if err != nil || len(next.Processes) != 1 || next.Processes[0].State != "launching" || next.Processes[0].PreparationID != r.Binding.PreparationID || next.NextAfter != "z-unknown" {
		t.Fatal(next, err)
	}
	end, err := k.PendingManagedProcesses(ctx, next.NextAfter, 1)
	if err != nil || len(end.Processes) != 0 || end.NextAfter != "" {
		t.Fatal(end, err)
	}
	replay, err := k.ReserveManagedProcess(ctx, r.Process.Request)
	if err != nil || replay.Created {
		t.Fatal(replay, err)
	}
	if _, err := k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID); !errors.Is(err, ErrManagedProcessHeld) {
		t.Fatal("restart replay granted another launch", err)
	}
	managedCapacity(t, k, 1)
}

func TestManagedStopRetainsIdentityWhenStartupFactWasNotSaved(t *testing.T) {
	ctx := context.Background()
	k, _, b := managedTaskFixture(t)
	r := managedReserve(t, k, b, "late-identity", "run", "execute")
	if _, err := k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID); err != nil {
		t.Fatal(err)
	}
	proof := ManagedStopProof{OwnerID: r.Process.Request.OwnerID, Disposition: "group_stopped", Identity: managedIdentity(90)}
	if err := k.ConfirmManagedProcessStopped(ctx, proof); err != nil {
		t.Fatal(err)
	}
	p, err := k.ManagedProcess(ctx, r.Process.Request.OwnerID)
	if err != nil || p.Identity != proof.Identity || p.State != "stopped" || !p.LaunchObservedAt.IsZero() {
		t.Fatal(p, err)
	}
	managedCapacity(t, k, 0)
}

func TestManagedStopFenceStillHoldsAfterDeadlineExtension(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "launch", true: "thread-rpc"}[started], func(t *testing.T) {
			ctx := context.Background()
			k, w, b := managedTaskFixture(t)
			r := managedReserve(t, k, b, "fenced", "run", "execute")
			if started {
				managedStart(t, k, r, 100)
			}
			k.Now = func() time.Time { return w.Deadline }
			d, err := k.ExpireWave(ctx, w.ID)
			if err != nil || len(d.ActionIDs) != 1 {
				t.Fatal(d, err)
			}
			if err := k.ChangeWaveDeadline(ctx, DeadlineChangeRequest{Key: "extension", Target: w.Target(), ExpectedDeadline: w.Deadline, Deadline: w.Deadline.Add(time.Hour), Reason: "operator approved extension"}); err != nil {
				t.Fatal(err)
			}
			if started {
				err = k.MarkManagedThreadRPC(ctx, r.Process.Request.OwnerID, "thread/start")
			} else {
				_, err = k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID)
			}
			if !errors.Is(err, ErrManagedProcessHeld) {
				t.Fatal("stopping preparation gained new work after extension", err)
			}
			managedCapacity(t, k, 1)
			proof := ManagedStopProof{OwnerID: r.Process.Request.OwnerID, Disposition: "not_started"}
			if started {
				proof.Disposition, proof.Identity = "group_stopped", managedIdentity(100)
			}
			if err := k.ConfirmManagedProcessStopped(ctx, proof); err != nil {
				t.Fatal(err)
			}
			if err := k.CompleteDeadlineAction(ctx, d.ActionIDs[0]); err != nil {
				t.Fatal(err)
			}
			current, err := k.SessionBinding(ctx, b.ID)
			if err != nil {
				t.Fatal(err)
			}
			next := managedReserve(t, k, current, "after-stop", "next", "execute")
			if _, err := k.BeginManagedProcessLaunch(ctx, next.Process.Request.OwnerID); err != nil {
				t.Fatal("fresh preparation held by old fence", err)
			}
		})
	}
}
func TestManagedBoundSessionMustResumeThreadBeforeReservingRun(t *testing.T) {
	ctx := context.Background()
	k, b := boundTaskFixture(t)
	if err := k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	r := managedReserve(t, k, current, "bound-owner", "bound-run", "execute")
	managedStart(t, k, r, 110)
	run := recoveryRun(k, r.Binding, r.Process.Request.RunID)
	for _, phase := range []string{"no-thread-rpc", "reply-not-bound"} {
		if phase == "reply-not-bound" {
			if err := k.MarkManagedThreadRPC(ctx, r.Process.Request.OwnerID, "thread/resume"); err != nil {
				t.Fatal(err)
			}
		}
		if err := k.ReserveSessionRun(ctx, r.Binding, sessionSnapshot(), run); !errors.Is(err, ErrManagedProcessHeld) {
			t.Fatal(phase, err)
		}
		task, err := k.Task(ctx, b.TaskID)
		if err != nil || task.Attempts != 0 {
			t.Fatal("unprepared app-server charged Attempt", phase, task, err)
		}
	}
	if err := k.BindSessionThread(ctx, r.Binding, b.ThreadID); err != nil {
		t.Fatal(err)
	}
	if err := k.ReserveSessionRun(ctx, r.Binding, sessionSnapshot(), run); err != nil {
		t.Fatal("confirmed resume did not admit run", err)
	}
}

func TestManagedPlannedRunCannotBeClaimedByTwoSessions(t *testing.T) {
	ctx := context.Background()
	k, _, task := managedTaskFixture(t)
	main, err := k.SessionBinding(ctx, task.ParentSessionID)
	if err != nil {
		t.Fatal(err)
	}
	first := managedReserve(t, k, task, "first-owner", "same-run", "execute")
	second := ManagedProcessRequest{OwnerID: "second-owner", RunID: "same-run", Mode: "execute", Binding: main}
	if _, err := k.ReserveManagedProcess(ctx, second); !errors.Is(err, ErrManagedProcessHeld) {
		t.Fatal("planned run admitted twice", err)
	}
	managedCapacity(t, k, 1)
	if err := k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: first.Process.Request.OwnerID, Disposition: "not_started"}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveManagedProcess(ctx, second); !errors.Is(err, ErrManagedProcessHeld) {
		t.Fatal("logical run transferred to another session", err)
	}
	current, err := k.SessionBinding(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	retry := managedReserve(t, k, current, "retry-same-session", "same-run", "execute")
	if _, err := k.BeginManagedProcessLaunch(ctx, retry.Process.Request.OwnerID); err != nil {
		t.Fatal("known unsent logical run could not retry", err)
	}
	managedCapacity(t, k, 1)
}
func TestManagedThreadRPCChecksDeadlineWithoutExpirySweep(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(map[bool]string{false: "start", true: "resume"}[bound], func(t *testing.T) {
			ctx := context.Background()
			k, w, b := managedTaskFixture(t)
			if bound {
				old := reserveSessionFixture(t, k, b)
				if err := k.BindSessionThread(ctx, old, "existing-thread"); err != nil {
					t.Fatal(err)
				}
				if err := k.MarkSessionStopped(ctx, b.ID, old.PreparationID); err != nil {
					t.Fatal(err)
				}
				var err error
				b, err = k.SessionBinding(ctx, b.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			r := managedReserve(t, k, b, "slow-initialize", "run", "execute")
			managedStart(t, k, r, 120)
			k.Now = func() time.Time { return w.Deadline }
			method := "thread/start"
			if bound {
				method = "thread/resume"
			}
			if err := k.MarkManagedThreadRPC(ctx, r.Process.Request.OwnerID, method); !errors.Is(err, ErrManagedProcessHeld) {
				t.Fatal("expired thread RPC admitted before sweep", err)
			}
			var intents int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='managed_thread_rpc' AND id=?", r.Binding.PreparationID).Scan(&intents); err != nil || intents != 0 {
				t.Fatal(intents, err)
			}
			managedCapacity(t, k, 1)
		})
	}
}

func TestManagedPlannedRunClaimIsAtomicAcrossConnections(t *testing.T) {
	ctx := context.Background()
	k, _, task := managedTaskFixture(t)
	other, err := OpenKernel(filepath.Join(k.Root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	main, err := k.SessionBinding(ctx, task.ParentSessionID)
	if err != nil {
		t.Fatal(err)
	}
	requests := []ManagedProcessRequest{{OwnerID: "concurrent-task", RunID: "one-logical-run", Mode: "execute", Binding: task}, {OwnerID: "concurrent-main", RunID: "one-logical-run", Mode: "execute", Binding: main}}
	stores := []*KernelStore{k, other}
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := range requests {
		go func() {
			<-start
			r, err := stores[i].ReserveManagedProcess(ctx, requests[i])
			if err == nil && !r.Created {
				err = errors.New("unexpected replay")
			}
			results <- err
		}()
	}
	close(start)
	created, held := 0, 0
	for range 2 {
		if err := <-results; err == nil {
			created++
		} else {
			held++
		}
	}
	if created != 1 || held != 1 {
		t.Fatal("planned run had multiple owners", created, held)
	}
	managedCapacity(t, k, 1)
	var owners int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='managed_process'").Scan(&owners); err != nil || owners != 1 {
		t.Fatal(owners, err)
	}
}
