package kernelcli

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func unknownThreadCreationFixture(t *testing.T, kind string) (string, *storage.KernelStore, contract.SessionBinding, contract.InputSnapshot, func()) {
	t.Helper()
	ctx := context.Background()
	root, w := controlWave(t)
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.Close() })
	sid := "main"
	target := w.Target()
	if kind == "task" {
		spec := contract.TaskSpec{ID: "work", Kind: "work", Instruction: "research", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
		plan := contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{}, Phases: []contract.PhaseSpec{{ID: "work", Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"}}}
		admitted, err := k.AdoptPlan(ctx, target, "initial", plan)
		if err != nil {
			t.Fatal(err)
		}
		task, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
		if err != nil {
			t.Fatal(err)
		}
		sid, target = task.SessionID, task.Target()
	}
	b, err := k.SessionBinding(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	r, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "lost-thread-owner", RunID: "future-unreserved-run", Mode: "execute", Binding: b})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.BeginManagedProcessLaunch(ctx, r.Process.Request.OwnerID); err != nil {
		t.Fatal(err)
	}
	identity := process.Identity{PID: 600242, BootID: "controlled-lost-start", StartTicks: 600242}
	if err = k.RecordManagedProcessStarted(ctx, r.Process.Request.OwnerID, identity); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkManagedThreadRPC(ctx, r.Process.Request.OwnerID, "thread/start"); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, sid, []contract.Target{target})
	if err != nil {
		t.Fatal(err)
	}
	stop := func() {
		if err := k.ConfirmManagedProcessStopped(ctx, storage.ManagedStopProof{OwnerID: r.Process.Request.OwnerID, Disposition: "group_stopped", Identity: identity}); err != nil {
			t.Fatal(err)
		}
	}
	return root, k, b, snapshot, stop
}
func TestKernelCLIUnboundThreadResolutionIsStoppedAtomicAndReplayable(t *testing.T) {
	for _, kind := range []string{"main", "task"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			root, k, b, snapshot, stop := unknownThreadCreationFixture(t, kind)
			action, command := "recover_main", "recover-thread"
			request := storage.ThreadCreationResolutionRequest{Key: "resolve-lost-start", Action: action, Expected: b, Snapshot: &snapshot, Reason: "operator confirmed lost thread/start after stopping owner"}
			if kind == "task" {
				command, request.Action, request.Snapshot = "retire-thread", "retire_task", nil
			}
			file := operatorFile(t, request)
			if _, err := cliCall(t, command, "--root", root, "--file", file); err == nil {
				t.Fatal("live creation disposed")
			}
			stop()
			stale := request
			stale.Expected.PreparationID = "stale-preparation"
			if _, err := cliCall(t, command, "--root", root, "--file", operatorFile(t, stale)); err == nil {
				t.Fatal("stale preparation disposed")
			}
			if _, err := k.DB.ExecContext(ctx, "CREATE TRIGGER fail_resolution BEFORE INSERT ON records WHEN NEW.kind='thread_creation_resolution' BEGIN SELECT RAISE(ABORT,'injected resolution failure'); END"); err != nil {
				t.Fatal(err)
			}
			if _, err := cliCall(t, command, "--root", root, "--file", file); err == nil {
				t.Fatal("receipt failure ignored")
			}
			state, err := k.SessionDispatchState(ctx, b.ID)
			if err != nil || !state.Creating || !state.ProcessStopped || !reflect.DeepEqual(state.Binding, b) {
				t.Fatal("failed receipt partially applied", state, err)
			}
			if _, err = k.DB.ExecContext(ctx, "DROP TRIGGER fail_resolution"); err != nil {
				t.Fatal(err)
			}
			first, err := cliCall(t, command, "--root", root, "--file", file)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := cliCall(t, command, "--root", root, "--file", file)
			if err != nil || !bytes.Equal(first, replay) {
				t.Fatal(string(replay), err)
			}
			var receipt storage.ThreadCreationResolution
			if err = json.Unmarshal(first, &receipt); err != nil || receipt.Before.PreparationID != b.PreparationID || receipt.After.ThreadID != "" {
				t.Fatal(receipt, err)
			}
			current, err := k.SessionDispatchState(ctx, b.ID)
			if err != nil || current.Creating || !current.ProcessStopped || current.Run != nil {
				t.Fatal(current, err)
			}
			if kind == "main" {
				if receipt.After.Generation != b.Generation+1 || receipt.After.PreparationID != "" || receipt.After.Closed {
					t.Fatal(receipt)
				}
			} else {
				task, err := k.Task(ctx, b.TaskID)
				if err != nil || task.State != contract.TaskFailed || task.Attempts != 0 || !receipt.After.Closed {
					t.Fatal("lost Task creation repeated", task, receipt, err)
				}
			}
			if kind == "main" {
				// A lost reply replay must never dispose a later unknown creation.
				if err = k.ReserveSessionThread(ctx, receipt.After); err != nil {
					t.Fatal(err)
				}
				newer, loadErr := k.SessionBinding(ctx, b.ID)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				if err = k.MarkSessionStopped(ctx, b.ID, newer.PreparationID); err != nil {
					t.Fatal(err)
				}
				if _, err = cliCall(t, command, "--root", root, "--file", file); err != nil {
					t.Fatal(err)
				}
				nextState, stateErr := k.SessionDispatchState(ctx, b.ID)
				if stateErr != nil || !nextState.Creating || nextState.Binding.Generation != newer.Generation || nextState.Binding.PreparationID != newer.PreparationID {
					t.Fatal("old receipt disposed newer creation", nextState, stateErr)
				}
			}
			var original contract.SessionBinding
			id := b.ID + ":g1"
			if err = k.Record(ctx, "thread_creation_intent", id, &original); err != nil || original.PreparationID != b.PreparationID {
				t.Fatal("old creation evidence lost", original, err)
			}
			request.Reason = "conflicting disposition"
			if _, err = cliCall(t, command, "--root", root, "--file", operatorFile(t, request)); err == nil {
				t.Fatal("changed replay accepted")
			}
		})
	}
}
func TestKernelCLIRetiresCanceledUnboundTaskWithoutChangingCancellation(t *testing.T) {
	root, k, b, _, stop := unknownThreadCreationFixture(t, "task")
	stop()
	ctx := context.Background()
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "cancel-lost-task", Target: b.Target, TaskID: b.TaskID, Action: "cancel", Reason: "operator canceled Task"}); err != nil {
		t.Fatal(err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	r := storage.ThreadCreationResolutionRequest{Key: "retire-canceled", Action: "retire_task", Expected: current, Reason: "retire unresolved creation after cancellation"}
	if _, err = cliCall(t, "retire-thread", "--root", root, "--file", operatorFile(t, r)); err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, b.TaskID)
	if err != nil || task.State != contract.TaskCanceled || task.Reason != "operator canceled Task" {
		t.Fatal(task, err)
	}
	wave, err := k.Wave(ctx, b.Target.WaveID)
	if err != nil || wave.State == domain.WaveCompleted {
		t.Fatal(wave, err)
	}
}
func TestKernelCLIUnboundRecoveryCannotDiscardBoundUnknownRun(t *testing.T) {
	root, _, b, _, snapshot := recoveryMainFixture(t, "unknown")
	r := storage.ThreadCreationResolutionRequest{Key: "discard-unknown", Action: "recover_main", Expected: b, Snapshot: &snapshot, Reason: "unknown result must remain unresolved"}
	if _, err := cliCall(t, "recover-thread", "--root", root, "--file", operatorFile(t, r)); err == nil {
		t.Fatal("bound unknown run discarded")
	}
}
