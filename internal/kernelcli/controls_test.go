package kernelcli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func operatorFile(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "operator.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func controlWave(t *testing.T) (string, domain.Wave) {
	t.Helper()
	root := cliRoot(t)
	raw, err := cliCall(t, "create", "--root", root, "--file", requestFile(t, "operator-request", "research the original request"))
	if err != nil {
		t.Fatal(err)
	}
	var w domain.Wave
	if err = json.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	return root, w
}
func TestKernelCLIVersionedControlsReplayAndCancelPriority(t *testing.T) {
	root, w := controlWave(t)
	pause := storage.OperatorControl{Key: "pause", Target: w.Target(), Action: "pause", Reason: "operator paused dispatch"}
	first, err := cliCall(t, "pause", "--root", root, "--file", operatorFile(t, pause))
	if err != nil {
		t.Fatal(err)
	}
	resume := pause
	resume.Key, resume.Action, resume.Reason = "resume", "resume", "operator resumed dispatch"
	if _, err = cliCall(t, "resume", "--root", root, "--file", operatorFile(t, resume)); err != nil {
		t.Fatal(err)
	}
	replay, err := cliCall(t, "pause", "--root", root, "--file", operatorFile(t, pause))
	if err != nil || !bytes.Equal(first, replay) {
		t.Fatal(string(replay), err)
	}
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	current, err := k.Wave(context.Background(), w.ID)
	if err != nil || current.Paused || current.MainTurns != 0 {
		t.Fatal("old pause replay changed current facts", current, err)
	}
	wrong := pause
	wrong.Key = "wrong-action"
	wrong.Action = "cancel"
	if _, err = cliCall(t, "pause", "--root", root, "--file", operatorFile(t, wrong)); err == nil {
		t.Fatal("file action overrode command")
	}
	stale := pause
	stale.Key = "stale"
	stale.Target.ScopeRevision++
	if _, err = cliCall(t, "pause", "--root", root, "--file", operatorFile(t, stale)); err == nil {
		t.Fatal("stale target accepted")
	}
	cancel := pause
	cancel.Key, cancel.Action, cancel.Reason = "cancel", "cancel", "operator canceled the Wave"
	if _, err = cliCall(t, "cancel", "--root", root, "--file", operatorFile(t, cancel)); err != nil {
		t.Fatal(err)
	}
	resume.Key = "resume-after-cancel"
	if _, err = cliCall(t, "resume", "--root", root, "--file", operatorFile(t, resume)); err == nil {
		t.Fatal("terminal Wave resumed")
	}
	current, err = k.Wave(context.Background(), w.ID)
	if err != nil || current.State != domain.WaveCanceled || current.MainTurns != 0 {
		t.Fatal(current, err)
	}
	if _, err = cliCall(t, "cancel", "--root", root, "--id", w.ID, "--file", operatorFile(t, cancel)); err == nil {
		t.Fatal("irrelevant flag silently ignored")
	}
}
func TestKernelCLISteerQueuesSafeBoundaryAndCancelSupersedes(t *testing.T) {
	root, w := controlWave(t)
	request := storage.WaveDirectionRequest{Key: "direction", Target: w.Target(), Instruction: "Clarify how the original evidence should be explained."}
	raw, err := cliCall(t, "steer", "--root", root, "--file", operatorFile(t, request))
	if err != nil {
		t.Fatal(err)
	}
	var direction storage.WaveDirection
	if err = json.Unmarshal(raw, &direction); err != nil || direction.State != "queued" {
		t.Fatal(direction, err)
	}
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	current, err := k.Wave(context.Background(), w.ID)
	if err != nil || current.ScopeRevision != 1 || current.PendingDirectionID != direction.ID || current.OriginalRequest != w.OriginalRequest || current.MainTurns != 0 {
		t.Fatal("CLI applied or ran direction prematurely", current, err)
	}
	cancel := storage.OperatorControl{Key: "cancel-direction", Target: w.Target(), Action: "cancel", Reason: "cancel instead of applying queued direction"}
	if _, err = cliCall(t, "cancel", "--root", root, "--file", operatorFile(t, cancel)); err != nil {
		t.Fatal(err)
	}
	if actual, err := k.ApplyWaveDirection(context.Background(), direction.ID); err != nil || actual.State != "canceled" {
		t.Fatal(actual, err)
	}
}
func TestKernelCLIControlHintObservesCommittedState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root, w := controlWave(t)
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	observed := make(chan bool, 1)
	server, err := ipc.Listen(root, nil, func() { current, err := k.Wave(context.Background(), w.ID); observed <- err == nil && current.Paused })
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	request := storage.OperatorControl{Key: "hinted-pause", Target: w.Target(), Action: "pause", Reason: "save before hint"}
	if _, err = cliCall(t, "pause", "--root", root, "--file", operatorFile(t, request)); err != nil {
		t.Fatal(err)
	}
	select {
	case committed := <-observed:
		if !committed {
			t.Fatal("hint preceded commit")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("saved mutation did not hint daemon")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("IPC did not stop")
	}
}
func TestKernelCLIInterruptPersistsExactGenerationWithoutFreeingCapacity(t *testing.T) {
	ctx := context.Background()
	root, w := controlWave(t)
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	spec := contract.TaskSpec{ID: "work", Kind: "work", Instruction: "research", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	plan := contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{}, Phases: []contract.PhaseSpec{{ID: "work", Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"}}}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "controlled-owner", RunID: "controlled-run", Mode: "execute", Binding: b})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.BeginManagedProcessLaunch(ctx, "controlled-owner"); err != nil {
		t.Fatal(err)
	}
	identity := process.Identity{PID: 600041, BootID: "controlled-fixture", StartTicks: 600041}
	if err = k.RecordManagedProcessStarted(ctx, "controlled-owner", identity); err != nil {
		t.Fatal(err)
	}
	if err = k.MarkManagedThreadRPC(ctx, "controlled-owner", "thread/start"); err != nil {
		t.Fatal(err)
	}
	if err = k.BindSessionThread(ctx, reservation.Binding, "controlled-thread"); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "controlled-run", ThreadID: b.ThreadID, State: "starting", Started: time.Now().UTC(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err = k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID = "unknown", "controlled-turn"
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	request := storage.SessionInterruptRequest{Key: "interrupt", Target: w.Target(), SessionID: b.ID, RunID: run.ID, PreparationID: b.PreparationID, Reason: "explicitly interrupt this generation"}
	stale := request
	stale.PreparationID = "stale-generation"
	if _, err = cliCall(t, "interrupt", "--root", root, "--file", operatorFile(t, stale)); err == nil {
		t.Fatal("stale generation interrupted")
	}
	raw, err := cliCall(t, "interrupt", "--root", root, "--file", operatorFile(t, request))
	if err != nil {
		t.Fatal(err)
	}
	var intent storage.SessionInterruption
	if err = json.Unmarshal(raw, &intent); err != nil || intent.State != "pending" || intent.OwnerID != "controlled-owner" {
		t.Fatal(intent, err)
	}
	state, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || state.Run == nil || state.Run.State != "unknown" || state.Binding.TaskState != contract.TaskBlocked || state.ProcessStopped {
		t.Fatal("interrupt fabricated stop/outcome", state, err)
	}
	capacity, err := k.ExecutionCapacity(ctx)
	if err != nil || capacity.HeldProcessSlots != 1 {
		t.Fatal("interrupt released unstopped owner", capacity, err)
	}
	replay, err := cliCall(t, "interrupt", "--root", root, "--file", operatorFile(t, request))
	if err != nil || !bytes.Equal(replay, raw) {
		t.Fatal(string(replay), err)
	}
}

func TestKernelCLITaskPauseDoesNotPauseSiblingOrWave(t *testing.T) {
	ctx := context.Background()
	root, w := controlWave(t)
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	first := contract.TaskSpec{ID: "first", Kind: "work", Instruction: "independent research", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	second := first
	second.ID = "second"
	plan := contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{}, Phases: []contract.PhaseSpec{{ID: "parallel", Tasks: []contract.TaskSpec{first, second}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{first.ID, second.ID}}, FailurePolicy: "drain_and_block"}}}
	admission, err := k.AdoptPlan(ctx, w.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.OperatorControl{Key: "pause-one", Target: w.Target(), TaskID: admission.CreatedTaskIDs[0], Action: "pause", Reason: "inspect one Task"}
	if _, err = cliCall(t, "pause", "--root", root, "--file", operatorFile(t, request)); err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admission.CreatedTaskIDs[0])
	if err != nil || !task.Paused {
		t.Fatal(task, err)
	}
	sibling, err := k.Task(ctx, admission.CreatedTaskIDs[1])
	if err != nil || sibling.Paused {
		t.Fatal(sibling, err)
	}
	wave, err := k.Wave(ctx, w.ID)
	if err != nil || wave.Paused {
		t.Fatal(wave, err)
	}
}
