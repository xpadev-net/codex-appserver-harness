package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func capacityMain(t *testing.T, k *KernelStore, id, cwd string) contract.SessionBinding {
	t.Helper()
	b, err := k.CreateOrchestrator(context.Background(), id, "configured", cwd, "read-only")
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestProcessCapacityIsReservedBeforeRPCAndReleasedOnlyByStoppingProof(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	a := capacityMain(t, k, "a", t.TempDir())
	b := capacityMain(t, k, "b", t.TempDir())
	aRequest := request
	aRequest.OrchestratorSessionID, aRequest.SourceID = a.ID, "a-budget"
	wave, err := k.CreateWave(ctx, aRequest)
	if err != nil {
		t.Fatal(err)
	}
	a = reserveSessionFixture(t, k, a)
	b = reserveSessionFixture(t, k, b)
	main, err := k.SessionBinding(ctx, request.OrchestratorSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionThread(ctx, main); !errors.Is(err, ErrExecutionCapacity) {
		t.Fatal(err)
	}
	stats, err := k.ExecutionCapacity(ctx)
	if err != nil || stats.HeldProcessSlots != 2 || stats.UnreconciledPreparations != 2 {
		t.Fatal(stats, err)
	}
	if err = k.ConfigureExecutionLimits(ctx, domain.LegacyExecutionLimits()); err == nil {
		t.Fatal("live process capacities were changed")
	}
	if err = k.BindSessionThread(ctx, a, "a-thread"); err != nil {
		t.Fatal(err)
	}
	a, err = k.SessionBinding(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "run", ThreadID: a.ThreadID, State: "starting", BaselineKnown: true, Started: k.clock()}
	if err = k.ReserveSessionRun(ctx, a, mainBudgetSnapshot(t, "capacity-input", a.ID, wave.Target()), run); err != nil {
		t.Fatal(err)
	}
	run.State = "unknown"
	if err = k.Put(ctx, "agent_run", run.ID, a.ID, run); err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionThread(ctx, main); !errors.Is(err, ErrExecutionCapacity) {
		t.Fatal("unknown start released capacity", err)
	}
	if err = k.MarkSessionStopped(ctx, a.ID, "wrong"); err == nil {
		t.Fatal("wrong stopping proof accepted")
	}
	if err = k.MarkSessionStopped(ctx, a.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionThread(ctx, main); err != nil {
		t.Fatal(err)
	}
	// Physical stopping permits another process, but not a second unknown turn.
	if err = k.ArchiveSessionRun(ctx, a.ID, run.ID, "unknown is not success"); err == nil {
		t.Fatal("unknown run archived")
	}
	var reserved ProcessReservation
	reserved, err = loadProcessReservationTx(ctx, k.DB, a.PreparationID)
	if err != nil || reserved.State != "released" || reserved.RunID != run.ID || reserved.StoppedAt.IsZero() {
		t.Fatal(reserved, err)
	}
}
func TestProcessCapacityAndLeasesAreAtomicAcrossConnections(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	other, err := OpenKernel(filepath.Join(k.Root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	bindings := []contract.SessionBinding{}
	for i := 0; i < 4; i++ {
		bindings = append(bindings, capacityMain(t, k, fmt.Sprint("m", i), t.TempDir()))
	}
	errs := make([]error, len(bindings))
	var group sync.WaitGroup
	for i := range bindings {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			store := k
			if i%2 == 1 {
				store = other
			}
			errs[i] = store.ReserveSessionThread(ctx, bindings[i])
		}(i)
	}
	group.Wait()
	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes < 1 || successes > 2 {
		t.Fatal("concurrent capacity overflow", successes, errs)
	}
	// Retry transient SQLite contention after all initial writers have settled.
	for i, err := range errs {
		if err != nil && successes < 2 {
			if err = k.ReserveSessionThread(ctx, bindings[i]); err == nil {
				successes++
			}
		}
	}
	stats, err := k.ExecutionCapacity(ctx)
	if err != nil || stats.HeldProcessSlots != 2 {
		t.Fatal(stats, err)
	}
	var held, leases int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_dispatch_intents WHERE state='held'").Scan(&held); err != nil {
		t.Fatal(err)
	}
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE state='held'").Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if held != 2 || leases != 2 {
		t.Fatal("reservation/lease/process latch diverged", held, leases)
	}
}
func TestWorkspaceLeaseGenerationFencesDelayedStoppingProof(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	cwd := t.TempDir()
	a := capacityMain(t, k, "a", cwd)
	b := capacityMain(t, k, "b", cwd)
	a = reserveSessionFixture(t, k, a)
	if err := k.ReserveSessionThread(ctx, b); !errors.Is(err, ErrResourceBusy) {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, a.ID, a.PreparationID); err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, k, b)
	if err := k.BindSessionThread(ctx, b, "b-thread"); err != nil {
		t.Fatal(err)
	}
	bound, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	b = bound
	r, err := loadProcessReservationTx(ctx, k.DB, b.PreparationID)
	if err != nil || r.Resources[0].Generation != 2 {
		t.Fatal(r, err)
	}
	if err = k.MarkSessionStopped(ctx, a.ID, a.PreparationID); err != nil {
		t.Fatal(err)
	}
	var state, owner string
	if err = k.DB.QueryRow("SELECT state,run_id FROM kernel_resource_leases WHERE resource=?", r.Resources[0].Resource).Scan(&state, &owner); err != nil {
		t.Fatal(err)
	}
	if state != "held" || owner != b.PreparationID {
		t.Fatal("old stopping proof released a new owner", state, owner)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	again := reserveSessionFixture(t, k, b)
	if err = k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err == nil {
		t.Fatal("old generation stopped a new preparation")
	}
	if again.PreparationID == b.PreparationID {
		t.Fatal("preparation identity was reused")
	}
}
func TestPreparationAdmissionFailureRollsBackAllReservations(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	b, err := k.SessionBinding(ctx, request.OrchestratorSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec("CREATE TRIGGER fail_capacity BEFORE INSERT ON kernel_dispatch_intents BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionThread(ctx, b); err == nil {
		t.Fatal("injected reservation failure ignored")
	}
	stats, err := k.ExecutionCapacity(ctx)
	if err != nil || stats.HeldProcessSlots != 0 {
		t.Fatal(stats, err)
	}
	current, err := k.SessionBinding(ctx, b.ID)
	if err != nil || current.PreparationID != "" {
		t.Fatal(current, err)
	}
	var count int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err = k.DB.Exec("DROP TRIGGER fail_capacity"); err != nil {
		t.Fatal(err)
	}
	reserveSessionFixture(t, k, current)
}
func TestExecutionPolicyWaveCapacityAndRestart(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	if err := k.ConfigureExecutionLimits(ctx, domain.LegacyExecutionLimits()); err != nil {
		t.Fatal(err)
	}
	wave, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.CreateWave(ctx, request); err != nil {
		t.Fatal("idempotent creation charged twice", err)
	}
	request.SourceID = "second"
	if _, err = k.CreateWave(ctx, request); !errors.Is(err, ErrExecutionCapacity) {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wave.State, wave.CompletedAt = domain.WaveCanceled, k.clock()
	if err = saveWave(ctx, tx, wave); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = k.CreateWave(ctx, request); err != nil {
		t.Fatal(err)
	}
	before, err := k.ExecutionCapacity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	reopened, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.ExecutionCapacity(ctx)
	if err != nil || after.Policy != before.Policy {
		t.Fatal(after, err)
	}
	if err = reopened.ConfigureExecutionLimits(ctx, domain.DefaultExecutionLimits()); err != nil {
		t.Fatal(err)
	}
	updated, err := reopened.ExecutionCapacity(ctx)
	if err != nil || updated.Policy.Revision != before.Policy.Revision+1 {
		t.Fatal(updated, err)
	}
}
func TestUntrackedPreparationStillCountsAndOwnsItsWorkspace(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	old, err := k.SessionBinding(ctx, request.OrchestratorSessionID)
	if err != nil {
		t.Fatal(err)
	}
	b := capacityMain(t, k, "new", old.CWD)
	old.PreparationID = "pre-feature-unknown"
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec("UPDATE kernel_sessions SET creating=1,process_stopped=0,payload=? WHERE id=?", raw, old.ID); err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionThread(ctx, b); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("old process workspace was stolen", err)
	}
	if err = k.MarkSessionStopped(ctx, old.ID, old.PreparationID); err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionThread(ctx, b); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptExecutionPolicyCannotResetLimitsOrAdmitProcesses(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('kernel_execution_policy','controller','controller',?)", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, request.OrchestratorSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionThread(ctx, b); err == nil {
		t.Fatal("corrupt policy fell back to defaults")
	}
	if _, err = k.CreateWave(ctx, request); err == nil {
		t.Fatal("corrupt policy admitted a Wave")
	}
	var stopped int
	if err = k.DB.QueryRow("SELECT process_stopped FROM kernel_sessions WHERE id=?", b.ID).Scan(&stopped); err != nil || stopped != 1 {
		t.Fatal(stopped, err)
	}
}

func TestWorktreeLeaseCoversSubdirectoriesAndKeepsLinkedWorktreesIndependent(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	repo := t.TempDir()
	for _, args := range [][]string{{"init", "--initial-branch=trunk"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.invalid"}, {"-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "base"}} {
		if _, err := workspace.Git(ctx, repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	subdir := filepath.Join(repo, "subdir")
	if err := os.Mkdir(subdir, 0700); err != nil {
		t.Fatal(err)
	}
	a := capacityMain(t, k, "repo-root", repo)
	b := capacityMain(t, k, "repo-child", subdir)
	a = reserveSessionFixture(t, k, a)
	if err := k.ReserveSessionThread(ctx, b); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("CWD within one worktree bypassed lease", err)
	}
	linked := filepath.Join(t.TempDir(), "linked")
	if _, err := workspace.Git(ctx, repo, "worktree", "add", "-b", "linked", "--", linked, "HEAD"); err != nil {
		t.Fatal(err)
	}
	c := capacityMain(t, k, "linked-tree", linked)
	if err := k.ReserveSessionThread(ctx, c); err != nil {
		t.Fatal("independent linked worktree could not reserve its own checkout", err)
	}
	// The same checkout may be used after its prior process is proven stopped.
	if err := k.MarkSessionStopped(ctx, a.ID, a.PreparationID); err != nil {
		t.Fatal(err)
	}
	if err := k.ReserveSessionThread(ctx, b); err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneWorkspaceBelowEmptyGitMarkerCanReserve(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	parent := t.TempDir()
	if err := os.Mkdir(filepath.Join(parent, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	cwd := filepath.Join(parent, "standalone")
	if err := os.Mkdir(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	b := capacityMain(t, k, "standalone", cwd)
	if err := k.ReserveSessionThread(ctx, b); err != nil {
		t.Fatal("empty placeholder became a broken repository", err)
	}
}
