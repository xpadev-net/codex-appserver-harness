package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func mainBudgetSnapshot(t *testing.T, id, session string, targets ...contract.Target) contract.InputSnapshot {
	t.Helper()
	raw, err := json.Marshal(struct {
		SchemaVersion int               `json:"schema_version"`
		SessionID     string            `json:"session_id"`
		Targets       []contract.Target `json:"targets"`
	}{1, session, targets})
	if err != nil {
		t.Fatal(err)
	}
	return contract.InputSnapshot{ID: id, Digest: fmt.Sprintf("%x", sha256.Sum256(raw)), Input: raw}
}

func observeBudgetTokens(t *testing.T, k *KernelStore, runID string, total int64, final bool) {
	t.Helper()
	var run contract.SessionRun
	ctx := context.Background()
	if err := k.Record(ctx, "agent_run", runID, &run); err != nil {
		t.Fatal(err)
	}
	var owner string
	if err := k.DB.QueryRow("SELECT session_id FROM kernel_run_bindings WHERE run_id=?", runID).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if err := k.ObserveRunTokens(ctx, RunTokenObservation{SessionID: owner, RunID: runID, ThreadID: run.ThreadID, TurnID: run.TurnID, TotalForRun: &total, Final: final}); err != nil {
		t.Fatal(err)
	}
}
func reserveBudgetMain(t *testing.T, k *KernelStore, id string, targets ...contract.Target) (contract.SessionBinding, contract.SessionRun, contract.InputSnapshot) {
	t.Helper()
	b := prepareWakeSession(t, k, "main")
	snapshot := mainBudgetSnapshot(t, "input-"+id, b.ID, targets...)
	run := contract.SessionRun{ID: id, ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
	if err := k.ReserveSessionRun(context.Background(), b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	return b, run, snapshot
}
func finishBudgetMain(t *testing.T, k *KernelStore, b contract.SessionBinding, run contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	run.State, run.TurnID = "completed", "turn-"+run.ID
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := k.ArchiveSessionRun(ctx, b.ID, run.ID, "test disposition after known completion"); err != nil {
		t.Fatal(err)
	}
}
func budgetWaves(t *testing.T, limits domain.KernelLimits, n int) (*KernelStore, []domain.Wave) {
	t.Helper()
	k, request := waveStoreFixture(t)
	request.Policy.Limits = limits
	waves := []domain.Wave{}
	for i := 0; i < n; i++ {
		request.SourceID = fmt.Sprint("budget-", i)
		w, err := k.CreateWave(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		waves = append(waves, w)
	}
	return k, waves
}
func TestSharedMainRunChargesStableWaveSharesOnceAndKeepsUnknownPartialUsage(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 2)
	b, run, _ := reserveBudgetMain(t, k, "shared", waves[1].Target(), waves[0].Target())
	for _, w := range waves {
		s, err := k.WaveRunBudget(ctx, w.ID)
		if err != nil || s.MainTurns != 1 || s.UnobservedRuns != 1 || s.ObservedTokens != 0 {
			t.Fatal(s, err)
		}
	}
	// Thread total is unrelated to this run's increment.
	run.State, run.TurnID = "running", "turn-shared"
	run.Usage = json.RawMessage(`{"last":{"totalTokens":5},"total":{"totalTokens":100005}}`)
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	sort.Slice(waves, func(i, j int) bool { return waves[i].ID < waves[j].ID })
	for i, w := range waves {
		expected := int64(3 - i)
		s, err := k.WaveRunBudget(ctx, w.ID)
		if err != nil || s.ObservedTokens != expected || s.UnobservedRuns != 1 {
			t.Fatal(s, expected, err)
		}
	}
	// A known, stopped result still permits delayed usage without rewriting it.
	finishBudgetMain(t, k, b, run)
	observeBudgetTokens(t, k, run.ID, 5, true)
	observeBudgetTokens(t, k, run.ID, 5, true)
	var sum int64
	for _, w := range waves {
		s, err := k.WaveRunBudget(ctx, w.ID)
		if err != nil || s.UnobservedRuns != 0 || s.MainTurns != 1 {
			t.Fatal(s, err)
		}
		sum += s.ObservedTokens
	}
	if sum != 5 {
		t.Fatal("shared run was double charged", sum)
	}
	observeBudgetTokens(t, k, run.ID, 7, false)
	for i, w := range waves {
		s, err := k.WaveRunBudget(ctx, w.ID)
		if err != nil || s.ObservedTokens != int64(4-i) || s.UnobservedRuns != 1 {
			t.Fatal("late higher partial usage lost uncertainty", s, err)
		}
	}
	observeBudgetTokens(t, k, run.ID, 2, true) // reordered lower facts cannot reduce usage.
	for i, w := range waves {
		s, err := k.WaveRunBudget(ctx, w.ID)
		if err != nil || s.ObservedTokens != int64(4-i) || s.UnobservedRuns != 1 {
			t.Fatal(s, err)
		}
	}
}
func TestMainThreadRotationPreservesWaveTurnBudget(t *testing.T) {
	ctx := context.Background()
	limits := domain.DefaultKernelLimits()
	limits.ThreadTurns = 1
	limits.MainTurns = 2
	k, waves := budgetWaves(t, limits, 1)
	w := waves[0]
	b, run, snapshot := reserveBudgetMain(t, k, "first", w.Target())
	finishBudgetMain(t, k, b, run)
	b = prepareWakeSession(t, k, b.ID)
	next := contract.SessionRun{ID: "too-many-thread-turns", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true}
	if err := k.ReserveSessionRun(ctx, b, snapshot, next); !errors.Is(err, ErrMainThreadTurns) {
		t.Fatal(err)
	}
	status, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil || status.MainTurns != 1 {
		t.Fatal("rejected admission spent a Wave turn", status, err)
	}
	if err = k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	if err = k.RebindMainThread(ctx, b, "rotated-thread", snapshot.ID, "trusted safe maintenance after applied inputs"); err != nil {
		t.Fatal(err)
	}
	b, run, _ = reserveBudgetMain(t, k, "second", w.Target())
	finishBudgetMain(t, k, b, run)
	b = prepareWakeSession(t, k, b.ID)
	next = recoveryRun(k, b, "too-many-wave-turns")
	if err = k.ReserveSessionRun(ctx, b, snapshot, next); !errors.Is(err, ErrWaveRunBudget) {
		t.Fatal(err)
	}
	status, err = k.WaveRunBudget(ctx, w.ID)
	if err != nil || status.MainTurns != 2 || status.UnobservedRuns != 2 {
		t.Fatal(status, err)
	}
}
func TestTokenLimitStopsNextTurnAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	limits := domain.DefaultKernelLimits()
	limits.Tokens = 5
	k, waves := budgetWaves(t, limits, 1)
	w := waves[0]
	b, run, snapshot := reserveBudgetMain(t, k, "first", w.Target())
	finishBudgetMain(t, k, b, run)
	observeBudgetTokens(t, k, run.ID, 5, false)
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	reopened, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	k = reopened
	b = prepareWakeSession(t, k, b.ID)
	if err = k.ReserveSessionRun(ctx, b, snapshot, recoveryRun(k, b, "second")); !errors.Is(err, ErrWaveRunBudget) {
		t.Fatal(err)
	}
	status, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil || status.ObservedTokens != 5 || status.MainTurns != 1 || status.UnobservedRuns != 1 {
		t.Fatal(status, err)
	}
}
func TestRunBudgetAdmissionRollsBackAllWaveCountersAndRejectsForgedScopes(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 2)
	b := prepareWakeSession(t, k, "main")
	run := recoveryRun(k, b, "failed")
	snapshot := mainBudgetSnapshot(t, "input", b.ID, waves[0].Target(), waves[1].Target())
	if _, err := k.DB.Exec("CREATE TRIGGER fail_run_budget BEFORE INSERT ON kernel_run_bindings BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if err := k.ReserveSessionRun(ctx, b, snapshot, run); err == nil {
		t.Fatal("injected failure ignored")
	}
	for _, w := range waves {
		s, err := k.WaveRunBudget(ctx, w.ID)
		if err != nil || s.MainTurns != 0 || s.UnobservedRuns != 0 {
			t.Fatal("partial multi-Wave charge", s, err)
		}
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_run_budget"); err != nil {
		t.Fatal(err)
	}
	for _, targets := range [][]contract.Target{{waves[0].Target(), waves[0].Target()}, {{WaveID: waves[0].ID, ScopeRevision: 2, PlanRevision: 1, PolicyRevision: 1}}} {
		bad := mainBudgetSnapshot(t, "bad", b.ID, targets...)
		if err := k.ReserveSessionRun(ctx, b, bad, run); err == nil {
			t.Fatal("invalid scope charged", targets)
		}
	}
	other := capacityMain(t, k, "other-parent", t.TempDir())
	foreign, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: other.ID, Source: "direct", SourceID: "foreign", OriginalRequest: "foreign request", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "foreign"}}, Policy: waves[0].Policy})
	if err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionRun(ctx, b, mainBudgetSnapshot(t, "foreign", b.ID, foreign.Target()), run); err == nil {
		t.Fatal("foreign Wave accepted")
	}
}
func TestTokenObservationRejectsWrongRuntimeAndNeverUsesThreadTotal(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 1)
	b, run, _ := reserveBudgetMain(t, k, "run", waves[0].Target())
	run.State, run.TurnID = "running", "turn"
	run.Usage = json.RawMessage(`{"total":{"totalTokens":12345}}`)
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	status, err := k.WaveRunBudget(ctx, waves[0].ID)
	if err != nil || status.ObservedTokens != 0 || status.UnobservedRuns != 1 {
		t.Fatal(status, err)
	}
	amount := int64(5)
	for _, obs := range []RunTokenObservation{{SessionID: "other", RunID: run.ID, ThreadID: run.ThreadID, TurnID: run.TurnID, TotalForRun: &amount}, {SessionID: b.ID, RunID: run.ID, ThreadID: "other", TurnID: run.TurnID, TotalForRun: &amount}, {SessionID: b.ID, RunID: run.ID, ThreadID: run.ThreadID, TurnID: "other", TotalForRun: &amount}} {
		if err = k.ObserveRunTokens(ctx, obs); err == nil {
			t.Fatal("forged observation accepted")
		}
	}
	observeBudgetTokens(t, k, run.ID, 5, true)
	run.Usage = json.RawMessage(`{"last":{"totalTokens":-1}}`)
	if err = k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	status, err = k.WaveRunBudget(ctx, waves[0].ID)
	if err != nil || status.ObservedTokens != 5 || status.UnobservedRuns != 0 {
		t.Fatal("invalid raw usage erased verified facts", status, err)
	}
}
func TestTokenSumSaturatesInsteadOfOverflowingAdmission(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 1)
	for i, amount := range []int64{math.MaxInt64, 1} {
		if _, err := k.DB.Exec("INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'tokens',?,0,?)", waves[0].ID, fmt.Sprint(i), amount, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	status, err := k.WaveRunBudget(ctx, waves[0].ID)
	if err != nil || status.ObservedTokens != math.MaxInt64 || !status.Saturated {
		t.Fatal(status, err)
	}
}

func TestKnownTaskTokensHoldLaterPhaseWithoutAnotherModelRun(t *testing.T) {
	ctx := context.Background()
	limits := domain.DefaultKernelLimits()
	limits.Tokens = 5
	k, waves := budgetWaves(t, limits, 1)
	w := waves[0]
	receipt, err := k.AdoptPlan(ctx, w.Target(), "plan", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, receipt.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	completeTemplateTask(t, k, task, "completed evidence", "", nil)
	observeBudgetTokens(t, k, "run-"+task.ID, 5, false)
	set := mustReady(t, k, w)
	if len(set.Held) != 1 || set.Held[0].Reason != "token_budget_exhausted" {
		t.Fatal(set)
	}
	status, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil || status.ObservedTokens != 5 || status.UnobservedRuns != 1 {
		t.Fatal(status, err)
	}
}
func TestBaselineTurnUsageCannotBeAttributedToNewRun(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 1)
	b := prepareWakeSession(t, k, "main")
	run := recoveryRun(k, b, "new-run")
	run.BaselineTurnIDs = []string{"old-turn"}
	if err := k.ReserveSessionRun(ctx, b, mainBudgetSnapshot(t, "scope", b.ID, waves[0].Target()), run); err != nil {
		t.Fatal(err)
	}
	run.State, run.TurnID = "unknown", "old-turn"
	run.Usage = json.RawMessage(`{"last":{"totalTokens":9999}}`)
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	status, err := k.WaveRunBudget(ctx, waves[0].ID)
	if err != nil || status.ObservedTokens != 0 || status.UnobservedRuns != 1 {
		t.Fatal(status, err)
	}
	total := int64(9999)
	if err = k.ObserveRunTokens(ctx, RunTokenObservation{SessionID: b.ID, RunID: run.ID, ThreadID: run.ThreadID, TurnID: run.TurnID, TotalForRun: &total, Final: true}); err == nil {
		t.Fatal("old baseline turn cost was charged to new run")
	}
}
