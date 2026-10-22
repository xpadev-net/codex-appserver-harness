package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func operationBudgetFixture(t *testing.T, limits WaveOperationLimits) (*KernelStore, domain.Wave) {
	t.Helper()
	k, r := waveStoreFixture(t)
	r.Policy.Stage = "P"
	r.Policy.Product = true
	r.OperationLimits = &limits
	w, err := k.CreateWave(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return k, w
}
func freshOperationBudgetPlan() contract.PlanProposal {
	p := storagePlanFixture()
	for i := range p.Phases {
		phase := &p.Phases[i]
		phase.ID += "-next"
		for j := range phase.Completion.RequiredTaskIDs {
			phase.Completion.RequiredTaskIDs[j] += "-next"
		}
		for j := range phase.Tasks {
			task := &phase.Tasks[j]
			task.ID += "-next"
			for d := range task.DependsOn {
				task.DependsOn[d].TaskID += "-next"
			}
		}
	}
	return p
}
func operationLedgerCount(t *testing.T, k *KernelStore, waveID, kind string) int {
	t.Helper()
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_budget_ledger WHERE wave_id=? AND kind=?", waveID, kind).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestWaveAdoptsOperationLimitsAtomically(t *testing.T) {
	ctx := context.Background()
	k, r := waveStoreFixture(t)
	bad := DefaultWaveOperationLimits()
	bad.ValidationJobs = 0
	r.OperationLimits = &bad
	if _, err := k.CreateWave(ctx, r); err == nil {
		t.Fatal("invalid limits admitted")
	}
	var n int
	k.DB.QueryRow("SELECT count(*) FROM kernel_waves").Scan(&n)
	if n != 0 {
		t.Fatal(n)
	}
	limits := DefaultWaveOperationLimits()
	limits.RunnerRetries = 0
	limits.ValidationJobs = 2
	r.OperationLimits = &limits
	w, err := k.CreateWave(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := k.WaveOperationLimits(ctx, w.ID)
	if err != nil || got != limits {
		t.Fatal(got, err)
	}
	changed := limits
	changed.ValidationJobs = 3
	r.OperationLimits = &changed
	if _, err := k.CreateWave(ctx, r); err == nil {
		t.Fatal("source replay replaced adopted limits")
	}
	got, err = k.WaveOperationLimits(ctx, w.ID)
	if err != nil || got != limits {
		t.Fatal(got, err)
	}
}
func TestValidationJobBudgetSurvivesReplayReplanAndRestart(t *testing.T) {
	ctx := context.Background()
	limits := DefaultWaveOperationLimits()
	limits.ValidationJobs = 2
	k, w := operationBudgetFixture(t, limits)
	plan, err := k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, plan.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	first := operationRequest(t, w, "runner", "task-job")
	first.SessionID = task.SessionID
	original, err := k.ReserveOperationCapacity(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := k.ReserveOperationCapacity(ctx, first)
	if err != nil || !reflect.DeepEqual(original, replay) {
		t.Fatal(replay, err)
	}
	releaseOperation(t, k, first.ID, "process_stopped")
	second := operationRequest(t, w, "runner", "parent-job")
	if _, err := k.ReserveOperationCapacity(ctx, second); err != nil {
		t.Fatal(err)
	}
	releaseOperation(t, k, second.ID, "process_stopped")
	if _, err := k.AdoptPlan(ctx, w.Target(), "parent", freshOperationBudgetPlan()); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	third := operationRequest(t, w, "runner", "post-replan-job")
	if _, err := k.ReserveOperationCapacity(ctx, third); !errors.Is(err, ErrOperationBudget) {
		t.Fatal("budget reset", err)
	}
	if n := operationLedgerCount(t, k, w.ID, "validation_job"); n != 2 {
		t.Fatal(n)
	}
	if replay, err := k.ReserveOperationCapacity(ctx, first); err != nil || replay.State != "released" {
		t.Fatal("old scope replay lost history", replay, err)
	}
}
func TestRunnerRetryRequiresClassifiedStoppedFailureAndSharesRootBudget(t *testing.T) {
	for _, classified := range []bool{false, true} {
		t.Run(map[bool]string{false: "unclassified", true: "infrastructure"}[classified], func(t *testing.T) {
			ctx := context.Background()
			k, w := operationBudgetFixture(t, DefaultWaveOperationLimits())
			root := operationRequest(t, w, "runner", "root")
			if _, err := k.ReserveOperationCapacity(ctx, root); err != nil {
				t.Fatal(err)
			}
			retry := root
			retry.ID = "retry"
			retry.RetryOf = root.ID
			if _, err := k.ReserveOperationCapacity(ctx, retry); err == nil {
				t.Fatal("retry before stop")
			}
			proof := OperationReleaseProof{OperationID: root.ID, ProofID: "root-stopped", Disposition: "process_stopped", Reason: "trusted runner observed outcome", RetryableInfrastructureFailure: classified}
			if err := k.ReleaseOperationCapacity(ctx, proof); err != nil {
				t.Fatal(err)
			}
			_, err := k.ReserveOperationCapacity(ctx, retry)
			if !classified {
				if err == nil {
					t.Fatal("unclassified retry accepted")
				}
				if n := operationLedgerCount(t, k, w.ID, "validation_job"); n != 1 {
					t.Fatal(n)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReserveOperationCapacity(ctx, retry); err != nil {
				t.Fatal(err)
			}
			if n := operationLedgerCount(t, k, w.ID, "validation_job"); n != 2 {
				t.Fatal(n)
			}
			if n := operationLedgerCount(t, k, w.ID, "runner_retry"); n != 1 {
				t.Fatal(n)
			}
			usage, err := k.WaveOperationBudget(ctx, w.ID)
			if err != nil || usage.ValidationJobs != 2 || usage.RunnerRetries != 1 {
				t.Fatal(usage, err)
			}
			proof.OperationID = retry.ID
			proof.ProofID = "retry-stopped"
			if err := k.ReleaseOperationCapacity(ctx, proof); err != nil {
				t.Fatal(err)
			}
			extra := retry
			extra.ID = "extra"
			extra.RetryOf = retry.ID
			if _, err := k.ReserveOperationCapacity(ctx, extra); !errors.Is(err, ErrOperationBudget) {
				t.Fatal("retry chain exceeded", err)
			}
			extra.RetryOf = root.ID
			if _, err := k.ReserveOperationCapacity(ctx, extra); !errors.Is(err, ErrOperationBudget) {
				t.Fatal("branch bypassed root retry count", err)
			}
		})
	}
}
func TestSameCandidateReviewsRetainCountAcrossPlanAndRestart(t *testing.T) {
	ctx := context.Background()
	k, w := operationBudgetFixture(t, DefaultWaveOperationLimits())
	var first OperationCapacityRequest
	for _, id := range []string{"review-1", "review-2"} {
		r := operationRequest(t, w, "cloud_review", id)
		if id == "review-1" {
			first = r
		}
		if _, err := k.ReserveOperationCapacity(ctx, r); err != nil {
			t.Fatal(err)
		}
		releaseOperation(t, k, id, "completed")
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture()); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "parent", freshOperationBudgetPlan()); err != nil {
		t.Fatal(err)
	}
	var err error
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	third := operationRequest(t, w, "cloud_review", "review-3")
	if _, err := k.ReserveOperationCapacity(ctx, third); !errors.Is(err, ErrOperationBudget) {
		t.Fatal("same candidate cap reset", err)
	}
	if n := operationLedgerCount(t, k, w.ID, "candidate_review"); n != 2 {
		t.Fatal(n)
	}
	if _, err := k.ReserveOperationCapacity(ctx, first); err != nil {
		t.Fatal("history replay charged", err)
	}
	third.CandidateSHA = strings.Repeat("b", 40)
	if _, err := k.ReserveOperationCapacity(ctx, third); err != nil {
		t.Fatal("new candidate blocked", err)
	}
}
func TestOperationBudgetAndCapacityRollbackTogether(t *testing.T) {
	ctx := context.Background()
	k, w := operationBudgetFixture(t, DefaultWaveOperationLimits())
	r := operationRequest(t, w, "runner", "failed-admission")
	if _, err := k.DB.Exec("CREATE TRIGGER fail_budget_dispatch BEFORE INSERT ON kernel_dispatch_intents BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveOperationCapacity(ctx, r); err == nil {
		t.Fatal("injected failure admitted")
	}
	for _, kind := range []string{"validation_job", "runner_retry"} {
		if n := operationLedgerCount(t, k, w.ID, kind); n != 0 {
			t.Fatal(kind, n)
		}
	}
	var n int
	k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='operation_budget'").Scan(&n)
	if n != 0 {
		t.Fatal(n)
	}
	k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE state='held'").Scan(&n)
	if n != 0 {
		t.Fatal(n)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_budget_dispatch"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveOperationCapacity(ctx, r); err != nil {
		t.Fatal(err)
	}
}
func TestOpenKernelRefusesUnaccountedOperationHistoryReadOnly(t *testing.T) {
	for _, missingKind := range []string{"wave_operation_limits", "operation_budget"} {
		t.Run(missingKind, func(t *testing.T) {
			ctx := context.Background()
			k, w := operationBudgetFixture(t, DefaultWaveOperationLimits())
			r := operationRequest(t, w, "runner", "historic")
			if _, err := k.ReserveOperationCapacity(ctx, r); err != nil {
				t.Fatal(err)
			}
			releaseOperation(t, k, r.ID, "process_stopped")
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=?", missingKind); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(k.Root, "state.db")
			k.Close()
			if err := os.Chmod(path, 0640); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenKernel(path)
			if err == nil {
				reopened.Close()
				t.Fatal("unaccounted history accepted")
			}
			if !strings.Contains(err.Error(), "offline reconciliation/migration required") {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("DB changed", err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0640 {
				t.Fatal(info, err)
			}
		})
	}
}

func TestConcurrentRunnerAdmissionsCannotExceedWaveJobBudget(t *testing.T) {
	ctx := context.Background()
	limits := DefaultWaveOperationLimits()
	limits.ValidationJobs = 1
	k, w := operationBudgetFixture(t, limits)
	capacity := domain.DefaultExecutionLimits()
	capacity.Runners = 2
	if err := k.ConfigureExecutionLimits(ctx, capacity); err != nil {
		t.Fatal(err)
	}
	requests := []OperationCapacityRequest{operationRequest(t, w, "runner", "parallel-a"), operationRequest(t, w, "runner", "parallel-b")}
	errs := make(chan error, 2)
	for _, r := range requests {
		go func() { _, err := k.ReserveOperationCapacity(ctx, r); errs <- err }()
	}
	accepted, held := 0, 0
	for range requests {
		err := <-errs
		if err == nil {
			accepted++
		} else if errors.Is(err, ErrOperationBudget) {
			held++
		} else {
			t.Fatal(err)
		}
	}
	if accepted != 1 || held != 1 {
		t.Fatal(accepted, held)
	}
	if n := operationLedgerCount(t, k, w.ID, "validation_job"); n != 1 {
		t.Fatal(n)
	}
	var leases int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE state='held'").Scan(&leases); err != nil || leases != 1 {
		t.Fatal(leases, err)
	}
}
