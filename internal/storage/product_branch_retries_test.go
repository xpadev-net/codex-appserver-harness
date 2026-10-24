package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func stoppedUnsentBranchFixture(t *testing.T) (*KernelStore, string, string, ProductBranchRemote, ProductBranchPublication) {
	t.Helper()
	k, id, owner, remote := branchPublicationFixture(t)
	ctx := context.Background()
	job, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	job, err = k.RecordProductBranchStopped(ctx, id, owner, job.Digest, branchStopFixture(k, job, k.clock(), false, false))
	if err != nil {
		t.Fatal(err)
	}
	return k, id, owner, remote, job
}

func TestProductBranchRetryAncestorReplayAfterMultipleStopsNeverSpendsAgain(t *testing.T) {
	ctx := context.Background()
	k, id, owner, _, original := stoppedUnsentBranchFixture(t)
	limits, err := k.WaveOperationLimits(ctx, original.Value.Product.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	limits.BranchRetries = 3
	if err := k.Put(ctx, "wave_operation_limits", original.Value.Product.Value.Target.WaveID, "controller", limits); err != nil {
		t.Fatal(err)
	}
	latest := original
	for attempt := 1; attempt <= 3; attempt++ {
		next, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, latest.Digest)
		if err != nil || next.Value.Attempt != attempt {
			t.Fatal(next, err)
		}
		if _, err := k.ClaimProductBranchPublication(ctx, id, owner, next.Digest); err != nil {
			t.Fatal(err)
		}
		latest, err = k.RecordProductBranchStopped(ctx, id, owner, next.Digest, branchStopFixture(k, next, k.clock(), false, attempt == 3))
		if err != nil {
			t.Fatal(err)
		}
		// The root digest is now older than the immediate parent. Replay only
		// acknowledges current history; the next reservation above explicitly
		// names the latest stopped digest instead.
		replayed, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, original.Digest)
		if err != nil || !reflect.DeepEqual(replayed, latest) {
			t.Fatal("ancestor replay lost successor", replayed, err)
		}
		budget, err := k.WaveOperationBudget(ctx, original.Value.Product.Value.Target.WaveID)
		if err != nil || budget.BranchRetries != attempt {
			t.Fatal("replay spent retry", budget, err)
		}
	}
	if latest.State != "published" {
		t.Fatal(latest)
	}
}

func TestProductBranchRetryLegacyMissingLimitStaysDisabled(t *testing.T) {
	ctx := context.Background()
	k, id, owner, _, old := stoppedUnsentBranchFixture(t)
	waveID := old.Value.Product.Value.Target.WaveID
	current, err := k.WaveOperationLimits(ctx, waveID)
	if err != nil || current.BranchRetries != 1 {
		t.Fatal("new Wave default missing", current, err)
	}
	raw, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	var legacy map[string]any
	if err := json.Unmarshal(raw, &legacy); err != nil {
		t.Fatal(err)
	}
	delete(legacy, "branch_retries")
	if err := k.Put(ctx, "wave_operation_limits", waveID, "controller", legacy); err != nil {
		t.Fatal(err)
	}
	decoded, err := k.WaveOperationLimits(ctx, waveID)
	if err != nil || decoded.BranchRetries != 0 || decoded.RunnerRetries != current.RunnerRetries || decoded.ValidationJobs != current.ValidationJobs {
		t.Fatal("legacy limits silently changed", decoded, err)
	}
	if _, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, old.Digest); !errors.Is(err, ErrOperationBudget) {
		t.Fatal("legacy Wave granted new retry authority", err)
	}
}

func TestProductBranchUnsentRetryPreservesHistoryAndClaimsNewIntentOnce(t *testing.T) {
	ctx := context.Background()
	k, id, owner, remote, old := stoppedUnsentBranchFixture(t)
	type retryResult struct {
		job ProductBranchPublication
		err error
	}
	results := make(chan retryResult, 5)
	for i := 0; i < 5; i++ {
		go func() {
			job, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, old.Digest)
			results <- retryResult{job, err}
		}()
	}
	first := <-results
	next, err := first.job, first.err
	for i := 1; i < 5; i++ {
		r := <-results
		if r.err != nil || !reflect.DeepEqual(r.job, next) {
			t.Fatal("concurrent retry created another intent", r, r.err)
		}
	}
	if err != nil || next.ID == old.ID || next.State != "prepared" || !productBranchRetryMatches(old, next.Value) {
		t.Fatal(next, err)
	}
	storedOld, err := loadProductBranch(ctx, k.DB, old.ID)
	if err != nil || !reflect.DeepEqual(storedOld, old) {
		t.Fatal("retry reset old history", storedOld, err)
	}
	replay, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, old.Digest)
	if err != nil || !reflect.DeepEqual(replay, next) {
		t.Fatal("retry replay changed intent", replay, err)
	}
	reserved, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
	if err != nil || !reflect.DeepEqual(reserved, next) {
		t.Fatal("ordinary reservation lost successor", reserved, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	claim, err := k.ClaimProductBranchPublication(ctx, id, owner, next.Digest)
	if err != nil || !claim.Start || claim.Publication.ID != next.ID {
		t.Fatal(claim, err)
	}
	if repeat, err := k.ClaimProductBranchPublication(ctx, id, owner, next.Digest); err != nil || repeat.Start {
		t.Fatal("retry relaunched", repeat, err)
	}
	if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, old.Digest); !errors.Is(err, ErrProductBranchHeld) {
		t.Fatal("old attempt fenced new push", err)
	}
	started := k.clock()
	if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, next.Digest); err != nil {
		t.Fatal(err)
	}
	finished, err := k.RecordProductBranchStopped(ctx, id, owner, next.Digest, branchStopFixture(k, next, started, true, true))
	if err != nil || finished.State != "published" {
		t.Fatal(finished, err)
	}
	budget, err := k.WaveOperationBudget(ctx, next.Value.Product.Value.Target.WaveID)
	if err != nil || budget.BranchRetries != 1 || budget.Limits.BranchRetries != 1 {
		t.Fatal(budget, err)
	}
	for _, job := range []ProductBranchPublication{old, next} {
		r, err := k.OperationReservation(ctx, job.ID)
		if err != nil || r.State != "released" {
			t.Fatal(r, err)
		}
	}
}

func TestProductBranchUnsentRetryRejectsSentUnknownPausedAndDisabled(t *testing.T) {
	for _, mode := range []string{"launching", "sent", "attempted", "published", "paused", "disabled", "foreign"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, id, owner, remote := branchPublicationFixture(t)
			job, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil {
				t.Fatal(err)
			}
			started := k.clock()
			if mode == "sent" || mode == "attempted" {
				if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, job.Digest); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "launching" {
				proof := branchStopFixture(k, job, started, mode == "attempted", mode == "published")
				if _, err := k.RecordProductBranchStopped(ctx, id, owner, job.Digest, proof); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "paused" {
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-before-retry", Target: job.Value.Product.Value.Target, Action: "pause", Reason: "operator paused"}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "disabled" {
				limits, err := k.WaveOperationLimits(ctx, job.Value.Product.Value.Target.WaveID)
				if err != nil {
					t.Fatal(err)
				}
				limits.BranchRetries = 0
				if err := k.Put(ctx, "wave_operation_limits", job.Value.Product.Value.Target.WaveID, "controller", limits); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "foreign" {
				owner = "other"
			}
			if _, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, job.Digest); err == nil {
				t.Fatal("unsafe retry admitted", mode)
			}
			budget, err := k.WaveOperationBudget(ctx, job.Value.Product.Value.Target.WaveID)
			if err != nil || budget.BranchRetries != 0 {
				t.Fatal("rejected retry charged budget", budget, err)
			}
		})
	}
}

func TestProductBranchUnsentRetryRollbackAndBudgetSurviveReopen(t *testing.T) {
	ctx := context.Background()
	k, id, owner, _, old := stoppedUnsentBranchFixture(t)
	if _, err := k.DB.Exec(`CREATE TRIGGER reject_branch_retry BEFORE INSERT ON records WHEN NEW.kind='product_branch_retry_budget' BEGIN SELECT RAISE(ABORT,'retry fixture rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, old.Digest); err == nil {
		t.Fatal("failed retry transaction committed")
	}
	budget, err := k.WaveOperationBudget(ctx, old.Value.Product.Value.Target.WaveID)
	if err != nil || budget.BranchRetries != 0 {
		t.Fatal("rolled back retry charged budget", budget, err)
	}
	if admitted, err := productBranchAdmitted(ctx, k.DB, productBranchRetryID(old.ID)); err != nil || admitted {
		t.Fatal("rollback retained launch authority", admitted, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER reject_branch_retry"); err != nil {
		t.Fatal(err)
	}
	next, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, old.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductBranchPublication(ctx, id, owner, next.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RecordProductBranchStopped(ctx, id, owner, next.Digest, branchStopFixture(k, next, k.clock(), false, false)); err != nil {
		t.Fatal(err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, next.Digest); !errors.Is(err, ErrOperationBudget) {
		t.Fatal("reopen reset retry budget", err)
	}
}

func TestProductBranchUnsentRetryLostEvidenceNeverFallsBack(t *testing.T) {
	for _, kind := range []string{"product_branch_publication", "product_branch_history", "product_branch_retry_budget", "ledger"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, id, owner, remote, old := stoppedUnsentBranchFixture(t)
			next, err := k.ReserveProductBranchUnsentRetry(ctx, id, owner, old.Digest)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "ledger" {
				_, err = k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE operation_key=? AND kind='branch_retry'", next.ID)
			} else {
				_, err = k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, next.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.WaveOperationBudget(ctx, next.Value.Product.Value.Target.WaveID); !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("lost evidence freed Wave budget", err)
			}
			if _, err := k.ProductBranchPublication(ctx, id); !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("lost successor fell back", err)
			}
			if _, err := k.ReserveProductBranchPublication(ctx, id, owner, remote); !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("lost successor regenerated", err)
			}
			r, err := k.OperationReservation(ctx, next.ID)
			if err != nil || r.State != "held" {
				t.Fatal("lost evidence released owner", r, err)
			}
		})
	}
}
