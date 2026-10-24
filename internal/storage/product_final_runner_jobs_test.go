package storage

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func finalRunnerFixture(t *testing.T) (*KernelStore, ProductFinalPlan, []ProductFinalHeadObservation, ProductRunnerRuntime) {
	t.Helper()
	k, job, coverage, heads := finalPlanFixture(t)
	plan, err := k.PrepareProductFinalPlan(context.Background(), job.Value.Product.Value.Target, coverage.Digest, heads)
	if err != nil {
		t.Fatal(err)
	}
	return k, plan, heads, ProductRunnerRuntime{Root: filepath.Join(k.Root, "final-runner"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}
}
func TestProductFinalRunnerUsesSharedCapacityAndClaimsOnce(t *testing.T) {
	ctx := context.Background()
	k, plan, _, runtime := finalRunnerFixture(t)
	before, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	job, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime)
	if err != nil || job.Value.Final == nil || job.Value.Spec.Candidate != plan.Value.Repositories[0].Candidate {
		t.Fatal(job, err)
	}
	budget, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
	if err != nil || budget.ValidationJobs != before.ValidationJobs+1 {
		t.Fatal("final verification did not share budget", before, budget, err)
	}
	replay, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime)
	if err != nil || !reflect.DeepEqual(replay, job) {
		t.Fatal(replay, err)
	}
	again, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
	if err != nil || !reflect.DeepEqual(again, budget) {
		t.Fatal("replay charged another job", again, err)
	}
	claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest)
	if err != nil || !claim.Start || claim.Job.FinalClaim == nil {
		t.Fatal(claim, err)
	}
	if claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil || claim.Start {
		t.Fatal("unknown launch relaunched", claim, err)
	}
	r, err := k.OperationReservation(ctx, job.ID)
	if err != nil || r.State != "held" {
		t.Fatal("missing result released capacity", r, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-final", Target: plan.Value.Target, Action: "cancel", Reason: "save only historical execution fact"}); err != nil {
		t.Fatal(err)
	}
	completion := productRunnerCompletionFixture(claim.Job)
	done, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, completion)
	if err != nil || done.State != "completed" {
		t.Fatal(done, err)
	}
	r, err = k.OperationReservation(ctx, job.ID)
	if err != nil || r.State != "released" || r.Proof == nil {
		t.Fatal(r, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	read, err := k.ReadProductRunnerJob(ctx, job.ID)
	if err != nil || !reflect.DeepEqual(read, done) {
		t.Fatal("historical final job lost", read, err)
	}
	if claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil || claim.Start {
		t.Fatal("completed final job relaunched", claim, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='wave_completion'").Scan(&count); err != nil || count != 0 {
		t.Fatal("final job completed parent", count, err)
	}
}
func TestProductFinalRunnerRequiresFreshCurrentSelectionAndIntactEvidence(t *testing.T) {
	for _, mode := range []string{"stale", "refresh", "head_change", "coverage_change", "pause", "missing_selection", "missing_history", "rollback", "wrong_profile", "wrong_repository", "wrong_digest"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, plan, heads, runtime := finalRunnerFixture(t)
			if mode == "wrong_profile" || mode == "wrong_repository" {
				repo, profile := int64(17), "unit"
				if mode == "wrong_profile" {
					profile = "unused"
				} else {
					repo++
				}
				if _, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, repo, profile, runtime); err == nil {
					t.Fatal("unplanned input admitted")
				}
				return
			}
			if mode == "rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_final_runner BEFORE INSERT ON records WHEN NEW.kind='product_runner_job' BEGIN SELECT RAISE(FAIL,'final runner rollback'); END"); err != nil {
					t.Fatal(err)
				}
				before, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime); err == nil {
					t.Fatal("rollback ignored")
				}
				after, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("partial budget", after, err)
				}
				return
			}
			job, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime)
			if err != nil {
				t.Fatal(err)
			}
			digest := job.Digest
			switch mode {
			case "stale", "refresh":
				now := k.clock().Add(31 * time.Second)
				k.Now = func() time.Time { return now }
				if mode == "refresh" {
					heads[0].ObservedAt = now
					refreshed, err := k.PrepareProductFinalPlan(ctx, plan.Value.Target, plan.Value.CoverageDigest, heads)
					if err != nil || refreshed.ID != plan.ID || !refreshed.CreatedAt.Equal(plan.CreatedAt) {
						t.Fatal("refresh rewrote plan", refreshed, err)
					}
				}
			case "head_change":
				heads[0].HeadSHA = strings.Repeat("b", 40)
				if _, err := k.PrepareProductFinalPlan(ctx, plan.Value.Target, plan.Value.CoverageDigest, heads); err != nil {
					t.Fatal(err)
				}
			case "coverage_change":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merged_work_item'"); err != nil {
					t.Fatal(err)
				}
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-final", Target: plan.Value.Target, Action: "pause", Reason: "hold"}); err != nil {
					t.Fatal(err)
				}
			case "missing_selection":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_final_plan_current'"); err != nil {
					t.Fatal(err)
				}
			case "missing_history":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_final_plan_selection_history'"); err != nil {
					t.Fatal(err)
				}
			case "wrong_digest":
				digest = "foreign"
			}
			claim, err := k.ClaimProductRunnerJob(ctx, job.ID, digest)
			if mode == "refresh" {
				if err != nil || !claim.Start || claim.Job.FinalClaim == nil || !claim.Job.FinalClaim.Selection.Heads[0].ObservedAt.Equal(heads[0].ObservedAt) {
					t.Fatal("fresh selection did not revive prepared job", claim, err)
				}
			} else if err == nil || claim.Start {
				t.Fatal("stale/invalid claim launched", claim, err)
			}
		})
	}
}
func TestProductFinalRunnerMissingHeadCannotRereserve(t *testing.T) {
	ctx := context.Background()
	k, plan, _, runtime := finalRunnerFixture(t)
	job, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_runner_job','product_runner_job_history','product_runner_job_terminal') AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("lost job regenerated", err)
	}
}

func TestProductFinalRunnerClaimProofLossAndRollbackRetainCapacity(t *testing.T) {
	for _, mode := range []string{"claim_history", "claim_rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, plan, _, runtime := finalRunnerFixture(t)
			job, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "claim_rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_final_claim BEFORE INSERT ON records WHEN NEW.kind='product_final_runner_claim' BEGIN SELECT RAISE(FAIL,'final claim rollback'); END"); err != nil {
					t.Fatal(err)
				}
				if claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err == nil || claim.Start {
					t.Fatal("claim rollback granted launch", claim, err)
				}
				read, err := k.ReadProductRunnerJob(ctx, job.ID)
				if err != nil || read.State != "prepared" || read.FinalClaim != nil {
					t.Fatal("partial claim", read, err)
				}
			} else {
				claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest)
				if err != nil || !claim.Start {
					t.Fatal(claim, err)
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_final_runner_claim' AND id=?", job.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := k.ReadProductRunnerJob(ctx, job.ID); err == nil {
					t.Fatal("lost final claim still authenticated")
				}
				if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, productRunnerCompletionFixture(claim.Job)); err == nil {
					t.Fatal("lost final claim released capacity")
				}
			}
			r, err := k.OperationReservation(ctx, job.ID)
			if err != nil || r.State != "held" {
				t.Fatal("invalid claim released capacity", r, err)
			}
		})
	}
}
