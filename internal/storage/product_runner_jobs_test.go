package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productRunnerFixture(t *testing.T) (*KernelStore, domain.Wave, ProductVerificationPlanSnapshot, ProductRunnerRuntime) {
	t.Helper()
	ctx := context.Background()
	k, w, task, run, _ := sealedVerificationFixture(t)
	receipt, err := k.AdoptProductVerificationTaskResult(ctx, task.SessionID, run.ID, "master")
	if err != nil {
		t.Fatal(err)
	}
	source, err := k.ReadProductTaskCandidate(ctx, receipt.ResultID)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := k.ReadProductVerificationPlan(ctx, source.Value.VerificationPlan.ID)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, plan, ProductRunnerRuntime{Root: filepath.Join(t.TempDir(), "runner"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}, ForbiddenPaths: []string{"/private/controller"}}
}

func TestProductRunnerIntentPinsFinalPlanPolicyAndClaimsOnce(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime := productRunnerFixture(t)
	job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
	if err != nil || job.State != "prepared" || job.Value.Spec.Candidate != plan.Value.Candidate || job.Value.Spec.Candidate == plan.Value.InputCandidate || job.Value.PlanDigest != plan.Digest || job.Value.PolicyDigest != plan.Value.PolicyDigest || job.Value.SessionID != w.OrchestratorSessionID {
		t.Fatal(job, err)
	}
	if _, err := os.Stat(runtime.Root); !os.IsNotExist(err) {
		t.Fatal("admission started filesystem work", err)
	}
	budget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	again, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
	if err != nil || !reflect.DeepEqual(again, job) {
		t.Fatal(again, err)
	}
	observed, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(budget, observed) {
		t.Fatal("replay consumed another job", observed, err)
	}
	if claim, err := k.ClaimProductRunnerJob(ctx, job.ID, "wrong"); err == nil || claim.Start {
		t.Fatal("wrong digest launched", claim, err)
	}
	claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest)
	if err != nil || !claim.Start || claim.Job.State != "launching" || claim.Job.ClaimedAt.IsZero() {
		t.Fatal(claim, err)
	}
	claim, err = k.ClaimProductRunnerJob(ctx, job.ID, job.Digest)
	if err != nil || claim.Start {
		t.Fatal("unknown launch automatically retried", claim, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil || claim.Start {
		t.Fatal("restart automatically retried unknown launch", claim, err)
	}
	r, err := k.OperationReservation(ctx, job.ID)
	if err != nil || r.State != "held" || r.Kind != "runner" || r.Request.Worktree != filepath.Join(runtime.Root, job.ID) {
		t.Fatal("unknown launch released capacity", r, err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil || p.Value.WorkItem.Phase != domain.Verifying {
		t.Fatal("admission invented validation success", p, err)
	}
}

func TestProductRunnerIntentRejectsChangedRuntimeAndUnplannedProfile(t *testing.T) {
	ctx := context.Background()
	k, _, plan, runtime := productRunnerFixture(t)
	if _, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unregistered", runtime); err == nil {
		t.Fatal("unknown profile accepted")
	}
	job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	runtime.Launcher[0] = "/bin/false"
	if _, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime); !errors.Is(err, ErrProductRunnerHeld) {
		t.Fatal("runtime replay changed launcher", err)
	}
	saved, err := k.ReadProductRunnerJob(ctx, job.ID)
	if err != nil || saved.Value.Spec.Launcher[0] != "/bin/true" {
		t.Fatal("caller mutated persisted spec", saved, err)
	}
}

func TestProductRunnerClaimRechecksWorkItemAndCanonicalPaths(t *testing.T) {
	for _, mode := range []string{"paused", "candidate", "root_alias", "released", "wave_paused", "deadline", "scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, runtime := productRunnerFixture(t)
			job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "wave_paused", "deadline", "scope":
				if mode == "wave_paused" {
					w.Paused = true
				} else if mode == "deadline" {
					w.Deadline = w.CreatedAt
				} else {
					w.OriginalRequest += " changed requirement"
				}
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := saveWave(ctx, tx, w); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "paused", "candidate":
				p, err := k.ReadProductTarget(ctx, w.Target(), "work")
				if err != nil {
					t.Fatal(err)
				}
				if mode == "paused" {
					p.Value.WorkItem.Paused = true
				} else {
					p.Value.Candidate.ID = "replacement"
					p.Value.Candidate.SHA = strings.Repeat("e", 40)
					p.Value.WorkItem.Candidate = "replacement"
				}
				if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
					t.Fatal(err)
				}
			case "root_alias":
				if err := os.Symlink(t.TempDir(), runtime.Root); err != nil {
					t.Fatal(err)
				}
			case "released":
				if err := k.ReleaseOperationCapacity(ctx, OperationReleaseProof{OperationID: job.ID, ProofID: "not-launched", Disposition: "not_started", Reason: "trusted owner did not start"}); err != nil {
					t.Fatal(err)
				}
			}
			if claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err == nil || claim.Start {
				t.Fatal("stale/aliased/released job launched", claim, err)
			}
		})
	}
}

func TestProductRunnerIntentRollbackAndLostEvidenceFailClosed(t *testing.T) {
	for _, mode := range []string{"rollback", "head", "history", "policy", "plan"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, runtime := productRunnerFixture(t)
			before, err := k.WaveOperationBudget(ctx, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_runner_job BEFORE INSERT ON records WHEN NEW.kind='product_runner_job' BEGIN SELECT RAISE(FAIL,'runner rollback'); END"); err != nil {
					t.Fatal(err)
				}
				if _, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime); err == nil || !strings.Contains(err.Error(), "runner rollback") {
					t.Fatal(err)
				}
				after, err := k.WaveOperationBudget(ctx, w.ID)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("partial budget reservation", after, err)
				}
				var count int
				if err := k.DB.QueryRow("SELECT count(*) FROM kernel_dispatch_intents WHERE run_id=?", productRunnerJobID(plan.ID, "unit")).Scan(&count); err != nil || count != 0 {
					t.Fatal("partial runner reservation", count, err)
				}
				return
			}
			job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
			if err != nil {
				t.Fatal(err)
			}
			kind := map[string]string{"head": "product_runner_job", "history": "product_runner_job_history", "policy": "product_verification_policy_history", "plan": "product_verification_plan_history"}[mode]
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=?", kind); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductRunnerJob(ctx, job.ID); err == nil {
				t.Fatal("lost proof accepted")
			}
			if claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err == nil || claim.Start {
				t.Fatal("lost proof launched", claim, err)
			}
			if _, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime); err == nil {
				t.Fatal("lost proof re-created intent")
			}
			r, err := k.OperationReservation(ctx, job.ID)
			if err != nil || r.State != "held" {
				t.Fatal("lost proof released capacity", r, err)
			}
		})
	}
}
