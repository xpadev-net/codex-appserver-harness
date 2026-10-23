package storage

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func productVerificationEvidenceFixture(t *testing.T) (*KernelStore, domain.Wave, ProductVerificationPlanSnapshot, ProductRunnerRuntime) {
	t.Helper()
	ctx := context.Background()
	k, w, task, run, _, _ := verificationSealFixtureWithPolicy(t, func(p *ProductVerificationPolicy) {
		p.Verification.RequiredProfiles = []string{"unit", "integration"}
		for _, id := range []string{"integration", "requested", "mapped", "unused"} {
			p.Verification.Profiles[id] = p.Verification.Profiles["unit"]
		}
	}, func(plan *verification.Plan) {
		plan.CriteriaTests[0].ProfileID = "mapped"
		plan.RequestedProfiles = []string{"requested"}
	})
	seal, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductSeal(ctx, task.ID, run.ID, seal.Digest); err != nil {
		t.Fatal(err)
	}
	proof := sealProofFixture(seal)
	proof.Frozen.SHA = strings.Repeat("d", 40)
	checked := seal.Value.Verification.CheckProof()
	proof.Verification = &checked
	if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof); err != nil {
		t.Fatal(err)
	}
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
	return k, w, plan, ProductRunnerRuntime{Root: filepath.Join(t.TempDir(), "runner"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}
}

func completeProductRunnerFixture(t *testing.T, k *KernelStore, plan ProductVerificationPlanSnapshot, runtime ProductRunnerRuntime, profileID, status string) ProductRunnerJob {
	t.Helper()
	ctx := context.Background()
	job, err := k.ReserveProductRunnerJob(ctx, plan.ID, profileID, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil {
		t.Fatal(err)
	}
	c := productRunnerCompletionFixture(job)
	c.Result.Status = status
	if status == "failed" {
		code := 7
		c.Result.ExitCode = &code
		c.Result.Reason = "fixture failure"
	}
	job, err = k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func TestProductVerificationEvidenceRequiresEveryPolicyAndMappingProfile(t *testing.T) {
	ctx := context.Background()
	k, _, plan, runtime := productVerificationEvidenceFixture(t)
	empty, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || empty.Value.Complete || empty.Value.Passed || !reflect.DeepEqual(empty.Value.Profiles, []string{"unit", "integration", "requested", "mapped"}) || len(empty.Value.Jobs) != 4 {
		t.Fatal(empty, err)
	}
	for _, job := range empty.Value.Jobs {
		if job.State != "not_run" || job.Completion != nil {
			t.Fatal(job)
		}
	}
	job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || prepared.Value.Passed || prepared.Value.Jobs[0].State != "prepared" {
		t.Fatal(prepared, err)
	}
	if _, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil {
		t.Fatal(err)
	}
	pending, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || pending.Value.Passed || pending.Value.Complete || pending.Value.Jobs[0].State != "launching" {
		t.Fatal(pending, err)
	}
	if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, productRunnerCompletionFixture(job)); err != nil {
		t.Fatal(err)
	}
	completeProductRunnerFixture(t, k, plan, runtime, "integration", "failed")
	partial, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || partial.Value.Complete || partial.Value.Passed {
		t.Fatal("missing mapping profile accepted", partial, err)
	}
	completeProductRunnerFixture(t, k, plan, runtime, "requested", "passed")
	mappingMissing, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || mappingMissing.Value.Complete || mappingMissing.Value.Passed {
		t.Fatal("mapping profile skipped", mappingMissing, err)
	}
	completeProductRunnerFixture(t, k, plan, runtime, "mapped", "passed")
	failed, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || !failed.Value.Complete || failed.Value.Passed || failed.Value.Jobs[1].Completion.Result.Status != "failed" {
		t.Fatal("test failure accepted", failed, err)
	}
}

func TestProductVerificationEvidencePassIsCurrentAndDoesNotChangeQuality(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime := productVerificationEvidenceFixture(t)
	for _, id := range []string{"unit", "integration", "requested", "mapped"} {
		completeProductRunnerFixture(t, k, plan, runtime, id, "passed")
	}
	passed, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || !passed.Value.Complete || !passed.Value.Passed || passed.Value.Candidate != plan.Value.Candidate || passed.Digest == "" {
		t.Fatal(passed, err)
	}
	again, err := k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || !reflect.DeepEqual(again, passed) {
		t.Fatal(again, err)
	}
	*passed.Value.Jobs[0].Completion.Result.ExitCode = 7
	again, err = k.ReadProductVerificationEvidence(ctx, plan.ID)
	if err != nil || !again.Value.Passed {
		t.Fatal("caller changed persisted facts", again, err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil || p.Value.WorkItem.Phase != domain.Verifying {
		t.Fatal("machine gate fabricated independent review", p, err)
	}
	p.Value.Candidate.ID = "new-active-candidate"
	p.Value.Candidate.SHA = strings.Repeat("e", 40)
	p.Value.WorkItem.Candidate = p.Value.Candidate.ID
	if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductVerificationEvidence(ctx, plan.ID); err == nil {
		t.Fatal("old success authorized replacement candidate")
	}
	if job, err := k.ReadProductRunnerJob(ctx, productRunnerJobID(plan.ID, "unit")); err != nil || job.State != "completed" {
		t.Fatal("lost historical execution", job, err)
	}
}

func TestProductVerificationEvidenceRejectsLostJobAndPlanProof(t *testing.T) {
	for _, kind := range []string{"product_runner_job", "product_runner_job_history", "product_runner_job_terminal", "product_verification_plan_history", "product_verification_policy_history"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, _, plan, runtime := productRunnerFixture(t)
			completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
			if passed, err := k.ReadProductVerificationEvidence(ctx, plan.ID); err != nil || !passed.Value.Passed {
				t.Fatal(passed, err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=?", kind); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductVerificationEvidence(ctx, plan.ID); err == nil {
				t.Fatal("lost evidence returned success", kind)
			}
		})
	}
}

func TestProductVerificationEvidenceRechecksPauseAndScope(t *testing.T) {
	for _, mode := range []string{"work_pause", "wave_pause", "scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, runtime := productRunnerFixture(t)
			completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
			if mode == "work_pause" {
				p, err := k.ReadProductTarget(ctx, w.Target(), "work")
				if err != nil {
					t.Fatal(err)
				}
				p.Value.WorkItem.Paused = true
				if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
					t.Fatal(err)
				}
			} else {
				if mode == "wave_pause" {
					w.Paused = true
				} else {
					w.OriginalRequest += " changed"
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
			}
			if _, err := k.ReadProductVerificationEvidence(ctx, plan.ID); err == nil {
				t.Fatal("stale/paused gate accepted", mode)
			}
		})
	}
}
