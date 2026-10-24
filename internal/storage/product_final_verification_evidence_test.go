package storage

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func completeFinalEvidenceFixture(t *testing.T, status string) (*KernelStore, ProductFinalPlan, ProductRunnerJob) {
	t.Helper()
	ctx := context.Background()
	k, plan, _, runtime := finalRunnerFixture(t)
	job, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest)
	if err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	c := productRunnerCompletionFixture(claim.Job)
	if status != "passed" {
		c.Result.Status = status
		if status == "failed" {
			code := 1
			c.Result.ExitCode = &code
		}
	}
	job, err = k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c)
	if err != nil {
		t.Fatal(err)
	}
	return k, plan, job
}
func TestProductFinalEvidenceRequiresFinalExecutionAndKeepsImmutableParentInput(t *testing.T) {
	ctx := context.Background()
	k, plan, _, runtime := finalRunnerFixture(t)
	target := plan.Value.Target
	before, err := k.ReadProductFinalVerificationEvidence(ctx, target)
	if err != nil || !before.Value.Current || before.Value.Complete || before.Value.Passed || before.Value.State != "pending" || before.Value.Repositories[0].Jobs[0].State != "not_run" {
		t.Fatal("premerge quality became final execution", before, err)
	}
	oldInput, err := k.BuildSessionInput(ctx, "main", []contract.Target{target})
	if err != nil {
		t.Fatal(err)
	}
	oldRef := inputContext(t, oldInput).Waves[0].Manifest
	var old savedInputManifest
	if err := k.Record(ctx, "session_input_manifest", oldRef.ID, &old); err != nil {
		t.Fatal(err)
	}
	oldBytes := append([]byte{}, old.Data...)
	job, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"prepared", "launching"} {
		if state == "launching" {
			if _, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil {
				t.Fatal(err)
			}
		}
		evidence, err := k.ReadProductFinalVerificationEvidence(ctx, target)
		if err != nil || evidence.Value.Complete || evidence.Value.Passed || evidence.Value.Repositories[0].Jobs[0].State != state {
			t.Fatal("unfinished final runner became pass", evidence, err)
		}
	}
	job, err = k.ReadProductRunnerJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	c := productRunnerCompletionFixture(job)
	if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, c); err != nil {
		t.Fatal(err)
	}
	passed, err := k.ReadProductFinalVerificationEvidence(ctx, target)
	if err != nil || !passed.Value.Current || !passed.Value.Complete || !passed.Value.Passed || passed.Value.State != "passed" {
		t.Fatal(passed, err)
	}
	summary := passed.Value.Repositories[0].Jobs[0]
	if summary.TestedSHA != plan.Value.Repositories[0].Candidate.SHA || summary.ResultDigest != c.ResultDigest || summary.LogDigest != c.LogDigest || summary.CompletionDigest == "" || summary.JobDigest != job.Digest || passed.Digest == before.Digest {
		t.Fatal("lost exact execution identity", passed)
	}
	raw, err := json.Marshal(passed)
	if err != nil || strings.Contains(string(raw), k.Root) || strings.Contains(string(raw), "private-profile-argument") || strings.Contains(string(raw), "trusted-runner") || strings.Contains(string(raw), "output.log") {
		t.Fatal("private runner execution data leaked", string(raw), err)
	}
	manifest := registrationManifest(t, k, "main", target)
	if manifest.ProductFinalVerification == nil || !reflect.DeepEqual(*manifest.ProductFinalVerification, passed) {
		t.Fatal("parent lacks exact final evidence", manifest.ProductFinalVerification)
	}
	if strings.Contains(string(oldInput.Input), summary.TestedSHA) {
		t.Fatal("final SHA expanded into inline wake")
	}
	if err := k.Record(ctx, "session_input_manifest", oldRef.ID, &old); err != nil || string(old.Data) != string(oldBytes) {
		t.Fatal("old input changed", err)
	}
	var snapshot sessionManifest
	if err := json.Unmarshal(old.Data, &snapshot); err != nil || snapshot.ProductFinalVerification == nil || snapshot.ProductFinalVerification.Digest != before.Digest {
		t.Fatal("old input relabeled", err)
	}
	tasks, err := k.Tasks(ctx, target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := k.SessionBinding(ctx, tasks[0].SessionID)
	if err != nil {
		t.Fatal(err)
	}
	wave, err := k.Wave(ctx, target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	taskManifest, err := k.sessionManifestTx(ctx, tx, binding, wave)
	tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	if taskManifest.ProductFinalVerification != nil {
		t.Fatal("Task received parent final scope")
	}
	w, err := k.Wave(ctx, target.WaveID)
	if err != nil || w.State == domain.WaveCompleted {
		t.Fatal("final evidence completed parent", w, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	reopened, err := k.ReadProductFinalVerificationEvidence(ctx, target)
	if err != nil || !reflect.DeepEqual(reopened, passed) {
		t.Fatal("reopen changed final execution facts", reopened, err)
	}
}
func TestProductFinalEvidenceRetainsFailedUnknownAndMissingProof(t *testing.T) {
	for _, mode := range []string{"failed", "canceled", "infrastructure_failure", "runner_head", "runner_terminal", "claim_history", "selection", "selection_history", "plan_head", "receipt", "plan_indexes"} {
		t.Run(mode, func(t *testing.T) {
			status := "passed"
			if mode == "failed" || mode == "canceled" || mode == "infrastructure_failure" {
				status = mode
			}
			ctx := context.Background()
			k, plan, job := completeFinalEvidenceFixture(t, status)
			if status == "passed" {
				kind, id := map[string]string{"runner_head": "product_runner_job", "runner_terminal": "product_runner_job_terminal", "claim_history": "product_final_runner_claim", "selection": "product_final_plan_current", "selection_history": "product_final_plan_selection_history", "plan_head": "product_final_plan", "receipt": "product_merged_work_item"}[mode], job.ID
				switch mode {
				case "selection":
					id = plan.Value.Target.WaveID
				case "plan_head":
					id = plan.ID
				case "receipt":
					for mergeID := range plan.Value.Repositories[0].MergeReceipts {
						id = mergeID
					}
				}
				if mode == "plan_indexes" {
					if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_final_plan%'"); err != nil {
						t.Fatal(err)
					}
				} else if mode == "selection_history" {
					if _, err := k.DB.Exec("DELETE FROM records WHERE kind=?", kind); err != nil {
						t.Fatal(err)
					}
				} else if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, id); err != nil {
					t.Fatal(err)
				}
			}
			evidence, err := k.ReadProductFinalVerificationEvidence(ctx, plan.Value.Target)
			if err != nil || evidence.Value.Passed {
				t.Fatal("failed/missing evidence passed", evidence, err)
			}
			if status != "passed" {
				if !evidence.Value.Complete || evidence.Value.State != "failed" || evidence.Value.Repositories[0].Jobs[0].Status != status {
					t.Fatal("lost exact failure disposition", evidence)
				}
			} else if evidence.Value.Complete || evidence.Value.State != "unavailable" || len(evidence.Value.Unavailable) == 0 {
				t.Fatal("lost proof silently disappeared", evidence)
			}
			manifest := registrationManifest(t, k, "main", plan.Value.Target)
			if manifest.ProductFinalVerification == nil || manifest.ProductFinalVerification.Value.Passed {
				t.Fatal("parent input failed open", manifest.ProductFinalVerification)
			}
		})
	}
}
func TestProductFinalEvidenceUnknownPlanAndChangedCoverageCannotPass(t *testing.T) {
	ctx := context.Background()
	k, job := knownCoverageFixture(t)
	initial, err := k.ReadProductFinalVerificationEvidence(ctx, job.Value.Product.Value.Target)
	if err != nil || initial.Value.State != "not_planned" || initial.Value.Passed {
		t.Fatal(initial, err)
	}
	k2, plan, finished := completeFinalEvidenceFixture(t, "passed")
	target := plan.Value.Target
	p, err := k2.ReadProductTarget(ctx, target, job.Value.Product.Value.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	next := p.Value
	next.WorkItem.Version++
	next.Candidate.ID = "new-after-final"
	next.WorkItem.Candidate = next.Candidate.ID
	if _, err := k2.SetProductTarget(ctx, p.Digest, next); err != nil {
		t.Fatal(err)
	}
	stale, err := k2.ReadProductFinalVerificationEvidence(ctx, target)
	if err != nil || stale.Value.Current || stale.Value.Passed || !stale.Value.Complete || stale.Value.State != "stale" || stale.Value.Repositories[0].Jobs[0].JobID != finished.ID {
		t.Fatal("old final pass covered a new local candidate", stale, err)
	}
	wrong := target
	wrong.PlanRevision++
	if _, err := k2.ReadProductFinalVerificationEvidence(ctx, wrong); err == nil {
		t.Fatal("caller selected another target")
	}
}

func TestProductFinalEvidenceIsAbsentFromGenericParent(t *testing.T) {
	k, w, b, _, _ := waveCompletionFixture(t)
	tx, err := k.kernelTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	manifest, err := k.sessionManifestTx(context.Background(), tx, b, w)
	if err != nil || manifest.ProductFinalVerification != nil || manifest.UnavailableProductFinalVerification != "" {
		t.Fatal("generic parent received product final scope", manifest.ProductFinalVerification, err)
	}
}
