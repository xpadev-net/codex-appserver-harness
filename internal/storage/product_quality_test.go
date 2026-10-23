package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productQualityFixture(t *testing.T, configure ...func(*contract.PlanProposal)) (*KernelStore, domain.Wave, ProductVerificationPlanSnapshot, ProductRunnerRuntime, domain.Task, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	k, w, task, run, _, _ := verificationSealFixtureWithPlan(t, func(plan *contract.PlanProposal) {
		verifier := plan.Phases[1].Tasks[0]
		plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: "quality", Tasks: []contract.TaskSpec{{ID: "quality-review", Kind: "review", Instruction: "independent code and test review", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: verifier.ID}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{"work"}}}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"quality-review"}}, FailurePolicy: "drain_and_block"})
		for _, apply := range configure {
			apply(plan)
		}
	}, nil)
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
	tasks, err := k.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	var review domain.Task
	for _, item := range tasks {
		if item.Spec.ID == "quality-review" {
			review = item
		}
	}
	review, reviewRun := seedProductQualityReview(t, k, w, plan, review)
	runtime := ProductRunnerRuntime{Root: k.Root + "/runner", LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}
	return k, w, plan, runtime, review, reviewRun
}

func TestProductQualityReusesOnlyExplicitlyCarriedExecutionEvidence(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime, review, run := productQualityFixture(t)
	if _, err := k.ApplyTaskResult(ctx, review.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
	before, err := k.CheckProductQuality(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	budget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := k.DB.QueryRow("SELECT payload FROM kernel_plans WHERE wave_id=? AND revision=?", w.ID, w.PlanRevision).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var admission PlanAdmission
	if err := json.Unmarshal(raw, &admission); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AdoptPlan(ctx, w.Target(), "explicit-quality-carry", admission.Plan); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.CheckProductQuality(ctx, w.Target(), "work"); err == nil {
		t.Fatal("plan adoption silently updated PR target")
	}
	product, err := k.ReadProductTarget(ctx, before.Value.Target, "work")
	if err != nil {
		t.Fatal(err)
	}
	product.Value.Target = w.Target()
	if _, err := k.SetProductTarget(ctx, product.Digest, product.Value); err != nil {
		t.Fatal(err)
	}
	after, err := k.CheckProductQuality(ctx, w.Target(), "work")
	if err != nil || after.Value.Target != w.Target() || after.Value.Candidate != before.Value.Candidate || len(after.Value.Verifications) != 1 || len(after.Value.Reviews) != 1 {
		t.Fatal(after, err)
	}
	observed, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(budget, observed) {
		t.Fatal("carry reran or recharged runner", observed, err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_plan_bindings WHERE wave_id=? AND revision=? AND task_id=?", w.ID, w.PlanRevision, plan.Value.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductVerificationEvidence(ctx, plan.ID); err == nil {
		t.Fatal("omitted verifier treated as explicit carry")
	}
}

func TestProductQualityCannotReuseProducerThreadForIndependentReview(t *testing.T) {
	tctx := context.Background()
	k, w, plan, runtime, review, run := productQualityFixture(t)
	var producer contract.SessionRun
	if err := k.Record(tctx, "agent_run", plan.Value.RunID, &producer); err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(tctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(tctx, "UPDATE kernel_sessions SET thread_id=? WHERE id=?", producer.ThreadID, review.SessionID)
	tx.Rollback()
	if err == nil || !strings.Contains(err.Error(), "UNIQUE") {
		t.Fatal("producer thread reused by reviewer", err)
	}
	if _, err := k.ApplyTaskResult(tctx, review.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
	if _, err := k.CheckProductQuality(tctx, w.Target(), "work"); err != nil {
		t.Fatal("failed thread reuse corrupted independent review", err)
	}
}

func TestProductQualityRequiresEveryIndependentReviewerRole(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime, review, run := productQualityFixture(t, func(plan *contract.PlanProposal) {
		phase := &plan.Phases[len(plan.Phases)-1]
		second := phase.Tasks[0]
		second.ID = "second-quality-role"
		second.Instruction = "independent second reviewer role"
		phase.Tasks = append(phase.Tasks, second)
		phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, second.ID)
	})
	if _, err := k.ApplyTaskResult(ctx, review.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
	if _, err := k.CheckProductQuality(ctx, w.Target(), "work"); !errors.Is(err, ErrProductQualityHeld) {
		t.Fatal("missing required role accepted", err)
	}
	tasks, err := k.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	var second domain.Task
	for _, task := range tasks {
		if task.Spec.ID == "second-quality-role" {
			second = task
		}
	}
	second, secondRun := seedProductQualityReview(t, k, w, plan, second)
	if _, err := k.ApplyTaskResult(ctx, second.SessionID, secondRun.ID); err != nil {
		t.Fatal(err)
	}
	quality, err := k.CheckProductQuality(ctx, w.Target(), "work")
	if err != nil || len(quality.Value.Reviews) != 2 || len(quality.Value.Criteria["AC"]) != 2 {
		t.Fatal(quality, err)
	}
}

func TestProductQualityRejectsStaleCandidateOrLostEvidence(t *testing.T) {
	for _, mode := range []string{"candidate", "review_history", "runner_terminal", "report_bytes"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, runtime, review, run := productQualityFixture(t)
			receipt, err := k.ApplyTaskResult(ctx, review.SessionID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
			if _, err := k.CheckProductQuality(ctx, w.Target(), "work"); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "candidate":
				p, err := k.ReadProductTarget(ctx, w.Target(), "work")
				if err != nil {
					t.Fatal(err)
				}
				p.Value.Candidate.ID = "replacement"
				p.Value.Candidate.SHA = strings.Repeat("e", 40)
				p.Value.WorkItem.Candidate = p.Value.Candidate.ID
				if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
					t.Fatal(err)
				}
			case "review_history":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_task_review_history' AND id=?", receipt.ResultID); err != nil {
					t.Fatal(err)
				}
			case "runner_terminal":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_runner_job_terminal'"); err != nil {
					t.Fatal(err)
				}
			case "report_bytes":
				var result contract.TaskResult
				if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(k.Root, "objects", result.Artifacts[0]), []byte("changed report bytes"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.CheckProductQuality(ctx, w.Target(), "work"); err == nil {
				t.Fatal("stale/lost evidence authorized quality", mode)
			}
		})
	}
}

func TestProductQualityPreservesConfirmedFindingsAfterReviewerRetirement(t *testing.T) {
	for _, mode := range []string{"retired", "missing_head", "missing_receipts", "missing_raw"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, _, review, run := productQualityFixture(t)
			var result contract.TaskResult
			if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
				t.Fatal(err)
			}
			result.Review.Verdict = "findings"
			result.Review.Findings = []contract.ReviewFinding{{ID: "defect", Classification: "implementation_defect", TargetDigest: result.Review.TargetDigest, Basis: "original AC", Description: "confirmed current Candidate defect"}}
			raw, err := marshalTaskResultFixture(result)
			if err != nil {
				t.Fatal(err)
			}
			run.Final = string(raw)
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := kernelRecordTx(ctx, tx, "agent_run", run.ID, review.SessionID, run); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			receipt, err := k.ApplyTaskResult(ctx, review.SessionID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			review, err = k.Task(ctx, review.ID)
			if err != nil {
				t.Fatal(err)
			}
			tx, err = k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			review.State = contract.TaskCanceled
			if err := saveTask(ctx, tx, review); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing_head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_task_review' AND id=?", receipt.ResultID)
			case "missing_receipts":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind IN ('product_task_review','product_task_review_history') AND id=?", receipt.ResultID)
			case "missing_raw":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='task_result' AND id=?", receipt.ResultID)
			}
			if err != nil {
				t.Fatal(err)
			}
			tx, err = k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			findingErr := k.productCandidateFindingsTx(ctx, tx, w, plan.Value.Candidate)
			tx.Rollback()
			if findingErr == nil {
				t.Fatal("retirement/evidence loss erased findings", mode)
			}
			if mode == "retired" && !errors.Is(findingErr, ErrProductQualityHeld) {
				t.Fatal(findingErr)
			}
		})
	}
}

func TestProductQualityRequiresMachineVerificationAndIndependentClaimedReview(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime, review, run := productQualityFixture(t)
	before, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.CheckProductQuality(ctx, w.Target(), "work"); !errors.Is(err, ErrProductQualityHeld) {
		t.Fatal("unfinished review accepted", err)
	}
	if _, err := k.ApplyTaskResult(ctx, review.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.CheckProductQuality(ctx, w.Target(), "work"); !errors.Is(err, ErrProductQualityHeld) {
		t.Fatal("clean review replaced machine verification", err)
	}
	completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
	quality, err := k.CheckProductQuality(ctx, w.Target(), "work")
	if err != nil || quality.Value.Candidate != plan.Value.Candidate || len(quality.Value.Verifications) != 1 || len(quality.Value.Reviews) != 1 || len(quality.Value.Criteria["AC"]) != 1 || quality.Digest == "" {
		t.Fatal(quality, err)
	}
	again, err := k.CheckProductQuality(ctx, w.Target(), "work")
	if err != nil || !reflect.DeepEqual(again, quality) {
		t.Fatal(again, err)
	}
	after, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil || !reflect.DeepEqual(before, after) || after.Value.WorkItem.Phase != domain.Verifying {
		t.Fatal("read gate published or approved", after, err)
	}
}

func TestProductQualityRejectsUnverifiedCriterionAndUncleanReport(t *testing.T) {
	for _, mode := range []string{"unverified", "findings", "inconclusive", "runner_failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, runtime, review, run := productQualityFixture(t)
			var result contract.TaskResult
			if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "unverified":
				result.CriterionResults[0].Status = "unverified"
			case "findings":
				result.Review.Verdict = "findings"
				result.Review.Findings = []contract.ReviewFinding{{ID: "defect", Classification: "implementation_defect", TargetDigest: result.Review.TargetDigest, Basis: "original AC", Description: "confirmed defect"}}
			case "inconclusive":
				result.Review.Verdict = "inconclusive"
			}
			raw, err := marshalTaskResultFixture(result)
			if err != nil {
				t.Fatal(err)
			}
			run.Final = string(raw)
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := kernelRecordTx(ctx, tx, "agent_run", run.ID, review.SessionID, run); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ApplyTaskResult(ctx, review.SessionID, run.ID); err != nil {
				t.Fatal(err)
			}
			status := "passed"
			if mode == "runner_failure" {
				status = "failed"
			}
			completeProductRunnerFixture(t, k, plan, runtime, "unit", status)
			if _, err := k.CheckProductQuality(ctx, w.Target(), "work"); !errors.Is(err, ErrProductQualityHeld) {
				t.Fatal("quality bypass", mode, err)
			}
		})
	}
}

func seedProductQualityReview(t *testing.T, k *KernelStore, w domain.Wave, plan ProductVerificationPlanSnapshot, review domain.Task) (domain.Task, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	subject, err := k.BindProductCandidateSubject(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := k.ProductSeal(ctx, plan.Value.TaskID, plan.Value.RunID)
	if err != nil {
		t.Fatal(err)
	}
	request := producer.Value.Workspace.Value.Request
	request.TaskID, request.BaseSHA = review.ID, plan.Value.Candidate.SHA
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, review.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	ready, err := k.FinishProductWorkspace(ctx, review.ID, workspaceProofFixture(t, k, saved))
	if err != nil {
		t.Fatal(err)
	}
	review, err = k.Task(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	report := mustImport(t, k, ArtifactImport{SessionID: review.SessionID, Target: review.Target(), Key: "quality-review", Kind: "review_report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: review.ID}}, "independent criterion AC review")
	_, _, review, reviewRun := seedProductSealRun(t, k, w, review, ready, func(result *contract.TaskResult) {
		result.SchemaVersion = 1
		result.ProductChanges = nil
		result.Artifacts = []string{report.ID}
		result.CriterionResults = []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "original AC assessed against exact code/tests", EvidenceIDs: []string{report.ID}}}
		result.Review = &contract.ReviewResult{TargetDigest: subject.Value.CandidateDigest, Verdict: "clean", Findings: []contract.ReviewFinding{}}
	})
	return review, reviewRun
}
