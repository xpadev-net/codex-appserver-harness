package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func productTemplateFixture(t *testing.T, classification string, configure ...func(*contract.PlanProposal)) (*KernelStore, domain.Wave, ProductVerificationPlanSnapshot, ProductRunnerRuntime, domain.Task) {
	t.Helper()
	return productTemplateRunnerFixture(t, classification, "passed", configure...)
}

func productTemplateRunnerFixture(t *testing.T, classification, runnerStatus string, configure ...func(*contract.PlanProposal)) (*KernelStore, domain.Wave, ProductVerificationPlanSnapshot, ProductRunnerRuntime, domain.Task) {
	t.Helper()
	ctx := context.Background()
	k, w, plan, runtime, review, run := productQualityFixture(t, func(p *contract.PlanProposal) {
		p.Templates = []contract.FixReviewTemplate{{ID: "product-repair", InputTaskID: "quality-review", Classifications: []string{"implementation_defect", "test_defect"}, FixInstruction: "repair the classified defect against original requirements", ReviewInstruction: "independently review repaired code and tests", MaxRounds: 2}}
		for _, apply := range configure {
			apply(p)
		}
	})
	var result contract.TaskResult
	if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
		t.Fatal(err)
	}
	result.Review.Verdict = "findings"
	result.Review.Findings = []contract.ReviewFinding{{ID: "defect", Classification: classification, TargetDigest: result.Review.TargetDigest, Basis: "original AC", Description: "observed product defect"}}
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
	review, err = k.Task(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	if runnerStatus != "pending" {
		completeProductRunnerFixture(t, k, plan, runtime, "unit", runnerStatus)
	}
	return k, w, plan, runtime, review
}

func TestProductTemplateCreatesBoundFreshRepairVerifierAndEveryReviewer(t *testing.T) {
	for _, classification := range []string{"implementation_defect", "test_defect"} {
		t.Run(classification, func(t *testing.T) {
			ctx := context.Background()
			k, w, _, _, review := productTemplateFixture(t, classification)
			budget, err := k.WaveOperationBudget(ctx, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			request := TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID}
			expansion, err := k.ExpandReviewTemplates(ctx, request)
			if err != nil || expansion.Product == nil || expansion.Admission.Target.PlanRevision != w.PlanRevision+1 || len(expansion.Product.Repairs) != 1 || len(expansion.Product.Replacements[review.ID]) != 1 {
				t.Fatal(expansion, err)
			}
			first, err := k.Task(ctx, structureID("task-", w.ID, expansion.Product.Repairs[0]))
			if err != nil || first.Attempts != 0 || first.SessionID == review.SessionID || !reflect.DeepEqual(first.Spec.WorkItemIDs, review.Spec.WorkItemIDs) {
				t.Fatal(first, err)
			}
			kind := "fix"
			if classification == "test_defect" {
				kind = "verification"
			}
			if first.Spec.Kind != kind {
				t.Fatal("wrong repair owner", first.Spec)
			}
			verifier, err := k.Task(ctx, structureID("task-", w.ID, expansion.Product.Verifier))
			if err != nil || verifier.Spec.Kind != "verification" {
				t.Fatal(verifier, err)
			}
			if classification == "implementation_defect" && verifier.ID == first.ID {
				t.Fatal("implementation self-verified")
			}
			latest, err := k.Wave(ctx, w.ID)
			if err != nil || latest.FixRounds != w.FixRounds+1 {
				t.Fatal(latest, err)
			}
			p, err := k.ReadProductTarget(ctx, latest.Target(), "work")
			if err != nil || p.Value.Candidate != expansion.Product.Candidate {
				t.Fatal("repair admission changed candidate", p, err)
			}
			again, err := k.ExpandReviewTemplates(ctx, request)
			if err != nil || !reflect.DeepEqual(again, expansion) {
				t.Fatal(again, err)
			}
			after, err := k.WaveOperationBudget(ctx, w.ID)
			if err != nil || !reflect.DeepEqual(budget, after) {
				t.Fatal("admission fabricated runner attempt", after, err)
			}
			set, err := k.ReadyTasks(ctx, ReadyRequest{Target: latest.Target()})
			if err != nil || set.Reason == "review_findings" {
				t.Fatal("saved product repair did not advance source phase", set, err)
			}
		})
	}
}

func TestProductTemplateRejectsUnknownAmbiguousOrExhaustedWithoutReplanning(t *testing.T) {
	for _, mode := range []string{"unclassified", "ambiguous", "limit"} {
		t.Run(mode, func(t *testing.T) {
			classification := "implementation_defect"
			if mode == "unclassified" {
				classification = "unclassified"
			}
			k, w, _, _, review := productTemplateFixture(t, classification, func(plan *contract.PlanProposal) {
				if mode == "ambiguous" {
					second := plan.Templates[0]
					second.ID = "ambiguous-owner"
					plan.Templates = append(plan.Templates, second)
				}
			})
			ctx := context.Background()
			if mode == "limit" {
				w.FixRounds = w.Policy.Limits.FixRounds
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
			if _, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID}); !errors.Is(err, ErrTemplateNeedsMain) {
				t.Fatal("unsupported repair admitted", mode, err)
			}
			latest, err := k.Wave(ctx, w.ID)
			if err != nil || latest.PlanRevision != w.PlanRevision || latest.FixRounds != w.FixRounds {
				t.Fatal("failed repair changed budget/plan", latest, err)
			}
		})
	}
}

func TestProductTemplateRollbackAndLostHistory(t *testing.T) {
	ctx := context.Background()
	k, w, _, _, review := productTemplateFixture(t, "implementation_defect")
	if _, err := k.DB.Exec("CREATE TRIGGER fail_product_repair BEFORE INSERT ON records WHEN NEW.kind='product_template_expansion_history' BEGIN SELECT RAISE(FAIL,'product repair rollback'); END"); err != nil {
		t.Fatal(err)
	}
	request := TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID}
	if _, err := k.ExpandReviewTemplates(ctx, request); err == nil || !strings.Contains(err.Error(), "product repair rollback") {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.PlanRevision != w.PlanRevision || current.FixRounds != w.FixRounds {
		t.Fatal("partial repair admission", current, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_product_repair"); err != nil {
		t.Fatal(err)
	}
	expansion, err := k.ExpandReviewTemplates(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_template_expansion_history' AND id=?", expansion.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ExpandReviewTemplates(ctx, request); err == nil {
		t.Fatal("replay ignored lost product repair history")
	}
}

func TestProductTemplateAutomaticProgressUsesNoMainOrWorkerRun(t *testing.T) {
	ctx := context.Background()
	k, w, _, _, _ := productTemplateFixture(t, "implementation_defect")
	var before int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='agent_run'").Scan(&before); err != nil {
		t.Fatal(err)
	}
	changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID)
	if err != nil || !changed {
		t.Fatal(changed, err)
	}
	latest, err := k.Wave(ctx, w.ID)
	if err != nil || latest.PlanRevision != w.PlanRevision+1 || latest.FixRounds != w.FixRounds+1 {
		t.Fatal(latest, err)
	}
	if _, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil {
		t.Fatal(err)
	}
	var after int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='agent_run'").Scan(&after); err != nil || before != after {
		t.Fatal("template planning started a model", before, after, err)
	}
}

func TestProductTemplateWaitsOnlyForPendingRunnerAndNotifiesTerminalExceptionsOnce(t *testing.T) {
	for _, status := range []string{"pending", "infrastructure_failure", "timeout", "canceled"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			k, w, _, _, review := productTemplateRunnerFixture(t, "implementation_defect", status)
			_, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID})
			if status == "pending" {
				if !errors.Is(err, ErrProductRunnerHeld) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrTemplateNeedsMain) || !strings.Contains(err.Error(), status) {
				t.Fatal("terminal runner exception silently waits", err)
			}
			changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID)
			if err != nil || changed != (status != "pending") {
				t.Fatal(changed, err)
			}
			if changed, err := k.ReconcileSessionTemplates(ctx, w.OrchestratorSessionID); err != nil || changed {
				t.Fatal("runner exception attention repeated", changed, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='parent_attention' AND json_extract(payload,'$.reason')='template_requires_main'").Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 1
			if status == "pending" {
				want = 0
			}
			if count != want {
				t.Fatal("wrong parent attention count", count, want)
			}
			latest, err := k.Wave(ctx, w.ID)
			if err != nil || latest.PlanRevision != w.PlanRevision || latest.FixRounds != w.FixRounds {
				t.Fatal("runner exception admitted repairs", latest, err)
			}
		})
	}
}

func TestProductTemplateRepeatLimitPersistsAcrossCandidateAndRoleChanges(t *testing.T) {
	ctx := context.Background()
	k, w, original, runtime, review := productTemplateFixture(t, "implementation_defect", func(plan *contract.PlanProposal) { plan.Templates[0].MaxRounds = 1 })
	expansion, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID})
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	fix, err := k.Task(ctx, structureID("task-", w.ID, expansion.Product.Repairs[0]))
	if err != nil {
		t.Fatal(err)
	}
	finishProductRepairWorkFixture(t, k, w, original, fix, strings.Repeat("e", 40))
	verifier, err := k.Task(ctx, structureID("task-", w.ID, expansion.Product.Verifier))
	if err != nil {
		t.Fatal(err)
	}
	final := finishProductRepairVerifierFixture(t, k, w, original, verifier)
	completeProductRunnerFixture(t, k, final, runtime, "unit", "passed")
	next, err := k.Task(ctx, expansion.Product.Replacements[review.ID][0])
	if err != nil {
		t.Fatal(err)
	}
	next, run := seedProductQualityReview(t, k, w, final, next)
	var result contract.TaskResult
	if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
		t.Fatal(err)
	}
	result.Review.Verdict = "findings"
	result.Review.Findings = []contract.ReviewFinding{{ID: "still-broken", Classification: "implementation_defect", TargetDigest: result.Review.TargetDigest, Basis: "AC", Description: "defect remains after replacement"}}
	raw, err := marshalTaskResultFixture(result)
	if err != nil {
		t.Fatal(err)
	}
	run.Final = string(raw)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := kernelRecordTx(ctx, tx, "agent_run", run.ID, next.SessionID, run); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyTaskResult(ctx, next.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: next.ID}); !errors.Is(err, ErrTemplateNeedsMain) {
		t.Fatal("new Candidate/role reset template limit", err)
	}
	latest, err := k.Wave(ctx, w.ID)
	if err != nil || latest.PlanRevision != w.PlanRevision || latest.FixRounds != w.FixRounds {
		t.Fatal("exhausted template changed counters", latest, err)
	}
}

func TestProductTemplateSequencesMultipleOwnersAndPreservesRoles(t *testing.T) {
	ctx := context.Background()
	k, w, original, runtime, first := productTemplateFixture(t, "implementation_defect", func(plan *contract.PlanProposal) {
		phase := &plan.Phases[len(plan.Phases)-1]
		second := phase.Tasks[0]
		second.ID = "second-product-review"
		second.Instruction = "independent additional required role"
		phase.Tasks = append(phase.Tasks, second)
		phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, second.ID)
		owner := plan.Templates[0]
		owner.ID = "second-owner"
		owner.InputTaskID = second.ID
		plan.Templates = append(plan.Templates, owner)
	})
	tasks, err := k.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	var second domain.Task
	for _, task := range tasks {
		if task.Spec.ID == "second-product-review" {
			second = task
		}
	}
	second, run := seedProductQualityReview(t, k, w, original, second)
	var result contract.TaskResult
	if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
		t.Fatal(err)
	}
	result.Review.Verdict = "findings"
	result.Review.Findings = []contract.ReviewFinding{{ID: "second-defect", Classification: "implementation_defect", TargetDigest: result.Review.TargetDigest, Basis: "AC", Description: "additional defect"}}
	raw, err := marshalTaskResultFixture(result)
	if err != nil {
		t.Fatal(err)
	}
	run.Final = string(raw)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := kernelRecordTx(ctx, tx, "agent_run", run.ID, second.SessionID, run); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyTaskResult(ctx, second.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	expansion, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: first.ID})
	if err != nil || len(expansion.Product.Repairs) != 2 || len(expansion.Product.Replacements[first.ID]) != 1 || len(expansion.Product.Replacements[second.ID]) != 1 {
		t.Fatal(expansion, err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i, local := range expansion.Product.Repairs {
		fix, err := k.Task(ctx, structureID("task-", w.ID, local))
		if err != nil {
			t.Fatal(err)
		}
		sha := strings.Repeat("e", 40)
		if i == 1 {
			sha = strings.Repeat("9", 40)
		}
		finishProductRepairWorkFixture(t, k, w, original, fix, sha)
	}
	verifier, err := k.Task(ctx, structureID("task-", w.ID, expansion.Product.Verifier))
	if err != nil {
		t.Fatal(err)
	}
	final := finishProductRepairVerifierFixture(t, k, w, original, verifier)
	completeProductRunnerFixture(t, k, final, runtime, "unit", "passed")
	for _, ids := range expansion.Product.Replacements {
		for _, id := range ids {
			next, err := k.Task(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			next, nextRun := seedProductQualityReview(t, k, w, final, next)
			if _, err := k.ApplyTaskResult(ctx, next.SessionID, nextRun.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	quality, err := k.CheckProductQuality(ctx, w.Target(), "work")
	if err != nil || len(quality.Value.Reviews) != 2 {
		t.Fatal("required product roles were lost", quality, err)
	}
}

func TestProductTemplateReplacementCompletesQualityWithoutErasingOldReport(t *testing.T) {
	for _, classification := range []string{"implementation_defect", "test_defect"} {
		t.Run(classification, func(t *testing.T) {
			ctx := context.Background()
			k, w, original, runtime, review := productTemplateFixture(t, classification)
			expansion, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID})
			if err != nil {
				t.Fatal(err)
			}
			w, err = k.Wave(ctx, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			if classification == "implementation_defect" {
				fix, err := k.Task(ctx, structureID("task-", w.ID, expansion.Product.Repairs[0]))
				if err != nil {
					t.Fatal(err)
				}
				finishProductRepairWorkFixture(t, k, w, original, fix, strings.Repeat("e", 40))
			}
			verifier, err := k.Task(ctx, structureID("task-", w.ID, expansion.Product.Verifier))
			if err != nil {
				t.Fatal(err)
			}
			final := finishProductRepairVerifierFixture(t, k, w, original, verifier)
			completeProductRunnerFixture(t, k, final, runtime, "unit", "passed")
			for _, id := range expansion.Product.Replacements[review.ID] {
				next, err := k.Task(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				next, nextRun := seedProductQualityReview(t, k, w, final, next)
				if _, err := k.ApplyTaskResult(ctx, next.SessionID, nextRun.ID); err != nil {
					t.Fatal(err)
				}
			}
			quality, err := k.CheckProductQuality(ctx, w.Target(), "work")
			if err != nil || quality.Value.Candidate != final.Value.Candidate {
				t.Fatal("repaired role lineage failed quality", quality, err)
			}
			old, err := k.ReadProductTaskReview(ctx, review.ResultID)
			if err != nil || old.Value.Review.Verdict != "findings" || old.Value.Candidate != original.Value.Candidate {
				t.Fatal("old review was rewritten", old, err)
			}
		})
	}
}

func prepareProductRepairFixture(t *testing.T, k *KernelStore, w domain.Wave, source ProductVerificationPlanSnapshot, task domain.Task) ProductWorkspace {
	t.Helper()
	ctx := context.Background()
	sealed, err := k.ProductSeal(ctx, source.Value.TaskID, source.Value.RunID)
	if err != nil {
		t.Fatal(err)
	}
	product, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.Kind == "verification" {
		if _, err := k.BindProductCandidateSubject(ctx, task.ID); err != nil {
			t.Fatal(err)
		}
	}
	request := sealed.Value.Workspace.Value.Request
	request.TaskID, request.BaseSHA = task.ID, product.Value.Candidate.SHA
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	ready, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved))
	if err != nil {
		t.Fatal(err)
	}
	return ready
}

func finishProductRepairVerifierFixture(t *testing.T, k *KernelStore, w domain.Wave, original ProductVerificationPlanSnapshot, task domain.Task) ProductVerificationPlanSnapshot {
	t.Helper()
	ctx := context.Background()
	ready := prepareProductRepairFixture(t, k, w, original, task)
	task, err := k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := k.ReadProductCandidateSubject(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	plan := verification.Plan{SchemaVersion: 1, WorkItemID: "work", ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, InputCandidateID: subject.Value.Candidate.ID, Revision: 1, CriteriaTests: []verification.Mapping{{CriterionID: "AC", TestRef: "tests/behavior_test.go::TestBehavior", ExpectationBasis: "original AC", ProfileID: "unit"}}, RequestedProfiles: []string{}, UnverifiedCriteria: []string{}, Blockers: []string{}, OnSuccess: "publish_pr"}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	artifact := mustImport(t, k, ArtifactImport{SessionID: task.SessionID, Target: task.Target(), Key: "repaired-verification-plan", Kind: "verification_plan", ContentType: "application/json", Origin: domain.ArtifactOrigin{Kind: "task", Reference: task.ID}}, string(raw))
	_, _, task, run := seedProductSealRun(t, k, w, task, ready, func(result *contract.TaskResult) {
		result.SchemaVersion = 3
		result.Artifacts = []string{artifact.ID}
		result.ProductVerification = &contract.ProductVerificationReference{SubjectID: subject.ID, SubjectDigest: subject.Digest, PlanArtifactID: artifact.ID}
	})
	seal, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductSeal(ctx, task.ID, run.ID, seal.Digest); err != nil {
		t.Fatal(err)
	}
	proof := sealProofFixture(seal)
	proof.Frozen.SHA = strings.Repeat("f", 40)
	checked := seal.Value.Verification.CheckProof()
	proof.Verification = &checked
	if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.AdoptProductVerificationTaskResult(ctx, task.SessionID, run.ID, "master")
	if err != nil {
		t.Fatal(err)
	}
	sidecar, err := k.ReadProductTaskCandidate(ctx, receipt.ResultID)
	if err != nil {
		t.Fatal(err)
	}
	final, err := k.ReadProductVerificationPlan(ctx, sidecar.Value.VerificationPlan.ID)
	if err != nil {
		t.Fatal(err)
	}
	return final
}

func finishProductRepairWorkFixture(t *testing.T, k *KernelStore, w domain.Wave, original ProductVerificationPlanSnapshot, fix domain.Task, sha string) {
	t.Helper()
	ctx := context.Background()
	ready := prepareProductRepairFixture(t, k, w, original, fix)
	fix, err := k.Task(ctx, fix.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, fix, run := seedProductSealRun(t, k, w, fix, ready)
	seal, err := k.ReserveProductSeal(ctx, fix.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductSeal(ctx, fix.ID, run.ID, seal.Digest); err != nil {
		t.Fatal(err)
	}
	proof := sealProofFixture(seal)
	proof.Frozen.SHA = sha
	if _, err := k.FinishProductSeal(ctx, fix.ID, run.ID, proof); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AdoptProductTaskResult(ctx, fix.SessionID, run.ID, "master"); err != nil {
		t.Fatal(err)
	}
}
