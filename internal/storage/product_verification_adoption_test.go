package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func sealedVerificationFixture(t *testing.T) (*KernelStore, domain.Wave, domain.Task, contract.SessionRun, ProductSeal) {
	t.Helper()
	ctx := context.Background()
	k, w, task, run, _, _ := verificationSealFixture(t)
	saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	proof := sealProofFixture(saved)
	proof.Frozen.SHA = strings.Repeat("d", 40)
	checked := saved.Value.Verification.CheckProof()
	proof.Verification = &checked
	sealed, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof)
	if err != nil || sealed.State != "sealed" {
		t.Fatal(sealed, err)
	}
	return k, w, task, run, sealed
}

func TestProductVerificationAdoptionKeepsRawResultAndCanonicalFinalPlan(t *testing.T) {
	ctx := context.Background()
	k, w, task, run, sealed := sealedVerificationFixture(t)
	if _, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID); !errors.Is(err, ErrTaskResultReferences) {
		t.Fatal("generic application adopted verification", err)
	}
	if _, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "master"); err == nil {
		t.Fatal("work/fix host profile adopted verification")
	}
	receipt, err := k.AdoptProductVerificationTaskResult(ctx, task.SessionID, run.ID, "master")
	if err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal(receipt, err)
	}
	sidecar, err := k.ReadProductTaskCandidate(ctx, receipt.ResultID)
	if err != nil || sidecar.Value.Candidate != *sealed.Candidate || sidecar.Value.VerificationPlan == nil {
		t.Fatal(sidecar, err)
	}
	plan, err := k.ReadProductVerificationPlan(ctx, sidecar.Value.VerificationPlan.ID)
	if err != nil || plan.Digest != sidecar.Value.VerificationPlan.Digest || plan.Value.Candidate != *sealed.Candidate || !reflect.DeepEqual(plan.Value.Plan, *sealed.VerificationPlan) || plan.Value.InputCandidate != sealed.Value.Verification.Subject.Value.Candidate {
		t.Fatal(plan, err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil || p.Value.Candidate != *sealed.Candidate || p.Value.WorkItem.Phase != domain.Verifying || p.Value.PRNumber != 0 {
		t.Fatal("adoption fabricated runner/approval", p, err)
	}
	var observed contract.SessionRun
	if err := k.Record(ctx, "agent_run", run.ID, &observed); err != nil || observed.Final != run.Final {
		t.Fatal("raw v3 result changed", err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	input, inputErr := k.taskResultInputTx(ctx, tx, w, receipt.ResultID)
	tx.Rollback()
	if inputErr != nil || input.ProductCandidate == nil || input.ProductCandidate.Value.VerificationPlan == nil || input.Result.SchemaVersion != 3 {
		t.Fatal(input, inputErr)
	}
	again, err := k.AdoptProductVerificationTaskResult(ctx, task.SessionID, run.ID, "master")
	if err != nil || !reflect.DeepEqual(again, receipt) {
		t.Fatal(again, err)
	}
	if _, err := k.AdoptProductVerificationTaskResult(ctx, task.SessionID, run.ID, "new-base"); err == nil {
		t.Fatal("replay changed base")
	}
}

func TestProductVerificationAdoptionRollbackKeepsOldCandidateAndTask(t *testing.T) {
	for _, stage := range []string{"plan_history", "completion"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run, sealed := sealedVerificationFixture(t)
			ddl := "CREATE TRIGGER fail_verification_adoption BEFORE INSERT ON records WHEN NEW.kind='product_verification_plan_history' BEGIN SELECT RAISE(FAIL,'verification adoption rollback'); END"
			if stage == "completion" {
				ddl = "CREATE TRIGGER fail_verification_adoption BEFORE UPDATE ON kernel_plan_bindings WHEN NEW.source_result_id<>'' BEGIN SELECT RAISE(FAIL,'verification adoption rollback'); END"
			}
			if _, err := k.DB.Exec(ddl); err != nil {
				t.Fatal(err)
			}
			if _, err := k.AdoptProductVerificationTaskResult(ctx, task.SessionID, run.ID, "master"); err == nil || !strings.Contains(err.Error(), "verification adoption rollback") {
				t.Fatal(err)
			}
			p, err := k.ReadProductTarget(ctx, w.Target(), "work")
			if err != nil || p.Value.Candidate != sealed.Value.Verification.Subject.Value.Candidate {
				t.Fatal("partial candidate adoption", p, err)
			}
			current, err := k.Task(ctx, task.ID)
			if err != nil || !reflect.DeepEqual(current, task) {
				t.Fatal("partial Task completion", current, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_verification_plan','product_verification_plan_history')").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
			if _, err := k.DB.Exec("DROP TRIGGER fail_verification_adoption"); err != nil {
				t.Fatal(err)
			}
			if receipt, err := k.AdoptProductVerificationTaskResult(ctx, task.SessionID, run.ID, "master"); err != nil || receipt.State != contract.TaskCompleted {
				t.Fatal(receipt, err)
			}
		})
	}
}

func TestProductVerificationAdoptionPlanLossInvalidatesCompletedEvidence(t *testing.T) {
	for _, mode := range []string{"head", "history", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run, _ := sealedVerificationFixture(t)
			receipt, err := k.AdoptProductVerificationTaskResult(ctx, task.SessionID, run.ID, "master")
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_verification_plan'")
			case "history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_verification_plan_history'")
			case "corrupt":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_verification_plan'")
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductTaskCandidate(ctx, receipt.ResultID); err == nil {
				t.Fatal("missing canonical plan accepted")
			}
			current, err := k.Task(ctx, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = k.completedTaskResultTx(ctx, tx, w, current)
			tx.Rollback()
			if err == nil {
				t.Fatal("plan loss did not invalidate dependency proof")
			}
		})
	}
}

func TestProductVerificationFinalCandidateBecomesIndependentReviewSubject(t *testing.T) {
	ctx := context.Background()
	k, w, task, run, sealed := sealedVerificationFixture(t)
	if _, err := k.AdoptProductVerificationTaskResult(ctx, task.SessionID, run.ID, "master"); err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	plan.Phases[0].Tasks[0].WorkItemIDs = []string{"work"}
	plan.Phases[1].Tasks[0].Kind = "verification"
	plan.Phases[1].Tasks[0].WorkItemIDs = []string{"work"}
	review := contract.TaskSpec{ID: "review-final", Kind: "review", Instruction: "independently review code and added tests", Criteria: []string{"AC"}, WorkItemIDs: []string{"work"}, DependsOn: []contract.TaskDependency{{TaskID: task.Spec.ID}}, InputArtifactIDs: []string{}}
	plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: "final-evaluation", Tasks: []contract.TaskSpec{review}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{review.ID}}, FailurePolicy: "drain_and_block"})
	admitted, err := k.AdoptPlan(ctx, w.Target(), "review-final-candidate", plan)
	if err != nil || len(admitted.CarriedTaskIDs) != 2 || len(admitted.CreatedTaskIDs) != 1 {
		t.Fatal(admitted, err)
	}
	subject, err := k.BindProductCandidateSubject(ctx, admitted.CreatedTaskIDs[0])
	if err != nil || subject.Value.Candidate != *sealed.Candidate || subject.Value.Sources[0].Value.VerificationPlan == nil {
		t.Fatal("review selected pre-test candidate", subject, err)
	}
}
