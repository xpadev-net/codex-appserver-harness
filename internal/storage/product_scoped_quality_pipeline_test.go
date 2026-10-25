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
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// This storage integration authors model/stop/runner facts through the existing
// fixture helpers. It is not native model, physical checkout or Cloud evidence.
func scopedQualityPipelineFixture(t *testing.T, configure ...func(*contract.TaskResult)) (*KernelStore, domain.Wave, ProductSplitChildScopeBinding, domain.Task, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	k, w, split, _ := githubScopedExecutionFixture(t, func(p *splitting.Proposal) {
		p.Children[0].Criteria = append(p.Children[0].Criteria, verification.Criterion{ID: "validate-left", Requirement: "validate the complete left input"})
		p.Children[0].Coverage = append(p.Children[0].Coverage, splitting.Coverage{ParentCriterion: "AC", ChildCriterion: "validate-left", PartID: "validate-left", PartCount: 3, Basis: "validation part"})
		for i := range p.Children {
			for j := range p.Children[i].Coverage {
				p.Children[i].Coverage[j].PartCount = 3
			}
		}
	})
	binding := split.Value.Children[0]
	workID := binding.Value.Child.Value.WorkItemID
	plan := storagePlanFixture()
	plan.Phases[1].Tasks[0].Kind = "verification"
	for i := range plan.Phases {
		for j := range plan.Phases[i].Tasks {
			plan.Phases[i].Tasks[j].WorkItemIDs = []string{workID}
		}
	}
	plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: "quality", Tasks: []contract.TaskSpec{{ID: "quality-review", Kind: "review", Instruction: "independently evaluate every local obligation", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: plan.Phases[1].Tasks[0].ID}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{workID}}}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"quality-review"}}, FailurePolicy: "drain_and_block"})
	admitted, err := k.AdoptPlan(ctx, w.Target(), "scoped-quality-pipeline", plan)
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	root, err := k.ReadProductSourceObservation(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSourceAt(ctx, w.Target(), root.ID, root.Value.Facts, k.clock()); err != nil {
		t.Fatal(err)
	}
	var tasks []domain.Task
	for _, id := range admitted.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, task)
	}
	repo := operationRepo(t)
	base, err := workspace.Resolve(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	request := ProductWorkspaceRequest{TaskID: tasks[0].ID, RepositoryID: 17, Repository: repo, BaseSHA: base}
	ready := scopedPipelineWorkspace(t, k, request)
	tasks[0], err = k.Task(ctx, tasks[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, writer, writerRun := seedProductSealRun(t, k, w, tasks[0], ready)
	seal, err := k.ReserveProductSeal(ctx, writer.ID, writerRun.ID)
	if err != nil {
		t.Fatal("writer reserve seal", err)
	}
	if _, err := k.ClaimProductSeal(ctx, writer.ID, writerRun.ID, seal.Digest); err != nil {
		t.Fatal("writer claim seal", err)
	}
	if _, err := k.FinishProductSeal(ctx, writer.ID, writerRun.ID, sealProofFixture(seal)); err != nil {
		t.Fatal("writer finish seal", err)
	}
	if receipt, err := k.AdoptProductTaskResult(ctx, writer.SessionID, writerRun.ID, "master"); err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal("scoped writer failed", receipt, err)
	}
	verifier := tasks[1]
	subject, err := k.BindProductCandidateSubject(ctx, verifier.ID)
	if err != nil {
		t.Fatal(err)
	}
	policy := verificationPolicyFixture(w.ID, repo, w.Policy.Revision)
	if _, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), policy); err != nil {
		t.Fatal(err)
	}
	request.TaskID, request.BaseSHA = verifier.ID, subject.Value.Candidate.SHA
	ready = scopedPipelineWorkspace(t, k, request)
	verifier, err = k.Task(ctx, verifier.ID)
	if err != nil {
		t.Fatal(err)
	}
	vplan := verification.Plan{SchemaVersion: 1, WorkItemID: workID, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, InputCandidateID: subject.Value.Candidate.ID, Revision: 1, CriteriaTests: []verification.Mapping{}, RequestedProfiles: []string{}, UnverifiedCriteria: []string{}, Blockers: []string{}, OnSuccess: "publish_pr"}
	for _, id := range binding.Value.Scope.Included {
		vplan.CriteriaTests = append(vplan.CriteriaTests, verification.Mapping{CriterionID: id, TestRef: "tests/behavior_test.go::TestBehavior", ExpectationBasis: "complete local requirement " + id, ProfileID: "unit"})
	}
	raw, err := json.Marshal(vplan)
	if err != nil {
		t.Fatal(err)
	}
	artifact := mustImport(t, k, ArtifactImport{SessionID: verifier.SessionID, Target: verifier.Target(), Key: "scoped-plan", Kind: "verification_plan", ContentType: "application/json", Origin: domain.ArtifactOrigin{Kind: "task", Reference: verifier.ID}}, string(raw))
	_, _, verifier, verifierRun := seedProductSealRun(t, k, w, verifier, ready, func(result *contract.TaskResult) {
		result.SchemaVersion = 3
		result.Artifacts = []string{artifact.ID}
		result.ProductVerification = &contract.ProductVerificationReference{SubjectID: subject.ID, SubjectDigest: subject.Digest, PlanArtifactID: artifact.ID}
	})
	seal, err = k.ReserveProductSeal(ctx, verifier.ID, verifierRun.ID)
	if err != nil {
		t.Fatal("verifier reserve seal", err)
	}
	if _, err := k.ClaimProductSeal(ctx, verifier.ID, verifierRun.ID, seal.Digest); err != nil {
		t.Fatal("verifier claim seal", err)
	}
	proof := sealProofFixture(seal)
	proof.Frozen.SHA = strings.Repeat("d", 40)
	checked := seal.Value.Verification.CheckProof()
	proof.Verification = &checked
	if _, err := k.FinishProductSeal(ctx, verifier.ID, verifierRun.ID, proof); err != nil {
		t.Fatal("verifier finish seal", err)
	}
	receipt, err := k.AdoptProductVerificationTaskResult(ctx, verifier.SessionID, verifierRun.ID, "master")
	if err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal("scoped verifier failed", receipt, err)
	}
	source, err := k.ReadProductTaskCandidate(ctx, receipt.ResultID)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := k.ReadProductVerificationPlan(ctx, source.Value.VerificationPlan.ID)
	if err != nil {
		t.Fatal(err)
	}
	runtime := ProductRunnerRuntime{Root: k.Root + "/runner", LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}
	completeProductRunnerFixture(t, k, verified, runtime, "unit", "passed")
	review := tasks[2]
	subject, err = k.BindProductCandidateSubject(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	request.TaskID, request.BaseSHA = review.ID, subject.Value.Candidate.SHA
	ready = scopedPipelineWorkspace(t, k, request)
	review, err = k.Task(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	if version, err := k.ProductReviewOutputVersion(ctx, review.ID); err != nil || version != 4 {
		t.Fatal("scoped model report profile was not selected", version, err)
	}
	report := mustImport(t, k, ArtifactImport{SessionID: review.SessionID, Target: review.Target(), Key: "local-review", Kind: "review_report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: review.ID}}, "independent evaluation of every local part")
	_, _, review, reviewRun := seedProductSealRun(t, k, w, review, ready, func(result *contract.TaskResult) {
		result.SchemaVersion = 4
		result.ProductChanges = nil
		result.Artifacts = []string{report.ID}
		result.CriterionResults = []contract.CriterionResult{{CriterionID: "AC", Status: "unverified", EvidenceIDs: []string{}, Basis: "this local part is not the complete original criterion"}}
		result.Review = &contract.ReviewResult{TargetDigest: subject.Value.CandidateDigest, Verdict: "clean", Findings: []contract.ReviewFinding{}}
		result.ProductLocalReview = &contract.ProductLocalReview{ScopeDigest: binding.Value.ScopeDigest, Criteria: []contract.CriterionResult{}}
		for _, id := range binding.Value.Scope.Included {
			result.ProductLocalReview.Criteria = append(result.ProductLocalReview.Criteria, contract.CriterionResult{CriterionID: id, Status: "claimed", EvidenceIDs: []string{report.ID}, Basis: "independently reviewed local behavior " + id})
		}
		for _, apply := range configure {
			apply(result)
		}
	})
	return k, w, binding, review, reviewRun
}

func scopedPipelineWorkspace(t *testing.T, k *KernelStore, request ProductWorkspaceRequest) ProductWorkspace {
	t.Helper()
	ctx := context.Background()
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, request.TaskID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	ready, err := k.FinishProductWorkspace(ctx, request.TaskID, workspaceProofFixture(t, k, saved))
	if err != nil {
		t.Fatal(err)
	}
	return ready
}

func TestProductScopedQualityPipelineRequiresAllIndependentLocalReviewParts(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"complete", "missing_part", "unverified_part", "parent_only", "foreign_scope"} {
		t.Run(mode, func(t *testing.T) {
			k, w, binding, review, run := scopedQualityPipelineFixture(t, func(result *contract.TaskResult) {
				switch mode {
				case "missing_part":
					result.ProductLocalReview.Criteria = result.ProductLocalReview.Criteria[:1]
				case "unverified_part":
					result.ProductLocalReview.Criteria[1].Status = "unverified"
				case "parent_only":
					result.SchemaVersion = 1
					result.ProductLocalReview = nil
					result.CriterionResults[0].Status = "claimed"
					result.CriterionResults[0].EvidenceIDs = result.Artifacts
				case "foreign_scope":
					result.ProductLocalReview.ScopeDigest = strings.Repeat("f", 64)
				}
			})
			receipt, err := k.ApplyTaskResult(ctx, review.SessionID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			quality, qualityErr := k.CheckProductQuality(ctx, w.Target(), binding.Value.Child.Value.WorkItemID)
			if mode == "complete" {
				if receipt.State != contract.TaskCompleted || qualityErr != nil || quality.Value.ScopeDigest != binding.Value.ScopeDigest {
					t.Fatal("complete local pipeline did not pass", receipt, quality, qualityErr)
				}
				var ids []string
				for id := range quality.Value.Criteria {
					ids = append(ids, id)
				}
				if len(ids) != len(binding.Value.Scope.Included) {
					t.Fatal("local review lost criteria", ids)
				}
				for _, id := range binding.Value.Scope.Included {
					if !reflect.DeepEqual(quality.Value.Criteria[id], []string{review.ID}) {
						t.Fatal("wrong local reviewer", id)
					}
				}
				coverage, err := k.ProductCoverage(ctx, w.Target())
				if err != nil || len(coverage.Value.Merged) != 0 {
					t.Fatal("local quality invented merge/parent coverage", coverage, err)
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration' AND id=?", binding.Value.Child.Value.WorkItemID); err != nil {
					t.Fatal(err)
				}
				if version, err := k.ProductReviewOutputVersion(ctx, review.ID); err == nil || version == 1 {
					t.Fatal("known child with missing metadata fell back to Root profile", version, err)
				}
			} else if qualityErr == nil || !errors.Is(qualityErr, ErrProductQualityHeld) {
				t.Fatal("incomplete local review passed quality", mode, receipt, qualityErr)
			}
		})
	}
}

func TestProductReviewOutputVersionPreservesOrdinaryRootReport(t *testing.T) {
	k, _, review, _, _ := productReviewFixture(t, "clean")
	if version, err := k.ProductReviewOutputVersion(context.Background(), review.ID); err != nil || version != 1 {
		t.Fatal("ordinary Root review changed profile", version, err)
	}
}
