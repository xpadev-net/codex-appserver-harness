package storage

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

type ProductVerificationSealInput struct {
	Subject      ProductCandidateSubjectSnapshot   `json:"subject"`
	Policy       ProductVerificationPolicySnapshot `json:"policy"`
	PlanArtifact domain.Artifact                   `json:"plan_artifact"`
	InputPlan    verification.Plan                 `json:"input_plan"`
	ScopeDigest  string                            `json:"scope_digest"`
}
type ProductVerificationCheckProof struct {
	SubjectDigest      string `json:"subject_digest"`
	PolicyDigest       string `json:"policy_digest"`
	PlanArtifactDigest string `json:"plan_artifact_digest"`
	Checks             string `json:"checks"`
}

func productVerificationScope(w domain.Wave, registration ProductWorkItemRegistrationSnapshot) verification.Scope {
	scope := verification.Scope{Revision: w.ScopeRevision, OriginalRequest: w.OriginalRequest, Included: append([]string{}, registration.Value.Definition.Criteria...), PreviouslyMerged: []string{}, Remaining: []string{}}
	for _, criterion := range w.Criteria {
		scope.Criteria = append(scope.Criteria, verification.Criterion{ID: criterion.ID, Requirement: criterion.Requirement})
		if !slices.Contains(scope.Included, criterion.ID) {
			scope.Remaining = append(scope.Remaining, criterion.ID)
		}
	}
	return scope
}

func (k *KernelStore) productVerificationSealInputTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task, saved ProductWorkspace, result contract.TaskResult) (*ProductVerificationSealInput, error) {
	if !UsesProductVerificationOutput(w, task) {
		return nil, nil
	}
	subject, err := k.productWorkspaceSubjectTx(ctx, tx, w, task, saved.Value)
	if err != nil {
		return nil, err
	}
	p, err := productWorkspaceVerificationPolicyTx(ctx, tx, w, task, saved.Value)
	if err != nil {
		return nil, err
	}
	ref := result.ProductVerification
	if ref == nil || subject == nil || ref.SubjectID != subject.ID || ref.SubjectDigest != subject.Digest {
		return nil, fmt.Errorf("%w: verification subject does not match its saved input", ErrProductChangeReference)
	}
	a, err := k.currentArtifactTx(ctx, tx, w, ref.PlanArtifactID)
	if err != nil {
		return nil, err
	}
	if a.TaskID != task.ID || a.SessionID != task.SessionID || a.PlanRevision != task.PlanRevision || a.Bytes > 1<<20 {
		return nil, fmt.Errorf("%w: bounded owned verification plan artifact required", ErrProductChangeReference)
	}
	raw, err := k.readArtifactObject(a)
	if err != nil {
		return nil, err
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, subject.Value.WorkItemID)
	if err != nil {
		return nil, err
	}
	scope := productVerificationScope(w, registration)
	plan, err := verification.DecodePlan(raw, subject.Value.WorkItemID, subject.Value.Candidate, scope, p.Value.Verification)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid verification plan: %v", ErrProductChangeReference, err)
	}
	if !plan.Ready() {
		return nil, fmt.Errorf("%w: verification plan retains blockers or unverified criteria", ErrProductChangeReference)
	}
	digest, err := contentDigest(scope)
	if err != nil {
		return nil, err
	}
	return &ProductVerificationSealInput{Subject: *subject, Policy: *p, PlanArtifact: a, InputPlan: plan, ScopeDigest: digest}, nil
}

func verificationCheckProof(input ProductVerificationSealInput) ProductVerificationCheckProof {
	return ProductVerificationCheckProof{SubjectDigest: input.Subject.Digest, PolicyDigest: input.Policy.Digest, PlanArtifactDigest: input.PlanArtifact.Digest, Checks: "test_paths_and_tracked_plan_files_v1"}
}

// CheckProof describes the checks a trusted stopped Git owner must actually
// perform; it is never a model tool or evidence by construction alone.
func (input ProductVerificationSealInput) CheckProof() ProductVerificationCheckProof {
	return verificationCheckProof(input)
}

func productWorkspaceComparisonBase(value ProductWorkspaceValue) string {
	if value.ComparisonBaseSHA != "" {
		return value.ComparisonBaseSHA
	}
	return value.Request.BaseSHA
}
