package storage

import (
	"context"
	"database/sql"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

type ProductVerificationPlanReference struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}
type ProductVerificationPlanValue struct {
	TaskID                 string            `json:"task_id"`
	RunID                  string            `json:"run_id"`
	ResultID               string            `json:"result_id"`
	Target                 contract.Target   `json:"target"`
	SealID                 string            `json:"seal_id"`
	SealDigest             string            `json:"seal_digest"`
	PolicyID               string            `json:"policy_id"`
	PolicyDigest           string            `json:"policy_digest"`
	OriginalArtifactID     string            `json:"original_artifact_id"`
	OriginalArtifactDigest string            `json:"original_artifact_digest"`
	InputCandidate         runner.Candidate  `json:"input_candidate"`
	Candidate              runner.Candidate  `json:"candidate"`
	Plan                   verification.Plan `json:"plan"`
}
type ProductVerificationPlanSnapshot struct {
	ID     string                       `json:"id"`
	Value  ProductVerificationPlanValue `json:"value"`
	Digest string                       `json:"digest"`
}

func productVerificationPlanID(taskID, runID string) string {
	return structureID("product-verification-plan-", taskID, runID)
}

func loadProductVerificationPlan(ctx context.Context, q queryer, id string) (ProductVerificationPlanSnapshot, error) {
	var saved ProductVerificationPlanSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_verification_plan", id, &saved); err != nil {
		return saved, err
	}
	v := saved.Value
	digest, err := contentDigest(v)
	if err != nil {
		return saved, err
	}
	if saved.ID != id || id != productVerificationPlanID(v.TaskID, v.RunID) || saved.Digest != digest || v.ResultID != structureID("result-", v.TaskID, v.RunID) {
		return saved, ErrTaskEvidence
	}
	sealed, err := sealedProductEvidence(ctx, q, v.TaskID, v.RunID)
	if err != nil {
		return saved, err
	}
	input := sealed.Value.Verification
	if input == nil || sealed.VerificationPlan == nil || sealed.ID != v.SealID || sealed.Digest != v.SealDigest || sealed.Value.Workspace.Value.Target != v.Target || input.Policy.ID != v.PolicyID || input.Policy.Digest != v.PolicyDigest || input.PlanArtifact.ID != v.OriginalArtifactID || input.PlanArtifact.Digest != v.OriginalArtifactDigest || input.Subject.Value.Candidate != v.InputCandidate || *sealed.Candidate != v.Candidate || !reflect.DeepEqual(*sealed.VerificationPlan, v.Plan) {
		return saved, ErrTaskEvidence
	}
	var history ProductVerificationPlanSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_verification_plan_history", id, &history); err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(saved, history) {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

func (k *KernelStore) ReadProductVerificationPlan(ctx context.Context, id string) (ProductVerificationPlanSnapshot, error) {
	return loadProductVerificationPlan(ctx, k.DB, id)
}

func (k *KernelStore) adoptProductVerificationPlanTx(ctx context.Context, tx *sql.Tx, task domain.Task, resultID string, sealed ProductSeal) (ProductVerificationPlanSnapshot, error) {
	var saved ProductVerificationPlanSnapshot
	input := sealed.Value.Verification
	if input == nil || sealed.VerificationPlan == nil || sealed.Candidate == nil {
		return saved, ErrTaskEvidence
	}
	w, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return saved, err
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, input.Subject.Value.WorkItemID)
	if err != nil {
		return saved, err
	}
	scope, err := productVerificationScopeTx(ctx, tx, w, registration)
	if err != nil {
		return saved, err
	}
	digest, err := contentDigest(scope)
	if err != nil {
		return saved, err
	}
	if digest != input.ScopeDigest {
		return saved, ErrTaskEvidence
	}
	plan, err := verification.Rebind(input.InputPlan, input.Subject.Value.WorkItemID, *sealed.Candidate, scope, input.Policy.Value.Verification)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(plan, *sealed.VerificationPlan) {
		return saved, ErrTaskEvidence
	}
	value := ProductVerificationPlanValue{TaskID: task.ID, RunID: sealed.Value.RunID, ResultID: resultID, Target: task.Target(), SealID: sealed.ID, SealDigest: sealed.Digest, PolicyID: input.Policy.ID, PolicyDigest: input.Policy.Digest, OriginalArtifactID: input.PlanArtifact.ID, OriginalArtifactDigest: input.PlanArtifact.Digest, InputCandidate: input.Subject.Value.Candidate, Candidate: *sealed.Candidate, Plan: plan}
	digest, err = contentDigest(value)
	if err != nil {
		return saved, err
	}
	saved = ProductVerificationPlanSnapshot{ID: productVerificationPlanID(task.ID, sealed.Value.RunID), Value: value, Digest: digest}
	for _, kind := range []string{"product_verification_plan", "product_verification_plan_history"} {
		if err := kernelRecordTx(ctx, tx, kind, saved.ID, task.ID, saved); err != nil {
			return saved, err
		}
	}
	return saved, nil
}
