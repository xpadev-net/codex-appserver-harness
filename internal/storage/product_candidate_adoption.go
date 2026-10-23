package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// ProductTaskCandidate is a controller sidecar; the raw model TaskResult stays
// unchanged. It records provenance, never verification or formal approval.
type ProductTaskCandidate struct {
	VerificationPlan *ProductVerificationPlanReference `json:"verification_plan,omitempty"`
	TaskID           string                            `json:"task_id"`
	RunID            string                            `json:"run_id"`
	ResultID         string                            `json:"result_id"`
	Target           contract.Target                   `json:"target"`
	SealID           string                            `json:"seal_id"`
	SealDigest       string                            `json:"seal_digest"`
	InputDigest      string                            `json:"input_digest"`
	OutputDigest     string                            `json:"output_digest"`
	Reference        contract.ProductChangeReference   `json:"reference"`
	Candidate        runner.Candidate                  `json:"candidate"`
	Repository       string                            `json:"repository"`
	BaseBranch       string                            `json:"base_branch"`
}
type ProductTaskCandidateSnapshot struct {
	Value  ProductTaskCandidate `json:"value"`
	Digest string               `json:"digest"`
}

func sealedProductEvidence(ctx context.Context, q queryer, taskID, runID string) (ProductSeal, error) {
	saved, err := loadProductSeal(ctx, q, productSealID(taskID, runID))
	if err != nil {
		return saved, err
	}
	if saved.State != "sealed" || saved.Candidate == nil || saved.Proof == nil || saved.Proof.Frozen == nil || saved.Value.Workspace.Proof == nil {
		return saved, ErrProductSealHeld
	}
	var history productSealRecord
	if err := taskEvidenceRecord(ctx, q, "product_seal_history", saved.ID, &history); err != nil {
		return saved, err
	}
	digest, err := contentDigest(saved)
	if err != nil {
		return saved, err
	}
	if history.Digest != digest || !reflect.DeepEqual(history.Seal, saved) {
		return saved, ErrTaskEvidence
	}
	v, prepared := saved.Value.Workspace.Value, saved.Value.Workspace.Proof
	pinned := workspace.PinnedCheckout{Repository: v.Request.Repository, CWD: v.CWD, Branch: v.Branch, GitDir: prepared.GitDir, CommonDir: prepared.CommonDir, BaseSHA: v.Request.BaseSHA}
	expected := runner.Candidate{ID: structureID("candidate-", saved.ID, saved.Proof.Frozen.SHA), SHA: saved.Proof.Frozen.SHA, BaseSHA: productWorkspaceComparisonBase(v), ScopeRevision: v.Target.ScopeRevision, PolicyRevision: v.Target.PolicyRevision}
	if !fixedSHA(expected.SHA) || saved.Proof.Outcome != "sealed" || saved.Proof.Frozen.PinnedCheckout != pinned || *saved.Candidate != expected {
		return saved, ErrTaskEvidence
	}
	if input := saved.Value.Verification; input != nil {
		expected := verificationCheckProof(*input)
		if saved.Proof.Verification == nil || *saved.Proof.Verification != expected || saved.VerificationPlan == nil || saved.VerificationPlan.InputCandidateID != saved.Candidate.ID {
			return saved, ErrTaskEvidence
		}
	} else if saved.Proof.Verification != nil || saved.VerificationPlan != nil {
		return saved, ErrTaskEvidence
	}
	reservation, err := loadOperationReservationTx(ctx, q, saved.ID)
	if err != nil {
		return saved, err
	}
	if reservation.State != "released" || reservation.Proof == nil || reservation.Proof.Disposition != "process_stopped" || reservation.Proof.ProofID != saved.Proof.ProofID {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

func (k *KernelStore) productSealForAdoptionTx(ctx context.Context, tx *sql.Tx, taskID, runID string) (ProductSeal, error) {
	saved, err := sealedProductEvidence(ctx, tx, taskID, runID)
	if err != nil {
		return saved, err
	}
	value, err := k.productSealEvidenceTx(ctx, tx, taskID, runID)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(value, saved.Value) {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

func loadProductTaskCandidate(ctx context.Context, q queryer, resultID string) (ProductTaskCandidateSnapshot, error) {
	var saved ProductTaskCandidateSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_task_candidate", resultID, &saved); err != nil {
		return saved, err
	}
	v := saved.Value
	digest, err := contentDigest(v)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || v.ResultID != resultID || resultID != structureID("result-", v.TaskID, v.RunID) {
		return saved, ErrTaskEvidence
	}
	sealed, err := sealedProductEvidence(ctx, q, v.TaskID, v.RunID)
	if err != nil {
		return saved, err
	}
	w := sealed.Value.Workspace
	ref := contract.ProductChangeReference{WorkItemID: w.Value.WorkItemID, WorkspaceID: w.ID, WorkspaceDigest: w.Digest}
	if v.SealID != sealed.ID || v.SealDigest != sealed.Digest || v.Target != w.Value.Target || v.InputDigest != sealed.Value.InputDigest || v.OutputDigest != sealed.Value.OutputDigest || v.Reference != ref || v.Candidate != *sealed.Candidate || v.BaseBranch == "" {
		return saved, ErrTaskEvidence
	}
	var candidate runner.Candidate
	if err := taskEvidenceRecord(ctx, q, "product_candidate", structureID("product-candidate-", ref.WorkItemID, v.Candidate.ID), &candidate); err != nil {
		return saved, err
	}
	if candidate != v.Candidate {
		return saved, ErrTaskEvidence
	}
	var result contract.TaskResult
	if err := taskEvidenceRecord(ctx, q, "task_result", resultID, &result); err != nil {
		return saved, err
	}
	if result.Validate() != nil || (result.SchemaVersion != 2 && result.SchemaVersion != 3) || result.TaskID != v.TaskID || result.Target != v.Target || result.Outcome != "result" || len(result.ProductChanges) != 1 || result.ProductChanges[0] != ref {
		return saved, ErrTaskEvidence
	}
	if result.SchemaVersion == 3 {
		if v.VerificationPlan == nil || sealed.Value.Verification == nil {
			return saved, ErrTaskEvidence
		}
		plan, err := loadProductVerificationPlan(ctx, q, v.VerificationPlan.ID)
		if err != nil {
			return saved, err
		}
		if plan.Digest != v.VerificationPlan.Digest || plan.Value.ResultID != resultID || plan.Value.Candidate != v.Candidate {
			return saved, ErrTaskEvidence
		}
	} else if v.VerificationPlan != nil {
		return saved, ErrTaskEvidence
	}
	var run contract.SessionRun
	if err := taskEvidenceRecord(ctx, q, "agent_run", v.RunID, &run); err != nil {
		return saved, err
	}
	if bytesDigest([]byte(run.Final)) != v.OutputDigest {
		return saved, ErrTaskEvidence
	}
	var final contract.TaskResult
	if err := contract.DecodeStrict([]byte(run.Final), &final); err != nil {
		return saved, ErrTaskEvidence
	}
	resultDigest, err := contentDigest(result)
	if err != nil {
		return saved, err
	}
	finalDigest, err := contentDigest(final)
	if err != nil {
		return saved, err
	}
	if resultDigest != finalDigest || run.ID != v.RunID || run.State != "completed" || run.TurnID == "" || !run.BaselineKnown || slices.Contains(run.BaselineTurnIDs, run.TurnID) {
		return saved, ErrTaskEvidence
	}
	var owner, thread, state string
	var applied, stopped bool
	if err := q.QueryRowContext(ctx, "SELECT session_id,thread_id,state,applied,process_stopped FROM kernel_run_bindings WHERE run_id=?", v.RunID).Scan(&owner, &thread, &state, &applied, &stopped); err != nil {
		return saved, err
	}
	if owner != w.Value.SessionID || thread != run.ThreadID || state != "completed" || !applied || !stopped {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

func (k *KernelStore) ReadProductTaskCandidate(ctx context.Context, resultID string) (ProductTaskCandidateSnapshot, error) {
	return loadProductTaskCandidate(ctx, k.DB, resultID)
}

func (k *KernelStore) adoptProductCandidateTx(ctx context.Context, tx *sql.Tx, task domain.Task, resultID string, sealed ProductSeal, baseBranch string) error {
	w, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return err
	}
	v := sealed.Value.Workspace.Value
	registration, err := loadProductWorkItemRegistration(ctx, tx, v.WorkItemID)
	if err != nil {
		return err
	}
	p := ProductTarget{Target: task.Target(), Repository: registration.Value.Definition.Repository, BaseBranch: baseBranch, Candidate: *sealed.Candidate}
	expected := ""
	var previous ProductTargetSnapshot
	err = taskEvidenceRecord(ctx, tx, "product_target", productTargetID(task.Target(), v.WorkItemID), &previous)
	if err == nil {
		previous, err = loadProductTargetTx(ctx, tx, previous.Value.Target, v.WorkItemID)
		if err != nil {
			return err
		}
		old := previous.Value
		if old.Repository != p.Repository || old.BaseBranch != baseBranch || old.WorkItem.ScopeRevision != task.ScopeRevision || old.WorkItem.PolicyRevision != task.PolicyRevision || old.WorkItem.Terminal() || old.WorkItem.Paused {
			return ErrProductSealHeld
		}
		p.WorkItem, p.PRNumber, expected = old.WorkItem, old.PRNumber, previous.Digest
		p.WorkItem.Version++
	} else if errors.Is(err, sql.ErrNoRows) {
		p.WorkItem = domain.WorkItem{ID: v.WorkItemID, RepositoryID: v.Request.RepositoryID, ScopeRevision: task.ScopeRevision, PolicyRevision: task.PolicyRevision, Version: 1, Attempt: 1, CreatedAt: k.clock()}
		p.WorkItem.Branch = v.Branch
		if w.Source == "github" {
			var repositoryID int64
			var issue int
			if n, err := fmt.Sscanf(registration.Value.SourceID, "r%d-i%d", &repositoryID, &issue); err != nil || n != 2 || issue < 1 || repositoryID != v.Request.RepositoryID || fmt.Sprintf("r%d-i%d", repositoryID, issue) != registration.Value.SourceID {
				return ErrTaskEvidence
			}
			p.WorkItem.Issue = issue
		}
	} else {
		return err
	}
	p.WorkItem.Candidate, p.WorkItem.Worktree, p.WorkItem.Phase, p.WorkItem.PhaseStartedAt = p.Candidate.ID, v.CWD, domain.Verifying, k.clock()
	p.WorkItem.LastCompletion, p.WorkItem.Reason, p.WorkItem.RecoveryPhase = nil, "", ""
	if _, err := k.setProductTargetTx(ctx, tx, expected, p); err != nil {
		return err
	}
	value := ProductTaskCandidate{TaskID: task.ID, RunID: sealed.Value.RunID, ResultID: resultID, Target: task.Target(), SealID: sealed.ID, SealDigest: sealed.Digest, InputDigest: sealed.Value.InputDigest, OutputDigest: sealed.Value.OutputDigest, Reference: contract.ProductChangeReference{WorkItemID: v.WorkItemID, WorkspaceID: sealed.Value.Workspace.ID, WorkspaceDigest: sealed.Value.Workspace.Digest}, Candidate: p.Candidate, Repository: p.Repository, BaseBranch: baseBranch}
	if sealed.Value.Verification != nil {
		plan, err := k.adoptProductVerificationPlanTx(ctx, tx, task, resultID, sealed)
		if err != nil {
			return err
		}
		value.VerificationPlan = &ProductVerificationPlanReference{ID: plan.ID, Digest: plan.Digest}
	}
	digest, err := contentDigest(value)
	if err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, "product_task_candidate", resultID, task.ID, ProductTaskCandidateSnapshot{Value: value, Digest: digest})
}
