package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrProductChangeReference = errors.New("product change reference is invalid")

type ProductWorkspaceInput struct {
	Conflict          *ProductBaseMergeSummary        `json:"conflict,omitempty"`
	PreparedHeadSHA   string                          `json:"prepared_head_sha,omitempty"`
	ComparisonBaseSHA string                          `json:"comparison_base_sha,omitempty"`
	Reference         contract.ProductChangeReference `json:"reference"`
	BaseSHA           string                          `json:"base_sha"`
	Branch            string                          `json:"branch"`
}

// UsesProductChangeOutput selects the bounded output profile only; it is not
// a dispatch, workspace, candidate or quality authorization.
func UsesProductChangeOutput(w domain.Wave, t domain.Task) bool {
	return w.Policy.Stage == "P" && w.Policy.Product && (t.Spec.Kind == "work" || t.Spec.Kind == "fix") && len(t.Spec.WorkItemIDs) == 1
}

// UsesProductVerificationOutput selects an output shape, never execution/write
// permission or a verification verdict.
func UsesProductVerificationOutput(w domain.Wave, t domain.Task) bool {
	return w.Policy.Stage == "P" && w.Policy.Product && t.Spec.Kind == "verification" && len(t.Spec.WorkItemIDs) == 1
}
func (k *KernelStore) productWorkspaceInputTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, w domain.Wave) (*ProductWorkspaceInput, error) {
	if b.Kind != "task" {
		return nil, nil
	}
	task, err := loadTask(ctx, tx, b.TaskID)
	if err != nil {
		return nil, err
	}
	if !UsesProductChangeOutput(w, task) && !UsesProductVerificationOutput(w, task) {
		return nil, nil
	}
	saved, err := k.productWorkspaceTx(ctx, tx, productWorkspaceID(task.ID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	registration, regErr := loadProductWorkItemRegistration(ctx, tx, task.Spec.WorkItemIDs[0])
	if errors.Is(regErr, sql.ErrNoRows) || errors.Is(regErr, ErrTaskEvidence) {
		return nil, nil
	}
	if regErr != nil {
		return nil, regErr
	}
	if !productRegistrationScope(registration, w) || registration.Digest != saved.Value.RegistrationDigest {
		return nil, nil
	}
	if saved.State != "ready" || saved.Value.SessionID != b.ID || saved.Value.Target != b.Target || saved.Value.Target != w.Target() || saved.Value.WorkItemID != task.Spec.WorkItemIDs[0] {
		return nil, nil
	}
	if UsesProductVerificationOutput(w, task) {
		subject, err := k.fixedProductSubjectInputTx(ctx, tx, w, task.Spec)
		if err != nil {
			return nil, err
		}
		if subject == nil || saved.Value.SubjectID != subject.ID || saved.Value.SubjectDigest != subject.Digest {
			return nil, ErrTaskEvidence
		}
		if _, err := k.productWorkspaceVerificationPolicyTx(ctx, tx, w, task, saved.Value); err != nil {
			return nil, err
		}
	}
	input := &ProductWorkspaceInput{Reference: contract.ProductChangeReference{WorkItemID: saved.Value.WorkItemID, WorkspaceID: saved.ID, WorkspaceDigest: saved.Digest}, BaseSHA: saved.Value.Request.BaseSHA, Branch: saved.Value.Branch}
	if saved.Proof != nil && saved.Proof.BaseMerge != nil {
		input.PreparedHeadSHA, input.ComparisonBaseSHA = saved.Proof.HeadSHA, saved.Value.ComparisonBaseSHA
		if saved.PreparedBaseConflict() {
			input.Conflict = publicBaseMergeSummary(saved.Proof.BaseMerge)
		}
	}
	return input, nil
}

// This checks a proposed output's input reference, never seals the filesystem or
// treats the model declaration as a candidate, verification or approval.
func (k *KernelStore) productChangeReferenceTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, w domain.Wave, task domain.Task, ref contract.ProductChangeReference) (bool, error) {
	if (!UsesProductChangeOutput(w, task) && !UsesProductVerificationOutput(w, task)) || ref.WorkItemID != task.Spec.WorkItemIDs[0] || ref.WorkspaceID != productWorkspaceID(task.ID) {
		return false, fmt.Errorf("%w: outside the assigned Task workspace", ErrProductChangeReference)
	}
	saved, err := k.productWorkspaceTx(ctx, tx, productWorkspaceID(task.ID))
	if err != nil {
		return false, err
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, task.Spec.WorkItemIDs[0])
	if err != nil {
		return false, err
	}
	if !productRegistrationScope(registration, w) || registration.Digest != saved.Value.RegistrationDigest {
		return false, fmt.Errorf("%w: product workspace registration is unavailable", ErrTaskEvidence)
	}
	if saved.Value.SessionID != b.ID || saved.Value.Target != task.Target() || saved.Value.WorkItemID != ref.WorkItemID || saved.Digest != ref.WorkspaceDigest {
		return false, fmt.Errorf("%w: does not match its saved workspace", ErrProductChangeReference)
	}
	return saved.State == "ready", nil
}
