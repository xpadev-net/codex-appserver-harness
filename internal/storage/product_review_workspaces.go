package storage

import (
	"context"
	"database/sql"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func (k *KernelStore) productWorkspaceSubjectTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task, value ProductWorkspaceValue) (*ProductCandidateSubjectSnapshot, error) {
	if task.Spec.Kind != "review" && task.Spec.Kind != "verification" {
		if value.SubjectID != "" || value.SubjectDigest != "" {
			return nil, ErrTaskEvidence
		}
		return nil, nil
	}
	subject, err := k.fixedProductSubjectInputTx(ctx, tx, w, task.Spec)
	if err != nil {
		return nil, err
	}
	if subject == nil || value.Request.BaseSHA != subject.Value.Candidate.SHA {
		return nil, ErrProductWorkspaceHeld
	}
	if value.Target.Valid() && (value.SubjectID != subject.ID || value.SubjectDigest != subject.Digest) {
		return nil, ErrTaskEvidence
	}
	var current ProductTargetSnapshot
	if err := taskEvidenceRecord(ctx, tx, "product_target", productTargetID(w.Target(), subject.Value.WorkItemID), &current); err != nil {
		return nil, err
	}
	if current.Value.Target.WaveID != w.ID {
		return nil, ErrTaskEvidence
	}
	current, err = loadProductTargetTx(ctx, tx, current.Value.Target, subject.Value.WorkItemID)
	if err != nil {
		return nil, err
	}
	if current.Value.Candidate != subject.Value.Candidate {
		return nil, ErrProductWorkspaceHeld
	}
	for _, source := range subject.Value.Sources {
		sealed, err := sealedProductEvidence(ctx, tx, source.Value.TaskID, source.Value.RunID)
		if err != nil {
			return nil, err
		}
		request := sealed.Value.Workspace.Value.Request
		if request.Repository != value.Request.Repository || request.RepositoryID != value.Request.RepositoryID {
			return nil, ErrProductWorkspaceHeld
		}
	}
	return subject, nil
}

func (k *KernelStore) productWorkspaceVerificationPolicyTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task, value ProductWorkspaceValue) (*ProductVerificationPolicySnapshot, error) {
	if task.Spec.Kind != "verification" {
		if value.VerificationPolicyID != "" || value.VerificationPolicyDigest != "" {
			return nil, ErrTaskEvidence
		}
		if value.ComparisonBaseSHA != "" {
			plan, err := k.baseWorkspacePlanTx(ctx, tx, value)
			if err != nil {
				return nil, err
			}
			if plan.Value.Input.Value.Preparation.Product == nil || value.ComparisonBaseSHA != plan.Value.Head.HeadSHA {
				return nil, ErrTaskEvidence
			}
		}
		return nil, nil
	}
	saved, err := loadProductVerificationPolicy(ctx, tx, w.ID, w.Policy.Revision, value.Request.RepositoryID)
	if err != nil {
		return nil, err
	}
	if saved.Value.Repository != value.Request.Repository {
		return nil, ErrProductWorkspaceHeld
	}
	if value.Target.Valid() {
		if value.VerificationPolicyID != saved.ID || value.VerificationPolicyDigest != saved.Digest {
			return nil, ErrTaskEvidence
		}
		subject, err := loadProductSubject(ctx, tx, task.ID)
		if err != nil {
			return nil, err
		}
		if value.ComparisonBaseSHA != subject.Value.Candidate.BaseSHA {
			return nil, ErrTaskEvidence
		}
	}
	return &saved, nil
}
