package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// TemplateSubject separates the replacement from output/evidence provenance.
// Ambiguous reports remain completed reports, but cannot start another review.
type TemplateSubject struct {
	TaskID         string          `json:"task_id"`
	Target         contract.Target `json:"target"`
	ResultID       string          `json:"result_id"`
	State          string          `json:"state"`
	ArtifactID     string          `json:"artifact_id"`
	Digest         string          `json:"digest"`
	ReplacedDigest string          `json:"replaced_digest"`
}
type templateTaskInput struct {
	InputDigest        string `json:"input_digest"`
	PreviousFixLocalID string `json:"previous_fix_local_id"`
	InputTaskLocalID   string `json:"input_task_local_id"`
}
type templateReviewTarget struct {
	Subject TemplateSubject `json:"subject"`
	Target  contract.Target `json:"target"`
	RunID   string          `json:"run_id"`
}

func (k *KernelStore) TemplateSubject(ctx context.Context, taskID string) (TemplateSubject, error) {
	var subject TemplateSubject
	err := k.Record(ctx, "template_subject", taskID, &subject)
	return subject, err
}
func (k *KernelStore) recordTemplateSubjectTx(ctx context.Context, tx *sql.Tx, task domain.Task, result contract.TaskResult) error {
	if task.Spec.Kind != "fix" || task.State != contract.TaskCompleted {
		return nil
	}
	var input templateTaskInput
	err := completionRecord(ctx, tx, "template_task_context", task.ID, &input)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(input.InputDigest) != 64 || strings.Trim(input.InputDigest, "0123456789abcdef") != "" {
		return fmt.Errorf("%w: template subject context is missing its original digest", ErrTaskEvidence)
	}
	subject := TemplateSubject{TaskID: task.ID, Target: task.Target(), ResultID: task.ResultID, State: "ambiguous", ReplacedDigest: input.InputDigest}
	var previous TemplateSubject
	if input.PreviousFixLocalID != "" {
		if err = completionRecord(ctx, tx, "template_subject", structureID("task-", task.WaveID, input.PreviousFixLocalID), &previous); err != nil {
			return err
		}
	}
	candidates := map[string]domain.Artifact{}
	explicit := map[string]domain.Artifact{}
	// Top-level artifacts declare outputs; evidence is kept as provenance only.
	for _, id := range result.Artifacts {
		a, err := loadArtifact(ctx, tx, id)
		if err != nil {
			return err
		}
		owned := a.TaskID == task.ID
		forwarded := previous.State == "bound" && previous.ArtifactID == a.ID
		if !owned && !forwarded {
			continue
		}
		if a.Digest != input.InputDigest {
			candidates[a.ID] = a
		}
		if a.Kind == "subject" {
			explicit[a.ID] = a
		}
	}
	if len(explicit) > 0 {
		candidates = explicit
	}
	if len(candidates) == 1 {
		for _, a := range candidates {
			if a.Digest == input.InputDigest {
				subject.State = "unchanged"
			} else {
				subject.State, subject.ArtifactID, subject.Digest = "bound", a.ID, a.Digest
			}
		}
	}
	if len(candidates) == 0 {
		subject.State = "unchanged"
	}
	return kernelRecordTx(ctx, tx, "template_subject", task.ID, task.WaveID, subject)
}
func (k *KernelStore) templateReviewSubjectTx(ctx context.Context, tx *sql.Tx, task domain.Task) (*TemplateSubject, error) {
	if task.Spec.Kind != "review" && task.Spec.Kind != "fix" {
		return nil, nil
	}
	var input templateTaskInput
	err := completionRecord(ctx, tx, "template_task_context", task.ID, &input)
	if errors.Is(err, sql.ErrNoRows) {
		if task.Spec.Kind == "fix" {
			return nil, nil
		}
		var lineage templateReviewLineage
		if err = completionRecord(ctx, tx, "template_review_lineage", task.ID, &lineage); errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: generated review has no subject context", ErrTemplateNeedsMain)
	}
	if err != nil {
		return nil, err
	}
	sourceLocal := input.InputTaskLocalID
	if task.Spec.Kind == "fix" {
		sourceLocal = input.PreviousFixLocalID
		if sourceLocal == "" {
			return nil, nil
		}
	}
	if sourceLocal == "" {
		return nil, fmt.Errorf("%w: generated review subject is unbound", ErrTemplateNeedsMain)
	}
	source, err := loadTask(ctx, tx, structureID("task-", task.WaveID, sourceLocal))
	if err != nil {
		return nil, err
	}
	var subject TemplateSubject
	if err = completionRecord(ctx, tx, "template_subject", source.ID, &subject); err != nil {
		return nil, err
	}
	if subject.State != "bound" {
		return nil, fmt.Errorf("%w: replacement subject is %s", ErrTemplateNeedsMain, subject.State)
	}
	if source.State != contract.TaskCompleted || source.ResultID != subject.ResultID || subject.TaskID != source.ID || subject.Target != source.Target() || source.ScopeRevision != task.ScopeRevision || source.PolicyRevision != task.PolicyRevision {
		return nil, fmt.Errorf("%w: replacement subject/result binding conflict", ErrTaskEvidence)
	}
	wave, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return nil, err
	}
	result, err := k.completedTaskResultTx(ctx, tx, wave, source)
	if err != nil {
		return nil, err
	}
	found := false
	for _, id := range result.Artifacts {
		found = found || id == subject.ArtifactID
	}
	if !found {
		return nil, fmt.Errorf("%w: replacement is not a bound output Artifact", ErrTaskEvidence)
	}
	a, err := k.currentArtifactTx(ctx, tx, wave, subject.ArtifactID)
	if err != nil {
		return nil, err
	}
	if a.Digest != subject.Digest || subject.Digest == subject.ReplacedDigest {
		return nil, fmt.Errorf("%w: replacement subject digest conflict", ErrTaskEvidence)
	}
	return &subject, nil
}

// TemplateReviewSubject is a pre-launch input check for Task_7/scheduler, for
// generated reviews and chained fixes. Nil means no preceding template subject;
// ambiguous/unchanged template results require main before a model is launched.
func (k *KernelStore) TemplateReviewSubject(ctx context.Context, taskID string) (*TemplateSubject, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		return nil, err
	}
	return k.templateReviewSubjectTx(ctx, tx, task)
}
