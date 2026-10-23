package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

var ErrProductQualityHeld = errors.New("product quality lacks current complete independent evidence")

type ProductQualityValue struct {
	Target             contract.Target     `json:"target"`
	WorkItemID         string              `json:"work_item_id"`
	ProductDigest      string              `json:"product_digest"`
	RegistrationDigest string              `json:"registration_digest"`
	PlanDigest         string              `json:"plan_digest"`
	Candidate          runner.Candidate    `json:"candidate"`
	Results            map[string]string   `json:"results"`
	Artifacts          map[string]string   `json:"artifacts"`
	Verifications      map[string]string   `json:"verifications"`
	Reviews            map[string]string   `json:"reviews"`
	Criteria           map[string][]string `json:"criteria"`
}

// ProductQualityProof is a read-time proof, never formal approval or a reusable
// send grant. Publication/merge must recheck quality and their own source fence.
type ProductQualityProof struct {
	Value  ProductQualityValue `json:"value"`
	Digest string              `json:"digest"`
}

func productQualityHeld(reason string) error {
	return fmt.Errorf("%w: %s", ErrProductQualityHeld, reason)
}

// CheckProductQuality derives local product quality from controller-owned facts.
// It changes no state and accepts no caller-selected results or reviewer subset.
func (k *KernelStore) CheckProductQuality(ctx context.Context, target contract.Target, workID string) (ProductQualityProof, error) {
	var proof ProductQualityProof
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return proof, err
	}
	defer tx.Rollback()
	return k.productQualityTx(ctx, tx, target, workID)
}

func (k *KernelStore) productQualityTx(ctx context.Context, tx *sql.Tx, target contract.Target, workID string) (ProductQualityProof, error) {
	var proof ProductQualityProof
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return proof, err
	}
	if !target.Valid() || w.Target() != target || w.Policy.Stage != "P" || !w.Policy.Product || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return proof, productQualityHeld("current live P Wave required")
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, workID)
	if err != nil {
		return proof, err
	}
	if !productRegistrationScope(registration, w) {
		return proof, productQualityHeld("current WorkItem registration required")
	}
	held, err := productWorkItemExecutionHeld(ctx, tx, w, workID)
	if err != nil {
		return proof, err
	}
	if held {
		return proof, productQualityHeld("WorkItem held")
	}
	product, err := loadProductTargetTx(ctx, tx, target, workID)
	if err != nil {
		return proof, err
	}
	var raw []byte
	var planDigest string
	if err := tx.QueryRowContext(ctx, "SELECT digest,payload FROM kernel_plans WHERE wave_id=? AND revision=?", w.ID, w.PlanRevision).Scan(&planDigest, &raw); err != nil {
		return proof, err
	}
	var admission PlanAdmission
	if err := json.Unmarshal(raw, &admission); err != nil {
		return proof, err
	}
	digest, err := contentDigest(admission.Plan)
	if err != nil {
		return proof, err
	}
	if admission.Target != target || admission.Digest != planDigest || planDigest != digest || admission.Plan.Validate() != nil {
		return proof, ErrTaskEvidence
	}
	value := ProductQualityValue{Target: target, WorkItemID: workID, ProductDigest: product.Digest, RegistrationDigest: registration.Digest, PlanDigest: planDigest, Candidate: product.Value.Candidate, Results: map[string]string{}, Artifacts: map[string]string{}, Verifications: map[string]string{}, Reviews: map[string]string{}, Criteria: map[string][]string{}}
	inputs := map[string]TaskResultInput{}
	tasks := map[string]domain.Task{}
	producerSessions := map[string]bool{}
	producerThreads := map[string]bool{}
	implementationThreads := map[string]bool{}
	reviewers := []domain.Task{}
	for _, phase := range admission.Plan.Phases {
		for _, spec := range phase.Tasks {
			if !slices.Contains(phase.Completion.RequiredTaskIDs, spec.ID) || len(spec.WorkItemIDs) > 0 && !slices.Contains(spec.WorkItemIDs, workID) {
				continue
			}
			if len(spec.WorkItemIDs) > 1 {
				return proof, productQualityHeld("multi-WorkItem Task evidence is not supported")
			}
			task, err := loadTask(ctx, tx, structureID("task-", w.ID, spec.ID))
			if err != nil {
				return proof, err
			}
			if !reflect.DeepEqual(task.Spec, spec) || task.ParentSessionID != w.OrchestratorSessionID || task.Paused || task.State != contract.TaskCompleted || task.ResultID == "" {
				return proof, productQualityHeld("required Task incomplete or changed")
			}
			if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
				return proof, err
			}
			input, err := k.taskResultInputTx(ctx, tx, w, task.ResultID)
			if err != nil {
				return proof, err
			}
			if len(input.Result.Blockers) > 0 || len(input.Result.Remaining) > 0 {
				return proof, productQualityHeld("required result still has blockers or remaining work")
			}
			inputs[task.ID], tasks[task.ID] = input, task
			inputDigest, err := contentDigest(input)
			if err != nil {
				return proof, err
			}
			value.Results[task.ResultID] = inputDigest
			for _, id := range resultArtifactReferences(input.Result) {
				a, err := k.currentArtifactTx(ctx, tx, w, id)
				if err != nil {
					return proof, err
				}
				value.Artifacts[id] = a.Digest
			}
			if len(spec.WorkItemIDs) == 0 {
				if spec.Kind == "review" && (input.Result.Review == nil || input.Result.Review.Verdict != "clean" || len(input.Result.Review.Findings) > 0) {
					return proof, productQualityHeld("shared required review is not clean")
				}
				continue
			}
			if spec.Kind == "review" {
				reviewers = append(reviewers, task)
				continue
			}
			if input.ProductCandidate == nil {
				return proof, productQualityHeld("required producer has no sealed Candidate evidence")
			}
			producerSessions[task.SessionID] = true
			var run contract.SessionRun
			if err := taskEvidenceRecord(ctx, tx, "agent_run", input.RunID, &run); err != nil {
				return proof, err
			}
			producerThreads[run.ThreadID] = true
			if spec.Kind != "verification" {
				implementationThreads[run.ThreadID] = true
			}
		}
	}
	for id, input := range inputs {
		task := tasks[id]
		if !UsesProductVerificationOutput(w, task) || input.ProductCandidate == nil || input.ProductCandidate.Value.Candidate != value.Candidate {
			continue
		}
		ref := input.ProductCandidate.Value.VerificationPlan
		if ref == nil {
			return proof, ErrTaskEvidence
		}
		var run contract.SessionRun
		if err := taskEvidenceRecord(ctx, tx, "agent_run", input.RunID, &run); err != nil {
			return proof, err
		}
		if implementationThreads[run.ThreadID] {
			return proof, productQualityHeld("verifier thread must be independent of implementation")
		}
		machine, err := k.productVerificationEvidenceTx(ctx, tx, ref.ID)
		if err != nil {
			return proof, err
		}
		if !machine.Value.Passed || machine.Value.Plan.Digest != ref.Digest || machine.Value.Candidate != value.Candidate {
			return proof, productQualityHeld("full current verification required")
		}
		value.Verifications[ref.ID] = machine.Digest
	}
	if len(value.Verifications) == 0 || len(reviewers) == 0 {
		return proof, productQualityHeld("final verifier and independent reviewer required")
	}
	seenSessions := map[string]bool{}
	seenThreads := map[string]bool{}
	for _, task := range reviewers {
		input := inputs[task.ID]
		review := input.ProductReview
		if review == nil || review.Value.Candidate != value.Candidate || review.Value.Review.Verdict != "clean" || len(review.Value.Review.Findings) != 0 || producerSessions[task.SessionID] || seenSessions[task.SessionID] {
			return proof, productQualityHeld("every required role needs separate clean review of final Candidate")
		}
		var run contract.SessionRun
		if err := taskEvidenceRecord(ctx, tx, "agent_run", input.RunID, &run); err != nil {
			return proof, err
		}
		if producerThreads[run.ThreadID] || seenThreads[run.ThreadID] {
			return proof, productQualityHeld("review thread must be independent of producers and other reviewers")
		}
		seenSessions[task.SessionID], seenThreads[run.ThreadID] = true, true
		value.Reviews[task.ResultID] = review.Digest
		for _, criterion := range task.Spec.Criteria {
			if !slices.Contains(registration.Value.Definition.Criteria, criterion) {
				continue
			}
			if !claimedCriterion(input.Result, criterion, "") {
				return proof, productQualityHeld("reviewer has not evaluated its assigned original criterion")
			}
			value.Criteria[criterion] = append(value.Criteria[criterion], task.ID)
		}
	}
	for _, criterion := range registration.Value.Definition.Criteria {
		if len(value.Criteria[criterion]) == 0 {
			return proof, productQualityHeld("every included original criterion needs independent evaluation")
		}
	}
	if err := k.productCandidateFindingsTx(ctx, tx, w, workID, value.Candidate); err != nil {
		return proof, err
	}
	digest, err = contentDigest(value)
	if err != nil {
		return proof, err
	}
	return ProductQualityProof{Value: value, Digest: digest}, nil
}

func (k *KernelStore) productCandidateFindingsTx(ctx context.Context, tx *sql.Tx, w domain.Wave, workID string, candidate runner.Candidate) error {
	// Candidate IDs change on resealing even when Git bytes do not. Retain all
	// product findings in this scope/policy and compare their authenticated SHA
	// and WorkItem, rather than allowing a fresh ID to erase unchanged defects.
	findings, err := inputIDsTx(ctx, tx, `SELECT r.id FROM records r JOIN kernel_tasks t ON t.id=r.work_item_id WHERE r.kind='task_result' AND t.wave_id=? AND json_extract(t.payload,'$.scope_revision')=? AND json_extract(t.payload,'$.policy_revision')=? AND json_array_length(json_extract(t.payload,'$.spec.work_item_ids'))>0 AND json_extract(r.payload,'$.outcome')='result' AND json_extract(r.payload,'$.review.verdict')='findings'
UNION SELECT id FROM records WHERE kind IN ('product_task_review','product_task_review_history') AND json_extract(payload,'$.value.target.wave_id')=? AND json_extract(payload,'$.value.target.scope_revision')=? AND json_extract(payload,'$.value.target.policy_revision')=? AND json_extract(payload,'$.value.review.verdict')='findings' ORDER BY 1`, w.ID, w.ScopeRevision, w.Policy.Revision, w.ID, w.ScopeRevision, w.Policy.Revision)
	if err != nil {
		return err
	}
	for _, id := range findings {
		review, err := k.productTaskReviewTx(ctx, tx, id)
		if err != nil {
			return err
		}
		subject, err := loadProductSubject(ctx, tx, review.Value.TaskID)
		if err != nil {
			return err
		}
		if subject.Value.WorkItemID == workID && review.Value.Candidate.SHA == candidate.SHA {
			return productQualityHeld("final Candidate bytes have confirmed findings; a reviewed replacement is required")
		}
	}
	return nil
}
