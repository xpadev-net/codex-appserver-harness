package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrArtifactAcceptanceHeld = errors.New("artifact acceptance lacks current complete independent evidence")

type ArtifactAcceptanceProof struct {
	Target          contract.Target             `json:"target"`
	Proposal        contract.ArtifactAcceptance `json:"proposal"`
	PlanDigest      string                      `json:"plan_digest"`
	ResultDigests   map[string]string           `json:"result_digests"`
	ArtifactDigests map[string]string           `json:"artifact_digests"`
	Digest          string                      `json:"digest"`
}

// CheckArtifactAcceptance derives a proof, never a Wave completion or GitHub
// approval. Completion must recheck these facts in its own write transaction.
func (k *KernelStore) CheckArtifactAcceptance(ctx context.Context, target contract.Target, p contract.ArtifactAcceptance) (ArtifactAcceptanceProof, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ArtifactAcceptanceProof{}, err
	}
	defer tx.Rollback()
	return k.checkArtifactAcceptanceTx(ctx, tx, target, p)
}
func acceptanceHeld(reason string) error {
	return fmt.Errorf("%w: %s", ErrArtifactAcceptanceHeld, reason)
}
func claimedCriterion(result contract.TaskResult, id, artifact string) bool {
	for _, criterion := range result.CriterionResults {
		if criterion.CriterionID == id && criterion.Status == "claimed" && len(criterion.EvidenceIDs) > 0 && (artifact == "" || slices.Contains(criterion.EvidenceIDs, artifact)) {
			return true
		}
	}
	return false
}
func (k *KernelStore) checkArtifactAcceptanceTx(ctx context.Context, tx *sql.Tx, target contract.Target, p contract.ArtifactAcceptance) (ArtifactAcceptanceProof, error) {
	proof := ArtifactAcceptanceProof{Target: target, Proposal: p, ResultDigests: map[string]string{}, ArtifactDigests: map[string]string{}}
	if !target.Valid() {
		return proof, errors.New("versioned acceptance target required")
	}
	if err := p.Validate(); err != nil {
		return proof, err
	}
	wave, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return proof, err
	}
	if wave.Target() != target || wave.Policy.Stage != "K" || wave.Policy.Product {
		return proof, acceptanceHeld("current generic K Wave required")
	}
	progress, _, _, err := k.activePhaseTx(ctx, tx, target)
	if err != nil {
		return proof, err
	}
	if progress.State != "execution_completed" {
		return proof, acceptanceHeld("required Phase execution or review progression is incomplete")
	}
	var planRaw []byte
	if err = tx.QueryRowContext(ctx, "SELECT digest,payload FROM kernel_plans WHERE wave_id=? AND revision=?", wave.ID, wave.PlanRevision).Scan(&proof.PlanDigest, &planRaw); err != nil {
		return proof, err
	}
	var admission PlanAdmission
	if err = json.Unmarshal(planRaw, &admission); err != nil {
		return proof, err
	}
	ids := []string{}
	for _, phase := range admission.Plan.Phases {
		for _, spec := range phase.Tasks {
			ids = append(ids, structureID("task-", wave.ID, spec.ID))
		}
	}
	tasks := map[string]domain.Task{}
	results := map[string]contract.TaskResult{}
	for _, id := range ids {
		task, err := loadTask(ctx, tx, id)
		if err != nil {
			return proof, err
		}
		if err = currentObservedTaskTx(ctx, tx, wave, task); err != nil {
			return proof, err
		}
		if task.State != contract.TaskCompleted {
			return proof, acceptanceHeld("required Task is not completed")
		}
		input, err := k.taskResultInputTx(ctx, tx, wave, task.ResultID)
		if err != nil {
			return proof, err
		}
		// Earlier findings can have a saved repair lineage. Phase progression has
		// verified that lineage and its later independent reviews above.
		repairedReview := task.Spec.Kind == "review" && input.Result.Review != nil && input.Result.Review.Verdict == "findings"
		if !repairedReview && (len(input.Result.Remaining) > 0 || len(input.Result.Blockers) > 0) {
			return proof, acceptanceHeld("required result still reports remaining work or blockers")
		}
		tasks[id] = task
		results[id] = input.Result
		proof.ResultDigests[task.ResultID] = input.Digest
	}
	criteria := map[string]bool{}
	covered := map[string]bool{}
	reviewed := map[string]bool{}
	for _, criterion := range wave.Criteria {
		criteria[criterion.ID] = true
	}
	outputs := map[string]domain.Artifact{}
	for _, id := range p.Outputs {
		a, err := k.currentArtifactTx(ctx, tx, wave, id)
		if err != nil {
			return proof, err
		}
		// A different clean review cannot erase a confirmed findings report
		// for these exact bytes, even after the old review leaves the plan.
		findings, err := inputIDsTx(ctx, tx, `SELECT r.id FROM records r JOIN kernel_tasks t ON t.id=r.work_item_id WHERE r.kind='task_result' AND t.wave_id=? AND json_extract(t.payload,'$.scope_revision')=? AND json_extract(t.payload,'$.policy_revision')=? AND json_extract(r.payload,'$.review.target_digest')=? AND json_extract(r.payload,'$.review.verdict')='findings' ORDER BY r.id`, wave.ID, wave.ScopeRevision, wave.Policy.Revision, a.Digest)
		if err != nil {
			return proof, err
		}
		for _, id := range findings {
			input, err := k.taskResultInputTx(ctx, tx, wave, id)
			if err != nil {
				return proof, err
			}
			if input.SourceSpec.Kind == "review" && input.Result.Outcome == "result" && input.Result.Review != nil && input.Result.Review.Verdict == "findings" && input.Result.Review.TargetDigest == a.Digest {
				return proof, acceptanceHeld("selected output has confirmed findings; select the independently reviewed replacement")
			}
		}
		producer, ok := tasks[a.TaskID]
		if !ok || producer.SessionID != a.SessionID || producer.Spec.Kind == "review" || !slices.Contains(results[producer.ID].Artifacts, id) {
			return proof, acceptanceHeld("final output must belong to a current applied producer result")
		}
		outputs[id] = a
		proof.ArtifactDigests[id] = a.Digest
	}
	for _, evaluation := range p.Evaluations {
		if !criteria[evaluation.CriterionID] {
			return proof, acceptanceHeld("unknown original criterion")
		}
		a := outputs[evaluation.ArtifactID]
		producer := tasks[a.TaskID]
		reviewer, ok := tasks[evaluation.ReviewTaskID]
		result := results[reviewer.ID]
		if !ok || reviewer.ID == producer.ID || reviewer.SessionID == a.SessionID || reviewer.Spec.Kind != "review" || result.Review == nil || result.Review.Verdict != "clean" || len(result.Review.Findings) != 0 || result.Review.TargetDigest != a.Digest {
			return proof, acceptanceHeld("separate clean review of this exact output digest required")
		}
		if !slices.Contains(producer.Spec.Criteria, evaluation.CriterionID) || !slices.Contains(reviewer.Spec.Criteria, evaluation.CriterionID) || !claimedCriterion(results[producer.ID], evaluation.CriterionID, a.ID) || !claimedCriterion(result, evaluation.CriterionID, "") {
			return proof, acceptanceHeld("producer and reviewer must evaluate the mapped original criterion")
		}
		binding, err := sessionBindingTx(ctx, tx, reviewer.SessionID)
		if err != nil {
			return proof, err
		}
		if binding.Sandbox != "read-only" {
			return proof, acceptanceHeld("independent reviewer must have read-only scope")
		}
		// Current-plan membership was checked above. A carried completed
		// reviewer retains its immutable original target and input grants.
		reviewedWave := wave
		reviewedWave.PlanRevision = reviewer.PlanRevision
		if err = artifactReadableTx(ctx, tx, binding, reviewedWave, a); err != nil {
			return proof, err
		}
		covered[evaluation.CriterionID] = true
		reviewed[a.ID] = true
	}
	if len(covered) != len(criteria) || len(reviewed) != len(outputs) {
		return proof, acceptanceHeld("every original criterion and proposed output needs independent evaluation")
	}
	proof.Digest, err = contentDigest(proof)
	return proof, err
}
