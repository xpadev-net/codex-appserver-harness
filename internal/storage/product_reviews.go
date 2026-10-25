package storage

import (
	"context"
	"database/sql"
	"reflect"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

type ProductTaskReviewValue struct {
	LocalReview     *contract.ProductLocalReview `json:"local_review,omitempty"`
	TaskID          string                       `json:"task_id"`
	RunID           string                       `json:"run_id"`
	ResultID        string                       `json:"result_id"`
	Target          contract.Target              `json:"target"`
	InputDigest     string                       `json:"input_digest"`
	OutputDigest    string                       `json:"output_digest"`
	SnapshotID      string                       `json:"snapshot_id"`
	SnapshotDigest  string                       `json:"snapshot_digest"`
	SubjectID       string                       `json:"subject_id"`
	SubjectDigest   string                       `json:"subject_digest"`
	WorkspaceID     string                       `json:"workspace_id"`
	WorkspaceDigest string                       `json:"workspace_digest"`
	Candidate       runner.Candidate             `json:"candidate"`
	Review          contract.ReviewResult        `json:"review"`
}
type ProductTaskReviewSnapshot struct {
	Value  ProductTaskReviewValue `json:"value"`
	Digest string                 `json:"digest"`
}

// UsesProductReviewOutput selects the independent report profile. It grants no
// execution, quality certification or publication permission.
func UsesProductReviewOutput(w domain.Wave, t domain.Task) bool {
	return w.Policy.Stage == "P" && w.Policy.Product && t.Spec.Kind == "review" && len(t.Spec.WorkItemIDs) == 1
}

// productReviewEvidenceTx authorizes only an independent review report. A clean
// verdict is not machine verification, publication permission or Cloud approval.
func (k *KernelStore) productReviewEvidenceTx(ctx context.Context, tx *sql.Tx, w domain.Wave, b contract.SessionBinding, task domain.Task, runID string, result contract.TaskResult) (ProductTaskReviewSnapshot, error) {
	var saved ProductTaskReviewSnapshot
	if !UsesProductReviewOutput(w, task) || (result.SchemaVersion != 1 && result.SchemaVersion != 4) || result.Outcome != "result" || result.Review == nil || len(result.WorkItemRefs) != 0 || len(result.ProductChanges) != 0 || task.Paused || w.Target() != task.Target() || w.DispatchPaused() || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) || b.Kind != "task" || b.TaskID != task.ID || b.Target != task.Target() || b.ParentSessionID != w.OrchestratorSessionID || b.Closed || b.Sandbox != "read-only" {
		return saved, ErrProductSubjectHeld
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return saved, err
	}
	var sourcePermit productExecutionPermit
	held, err := k.productWorkItemExecutionPermitTx(ctx, tx, w, task.Spec.WorkItemIDs[0], &sourcePermit)
	if err != nil {
		return saved, err
	}
	if held {
		return saved, ErrProductSubjectHeld
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, task.Spec.WorkItemIDs[0])
	if err != nil {
		return saved, err
	}
	binding, err := productSplitChildScopeTx(ctx, tx, w, registration)
	if err != nil {
		return saved, err
	}
	if !productLocalReviewMatches(binding, result.SchemaVersion, result.ProductLocalReview) {
		return saved, ErrProductChangeReference
	}
	subject, err := k.fixedProductSubjectInputTx(ctx, tx, w, task.Spec)
	if err != nil {
		return saved, err
	}
	if subject == nil {
		return saved, ErrProductSubjectHeld
	}
	if result.Review.TargetDigest != subject.Value.CandidateDigest {
		return saved, ErrProductChangeReference
	}
	target, err := loadProductTargetTx(ctx, tx, w.Target(), subject.Value.WorkItemID)
	if err != nil {
		return saved, err
	}
	if target.Value.Candidate != subject.Value.Candidate {
		return saved, ErrProductSubjectHeld
	}
	ready, err := k.fixedProductWorkspaceInputTx(ctx, tx, w, task.Spec)
	if err != nil {
		return saved, err
	}
	if ready == nil || ready.Value.SessionID != b.ID || ready.Value.CWD != b.CWD || ready.Value.SubjectID != subject.ID || ready.Value.SubjectDigest != subject.Digest || ready.Value.Request.BaseSHA != subject.Value.Candidate.SHA {
		return saved, ErrProductSubjectHeld
	}
	input, err := k.taskInputWithWorkspaceTx(ctx, tx, w, task.Spec, ready)
	if err != nil {
		return saved, err
	}
	if input != task.InputDigest {
		return saved, ErrProductSubjectHeld
	}
	var owner, thread, state, snapshotID, active string
	var stopped, applied, sessionStopped, creating bool
	if err := tx.QueryRowContext(ctx, "SELECT session_id,thread_id,state,snapshot_id,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", runID).Scan(&owner, &thread, &state, &snapshotID, &stopped, &applied); err != nil {
		return saved, err
	}
	if err := tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &sessionStopped, &creating); err != nil {
		return saved, err
	}
	if owner != b.ID || thread != b.ThreadID || state != "completed" || !stopped || applied || active != runID || !sessionStopped || creating {
		return saved, ErrProductSubjectHeld
	}
	var run contract.SessionRun
	if err := taskEvidenceRecord(ctx, tx, "agent_run", runID, &run); err != nil {
		return saved, err
	}
	if run.ID != runID || run.ThreadID != thread || run.State != "completed" || run.TurnID == "" || !run.BaselineKnown || slices.Contains(run.BaselineTurnIDs, run.TurnID) {
		return saved, ErrTaskEvidence
	}
	var final contract.TaskResult
	if err := contract.DecodeStrict([]byte(run.Final), &final); err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(final, result) {
		return saved, ErrTaskEvidence
	}
	snapshot, manifest, err := k.productRunInputTx(ctx, tx, b, w, task, runID, snapshotID, thread)
	if err != nil {
		return saved, err
	}
	if manifest.ProductSubject == nil || !reflect.DeepEqual(*manifest.ProductSubject, *subject) || manifest.ProductWorkspace != nil || manifest.ProductVerificationPolicy != nil {
		return saved, ErrTaskEvidence
	}
	value := ProductTaskReviewValue{LocalReview: result.ProductLocalReview, TaskID: task.ID, RunID: runID, ResultID: structureID("result-", task.ID, runID), Target: task.Target(), InputDigest: input, OutputDigest: bytesDigest([]byte(run.Final)), SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest, SubjectID: subject.ID, SubjectDigest: subject.Digest, WorkspaceID: ready.ID, WorkspaceDigest: ready.Digest, Candidate: subject.Value.Candidate, Review: *result.Review}
	digest, err := contentDigest(value)
	if err == nil && !sourcePermit.current(k, tx) {
		return saved, ErrProductSubjectHeld
	}
	return ProductTaskReviewSnapshot{Value: value, Digest: digest}, err
}

func (k *KernelStore) productTaskReviewTx(ctx context.Context, q queryer, id string) (ProductTaskReviewSnapshot, error) {
	var saved ProductTaskReviewSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_task_review", id, &saved); err != nil {
		return saved, err
	}
	v := saved.Value
	digest, err := contentDigest(v)
	if err != nil {
		return saved, err
	}
	if digest != saved.Digest || v.ResultID != id || id != structureID("result-", v.TaskID, v.RunID) || !validSourceDigest(v.InputDigest) || !validSourceDigest(v.OutputDigest) || !validSourceDigest(v.SnapshotDigest) || v.SnapshotID == "" {
		return saved, ErrTaskEvidence
	}
	var history ProductTaskReviewSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_task_review_history", id, &history); err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(history, saved) {
		return saved, ErrTaskEvidence
	}
	subject, err := loadProductSubject(ctx, q, v.TaskID)
	if err != nil {
		return saved, err
	}
	workspace, err := k.productWorkspaceTx(ctx, q, v.WorkspaceID)
	if err != nil {
		return saved, err
	}
	if subject.ID != v.SubjectID || subject.Digest != v.SubjectDigest || subject.Value.Spec.Kind != "review" || subject.Value.Target != v.Target || subject.Value.Candidate != v.Candidate || subject.Value.CandidateDigest != v.Review.TargetDigest || workspace.ID != productWorkspaceID(v.TaskID) || workspace.Digest != v.WorkspaceDigest || workspace.State != "ready" || workspace.Value.SubjectID != v.SubjectID || workspace.Value.SubjectDigest != v.SubjectDigest || workspace.Value.SessionID != subject.Value.SessionID {
		return saved, ErrTaskEvidence
	}
	var result contract.TaskResult
	if err := taskEvidenceRecord(ctx, q, "task_result", id, &result); err != nil {
		return saved, err
	}
	var run contract.SessionRun
	if err := taskEvidenceRecord(ctx, q, "agent_run", v.RunID, &run); err != nil {
		return saved, err
	}
	var final contract.TaskResult
	if err := contract.DecodeStrict([]byte(run.Final), &final); err != nil {
		return saved, err
	}
	expectedSchema := 1
	if v.LocalReview != nil {
		expectedSchema = 4
	}
	if result.Validate() != nil || result.SchemaVersion != expectedSchema || !reflect.DeepEqual(result.ProductLocalReview, v.LocalReview) || result.TaskID != v.TaskID || result.Target != v.Target || result.Outcome != "result" || result.Review == nil || !reflect.DeepEqual(*result.Review, v.Review) || !reflect.DeepEqual(final, result) || bytesDigest([]byte(run.Final)) != v.OutputDigest || run.ID != v.RunID || run.State != "completed" || run.TurnID == "" || !run.BaselineKnown || slices.Contains(run.BaselineTurnIDs, run.TurnID) {
		return saved, ErrTaskEvidence
	}
	var owner, thread, state string
	var applied, stopped bool
	if err := q.QueryRowContext(ctx, "SELECT session_id,thread_id,state,applied,process_stopped FROM kernel_run_bindings WHERE run_id=?", v.RunID).Scan(&owner, &thread, &state, &applied, &stopped); err != nil {
		return saved, err
	}
	if owner != subject.Value.SessionID || thread != run.ThreadID || state != "completed" || !applied || !stopped {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

func (k *KernelStore) ReadProductTaskReview(ctx context.Context, resultID string) (ProductTaskReviewSnapshot, error) {
	return k.productTaskReviewTx(ctx, k.DB, resultID)
}

// Local criterion IDs have meaning only under this exact immutable scope.
func productLocalReviewMatches(binding *ProductSplitChildScopeBinding, version int, local *contract.ProductLocalReview) bool {
	if binding == nil {
		return version == 1 && local == nil
	}
	if version != 4 || local == nil || local.ScopeDigest != binding.Value.ScopeDigest || len(local.Criteria) != len(binding.Value.Scope.Included) {
		return false
	}
	known := map[string]bool{}
	for _, id := range binding.Value.Scope.Included {
		known[id] = true
	}
	for _, criterion := range local.Criteria {
		if !known[criterion.CriterionID] {
			return false
		}
		delete(known, criterion.CriterionID)
	}
	return len(known) == 0
}
