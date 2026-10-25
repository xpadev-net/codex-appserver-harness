package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

var ErrProductSubjectHeld = errors.New("product candidate subject is unavailable or requires reconciliation")

type ProductCandidateSubject struct {
	TaskID             string                         `json:"task_id"`
	SessionID          string                         `json:"session_id"`
	ParentSessionID    string                         `json:"parent_session_id"`
	Target             contract.Target                `json:"target"`
	Spec               contract.TaskSpec              `json:"spec"`
	WorkItemID         string                         `json:"work_item_id"`
	RegistrationDigest string                         `json:"registration_digest"`
	OriginInputDigest  string                         `json:"origin_input_digest"`
	Candidate          runner.Candidate               `json:"candidate"`
	CandidateDigest    string                         `json:"candidate_digest"`
	Sources            []ProductTaskCandidateSnapshot `json:"sources"`
}
type ProductCandidateSubjectSnapshot struct {
	ID        string                  `json:"id"`
	Value     ProductCandidateSubject `json:"value"`
	Digest    string                  `json:"digest"`
	CreatedAt time.Time               `json:"created_at"`
}
type productSubjectRecord struct {
	Subject ProductCandidateSubjectSnapshot `json:"subject"`
	Digest  string                          `json:"digest"`
}

func usesProductSubject(w domain.Wave, spec contract.TaskSpec) bool {
	return w.Policy.Stage == "P" && w.Policy.Product && (spec.Kind == "review" || spec.Kind == "verification") && len(spec.WorkItemIDs) == 1
}
func productSubjectID(taskID string) string { return structureID("product-subject-", taskID, "v1") }

func loadProductSubject(ctx context.Context, q queryer, taskID string) (ProductCandidateSubjectSnapshot, error) {
	var record productSubjectRecord
	id := productSubjectID(taskID)
	if err := taskEvidenceRecord(ctx, q, "product_candidate_subject", id, &record); err != nil {
		return record.Subject, err
	}
	saved, v := record.Subject, record.Subject.Value
	digest, err := contentDigest(saved)
	if err != nil {
		return saved, err
	}
	valueDigest, err := contentDigest(v)
	if err != nil {
		return saved, err
	}
	candidateDigest, err := contentDigest(v.Candidate)
	if err != nil {
		return saved, err
	}
	if record.Digest != digest || saved.Digest != valueDigest || saved.ID != id || v.TaskID != taskID || saved.CreatedAt.IsZero() || !v.Target.Valid() || v.SessionID == "" || v.ParentSessionID == "" || (v.Spec.Kind != "review" && v.Spec.Kind != "verification") || len(v.Spec.WorkItemIDs) != 1 || v.Spec.WorkItemIDs[0] != v.WorkItemID || !validSourceDigest(v.OriginInputDigest) || !validSourceDigest(v.RegistrationDigest) || v.CandidateDigest != candidateDigest || v.Candidate.ScopeRevision != v.Target.ScopeRevision || v.Candidate.PolicyRevision != v.Target.PolicyRevision || len(v.Sources) == 0 || len(v.Sources) > 32 {
		return saved, ErrTaskEvidence
	}
	var history productSubjectRecord
	if err := taskEvidenceRecord(ctx, q, "product_candidate_subject_history", id, &history); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return saved, ErrTaskEvidence
		}
		return saved, err
	}
	if history.Digest != digest || !reflect.DeepEqual(history.Subject, saved) {
		return saved, ErrTaskEvidence
	}
	previous := ""
	for _, source := range v.Sources {
		actual, err := loadProductTaskCandidate(ctx, q, source.Value.ResultID)
		if err != nil {
			return saved, err
		}
		if !reflect.DeepEqual(actual, source) || source.Value.ResultID <= previous || source.Value.TaskID == taskID || source.Value.Target.WaveID != v.Target.WaveID || source.Value.Candidate != v.Candidate || source.Value.Reference.WorkItemID != v.WorkItemID {
			return saved, ErrTaskEvidence
		}
		producer, err := loadTask(ctx, q, source.Value.TaskID)
		if err != nil {
			return saved, err
		}
		if producer.SessionID == v.SessionID {
			return saved, ErrTaskEvidence
		}
		previous = source.Value.ResultID
	}
	return saved, nil
}

func (k *KernelStore) ReadProductCandidateSubject(ctx context.Context, taskID string) (ProductCandidateSubjectSnapshot, error) {
	return loadProductSubject(ctx, k.DB, taskID)
}

func (k *KernelStore) fixedProductSubjectInputTx(ctx context.Context, tx *sql.Tx, w domain.Wave, spec contract.TaskSpec) (*ProductCandidateSubjectSnapshot, error) {
	if !usesProductSubject(w, spec) {
		return nil, nil
	}
	taskID := structureID("task-", w.ID, spec.ID)
	saved, err := loadProductSubject(ctx, tx, taskID)
	if errors.Is(err, sql.ErrNoRows) {
		var history bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_candidate_subject_history' AND id=?)", productSubjectID(taskID)).Scan(&history); err != nil {
			return nil, err
		}
		if history {
			return nil, ErrTaskEvidence
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		return nil, err
	}
	v := saved.Value
	if !reflect.DeepEqual(v.Spec, spec) || v.Target != task.Target() || v.Target.WaveID != w.ID || v.Target.ScopeRevision != w.ScopeRevision || v.Target.PolicyRevision != w.Policy.Revision || v.SessionID != task.SessionID || v.ParentSessionID != w.OrchestratorSessionID {
		return nil, ErrTaskEvidence
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, v.WorkItemID)
	if err != nil {
		return nil, err
	}
	if !productRegistrationScope(registration, w) || registration.Digest != v.RegistrationDigest || registration.Value.Definition.RepositoryID < 1 {
		return nil, ErrTaskEvidence
	}
	return &saved, nil
}

// BindProductCandidateSubject fixes input data before any model/thread/run. It
// selects declared, applied producer results; callers cannot submit a SHA. This
// is no checkout permission, dispatch, runner, review or verification success.
func (k *KernelStore) BindProductCandidateSubject(ctx context.Context, taskID string) (ProductCandidateSubjectSnapshot, error) {
	var saved ProductCandidateSubjectSnapshot
	if taskID == "" || len(taskID) > contract.MaxInline || !utf8.ValidString(taskID) || strings.ContainsAny(taskID, "\x00\r\n") {
		return saved, errors.New("bounded Task identity required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	saved, err = loadProductSubject(ctx, tx, taskID)
	if err == nil {
		return saved, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return saved, err
	}
	var history bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_candidate_subject_history' AND id=?)", productSubjectID(taskID)).Scan(&history); err != nil {
		return saved, err
	}
	if history {
		return saved, ErrTaskEvidence
	}
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		return saved, err
	}
	w, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return saved, err
	}
	if err := w.Validate(); err != nil {
		return saved, err
	}
	if !usesProductSubject(w, task.Spec) || w.Target() != task.Target() || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || !k.clock().Before(w.Deadline) || task.State != contract.TaskReady || task.Paused || task.Attempts != 0 {
		return saved, ErrProductSubjectHeld
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return saved, err
	}
	b, err := sessionBindingTx(ctx, tx, task.SessionID)
	if err != nil {
		return saved, err
	}
	var active string
	var stopped, creating, used bool
	if err := tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating,EXISTS(SELECT 1 FROM kernel_run_bindings WHERE session_id=?) FROM kernel_sessions WHERE id=?", b.ID, b.ID).Scan(&active, &stopped, &creating, &used); err != nil {
		return saved, err
	}
	if b.Kind != "task" || b.TaskID != task.ID || b.Target != task.Target() || b.ParentSessionID != w.OrchestratorSessionID || b.Closed || b.ThreadID != "" || b.Sandbox != "read-only" || active != "" || !stopped || creating || used {
		return saved, ErrProductSubjectHeld
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
	if !productRegistrationScope(registration, w) {
		return saved, ErrTaskEvidence
	}
	for _, criterion := range task.Spec.Criteria {
		if !slices.Contains(registration.Value.Definition.Criteria, criterion) {
			return saved, ErrProductSubjectHeld
		}
	}
	sources, err := k.productSubjectSourcesTx(ctx, tx, w, task)
	if err != nil {
		return saved, err
	}
	// An explicitly carried producer keeps its original plan target. Reading
	// its current scope's candidate is data binding, not a PR dispatch refresh.
	var current ProductTargetSnapshot
	err = taskEvidenceRecord(ctx, tx, "product_target", productTargetID(w.Target(), task.Spec.WorkItemIDs[0]), &current)
	if err != nil {
		return saved, err
	}
	if current.Value.Target.WaveID != w.ID {
		return saved, ErrTaskEvidence
	}
	current, err = loadProductTargetTx(ctx, tx, current.Value.Target, task.Spec.WorkItemIDs[0])
	if err != nil {
		return saved, err
	}
	if current.Value.Candidate != sources[0].Value.Candidate || current.Value.Repository != sources[0].Value.Repository || current.Value.BaseBranch != sources[0].Value.BaseBranch || current.Value.Repository != registration.Value.Definition.Repository {
		return saved, ErrProductSubjectHeld
	}
	origin, err := k.taskInputDigestTx(ctx, tx, w, task.Spec)
	if err != nil {
		return saved, err
	}
	candidateDigest, err := contentDigest(current.Value.Candidate)
	if err != nil {
		return saved, err
	}
	value := ProductCandidateSubject{TaskID: task.ID, SessionID: b.ID, ParentSessionID: w.OrchestratorSessionID, Target: task.Target(), Spec: task.Spec, WorkItemID: task.Spec.WorkItemIDs[0], RegistrationDigest: registration.Digest, OriginInputDigest: origin, Candidate: current.Value.Candidate, CandidateDigest: candidateDigest, Sources: sources}
	digest, err := contentDigest(value)
	if err != nil {
		return saved, err
	}
	saved = ProductCandidateSubjectSnapshot{ID: productSubjectID(task.ID), Value: value, Digest: digest, CreatedAt: k.clock()}
	checksum, err := contentDigest(saved)
	if err != nil {
		return saved, err
	}
	for _, kind := range []string{"product_candidate_subject", "product_candidate_subject_history"} {
		if err := kernelRecordTx(ctx, tx, kind, saved.ID, w.ID, productSubjectRecord{Subject: saved, Digest: checksum}); err != nil {
			return saved, err
		}
	}
	if err := k.materializeTaskInputTx(ctx, tx, w, task); err != nil {
		return saved, err
	}
	if !sourcePermit.current(k, tx) {
		return saved, ErrProductSubjectHeld
	}
	return saved, tx.Commit()
}

func (k *KernelStore) productSubjectSourcesTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) ([]ProductTaskCandidateSnapshot, error) {
	primary := map[string]ProductTaskCandidateSnapshot{}
	contextual := map[string]ProductTaskCandidateSnapshot{}
	for _, dep := range task.Spec.DependsOn {
		producer, err := loadTask(ctx, tx, structureID("task-", w.ID, dep.TaskID))
		if err != nil {
			return nil, err
		}
		if err := currentObservedTaskTx(ctx, tx, w, producer); err != nil {
			return nil, err
		}
		result, err := k.completedTaskResultTx(ctx, tx, w, producer)
		if err != nil {
			return nil, err
		}
		if result.SchemaVersion != 2 && result.SchemaVersion != 3 {
			continue
		}
		source, err := loadProductTaskCandidate(ctx, tx, producer.ResultID)
		if err != nil {
			return nil, err
		}
		if source.Value.Reference.WorkItemID == task.Spec.WorkItemIDs[0] {
			primary[source.Value.ResultID] = source
		}
	}
	for _, id := range task.Spec.SourceResultIDs {
		input, err := k.taskResultInputTx(ctx, tx, w, id)
		if err != nil {
			return nil, err
		}
		if input.ProductCandidate != nil && input.ProductCandidate.Value.Reference.WorkItemID == task.Spec.WorkItemIDs[0] {
			contextual[id] = *input.ProductCandidate
		}
	}
	if len(primary) == 0 {
		primary = contextual
	}
	if len(primary) == 0 || len(primary) > 32 {
		return nil, ErrProductSubjectHeld
	}
	ids := make([]string, 0, len(primary))
	for id := range primary {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	sources := make([]ProductTaskCandidateSnapshot, 0, len(ids))
	for _, id := range ids {
		source := primary[id]
		if len(sources) > 0 && (source.Value.Candidate != sources[0].Value.Candidate || source.Value.Repository != sources[0].Value.Repository || source.Value.BaseBranch != sources[0].Value.BaseBranch) {
			return nil, ErrProductSubjectHeld
		}
		sources = append(sources, source)
	}
	return sources, nil
}
