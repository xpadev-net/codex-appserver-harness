package storage

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

var ErrProductRunnerHeld = errors.New("product runner requires owner reconciliation")

// ProductRunnerRuntime is private host configuration, never model input. The
// profile command comes exclusively from the previously pinned policy.
type ProductRunnerRuntime struct {
	Root             string   `json:"root"`
	LauncherContract string   `json:"launcher_contract"`
	Launcher         []string `json:"launcher"`
	ForbiddenPaths   []string `json:"forbidden_paths"`
}

func (runtime ProductRunnerRuntime) Validate() error { return validateProductRunnerRuntime(runtime) }

type ProductRunnerJobValue struct {
	Final        *ProductFinalRunnerSource `json:"final,omitempty"`
	RetryOf      string                    `json:"retry_of,omitempty"`
	PlanID       string                    `json:"plan_id"`
	PlanDigest   string                    `json:"plan_digest"`
	PolicyID     string                    `json:"policy_id"`
	PolicyDigest string                    `json:"policy_digest"`
	SessionID    string                    `json:"session_id"`
	Spec         runner.Spec               `json:"spec"`
}

// A launching intent survives restart and cannot be claimed a second time.
// Absence of a result is not proof that launching did not start a process.
type ProductRunnerJob struct {
	FinalClaim *ProductFinalRunnerClaim `json:"final_claim,omitempty"`
	Completion *runner.Completion       `json:"completion,omitempty"`
	FinishedAt *time.Time               `json:"finished_at,omitempty"`
	ID         string                   `json:"id"`
	Value      ProductRunnerJobValue    `json:"value"`
	Digest     string                   `json:"digest"`
	State      string                   `json:"state"`
	CreatedAt  time.Time                `json:"created_at"`
	ClaimedAt  time.Time                `json:"claimed_at"`
}

type ProductRunnerClaim struct {
	Job   ProductRunnerJob `json:"job"`
	Start bool             `json:"start"`
}

type productRunnerRecord struct {
	Job    ProductRunnerJob `json:"job"`
	Digest string           `json:"digest"`
}

func productRunnerJobID(planID, profileID string) string {
	return structureID("product-runner-", planID, profileID)
}

func saveProductRunnerJobTx(ctx context.Context, tx *sql.Tx, job ProductRunnerJob) error {
	digest, err := contentDigest(job)
	if err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, "product_runner_job", job.ID, job.Value.PlanID, productRunnerRecord{Job: job, Digest: digest})
}

func loadProductRunnerJob(ctx context.Context, q queryer, id string) (job ProductRunnerJob, err error) {
	var record productRunnerRecord
	if err := taskEvidenceRecord(ctx, q, "product_runner_job", id, &record); err != nil {
		return record.Job, err
	}
	// Once the head exists, missing subsidiary evidence is corruption, not an
	// absent job eligible for fresh admission.
	defer func() {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrTaskEvidence
		}
	}()
	job = record.Job
	digest, err := contentDigest(job)
	if err != nil {
		return job, err
	}
	valueDigest, err := contentDigest(job.Value)
	if err != nil {
		return job, err
	}
	expectedID := productRunnerJobID(job.Value.PlanID, job.Value.Spec.ProfileID)
	if job.Value.Final != nil {
		expectedID = productFinalRunnerJobID(job.Value.PlanID, job.Value.Final.RepositoryID, job.Value.Spec.ProfileID)
		if job.Value.RetryOf != "" {
			return job, ErrTaskEvidence
		}
	}
	if job.Value.RetryOf != "" {
		expectedID = productRunnerRetryID(job.Value.RetryOf)
	}
	if record.Digest != digest || job.Digest != valueDigest || job.ID != id || id != expectedID || job.CreatedAt.IsZero() || job.Value.Spec.JobID != id || job.Value.Spec.Validate() != nil || job.State != "prepared" && job.State != "launching" && job.State != "completed" || (job.State == "prepared") != job.ClaimedAt.IsZero() {
		return job, ErrTaskEvidence
	}
	if job.State == "completed" {
		if job.Completion == nil || job.FinishedAt == nil || job.FinishedAt.IsZero() || job.Completion.Validate(job.Value.Spec) != nil || job.Completion.Result.Started.Before(job.ClaimedAt) {
			return job, ErrTaskEvidence
		}
		var terminal ProductRunnerJob
		if err := taskEvidenceRecord(ctx, q, "product_runner_job_terminal", id, &terminal); err != nil {
			return job, err
		}
		if !reflect.DeepEqual(terminal, job) {
			return job, ErrTaskEvidence
		}
		r, err := loadOperationReservationTx(ctx, q, id)
		if err != nil {
			return job, err
		}
		if r.State != "released" || r.Proof == nil || *r.Proof != productRunnerReleaseProof(job) {
			return job, ErrTaskEvidence
		}
	} else if job.Completion != nil || job.FinishedAt != nil {
		return job, ErrTaskEvidence
	}
	var history ProductRunnerJobValue
	if err := taskEvidenceRecord(ctx, q, "product_runner_job_history", id, &history); err != nil {
		return job, err
	}
	if !reflect.DeepEqual(history, job.Value) {
		return job, ErrTaskEvidence
	}
	if job.Value.RetryOf != "" {
		previous, err := loadProductRunnerJob(ctx, q, job.Value.RetryOf)
		if err != nil {
			return job, err
		}
		if !productRunnerRetryValueMatches(previous, job.Value) {
			return job, ErrTaskEvidence
		}
	}
	if job.Value.Final == nil && job.FinalClaim != nil {
		return job, ErrTaskEvidence
	}
	if job.Value.Final != nil {
		return job, validateProductFinalRunnerSource(ctx, q, job)
	}
	plan, err := loadProductVerificationPlan(ctx, q, job.Value.PlanID)
	if err != nil {
		return job, err
	}
	sealed, err := sealedProductEvidence(ctx, q, plan.Value.TaskID, plan.Value.RunID)
	if err != nil {
		return job, err
	}
	policy, err := loadProductVerificationPolicy(ctx, q, plan.Value.Target.WaveID, plan.Value.Target.PolicyRevision, sealed.Value.Verification.Policy.Value.RepositoryID)
	if err != nil {
		return job, err
	}
	profiles, err := plan.Value.Plan.Profiles(policy.Value.Verification)
	if err != nil {
		return job, err
	}
	s := job.Value.Spec
	if job.Value.SessionID != sealed.Value.Workspace.Value.ParentSessionID || job.Value.PlanDigest != plan.Digest || job.Value.PolicyID != plan.Value.PolicyID || job.Value.PolicyDigest != plan.Value.PolicyDigest || policy.ID != job.Value.PolicyID || policy.Digest != job.Value.PolicyDigest || s.Candidate != plan.Value.Candidate || s.Repository != policy.Value.Repository || !slices.Contains(profiles, s.ProfileID) || !reflect.DeepEqual(s.Profile, policy.Value.Verification.Profiles[s.ProfileID]) {
		return job, ErrTaskEvidence
	}
	r, err := loadOperationReservationTx(ctx, q, id)
	if err != nil {
		return job, err
	}
	if r.Kind != "runner" || r.SessionID != job.Value.SessionID || r.Request.Target != plan.Value.Target || r.Request.RetryOf != job.Value.RetryOf || r.Request.Worktree != filepath.Join(s.Root, id) || job.State != "completed" && r.State != "held" {
		return job, ErrTaskEvidence
	}
	return job, nil
}

func (k *KernelStore) ReadProductRunnerJob(ctx context.Context, id string) (ProductRunnerJob, error) {
	return loadProductRunnerJob(ctx, k.DB, id)
}

func validateProductRunnerRuntime(runtime ProductRunnerRuntime) error {
	if !filepath.IsAbs(runtime.Root) || filepath.Clean(runtime.Root) != runtime.Root || len(runtime.Launcher) == 0 || len(runtime.Launcher) > 128 || len(runtime.ForbiddenPaths) > 128 {
		return errors.New("bounded private runner runtime required")
	}
	for _, value := range append(append([]string{runtime.Root, runtime.LauncherContract}, runtime.Launcher...), runtime.ForbiddenPaths...) {
		if !utf8.ValidString(value) || len(value) > 8192 || strings.ContainsRune(value, '\x00') {
			return errors.New("bounded UTF-8 runner runtime required")
		}
	}
	canonical, err := resourcePath(runtime.Root)
	if err != nil {
		return err
	}
	if canonical != runtime.Root {
		return ErrProductRunnerHeld
	}
	for _, path := range runtime.ForbiddenPaths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("absolute forbidden runner path required")
		}
	}
	return nil
}

func (k *KernelStore) productRunnerPreflightTx(ctx context.Context, tx *sql.Tx, plan ProductVerificationPlanSnapshot, allowCarried bool) (domain.Wave, error) {
	w, err := loadWave(ctx, tx, plan.Value.Target.WaveID)
	if err != nil {
		return w, err
	}
	if err := w.Validate(); err != nil {
		return w, err
	}
	sameVersion := w.Target() == plan.Value.Target
	// Read-only evidence may survive explicit carry. currentObservedTaskTx below
	// verifies its current-plan result/input binding; dispatch always passes false.
	if allowCarried && w.ID == plan.Value.Target.WaveID && w.ScopeRevision == plan.Value.Target.ScopeRevision && w.Policy.Revision == plan.Value.Target.PolicyRevision && w.PlanRevision > plan.Value.Target.PlanRevision {
		sameVersion = true
	}
	if !sameVersion || w.Policy.Stage != "P" || !w.Policy.Product || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return w, ErrProductRunnerHeld
	}
	task, err := loadTask(ctx, tx, plan.Value.TaskID)
	if err != nil {
		return w, err
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return w, err
	}
	if task.State != contract.TaskCompleted || task.ResultID != plan.Value.ResultID || task.Paused {
		return w, ErrProductRunnerHeld
	}
	source, err := loadProductTaskCandidate(ctx, tx, plan.Value.ResultID)
	if err != nil {
		return w, err
	}
	if source.Value.VerificationPlan == nil || source.Value.VerificationPlan.ID != plan.ID || source.Value.VerificationPlan.Digest != plan.Digest {
		return w, ErrTaskEvidence
	}
	held, err := productWorkItemExecutionHeld(ctx, tx, w, plan.Value.Plan.WorkItemID)
	if err != nil {
		return w, err
	}
	target, err := loadProductTargetTx(ctx, tx, w.Target(), plan.Value.Plan.WorkItemID)
	if err != nil {
		return w, err
	}
	if held || target.Value.Candidate != plan.Value.Candidate {
		return w, ErrProductRunnerHeld
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, plan.Value.Plan.WorkItemID)
	if err != nil {
		return w, err
	}
	sealed, err := sealedProductEvidence(ctx, tx, plan.Value.TaskID, plan.Value.RunID)
	if err != nil {
		return w, err
	}
	scope, err := productVerificationScopeTx(ctx, tx, w, registration)
	if err != nil {
		return w, err
	}
	digest, err := contentDigest(scope)
	if err != nil {
		return w, err
	}
	if !productRegistrationScope(registration, w) || digest != sealed.Value.Verification.ScopeDigest {
		return w, ErrProductRunnerHeld
	}
	return w, nil
}

// ReserveProductRunnerJob saves a job and capacity/budget/workspace lease in one
// transaction. It launches nothing, consumes no LLM slot and grants no success.
func (k *KernelStore) ReserveProductRunnerJob(ctx context.Context, planID, profileID string, runtime ProductRunnerRuntime) (ProductRunnerJob, error) {
	var job ProductRunnerJob
	if err := validateProductRunnerRuntime(runtime); err != nil {
		return job, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	plan, err := loadProductVerificationPlan(ctx, tx, planID)
	if err != nil {
		return job, err
	}
	sealed, err := sealedProductEvidence(ctx, tx, plan.Value.TaskID, plan.Value.RunID)
	if err != nil {
		return job, err
	}
	p := sealed.Value.Verification.Policy
	profiles, err := plan.Value.Plan.Profiles(p.Value.Verification)
	if err != nil || !slices.Contains(profiles, profileID) {
		return job, ErrProductRunnerHeld
	}
	id := productRunnerJobID(planID, profileID)
	spec := runner.Spec{JobID: id, Repository: p.Value.Repository, Root: runtime.Root, Candidate: plan.Value.Candidate, ProfileID: profileID, Profile: p.Value.Verification.Profiles[profileID], LauncherContract: runtime.LauncherContract, Launcher: slices.Clone(runtime.Launcher), ForbiddenPaths: slices.Clone(runtime.ForbiddenPaths)}
	if err := spec.Validate(); err != nil {
		return job, err
	}
	w, err := loadWave(ctx, tx, plan.Value.Target.WaveID)
	if err != nil {
		return job, err
	}
	value := ProductRunnerJobValue{PlanID: plan.ID, PlanDigest: plan.Digest, PolicyID: p.ID, PolicyDigest: p.Digest, SessionID: w.OrchestratorSessionID, Spec: spec}
	job, err = loadProductRunnerJob(ctx, tx, id)
	if err == nil {
		if !reflect.DeepEqual(job.Value, value) {
			return job, ErrProductRunnerHeld
		}
		return job, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return job, err
	}
	var history bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_runner_job_history' AND id=?)", id).Scan(&history); err != nil {
		return job, err
	}
	if history {
		return job, ErrTaskEvidence
	}
	if _, err := k.productRunnerPreflightTx(ctx, tx, plan, false); err != nil {
		return job, err
	}
	reservation, err := k.reserveOperationCapacityTx(ctx, tx, OperationCapacityRequest{ID: id, Kind: "runner", SessionID: value.SessionID, Target: w.Target(), Worktree: filepath.Join(spec.Root, id)})
	if err != nil {
		return job, err
	}
	if reservation.State != "held" {
		return job, ErrProductRunnerHeld
	}
	digest, err := contentDigest(value)
	if err != nil {
		return job, err
	}
	job = ProductRunnerJob{ID: id, Value: value, Digest: digest, State: "prepared", CreatedAt: k.clock()}
	if err := kernelRecordTx(ctx, tx, "product_runner_job_history", id, plan.ID, value); err != nil {
		return job, err
	}
	if err := saveProductRunnerJobTx(ctx, tx, job); err != nil {
		return job, err
	}
	return job, tx.Commit()
}

// ClaimProductRunnerJob commits before process launch. Replay returns Start=false
// even when no completion file exists; only a trusted reconciler can settle it.
func (k *KernelStore) ClaimProductRunnerJob(ctx context.Context, id, digest string) (ProductRunnerClaim, error) {
	var claim ProductRunnerClaim
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return claim, err
	}
	defer tx.Rollback()
	job, err := loadProductRunnerJob(ctx, tx, id)
	if err != nil {
		return claim, err
	}
	claim.Job = job
	if job.Digest != digest {
		return claim, ErrProductRunnerHeld
	}
	if job.State != "prepared" {
		return claim, tx.Commit()
	}
	if job.Value.Final != nil {
		return k.claimProductFinalRunnerTx(ctx, tx, job)
	}
	plan, err := loadProductVerificationPlan(ctx, tx, job.Value.PlanID)
	if err != nil {
		return claim, err
	}
	w, err := k.productRunnerPreflightTx(ctx, tx, plan, false)
	if err != nil {
		return claim, err
	}
	s := job.Value.Spec
	if err := validateProductRunnerRuntime(ProductRunnerRuntime{Root: s.Root, LauncherContract: s.LauncherContract, Launcher: s.Launcher, ForbiddenPaths: s.ForbiddenPaths}); err != nil {
		return claim, err
	}
	r, err := loadOperationReservationTx(ctx, tx, id)
	if err != nil {
		return claim, err
	}
	if r.Kind != "runner" || r.State != "held" || r.Request.Target != w.Target() || r.SessionID != w.OrchestratorSessionID || job.Value.SessionID != r.SessionID || r.Request.Worktree != filepath.Join(s.Root, id) {
		return claim, ErrProductRunnerHeld
	}
	if err := operationCapacityOwnerTx(ctx, tx, r.Request, k.clock()); err != nil {
		return claim, err
	}
	job.State, job.ClaimedAt = "launching", k.clock()
	if err := saveProductRunnerJobTx(ctx, tx, job); err != nil {
		return claim, err
	}
	claim.Job = job
	if err := tx.Commit(); err != nil {
		return claim, err
	}
	claim.Start = true
	return claim, nil
}
