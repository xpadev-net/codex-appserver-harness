package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

// A new observation of the same HEAD refreshes selection evidence without
// changing the immutable plan or its original admission time.
type ProductFinalPlanSelection struct {
	ID         string                        `json:"id"`
	Digest     string                        `json:"digest"`
	Heads      []ProductFinalHeadObservation `json:"heads"`
	ObservedAt time.Time                     `json:"observed_at"`
}
type ProductFinalRunnerClaim struct {
	Selection ProductFinalPlanSelection `json:"selection"`
	ClaimedAt time.Time                 `json:"claimed_at"`
}
type ProductFinalRunnerSource struct {
	RepositoryID int64                     `json:"repository_id"`
	Selection    ProductFinalPlanSelection `json:"selection"`
}

func finalSelectionHistoryID(selection ProductFinalPlanSelection) (string, error) {
	digest, err := contentDigest(selection)
	return structureID("product-final-selection-", selection.ID, digest), err
}
func (k *KernelStore) saveProductFinalSelectionTx(ctx context.Context, tx *sql.Tx, plan ProductFinalPlan, heads []ProductFinalHeadObservation) error {
	selection := ProductFinalPlanSelection{ID: plan.ID, Digest: plan.Digest, Heads: heads, ObservedAt: k.clock()}
	id, err := finalSelectionHistoryID(selection)
	if err != nil {
		return err
	}
	if err := kernelRecordTx(ctx, tx, "product_final_plan_selection_history", id, plan.Value.Target.WaveID, selection); err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, "product_final_plan_current", plan.Value.Target.WaveID, plan.Value.Target.WaveID, selection)
}
func validateFinalSelection(ctx context.Context, q queryer, plan ProductFinalPlan, selection ProductFinalPlanSelection) error {
	if selection.ID != plan.ID || selection.Digest != plan.Digest || selection.ObservedAt.Before(plan.CreatedAt) || len(selection.Heads) != len(plan.Admission) {
		return ErrTaskEvidence
	}
	id, err := finalSelectionHistoryID(selection)
	if err != nil {
		return err
	}
	var history ProductFinalPlanSelection
	if err := taskEvidenceRecord(ctx, q, "product_final_plan_selection_history", id, &history); err != nil {
		return errors.Join(ErrTaskEvidence, err)
	}
	if !reflect.DeepEqual(history, selection) {
		return ErrTaskEvidence
	}
	for i, head := range selection.Heads {
		original := plan.Admission[i]
		if head.ObservedAt.IsZero() || head.ObservedAt.After(selection.ObservedAt) || selection.ObservedAt.Sub(head.ObservedAt) > 30*time.Second {
			return ErrTaskEvidence
		}
		original.ObservedAt = head.ObservedAt
		if !reflect.DeepEqual(original, head) {
			return ErrTaskEvidence
		}
	}
	return nil
}
func productFinalRunnerJobID(planID string, repositoryID int64, profileID string) string {
	return structureID("product-final-runner-", planID, fmt.Sprint(repositoryID)+":"+profileID)
}
func finalRunnerRepository(plan ProductFinalPlan, id int64) (ProductFinalRepository, error) {
	for _, repo := range plan.Value.Repositories {
		if repo.RepositoryID == id {
			return repo, nil
		}
	}
	return ProductFinalRepository{}, ErrProductRunnerHeld
}
func validateProductFinalRunnerSource(ctx context.Context, q queryer, job ProductRunnerJob) error {
	// The saved selection is historical proof. Current source/HEAD checks belong
	// to reserve/claim; stopped results must remain recordable after cancellation.
	source := job.Value.Final
	if source == nil || source.RepositoryID < 1 || job.Value.RetryOf != "" {
		return ErrTaskEvidence
	}
	// Read-only final-plan authentication does not require a current Wave target.
	k := &KernelStore{}
	plan, err := k.readProductFinalPlanTx(ctx, q, job.Value.PlanID)
	if err != nil {
		return err
	}
	if source.Selection.ObservedAt.After(job.CreatedAt) || job.CreatedAt.Before(plan.CreatedAt) {
		return ErrTaskEvidence
	}
	if err := validateFinalSelection(ctx, q, plan, source.Selection); err != nil {
		return err
	}
	if job.State == "prepared" {
		if job.FinalClaim != nil {
			return ErrTaskEvidence
		}
		var granted bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_final_runner_claim' AND id=?)", job.ID).Scan(&granted); err != nil {
			return err
		}
		if granted {
			return ErrTaskEvidence
		}
	} else {
		if job.FinalClaim == nil || !job.FinalClaim.ClaimedAt.Equal(job.ClaimedAt) {
			return ErrTaskEvidence
		}
		claimed := job.FinalClaim.Selection
		if err := validateFinalSelection(ctx, q, plan, claimed); err != nil {
			return err
		}
		if claimed.ObservedAt.After(job.ClaimedAt) || job.ClaimedAt.Sub(claimed.ObservedAt) > 30*time.Second {
			return ErrTaskEvidence
		}
		for _, head := range claimed.Heads {
			if head.ObservedAt.After(job.ClaimedAt) || job.ClaimedAt.Sub(head.ObservedAt) > 30*time.Second {
				return ErrTaskEvidence
			}
		}
		var history ProductFinalRunnerClaim
		if err := taskEvidenceRecord(ctx, q, "product_final_runner_claim", job.ID, &history); err != nil {
			return err
		}
		if !reflect.DeepEqual(history, *job.FinalClaim) {
			return ErrTaskEvidence
		}
	}
	repo, err := finalRunnerRepository(plan, source.RepositoryID)
	if err != nil {
		return err
	}
	policy, err := loadProductVerificationPolicy(ctx, q, plan.Value.Target.WaveID, plan.Value.Target.PolicyRevision, repo.RepositoryID)
	if err != nil {
		return err
	}
	s := job.Value.Spec
	if job.Value.PlanDigest != plan.Digest || job.Value.PolicyID != policy.ID || job.Value.PolicyDigest != policy.Digest || repo.Policy.ID != policy.ID || repo.Policy.Digest != policy.Digest || s.Candidate != repo.Candidate || s.Repository != policy.Value.Repository || !slices.Contains(repo.Profiles, s.ProfileID) || !reflect.DeepEqual(s.Profile, policy.Value.Verification.Profiles[s.ProfileID]) {
		return ErrTaskEvidence
	}
	r, err := loadOperationReservationTx(ctx, q, job.ID)
	if err != nil {
		return err
	}
	w, err := loadWave(ctx, q, plan.Value.Target.WaveID)
	if err != nil {
		return err
	}
	parent := w.OrchestratorSessionID
	if job.Value.SessionID != parent || r.Kind != "runner" || r.SessionID != parent || r.Request.Target != plan.Value.Target || r.Request.RetryOf != "" || r.Request.Worktree != filepath.Join(s.Root, job.ID) || job.State != "completed" && r.State != "held" {
		return ErrTaskEvidence
	}
	return nil
}
func (k *KernelStore) finalRunnerPreflightTx(ctx context.Context, tx *sql.Tx, plan ProductFinalPlan) (ProductFinalPlanSelection, domain.Wave, error) {
	var selection ProductFinalPlanSelection
	w, err := loadWave(ctx, tx, plan.Value.Target.WaveID)
	if err != nil {
		return selection, w, err
	}
	if w.Target() != plan.Value.Target || w.Policy.Stage != "P" || !w.Policy.Product || w.DispatchPaused() || w.State == domain.WaveBlocked || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) {
		return selection, w, ErrProductRunnerHeld
	}
	coverage, err := k.productCoverageTx(ctx, tx, w)
	if err != nil {
		return selection, w, err
	}
	if coverage.Digest != plan.Value.CoverageDigest || len(coverage.Value.Merged) == 0 || len(coverage.Value.Residual)+len(coverage.Value.UnmergedWorkItems)+len(coverage.Value.Pending)+len(coverage.Value.Unavailable) != 0 {
		return selection, w, ErrProductRunnerHeld
	}
	if err := taskEvidenceRecord(ctx, tx, "product_final_plan_current", w.ID, &selection); err != nil {
		return selection, w, err
	}
	if err := validateFinalSelection(ctx, tx, plan, selection); err != nil {
		return selection, w, err
	}
	if selection.ObservedAt.After(k.clock()) || k.clock().Sub(selection.ObservedAt) > 30*time.Second {
		return selection, w, ErrProductRunnerHeld
	}
	for _, head := range selection.Heads {
		if head.ObservedAt.After(k.clock()) || k.clock().Sub(head.ObservedAt) > 30*time.Second {
			return selection, w, ErrProductRunnerHeld
		}
	}
	return selection, w, nil
}

// ReserveProductFinalRunnerJob uses the ordinary runner capacity, budget,
// workspace lease and immutable lifecycle. It never creates a Task or success.
func (k *KernelStore) ReserveProductFinalRunnerJob(ctx context.Context, planID string, repoID int64, profileID string, runtime ProductRunnerRuntime) (ProductRunnerJob, error) {
	var job ProductRunnerJob
	if err := validateProductRunnerRuntime(runtime); err != nil {
		return job, err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	plan, err := k.readProductFinalPlanTx(ctx, tx, planID)
	if err != nil {
		return job, err
	}
	repo, err := finalRunnerRepository(plan, repoID)
	if err != nil {
		return job, err
	}
	if !slices.Contains(repo.Profiles, profileID) {
		return job, ErrProductRunnerHeld
	}
	policy, err := loadProductVerificationPolicy(ctx, tx, plan.Value.Target.WaveID, plan.Value.Target.PolicyRevision, repoID)
	if err != nil {
		return job, err
	}
	id := productFinalRunnerJobID(planID, repoID, profileID)
	spec := runner.Spec{JobID: id, Repository: policy.Value.Repository, Root: runtime.Root, Candidate: repo.Candidate, ProfileID: profileID, Profile: policy.Value.Verification.Profiles[profileID], LauncherContract: runtime.LauncherContract, Launcher: slices.Clone(runtime.Launcher), ForbiddenPaths: slices.Clone(runtime.ForbiddenPaths)}
	if err := spec.Validate(); err != nil {
		return job, err
	}
	if old, err := loadProductRunnerJob(ctx, tx, id); err == nil {
		if old.Value.Final == nil || old.Value.Final.RepositoryID != repoID || !reflect.DeepEqual(old.Value.Spec, spec) {
			return job, ErrProductRunnerHeld
		}
		return old, tx.Commit()
	} else if err != sql.ErrNoRows {
		return job, err
	}
	orphan, err := productRunnerJobAdmittedTx(ctx, tx, id)
	if err != nil {
		return job, err
	}
	if orphan {
		return job, ErrTaskEvidence
	}
	selection, w, err := k.finalRunnerPreflightTx(ctx, tx, plan)
	if err != nil {
		return job, err
	}
	reservation, err := k.reserveOperationCapacityTx(ctx, tx, OperationCapacityRequest{ID: id, Kind: "runner", SessionID: w.OrchestratorSessionID, Target: w.Target(), Worktree: filepath.Join(spec.Root, id)})
	if err != nil {
		return job, err
	}
	if reservation.State != "held" {
		return job, ErrProductRunnerHeld
	}
	value := ProductRunnerJobValue{Final: &ProductFinalRunnerSource{RepositoryID: repoID, Selection: selection}, PlanID: plan.ID, PlanDigest: plan.Digest, PolicyID: policy.ID, PolicyDigest: policy.Digest, SessionID: w.OrchestratorSessionID, Spec: spec}
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
func (k *KernelStore) claimProductFinalRunnerTx(ctx context.Context, tx *sql.Tx, job ProductRunnerJob) (ProductRunnerClaim, error) {
	claim := ProductRunnerClaim{Job: job}
	plan, err := k.readProductFinalPlanTx(ctx, tx, job.Value.PlanID)
	if err != nil {
		return claim, err
	}
	selection, w, err := k.finalRunnerPreflightTx(ctx, tx, plan)
	if err != nil {
		return claim, err
	}
	s := job.Value.Spec
	if err := validateProductRunnerRuntime(ProductRunnerRuntime{Root: s.Root, LauncherContract: s.LauncherContract, Launcher: s.Launcher, ForbiddenPaths: s.ForbiddenPaths}); err != nil {
		return claim, err
	}
	r, err := loadOperationReservationTx(ctx, tx, job.ID)
	if err != nil {
		return claim, err
	}
	if err := operationCapacityOwnerTx(ctx, tx, r.Request, k.clock()); err != nil {
		return claim, err
	}
	if r.State != "held" || r.Request.Target != w.Target() {
		return claim, ErrProductRunnerHeld
	}
	job.State, job.ClaimedAt = "launching", k.clock()
	job.FinalClaim = &ProductFinalRunnerClaim{Selection: selection, ClaimedAt: job.ClaimedAt}
	if err := kernelRecordTx(ctx, tx, "product_final_runner_claim", job.ID, plan.ID, *job.FinalClaim); err != nil {
		return claim, err
	}
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
