package storage

import (
	"context"
	"database/sql"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

// ProductRunnerObservation describes only the persisted job selected by the
// canonical plan. Awaiting a result supplies no inference about process absence.
type ProductRunnerObservation struct {
	ProfileID  string             `json:"profile_id"`
	JobID      string             `json:"job_id"`
	JobDigest  string             `json:"job_digest"`
	State      string             `json:"state"`
	Completion *runner.Completion `json:"completion,omitempty"`
}

type ProductVerificationEvidenceValue struct {
	Plan      ProductVerificationPlanReference `json:"plan"`
	Policy    ProductVerificationPlanReference `json:"policy"`
	Candidate runner.Candidate                 `json:"candidate"`
	Profiles  []string                         `json:"profiles"`
	Jobs      []ProductRunnerObservation       `json:"jobs"`
	Complete  bool                             `json:"complete"`
	Passed    bool                             `json:"passed"`
}

type ProductVerificationEvidence struct {
	Value  ProductVerificationEvidenceValue `json:"value"`
	Digest string                           `json:"digest"`
}

// ReadProductVerificationEvidence recomputes a current machine-verification
// gate in a single transaction. No caller supplies results, profiles, Candidate
// or a pass Boolean. The returned snapshot is not publication/merge approval;
// consumers must recheck it at their own effect boundary, never cache Passed.
func (k *KernelStore) ReadProductVerificationEvidence(ctx context.Context, planID string) (ProductVerificationEvidence, error) {
	var saved ProductVerificationEvidence
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	return k.productVerificationEvidenceTx(ctx, tx, planID)
}

func (k *KernelStore) productVerificationEvidenceTx(ctx context.Context, tx *sql.Tx, planID string) (ProductVerificationEvidence, error) {
	var saved ProductVerificationEvidence
	plan, err := loadProductVerificationPlan(ctx, tx, planID)
	if err != nil {
		return saved, err
	}
	if _, err := k.productRunnerPreflightTx(ctx, tx, plan, true); err != nil {
		return saved, err
	}
	return productVerificationEvidenceFromPlan(ctx, tx, plan)
}

// Static audit of the immutable plan, policy and stopped runner evidence. It
// grants no new execution or publication. Current gates run preflight above;
// merged coverage uses this audit after current execution is no longer allowed.
func productVerificationEvidenceFromPlan(ctx context.Context, q queryer, plan ProductVerificationPlanSnapshot) (ProductVerificationEvidence, error) {
	var saved ProductVerificationEvidence
	sealed, err := sealedProductEvidence(ctx, q, plan.Value.TaskID, plan.Value.RunID)
	if err != nil {
		return saved, err
	}
	p := sealed.Value.Verification.Policy
	current, err := loadProductVerificationPolicy(ctx, q, p.Value.WaveID, p.Value.PolicyRevision, p.Value.RepositoryID)
	if err != nil {
		return saved, err
	}
	if current.ID != plan.Value.PolicyID || current.Digest != plan.Value.PolicyDigest {
		return saved, ErrTaskEvidence
	}
	profiles, err := plan.Value.Plan.Profiles(current.Value.Verification)
	if err != nil {
		return saved, err
	}
	value := ProductVerificationEvidenceValue{Plan: ProductVerificationPlanReference{ID: plan.ID, Digest: plan.Digest}, Policy: ProductVerificationPlanReference{ID: current.ID, Digest: current.Digest}, Candidate: plan.Value.Candidate, Profiles: profiles, Jobs: []ProductRunnerObservation{}}
	results := []runner.Result{}
	for _, profileID := range profiles {
		id := productRunnerJobID(plan.ID, profileID)
		observation := ProductRunnerObservation{ProfileID: profileID, JobID: id, State: "not_run"}
		job, err := latestProductRunnerJobTx(ctx, q, plan.ID, profileID)
		if errors.Is(err, sql.ErrNoRows) {
			// A missing head after admission is lost evidence, never a new job.
			admitted, err := productRunnerJobAdmittedTx(ctx, q, id)
			if err != nil {
				return saved, err
			}
			if admitted {
				return saved, ErrTaskEvidence
			}
		} else if err != nil {
			return saved, err
		} else {
			if job.Value.PlanID != plan.ID || job.Value.PlanDigest != plan.Digest || job.Value.Spec.Candidate != value.Candidate || job.Value.Spec.ProfileID != profileID {
				return saved, ErrTaskEvidence
			}
			observation.JobID, observation.JobDigest, observation.State = job.ID, job.Digest, job.State
			if job.State == "completed" {
				observation.Completion = job.Completion
				results = append(results, job.Completion.Result)
			}
		}
		value.Jobs = append(value.Jobs, observation)
	}
	value.Complete = len(results) == len(profiles)
	value.Passed = value.Complete && verification.PassedPlan(value.Candidate, current.Value.Verification, plan.Value.Plan, results)
	digest, err := contentDigest(value)
	if err != nil {
		return saved, err
	}
	return ProductVerificationEvidence{Value: value, Digest: digest}, nil
}
