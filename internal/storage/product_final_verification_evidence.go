package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

var ErrProductFinalEvidenceBound = errors.New("final verification evidence exceeds its bounded input")

type ProductFinalVerificationJob struct {
	ProfileID        string `json:"profile_id"`
	JobID            string `json:"job_id"`
	JobDigest        string `json:"job_digest,omitempty"`
	State            string `json:"state"`
	Status           string `json:"status,omitempty"`
	TestedSHA        string `json:"tested_sha,omitempty"`
	CompletionDigest string `json:"completion_digest,omitempty"`
	ResultDigest     string `json:"result_digest,omitempty"`
	LogDigest        string `json:"log_digest,omitempty"`
	LogBytes         int64  `json:"log_bytes,omitempty"`
}
type ProductFinalVerificationRepository struct {
	RepositoryID int64                            `json:"repository_id"`
	Repository   string                           `json:"repository"`
	BaseBranch   string                           `json:"base_branch"`
	Candidate    runner.Candidate                 `json:"candidate"`
	Policy       ProductVerificationPlanReference `json:"policy"`
	Profiles     []string                         `json:"profiles"`
	Jobs         []ProductFinalVerificationJob    `json:"jobs"`
}
type ProductFinalVerificationUnavailable struct {
	RepositoryID int64  `json:"repository_id,omitempty"`
	ProfileID    string `json:"profile_id,omitempty"`
	JobID        string `json:"job_id,omitempty"`
	Reason       string `json:"reason"`
}
type ProductFinalVerificationValue struct {
	Target         contract.Target                       `json:"target"`
	PlanTarget     contract.Target                       `json:"plan_target"`
	Plan           ProductVerificationPlanReference      `json:"plan"`
	CoverageDigest string                                `json:"coverage_digest"`
	Current        bool                                  `json:"current"`
	Complete       bool                                  `json:"complete"`
	Passed         bool                                  `json:"passed"`
	State          string                                `json:"state"`
	Repositories   []ProductFinalVerificationRepository  `json:"repositories"`
	Unavailable    []ProductFinalVerificationUnavailable `json:"unavailable"`
}
type ProductFinalVerificationEvidence struct {
	Value  ProductFinalVerificationValue `json:"value"`
	Digest string                        `json:"digest"`
}

// ReadProductFinalVerificationEvidence observes persisted exact-SHA execution
// facts. It neither refreshes remote HEAD/source nor grants final acceptance.
// Consumers must independently recheck fresh selection/control at an effect.
func (k *KernelStore) ReadProductFinalVerificationEvidence(ctx context.Context, target contract.Target) (ProductFinalVerificationEvidence, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductFinalVerificationEvidence{}, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return ProductFinalVerificationEvidence{}, err
	}
	if w.Target() != target || w.Policy.Stage != "P" || !w.Policy.Product {
		return ProductFinalVerificationEvidence{}, ErrProductFinalPlanHeld
	}
	return k.productFinalVerificationEvidenceTx(ctx, tx, w)
}
func finalVerificationEvidence(value ProductFinalVerificationValue) (ProductFinalVerificationEvidence, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return ProductFinalVerificationEvidence{}, err
	}
	if len(raw) > 1<<20 {
		return ProductFinalVerificationEvidence{}, ErrProductFinalEvidenceBound
	}
	digest, err := contentDigest(value)
	return ProductFinalVerificationEvidence{Value: value, Digest: digest}, err
}
func (k *KernelStore) productFinalVerificationEvidenceTx(ctx context.Context, tx *sql.Tx, w domain.Wave) (ProductFinalVerificationEvidence, error) {
	value := ProductFinalVerificationValue{Target: w.Target(), State: "not_planned", Repositories: []ProductFinalVerificationRepository{}, Unavailable: []ProductFinalVerificationUnavailable{}}
	unavailable := func(reason string) (ProductFinalVerificationEvidence, error) {
		value.State = "unavailable"
		value.Unavailable = append(value.Unavailable, ProductFinalVerificationUnavailable{Reason: reason})
		return finalVerificationEvidence(value)
	}
	var selection ProductFinalPlanSelection
	err := taskEvidenceRecord(ctx, tx, "product_final_plan_current", w.ID, &selection)
	if err == sql.ErrNoRows {
		var admitted bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE work_item_id=? AND kind IN ('product_final_plan','product_final_plan_history','product_final_plan_selection_history')) OR EXISTS(SELECT 1 FROM kernel_dispatch_intents WHERE wave_id=? AND run_id LIKE 'product-final-runner-%')", w.ID, w.ID).Scan(&admitted); err != nil {
			return ProductFinalVerificationEvidence{}, err
		}
		if admitted {
			return unavailable("current_selection_missing")
		}
		return finalVerificationEvidence(value)
	}
	if err != nil {
		return unavailable("current_selection_invalid")
	}
	plan, err := k.readProductFinalPlanTx(ctx, tx, selection.ID)
	if err != nil {
		return unavailable("plan_evidence_unavailable")
	}
	value.Plan = ProductVerificationPlanReference{ID: plan.ID, Digest: plan.Digest}
	value.PlanTarget, value.CoverageDigest = plan.Value.Target, plan.Value.CoverageDigest
	if err := validateFinalSelection(ctx, tx, plan, selection); err != nil {
		return unavailable("selection_evidence_unavailable")
	}
	coverage, err := k.productCoverageTx(ctx, tx, w)
	if errors.Is(err, ErrProductCoverageHeld) {
		return unavailable("coverage_metadata_exceeds_bound")
	}
	if err != nil {
		return ProductFinalVerificationEvidence{}, err
	}
	value.Current = plan.Value.Target == w.Target() && coverage.Digest == plan.Value.CoverageDigest && len(coverage.Value.Merged) > 0 && len(coverage.Value.Residual)+len(coverage.Value.UnmergedWorkItems)+len(coverage.Value.Pending)+len(coverage.Value.Unavailable) == 0
	if value.Current {
		if err := k.requireProductSplitFinalSourcesTx(ctx, tx, w, coverage, plan.Value.SplitSourcesDigest); err != nil {
			if ctx.Err() != nil {
				return ProductFinalVerificationEvidence{}, ctx.Err()
			}
			if !readinessHoldError(err) && !errors.Is(err, ErrProductFinalPlanHeld) {
				return ProductFinalVerificationEvidence{}, err
			}
			value.Current = false
			return unavailable("split_sources_changed_or_unavailable")
		}
	}
	jobs := 0
	for _, repo := range plan.Value.Repositories {
		jobs += len(repo.Profiles)
	}
	if jobs < 1 || jobs > 1024 {
		return ProductFinalVerificationEvidence{}, ErrProductFinalEvidenceBound
	}
	complete, passed := true, true
	for _, repo := range plan.Value.Repositories {
		observed := ProductFinalVerificationRepository{RepositoryID: repo.RepositoryID, Repository: repo.Repository, BaseBranch: repo.BaseBranch, Candidate: repo.Candidate, Policy: repo.Policy, Profiles: repo.Profiles, Jobs: []ProductFinalVerificationJob{}}
		for _, profile := range repo.Profiles {
			id := productFinalRunnerJobID(plan.ID, repo.RepositoryID, profile)
			entry := ProductFinalVerificationJob{ProfileID: profile, JobID: id, State: "not_run"}
			job, err := loadProductRunnerJob(ctx, tx, id)
			if err == sql.ErrNoRows {
				admitted, checkErr := productRunnerJobAdmittedTx(ctx, tx, id)
				if checkErr != nil {
					return ProductFinalVerificationEvidence{}, checkErr
				}
				if admitted {
					err = ErrTaskEvidence
				}
			}
			if err != nil && err != sql.ErrNoRows {
				entry.State = "unavailable"
				value.Unavailable = append(value.Unavailable, ProductFinalVerificationUnavailable{RepositoryID: repo.RepositoryID, ProfileID: profile, JobID: id, Reason: "runner_evidence_unavailable"})
			} else if err == nil {
				// The common loader authenticates the complete source/policy/selection,
				// exact Candidate/profile, terminal receipt and capacity release proof.
				if job.Value.Final == nil || job.Value.PlanID != plan.ID || job.Value.PlanDigest != plan.Digest || job.Value.Final.RepositoryID != repo.RepositoryID || job.Value.Spec.Candidate != repo.Candidate || job.Value.Spec.ProfileID != profile {
					return ProductFinalVerificationEvidence{}, ErrTaskEvidence
				}
				entry.JobDigest, entry.State = job.Digest, job.State
				if job.State == "completed" {
					c := job.Completion
					entry.Status, entry.TestedSHA = c.Result.Status, c.Result.TestedSHA
					entry.ResultDigest, entry.LogDigest, entry.LogBytes = c.ResultDigest, c.LogDigest, c.LogBytes
					entry.CompletionDigest, err = contentDigest(c)
					if err != nil {
						return ProductFinalVerificationEvidence{}, err
					}
				}
			}
			complete = complete && entry.State == "completed"
			passed = passed && entry.State == "completed" && entry.Status == "passed" && entry.TestedSHA == repo.Candidate.SHA
			observed.Jobs = append(observed.Jobs, entry)
		}
		value.Repositories = append(value.Repositories, observed)
	}
	value.Complete = complete && len(value.Unavailable) == 0
	value.Passed = value.Current && value.Complete && passed
	switch {
	case len(value.Unavailable) > 0:
		value.State = "unavailable"
	case !value.Current:
		value.State = "stale"
	case value.Passed:
		value.State = "passed"
	case value.Complete:
		value.State = "failed"
	default:
		value.State = "pending"
	}
	return finalVerificationEvidence(value)
}
