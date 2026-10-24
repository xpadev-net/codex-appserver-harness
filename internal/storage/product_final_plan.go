package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

var ErrProductFinalPlanHeld = errors.New("final verification requires complete merged scope and fresh fixed repository heads")

// Observations come from the trusted read adapter, never a model tool. The
// adapter must bookend the repository/ref and positively check every ancestry.
type ProductFinalHeadObservation struct {
	RepositoryID      int64     `json:"repository_id"`
	Repository        string    `json:"repository"`
	BaseBranch        string    `json:"base_branch"`
	HeadSHA           string    `json:"head_sha"`
	AncestorMergeSHAs []string  `json:"ancestor_merge_shas"`
	ObservedAt        time.Time `json:"observed_at"`
}
type ProductFinalRepository struct {
	RepositoryID  int64                            `json:"repository_id"`
	Repository    string                           `json:"repository"`
	BaseBranch    string                           `json:"base_branch"`
	Candidate     runner.Candidate                 `json:"candidate"`
	Policy        ProductVerificationPlanReference `json:"policy"`
	Profiles      []string                         `json:"profiles"`
	MergeReceipts map[string]string                `json:"merge_receipts"`
}
type ProductFinalPlanValue struct {
	Target         contract.Target          `json:"target"`
	CoverageDigest string                   `json:"coverage_digest"`
	Repositories   []ProductFinalRepository `json:"repositories"`
}
type ProductFinalPlan struct {
	ID        string                        `json:"id"`
	Value     ProductFinalPlanValue         `json:"value"`
	Digest    string                        `json:"digest"`
	Admission []ProductFinalHeadObservation `json:"admission"`
	CreatedAt time.Time                     `json:"created_at"`
}

func productFinalPlanID(value ProductFinalPlanValue) (string, string, error) {
	digest, err := contentDigest(value)
	return structureID("product-final-plan-", value.Target.WaveID, digest), digest, err
}

func (k *KernelStore) readProductFinalPlanTx(ctx context.Context, q *sql.Tx, id string) (ProductFinalPlan, error) {
	var plan, history ProductFinalPlan
	err := taskEvidenceRecord(ctx, q, "product_final_plan", id, &plan)
	if err == sql.ErrNoRows {
		var orphan bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_final_plan_history','product_final_plan_admission'))", id).Scan(&orphan); err != nil {
			return plan, err
		}
		if orphan {
			return plan, ErrTaskEvidence
		}
		return plan, sql.ErrNoRows
	}
	if err != nil {
		return plan, err
	}
	var admission string
	if err := taskEvidenceRecord(ctx, q, "product_final_plan_history", id, &history); err != nil {
		return plan, errors.Join(ErrTaskEvidence, err)
	}
	if err := taskEvidenceRecord(ctx, q, "product_final_plan_admission", id, &admission); err != nil {
		return plan, errors.Join(ErrTaskEvidence, err)
	}
	expectedID, digest, err := productFinalPlanID(plan.Value)
	if err != nil || expectedID != id || plan.ID != id || digest != plan.Digest || admission != digest || !reflect.DeepEqual(history, plan) || plan.CreatedAt.IsZero() || plan.CreatedAt.After(k.clock()) || !plan.Value.Target.Valid() || len(plan.Value.Repositories) < 1 || len(plan.Value.Repositories) > 128 || len(plan.Admission) != len(plan.Value.Repositories) {
		return plan, ErrTaskEvidence
	}
	for i, repo := range plan.Value.Repositories {
		head := plan.Admission[i]
		if repo.RepositoryID < 1 || i > 0 && repo.RepositoryID <= plan.Value.Repositories[i-1].RepositoryID || repo.Candidate.ID != structureID("product-final-candidate-", plan.Value.CoverageDigest, fmt.Sprint(repo.RepositoryID)+":"+repo.Candidate.SHA) {
			return plan, ErrTaskEvidence
		}
		policy, err := loadProductVerificationPolicy(ctx, q, plan.Value.Target.WaveID, plan.Value.Target.PolicyRevision, repo.RepositoryID)
		if err != nil {
			return plan, errors.Join(ErrTaskEvidence, err)
		}
		profiles, err := policy.Value.Verification.ExecutionSet(repo.Profiles)
		if err != nil || repo.Policy.ID != policy.ID || repo.Policy.Digest != policy.Digest || !slices.Equal(profiles, repo.Profiles) || len(repo.MergeReceipts) == 0 || !fixedSHA(repo.Candidate.SHA) || repo.Candidate.BaseSHA != repo.Candidate.SHA || repo.Candidate.ScopeRevision != plan.Value.Target.ScopeRevision || repo.Candidate.PolicyRevision != plan.Value.Target.PolicyRevision || repo.RepositoryID != head.RepositoryID || repo.Repository != head.Repository || repo.BaseBranch != head.BaseBranch || repo.Candidate.SHA != head.HeadSHA || head.ObservedAt.IsZero() || head.ObservedAt.After(plan.CreatedAt) || plan.CreatedAt.Sub(head.ObservedAt) > 30*time.Second {
			return plan, ErrTaskEvidence
		}
		ancestors := map[string]bool{}
		for _, sha := range head.AncestorMergeSHAs {
			if !fixedSHA(sha) || ancestors[sha] {
				return plan, ErrTaskEvidence
			}
			ancestors[sha] = true
		}
		expectedAncestors := map[string]bool{}
		expectedProfiles := map[string]bool{}
		for mergeID, receiptDigest := range repo.MergeReceipts {
			job, err := k.readProductMergeTx(ctx, q, mergeID)
			if err != nil {
				return plan, errors.Join(ErrTaskEvidence, err)
			}
			receipt, err := readMergedWorkItem(ctx, q, job, k.clock())
			if err != nil {
				return plan, errors.Join(ErrTaskEvidence, err)
			}
			p := receipt.Value.Product.Value
			if receipt.Digest != receiptDigest || p.Target.WaveID != plan.Value.Target.WaveID || p.Target.ScopeRevision != plan.Value.Target.ScopeRevision || p.Target.PolicyRevision != plan.Value.Target.PolicyRevision || p.WorkItem.RepositoryID != repo.RepositoryID || p.Repository != repo.Repository || p.BaseBranch != repo.BaseBranch || !ancestors[receipt.Value.MergeSHA] || receipt.Value.RecordedAt.After(plan.CreatedAt) {
				return plan, ErrTaskEvidence
			}
			expectedAncestors[receipt.Value.MergeSHA] = true
			var ready ProductPublicationReadiness
			if err := taskEvidenceRecord(ctx, q, "product_publication_readiness_history", job.Value.ReadinessID, &ready); err != nil {
				return plan, errors.Join(ErrTaskEvidence, err)
			}
			profiles, err := productFinalVerificationProfiles(ctx, q, policy, ready.Value.Quality.Value.Verifications)
			if err != nil {
				return plan, errors.Join(ErrTaskEvidence, err)
			}
			for _, profile := range profiles {
				expectedProfiles[profile] = true
			}
		}
		var requested []string
		for profile := range expectedProfiles {
			requested = append(requested, profile)
		}
		expected, err := policy.Value.Verification.ExecutionSet(requested)
		if err != nil || !slices.Equal(expected, repo.Profiles) {
			return plan, ErrTaskEvidence
		}
		if len(expectedAncestors) != len(ancestors) {
			return plan, ErrTaskEvidence
		}
	}
	return plan, nil
}

func (k *KernelStore) ReadProductFinalPlan(ctx context.Context, id string) (ProductFinalPlan, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductFinalPlan{}, err
	}
	defer tx.Rollback()
	return k.readProductFinalPlanTx(ctx, tx, id)
}

// PrepareProductFinalPlan pins the whole merged scope and the union of host
// required/independent verifier profiles to actual observed default-branch SHAs.
// It reserves no runner, issues no pass result and cannot complete a Wave.
func (k *KernelStore) PrepareProductFinalPlan(ctx context.Context, target contract.Target, coverageDigest string, heads []ProductFinalHeadObservation) (ProductFinalPlan, error) {
	var plan ProductFinalPlan
	encoded, err := json.Marshal(heads)
	var detached []ProductFinalHeadObservation
	if err != nil || len(encoded) > 1<<20 || json.Unmarshal(encoded, &detached) != nil || len(detached) < 1 || len(detached) > 128 {
		return plan, ErrProductFinalPlanHeld
	}
	heads = detached
	sort.Slice(heads, func(i, j int) bool { return heads[i].RepositoryID < heads[j].RepositoryID })
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return plan, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return plan, err
	}
	if w.Target() != target || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return plan, ErrProductFinalPlanHeld
	}
	coverage, err := k.productCoverageTx(ctx, tx, w)
	if err != nil {
		return plan, err
	}
	if coverage.Digest != coverageDigest || len(coverage.Value.Merged) == 0 || len(coverage.Value.Residual)+len(coverage.Value.UnmergedWorkItems)+len(coverage.Value.Pending)+len(coverage.Value.Unavailable) != 0 {
		return plan, ErrProductFinalPlanHeld
	}
	byRepo := map[int64]*ProductFinalRepository{}
	profileSets := map[int64]map[string]bool{}
	ancestorSets := map[int64]map[string]bool{}
	for _, fact := range coverage.Value.Merged {
		job, err := k.readProductMergeTx(ctx, tx, fact.MergeID)
		if err != nil {
			return plan, err
		}
		p := job.Value.Product.Value
		policy, err := loadProductVerificationPolicy(ctx, tx, target.WaveID, target.PolicyRevision, p.WorkItem.RepositoryID)
		if err != nil {
			return plan, err
		}
		repo := byRepo[p.WorkItem.RepositoryID]
		if repo == nil {
			repo = &ProductFinalRepository{RepositoryID: p.WorkItem.RepositoryID, Repository: p.Repository, BaseBranch: p.BaseBranch, Policy: ProductVerificationPlanReference{ID: policy.ID, Digest: policy.Digest}, MergeReceipts: map[string]string{}}
			byRepo[repo.RepositoryID], profileSets[repo.RepositoryID], ancestorSets[repo.RepositoryID] = repo, map[string]bool{}, map[string]bool{}
		}
		if repo.Repository != p.Repository || repo.BaseBranch != p.BaseBranch {
			return plan, ErrTaskEvidence
		}
		repo.MergeReceipts[fact.MergeID] = fact.ReceiptDigest
		ancestorSets[repo.RepositoryID][fact.MergeSHA] = true
		var ready ProductPublicationReadiness
		if err := taskEvidenceRecord(ctx, tx, "product_publication_readiness_history", job.Value.ReadinessID, &ready); err != nil {
			return plan, err
		}
		profiles, err := productFinalVerificationProfiles(ctx, tx, policy, ready.Value.Quality.Value.Verifications)
		if err != nil {
			return plan, err
		}
		for _, id := range profiles {
			profileSets[repo.RepositoryID][id] = true
		}

	}
	if len(heads) != len(byRepo) {
		return plan, ErrProductFinalPlanHeld
	}
	plan.Value = ProductFinalPlanValue{Target: target, CoverageDigest: coverageDigest, Repositories: []ProductFinalRepository{}}
	plan.CreatedAt, plan.Admission = k.clock().UTC(), heads
	for i := range heads {
		head := &heads[i]
		repo := byRepo[head.RepositoryID]
		if repo == nil || i > 0 && head.RepositoryID == heads[i-1].RepositoryID || head.Repository != repo.Repository || head.BaseBranch != repo.BaseBranch || !fixedSHA(head.HeadSHA) || head.ObservedAt.IsZero() || head.ObservedAt.After(plan.CreatedAt) || plan.CreatedAt.Sub(head.ObservedAt) > 30*time.Second {
			return plan, ErrProductFinalPlanHeld
		}
		sort.Strings(head.AncestorMergeSHAs)
		if len(head.AncestorMergeSHAs) != len(ancestorSets[repo.RepositoryID]) {
			return plan, ErrProductFinalPlanHeld
		}
		for j, sha := range head.AncestorMergeSHAs {
			if !ancestorSets[repo.RepositoryID][sha] || j > 0 && sha == head.AncestorMergeSHAs[j-1] {
				return plan, ErrProductFinalPlanHeld
			}
		}
		policy, err := loadProductVerificationPolicy(ctx, tx, target.WaveID, target.PolicyRevision, repo.RepositoryID)
		if err != nil {
			return plan, err
		}
		var requested []string
		for id := range profileSets[repo.RepositoryID] {
			requested = append(requested, id)
		}
		repo.Profiles, err = policy.Value.Verification.ExecutionSet(requested)
		if err != nil {
			return plan, err
		}
		repo.Candidate = runner.Candidate{ID: structureID("product-final-candidate-", coverageDigest, fmt.Sprint(repo.RepositoryID)+":"+head.HeadSHA), SHA: head.HeadSHA, BaseSHA: head.HeadSHA, ScopeRevision: target.ScopeRevision, PolicyRevision: target.PolicyRevision}
		plan.Value.Repositories = append(plan.Value.Repositories, *repo)
	}
	encoded, err = json.Marshal(plan)
	if err != nil || len(encoded) > 1<<20 {
		return ProductFinalPlan{}, ErrProductFinalPlanHeld
	}
	plan.ID, plan.Digest, err = productFinalPlanID(plan.Value)
	if err != nil {
		return plan, err
	}
	if old, err := k.readProductFinalPlanTx(ctx, tx, plan.ID); err == nil {
		if !reflect.DeepEqual(old.Value, plan.Value) {
			return plan, ErrTaskEvidence
		}
		if err := kernelRecordTx(ctx, tx, "product_final_plan_current", target.WaveID, target.WaveID, ProductVerificationPlanReference{ID: old.ID, Digest: old.Digest}); err != nil {
			return plan, err
		}
		return old, tx.Commit()
	} else if err != sql.ErrNoRows {
		return plan, err
	}
	for kind, value := range map[string]any{"product_final_plan": plan, "product_final_plan_history": plan, "product_final_plan_admission": plan.Digest} {
		if err := kernelRecordTx(ctx, tx, kind, plan.ID, target.WaveID, value); err != nil {
			return plan, err
		}
	}
	if err := kernelRecordTx(ctx, tx, "product_final_plan_current", target.WaveID, target.WaveID, ProductVerificationPlanReference{ID: plan.ID, Digest: plan.Digest}); err != nil {
		return plan, err
	}
	return plan, tx.Commit()
}

// Authenticate the original independent plans before carrying any optional
// profile into final merged-base verification.
func productFinalVerificationProfiles(ctx context.Context, q queryer, policy ProductVerificationPolicySnapshot, plans map[string]string) ([]string, error) {
	requested := map[string]bool{}
	for id := range plans {
		plan, err := loadProductVerificationPlan(ctx, q, id)
		if err != nil {
			return nil, err
		}
		if plan.Value.PolicyID != policy.ID || plan.Value.PolicyDigest != policy.Digest {
			return nil, ErrTaskEvidence
		}
		profiles, err := plan.Value.Plan.Profiles(policy.Value.Verification)
		if err != nil {
			return nil, err
		}
		for _, profile := range profiles {
			requested[profile] = true
		}
	}
	var profiles []string
	for profile := range requested {
		profiles = append(profiles, profile)
	}
	return policy.Value.Verification.ExecutionSet(profiles)
}
