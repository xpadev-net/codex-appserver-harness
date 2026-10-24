package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func finalPlanFixture(t *testing.T) (*KernelStore, ProductMerge, ProductCoverageLedger, []ProductFinalHeadObservation) {
	t.Helper()
	ctx := context.Background()
	k, job := knownCoverageFixture(t)
	receipt, _, err := k.RecordProductMergedWorkItem(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	coverage, err := k.ProductCoverage(ctx, job.Value.Product.Value.Target)
	if err != nil {
		t.Fatal(err)
	}
	p := job.Value.Product.Value
	heads := []ProductFinalHeadObservation{{RepositoryID: p.WorkItem.RepositoryID, Repository: p.Repository, BaseBranch: p.BaseBranch, HeadSHA: strings.Repeat("f", 40), AncestorMergeSHAs: []string{receipt.Value.MergeSHA}, ObservedAt: k.clock()}}
	return k, job, coverage, heads
}

func TestProductFinalPlanPinsMergedBaseAndReplaysWithoutExecution(t *testing.T) {
	ctx := context.Background()
	k, job, coverage, heads := finalPlanFixture(t)
	target := job.Value.Product.Value.Target
	before, err := k.WaveOperationBudget(ctx, target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := k.PrepareProductFinalPlan(ctx, target, coverage.Digest, heads)
	if err != nil {
		t.Fatal(err)
	}
	repo := plan.Value.Repositories[0]
	if repo.Candidate.SHA != heads[0].HeadSHA || repo.Candidate.SHA == job.Value.Product.Value.Candidate.SHA || repo.Candidate.BaseSHA != repo.Candidate.SHA || !reflect.DeepEqual(repo.Profiles, []string{"unit"}) || len(repo.MergeReceipts) != 1 {
		t.Fatal(plan)
	}
	original := heads[0].HeadSHA
	heads[0].HeadSHA = strings.Repeat("a", 40)
	later, err := k.PrepareProductFinalPlan(ctx, target, coverage.Digest, heads)
	if err != nil || later.ID == plan.ID {
		t.Fatal(later, err)
	}
	heads[0].HeadSHA = original
	now := k.clock().Add(time.Second)
	k.Now = func() time.Time { return now }
	heads[0].ObservedAt = now
	replay, err := k.PrepareProductFinalPlan(ctx, target, coverage.Digest, heads)
	if err != nil || !reflect.DeepEqual(replay, plan) {
		t.Fatal("same head duplicated or retimestamped plan", replay, err)
	}
	var current ProductVerificationPlanReference
	if err := k.Record(ctx, "product_final_plan_current", target.WaveID, &current); err != nil || current.ID != plan.ID {
		t.Fatal("reselected old head did not update current pointer", current, err)
	}
	read, err := k.ReadProductFinalPlan(ctx, plan.ID)
	if err != nil || !reflect.DeepEqual(read, plan) {
		t.Fatal(read, err)
	}
	after, err := k.WaveOperationBudget(ctx, target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("planning spent execution budget", after, err)
	}
	w, err := k.Wave(ctx, target.WaveID)
	if err != nil || w.State == domain.WaveCompleted {
		t.Fatal("plan became acceptance", w, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_final_runner_job','wave_completion')").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	read, err = k.ReadProductFinalPlan(ctx, plan.ID)
	if err != nil || !reflect.DeepEqual(read, plan) {
		t.Fatal("immutable plan not retained on reopen", read, err)
	}
}

func TestProductFinalPlanRejectsUnprovenOrChangedScope(t *testing.T) {
	for _, mode := range []string{"digest", "sha", "repo", "base", "stale", "future", "ancestor", "extra_ancestor", "duplicate", "pause", "new_candidate", "receipt_loss"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, coverage, heads := finalPlanFixture(t)
			target := job.Value.Product.Value.Target
			digest := coverage.Digest
			switch mode {
			case "digest":
				digest = "wrong"
			case "sha":
				heads[0].HeadSHA = "master"
			case "repo":
				heads[0].RepositoryID++
			case "base":
				heads[0].BaseBranch = "other"
			case "stale":
				heads[0].ObservedAt = k.clock().Add(-31 * time.Second)
			case "future":
				heads[0].ObservedAt = k.clock().Add(time.Second)
			case "ancestor":
				heads[0].AncestorMergeSHAs = nil
			case "extra_ancestor":
				heads[0].AncestorMergeSHAs = append(heads[0].AncestorMergeSHAs, strings.Repeat("b", 40))
			case "duplicate":
				heads = append(heads, heads[0])
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "final-pause", Target: target, Action: "pause", Reason: "hold final verification"}); err != nil {
					t.Fatal(err)
				}
			case "new_candidate":
				current, err := k.ReadProductTarget(ctx, target, job.Value.Product.Value.WorkItem.ID)
				if err != nil {
					t.Fatal(err)
				}
				p := current.Value
				p.Candidate.ID = "new-final-candidate"
				p.WorkItem.Candidate = p.Candidate.ID
				p.WorkItem.Version++
				if _, err := k.SetProductTarget(ctx, current.Digest, p); err != nil {
					t.Fatal(err)
				}
			case "receipt_loss":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merged_work_item' AND id=?", job.ID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.PrepareProductFinalPlan(ctx, target, digest, heads); err == nil {
				t.Fatal("unproven final plan admitted")
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_final_plan%'").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial plan", count, err)
			}
		})
	}
}

func TestProductFinalPlanMissingEvidenceAndRollbackCannotRegenerate(t *testing.T) {
	for _, mode := range []string{"head", "history", "admission", "receipt", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, coverage, heads := finalPlanFixture(t)
			target := job.Value.Product.Value.Target
			if mode == "rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_final_plan BEFORE INSERT ON records WHEN NEW.kind='product_final_plan_history' BEGIN SELECT RAISE(FAIL,'final plan rollback'); END"); err != nil {
					t.Fatal(err)
				}
				if _, err := k.PrepareProductFinalPlan(ctx, target, coverage.Digest, heads); err == nil {
					t.Fatal("rollback ignored")
				}
				var count int
				if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_final_plan%'").Scan(&count); err != nil || count != 0 {
					t.Fatal(count, err)
				}
				return
			}
			plan, err := k.PrepareProductFinalPlan(ctx, target, coverage.Digest, heads)
			if err != nil {
				t.Fatal(err)
			}
			kind := map[string]string{"head": "product_final_plan", "history": "product_final_plan_history", "admission": "product_final_plan_admission", "receipt": "product_merged_work_item"}[mode]
			id := plan.ID
			if mode == "receipt" {
				id = job.ID
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductFinalPlan(ctx, plan.ID); !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("missing evidence authenticated", err)
			}
			if _, err := k.PrepareProductFinalPlan(ctx, target, coverage.Digest, heads); err == nil {
				t.Fatal("lost evidence regenerated")
			}
		})
	}
}

func TestProductFinalPlanCarriesRequiredRequestedAndMappedProfiles(t *testing.T) {
	ctx := context.Background()
	k, w, plan, _ := productVerificationEvidenceFixture(t)
	policy, err := k.ReadProductVerificationPolicy(ctx, w.Target(), 17)
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := productFinalVerificationProfiles(ctx, k.DB, policy, map[string]string{plan.ID: "machine-evidence"})
	if err != nil || !reflect.DeepEqual(profiles, []string{"unit", "integration", "mapped", "requested"}) {
		t.Fatal("optional independent verification profiles lost", profiles, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_verification_plan_history' AND id=?", plan.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := productFinalVerificationProfiles(ctx, k.DB, policy, map[string]string{plan.ID: "machine-evidence"}); err == nil {
		t.Fatal("missing original verification plan admitted")
	}
}
