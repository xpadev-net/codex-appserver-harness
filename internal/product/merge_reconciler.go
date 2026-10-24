package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// MergeReconciler measures only a positive original-head merged PR after the
// saved physical owner stop. It has no write transport or business grant; open,
// absent and changed resources stay held even after the client has stopped.
type MergeReconciler struct {
	Store *storage.KernelStore
	API   func(storage.ProductTarget) ResourceAPI
}

func (r MergeReconciler) Reconcile(ctx context.Context, id string) (storage.ProductMerge, error) {
	var empty storage.ProductMerge
	if r.Store == nil || r.API == nil {
		return empty, errors.New("merge reconciliation store and read transport required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	job, err := r.Store.ReadProductMerge(ctx, id)
	if err != nil {
		return empty, err
	}
	if job.State == "merged" {
		return job, nil
	}
	if job.State != "stopped_unknown" || job.Stop == nil || job.Stop.Disposition != "outcome_unknown" {
		return empty, storage.ErrProductMergeHeld
	}
	p := job.Value.Product.Value
	transport := r.API(p)
	if transport == nil {
		return empty, errors.New("merge reconciliation read transport missing")
	}
	ctx = github.WithResourceReadBudget(ctx, 8<<20)
	api := &budgetResourceAPI{ResourceAPI: transport}
	readRepo := func() (Repository, error) {
		var repo Repository
		raw, err := api.ResourceGET(ctx, "")
		if err != nil {
			return repo, err
		}
		err = json.Unmarshal(raw, &repo)
		return repo, err
	}
	repo, err := readRepo()
	if err != nil {
		return empty, err
	}
	if repo.ID != p.WorkItem.RepositoryID || repo.Name != p.Repository || repo.DefaultBranch != p.BaseBranch {
		return empty, storage.ErrProductMergeHeld
	}
	readPR := func() (github.PR, error) {
		var pr github.PR
		raw, err := api.ResourceGET(ctx, fmt.Sprintf("pulls/%d", p.PRNumber))
		if err != nil {
			return pr, err
		}
		err = json.Unmarshal(raw, &pr)
		return pr, err
	}
	pr, err := readPR()
	if err != nil {
		return empty, err
	}
	if !matched(pr, storage.PRDispatchClaim{Product: job.Value.Product}) || pr.State != "closed" || !pr.Merged || pr.Draft || len(pr.MergeCommitSHA) != 40 || strings.Trim(pr.MergeCommitSHA, "0123456789abcdef") != "" {
		return empty, storage.ErrProductMergeHeld
	}
	finalPR, err := readPR()
	if err != nil {
		return empty, err
	}
	finalRepo, err := readRepo()
	if err != nil {
		return empty, err
	}
	if finalPR != pr || finalRepo.ID != repo.ID || finalRepo.Name != repo.Name || finalRepo.DefaultBranch != repo.DefaultBranch {
		return empty, storage.ErrProductMergeHeld
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	proofID, err := observationID()
	if err != nil {
		return empty, err
	}
	now := time.Now().UTC()
	if r.Store.Now != nil {
		now = r.Store.Now().UTC()
	}
	proof := storage.ProductMergeReconciliation{MergeID: job.ID, StopProofID: job.Stop.ProofID, MergeSHA: pr.MergeCommitSHA, Observation: storage.PRResourceObservation{ProofID: "merge-" + proofID, RepositoryID: repo.ID, HeadRepositoryID: pr.Head.Repo.ID, BaseRepositoryID: pr.Base.Repo.ID, Number: pr.Number, MarkerWorkItemID: p.WorkItem.ID, HeadSHA: pr.Head.SHA, HeadBranch: pr.Head.Ref, BaseBranch: pr.Base.Ref, State: pr.State, Merged: pr.Merged, Draft: pr.Draft, ObservedAt: now}}
	return r.Store.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, proof)
}
