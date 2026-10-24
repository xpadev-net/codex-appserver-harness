package product

import (
	"context"
	"errors"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// FinalCommitFetcher is a synchronous host Git owner. Unknown starts or missing
// receipts never trigger a second command. A completed receipt remains a
// historical object-availability fact, not current selection or test success.
type FinalCommitFetcher struct {
	Planner FinalPlanner
	Remotes map[int64]string
}

func (f FinalCommitFetcher) Ensure(ctx context.Context, target contract.Target, repositoryID int64) (storage.ProductFinalFetch, error) {
	var saved storage.ProductFinalFetch
	k := f.Planner.Store
	remote := f.Remotes[repositoryID]
	if k == nil || remote == "" {
		return saved, storage.ErrProductFinalFetchHeld
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	plan, err := f.Planner.Prepare(ctx, target)
	if err != nil {
		return saved, err
	}
	var repo *storage.ProductFinalRepository
	for i := range plan.Value.Repositories {
		if plan.Value.Repositories[i].RepositoryID == repositoryID {
			repo = &plan.Value.Repositories[i]
			break
		}
	}
	if repo == nil {
		return saved, storage.ErrProductFinalFetchHeld
	}
	policy, err := k.ReadProductVerificationPolicy(ctx, target, repositoryID)
	if err != nil {
		return saved, err
	}
	if policy.ID != repo.Policy.ID || policy.Digest != repo.Policy.Digest {
		return saved, storage.ErrTaskEvidence
	}
	if err := sameUserDirectory(k.Root, true); err != nil {
		return saved, err
	}
	if err := sameUserDirectory(policy.Value.Repository, false); err != nil {
		return saved, err
	}
	pin, err := workspace.PinRepository(workspace.WithStoppedCommands(ctx), policy.Value.Repository)
	if err != nil {
		return saved, err
	}
	for _, path := range []string{pin.GitDir, pin.CommonDir} {
		if err := sameUserDirectory(path, false); err != nil {
			return saved, err
		}
	}
	saved, err = k.ReserveProductFinalFetch(ctx, storage.ProductFinalFetchRequest{PlanID: plan.ID, RepositoryID: repositoryID, Source: pin, Remote: remote})
	if err != nil {
		return saved, err
	}
	if saved.State == "available" {
		return saved, nil
	}
	saved, start, err := k.ClaimProductFinalFetch(ctx, saved.ID, saved.Digest)
	if err != nil {
		return saved, err
	}
	if !start {
		return saved, storage.ErrProductFinalFetchHeld
	}
	observed, fetchErr := workspace.FetchPinnedCommit(ctx, saved.Value.Request.Source, saved.Value.Request.Remote, saved.Value.SHA, func(send context.Context) error {
		// Re-observe actual source/HEAD/capability policy immediately before the
		// durable send fence, rather than converting a past plan into permission.
		current, err := f.Planner.Prepare(send, target)
		if err != nil {
			return err
		}
		if current.ID != saved.Value.Request.PlanID || current.Digest != saved.Value.PlanDigest {
			return storage.ErrProductFinalFetchHeld
		}
		return k.AuthorizeProductFinalFetch(send, saved.ID, saved.Digest)
	})
	if errors.Is(fetchErr, workspace.ErrGitStopUnconfirmed) {
		return saved, fetchErr
	}
	outcome := "failed"
	reason := "fixed commit Git owner stopped without an available commit"
	if observed.Available && fetchErr == nil {
		outcome = "available"
		reason = "fixed commit available after Git owner stopped"
	} else {
		observed.Available = false
	}
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer stop()
	finished, finishErr := k.FinishProductFinalFetch(cleanup, saved.ID, saved.Digest, storage.ProductFinalFetchProof{ProofID: saved.ID + ":stopped", Observation: observed, Outcome: outcome, Reason: reason})
	if finishErr != nil {
		return saved, errors.Join(fetchErr, finishErr)
	}
	return finished, fetchErr
}
