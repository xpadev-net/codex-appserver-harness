package product

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// BranchPublisher is the controller's synchronous Git adapter. Push must join
// all commands before returning, or return ErrGitStopUnconfirmed. Production
// callers explicitly supply workspace.PublishPinned; workers never receive it.
type BranchPublisher struct {
	Store *storage.KernelStore
	Gate  PublicationGate
	API   APIFactory
	Push  func(context.Context, workspace.FrozenCheckout, string, string, func(context.Context) error) (workspace.BranchPublication, error)
}

func (p BranchPublisher) now() time.Time {
	if p.Store.Now != nil {
		return p.Store.Now().UTC()
	}
	return time.Now().UTC()
}

func branchRemote(ctx context.Context, api API, target storage.ProductTarget) (storage.ProductBranchRemote, error) {
	r, err := api.Repository(ctx)
	if err != nil {
		return storage.ProductBranchRemote{}, err
	}
	if r.ID != target.WorkItem.RepositoryID || r.Name != target.Repository || r.DefaultBranch != target.BaseBranch || r.CloneURL == "" {
		return storage.ProductBranchRemote{}, errors.New("branch repository identity/default branch changed")
	}
	return storage.ProductBranchRemote{RepositoryID: r.ID, Repository: r.Name, DefaultBranch: r.DefaultBranch, URL: r.CloneURL}, nil
}

// Publish owns only the branch sub-operation. HTTP publication retains its own
// one-use fence. Replays never relaunch a claimed Git owner, even when the
// observed remote already matches the candidate.
func (p BranchPublisher) Publish(ctx context.Context, claim storage.PRDispatchClaim) error {
	if p.Store == nil || p.Gate == nil || p.API == nil || p.Push == nil {
		return errors.New("explicit branch store, gate, read transport and joined Git transport required")
	}
	publication, err := p.Gate(ctx, claim.Product)
	if err != nil {
		return err
	}
	api := p.API(claim.Product.Value, func(context.Context, string, string) error {
		return errors.New("branch metadata transport forbids HTTP writes")
	})
	if api == nil {
		return errors.New("missing branch metadata transport")
	}
	remote, err := branchRemote(ctx, api, claim.Product.Value)
	if err != nil {
		return err
	}
	job, err := p.Store.ReserveProductBranchPublication(ctx, claim.OperationID, claim.OwnerID, remote)
	if err != nil {
		return err
	}
	switch job.State {
	case "stopped":
		if job.Stop != nil && !job.Stop.Observation.Attempted && job.MayHaveSentAt.IsZero() {
			job, err = p.Store.ReserveProductBranchUnsentRetry(ctx, claim.OperationID, claim.OwnerID, job.Digest)
			if err != nil {
				return err
			}
			break
		}
		fallthrough
	case "published":
		head, err := api.BranchHead(ctx, job.Value.Branch)
		if err != nil {
			return err
		}
		if head != job.Value.Source.SHA {
			return storage.ErrProductBranchHeld
		}
		if job.State == "published" {
			return nil
		}
		_, err = p.Store.ConfirmProductBranchRemote(ctx, claim.OperationID, claim.OwnerID, job.Digest, storage.ProductBranchRemoteProof{PublicationID: job.ID, ProofID: job.ID + ":read-after-stop", StopProofID: job.Stop.ProofID, RemoteSHA: head, ReadAt: p.now()})
		return err
	case "prepared":
	default:
		return storage.ErrProductBranchHeld
	}
	physical, err := p.Store.ClaimProductBranchPublication(ctx, claim.OperationID, claim.OwnerID, job.Digest)
	if err != nil {
		return err
	}
	if !physical.Start {
		return storage.ErrProductBranchHeld
	}
	started := p.now()
	var marked atomic.Bool
	observed, pushErr := p.Push(ctx, job.Value.Source, job.Value.Remote.URL, job.Value.Branch, func(writeCtx context.Context) error {
		fresh, err := p.Gate(writeCtx, claim.Product)
		if err != nil {
			return err
		}
		if fresh != publication {
			return errors.New("branch publication facts changed")
		}
		current, err := branchRemote(writeCtx, api, claim.Product.Value)
		if err != nil {
			return err
		}
		if current != job.Value.Remote {
			return errors.New("branch remote changed before push")
		}
		if _, err := p.Store.MarkProductBranchMayHaveSent(writeCtx, claim.OperationID, claim.OwnerID, job.Digest); err != nil {
			return err
		}
		marked.Store(true)
		return nil
	})
	if errors.Is(pushErr, workspace.ErrGitStopUnconfirmed) {
		return errors.Join(storage.ErrProductBranchHeld, pushErr)
	}
	if observed.Attempted && !marked.Load() {
		// A broken host transport cannot provide a valid stop receipt for an
		// unjournaled write. Retain ownership for explicit reconciliation.
		return errors.Join(storage.ErrProductBranchHeld, errors.New("Git transport omitted branch send fence"), pushErr)
	}
	finished := p.now()
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	proof := storage.ProductBranchStop{PublicationID: job.ID, ProofID: job.ID + ":joined", Reason: "synchronous branch transport returned after joining commands", StartedAt: started, FinishedAt: finished, Observation: observed, RemoteVerified: pushErr == nil && observed.RemoteSHA == job.Value.Source.SHA}
	stopped, err := p.Store.RecordProductBranchStopped(cleanup, claim.OperationID, claim.OwnerID, job.Digest, proof)
	if err != nil || pushErr != nil {
		return errors.Join(pushErr, err)
	}
	if stopped.State != "published" {
		return storage.ErrProductBranchHeld
	}
	return nil
}
