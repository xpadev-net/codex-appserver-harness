// Package product owns controller-side external PR adapters, separate from model
// dispatch. Callers must hold the controller lock and supply canonical gates.
package product

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type Publication struct{ Title, Body string }

// Verify reads the canonical WorkItem/config and required validation evidence.
// It must not use model claims or the cached projection as proof of those gates.
type PublicationGate func(context.Context, storage.ProductTargetSnapshot) (Publication, error)
type Repository struct {
	ID            int64  `json:"id"`
	DefaultBranch string `json:"default_branch"`
	Name          string `json:"full_name"`
	CloneURL      string `json:"clone_url"`
}

// Every HTTP mutation must call BeforeWrite immediately before sending it.
// Methods are synchronous: they return only after their transport has stopped.
type API interface {
	Repository(context.Context) (Repository, error)
	BranchHead(context.Context, string) (string, error)
	Pull(context.Context, int) (github.PR, error)
	FindPR(context.Context, string, string) (*github.PR, error)
	Publish(context.Context, string, string, string, string, string) (github.PR, error)
}
type APIFactory func(storage.ProductTarget, func(context.Context, string, string) error) API
type Publisher struct {
	Store *storage.KernelStore
	Gate  PublicationGate
	API   APIFactory
	// Branch publishes the sealed candidate before the HTTP publication fence.
	Branch func(context.Context, storage.PRDispatchClaim) error
}

type liveAPI struct{ github.Client }

func (a liveAPI) Repository(ctx context.Context) (Repository, error) {
	var r Repository
	err := a.Client.API(ctx, "GET", "", nil, &r)
	return r, err
}
func (a liveAPI) BranchHead(ctx context.Context, branch string) (string, error) {
	var ref struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	err := a.Client.API(ctx, "GET", "git/ref/heads/"+url.PathEscape(branch), nil, &ref)
	return ref.Object.SHA, err
}

// GitHubAPI explicitly selects the authenticated gh adapter. It never passes
// controller credentials to a worker or chooses a base from branch-name guesses.
func GitHubAPI(p storage.ProductTarget, before func(context.Context, string, string) error) API {
	return liveAPI{github.Client{Repository: p.Repository, RepositoryID: p.WorkItem.RepositoryID, BeforeWrite: before}}
}
func operationMarker(id string) string { return "<!-- harness-operation:" + id + " -->" }
func matched(p github.PR, claim storage.PRDispatchClaim) bool {
	w := claim.Product.Value
	return p.Number > 0 && p.Head.Repo.ID == w.WorkItem.RepositoryID && p.Base.Repo.ID == w.WorkItem.RepositoryID && p.Head.Ref == w.WorkItem.Branch && p.Base.Ref == w.BaseBranch && p.Head.SHA == w.Candidate.SHA && strings.Contains(p.Body, github.Marker(w.WorkItem.ID)) && (w.PRNumber == 0 || w.PRNumber == p.Number)
}
func observation(p github.PR, claim storage.PRDispatchClaim, proof string) storage.PRResourceObservation {
	return storage.PRResourceObservation{ProofID: proof, RepositoryID: claim.Product.Value.WorkItem.RepositoryID, HeadRepositoryID: p.Head.Repo.ID, BaseRepositoryID: p.Base.Repo.ID, Number: p.Number, MarkerWorkItemID: claim.Product.Value.WorkItem.ID, HeadSHA: p.Head.SHA, HeadBranch: p.Head.Ref, BaseBranch: p.Base.Ref, State: p.State, Merged: p.Merged, Draft: p.Draft, ObservedAt: time.Now().UTC()}
}
func verifyRepository(ctx context.Context, api API, p storage.ProductTarget) error {
	r, err := api.Repository(ctx)
	if err != nil {
		return err
	}
	if r.ID != p.WorkItem.RepositoryID || r.DefaultBranch == "" || r.DefaultBranch != p.BaseBranch {
		return errors.New("repository identity/default branch changed")
	}
	return nil
}
func (p Publisher) Publish(ctx context.Context, id, owner string) (storage.PRCompletionReceipt, error) {
	var empty storage.PRCompletionReceipt
	if p.Store == nil || p.Gate == nil || p.API == nil {
		return empty, errors.New("explicit PR store, canonical gate and transport required")
	}
	if err := p.Store.Record(ctx, "product_pr_completion", id, &empty); err == nil {
		if empty.OperationID != id || empty.Outcome != "resource_bound" {
			return empty, storage.ErrPRDispatchHeld
		}
		return empty, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return empty, err
	}
	claim, err := p.Store.PreparePRDispatch(ctx, id, owner)
	if err != nil {
		return empty, err
	}
	// Before the write guard consumes the claim, errors retain a known-unsent
	// prepared operation. The dispatcher owns bounded retry or explicit cancel.
	publication, err := p.Gate(ctx, claim.Product)
	if err != nil {
		return empty, err
	}
	if err = github.CheckClosingText(publication.Title + "\n" + publication.Body); err != nil {
		return empty, err
	}
	body := operationMarker(id) + "\n" + publication.Body
	packet, err := p.Store.SavePRPublicationPayload(ctx, id, owner, publication.Title, body)
	if err != nil {
		return empty, err
	}
	var marked atomic.Bool
	var api API
	before := func(writeCtx context.Context, method, endpoint string) error {
		if method == "POST" && endpoint == "pulls" {
			if claim.Product.Value.PRNumber != 0 {
				return errors.New("existing bound PR cannot be recreated")
			}
		} else if method == "PATCH" && strings.HasPrefix(endpoint, "pulls/") {
			number, e := strconv.Atoi(strings.TrimPrefix(endpoint, "pulls/"))
			if e != nil || number < 1 {
				return errors.New("invalid publication endpoint")
			}
			current, e := api.Pull(writeCtx, number)
			if e != nil {
				return e
			}
			if !matched(current, claim) || current.State != "open" {
				return errors.New("PR changed before publication")
			}
		} else {
			return errors.New("unexpected publication mutation")
		}
		if err := verifyRepository(writeCtx, api, claim.Product.Value); err != nil {
			return err
		}
		head, err := api.BranchHead(writeCtx, claim.Product.Value.WorkItem.Branch)
		if err != nil {
			return err
		}
		if head != claim.Product.Value.Candidate.SHA {
			return errors.New("remote branch is not the fixed candidate")
		}
		fresh, err := p.Gate(writeCtx, claim.Product)
		if err != nil {
			return err
		}
		if fresh != publication {
			return errors.New("canonical publication facts changed")
		}
		if _, err = p.Store.MarkPRMayHaveSent(writeCtx, id, owner); err != nil {
			return err
		}
		marked.Store(true)
		return nil
	}
	api = p.API(claim.Product.Value, before)
	if api == nil {
		return empty, errors.New("missing PR transport")
	}
	if err = verifyRepository(ctx, api, claim.Product.Value); err != nil {
		return empty, err
	}
	if p.Branch != nil {
		if err := p.Branch(ctx, claim); err != nil {
			return empty, err
		}
	}
	// The built-in transport resolves the WorkItem marker again before its single
	// guarded POST/PATCH. Any ambiguity returns an error without retrying a write.
	result, err := api.Publish(ctx, claim.Product.Value.BaseBranch, claim.Product.Value.WorkItem.Branch, claim.Product.Value.WorkItem.ID, packet.Title, packet.Body)
	if err != nil {
		if !marked.Load() {
			return empty, err
		}
		return empty, errors.Join(storage.ErrPRReconciliationRequired, err)
	}
	if !marked.Load() {
		return empty, errors.New("transport omitted the publication write guard")
	}
	if !matched(result, claim) || result.Title != packet.Title || result.Body != github.Marker(claim.Product.Value.WorkItem.ID)+"\n"+packet.Body {
		return empty, storage.ErrPRReconciliationRequired
	}
	return p.Store.BindObservedPRPublication(ctx, id, storage.PRTransportProof{ID: id + ":response", OwnerID: owner, Disposition: "response_received"}, observation(result, claim, id+":response"))
}

// ReconcilePublication performs only reads after the caller proves the previous
// synchronous transport finished, or verifies its stopped owner after restart.
// It never re-runs current business gates or resends an old-scope write.
func (p Publisher) ReconcilePublication(ctx context.Context, id string, proof storage.PRTransportProof) (storage.PRCompletionReceipt, error) {
	var receipt storage.PRCompletionReceipt
	if p.Store == nil || p.API == nil {
		return receipt, errors.New("PR store and read transport required")
	}
	if err := p.Store.Record(ctx, "product_pr_completion", id, &receipt); err == nil {
		if receipt.OperationID != id || receipt.Outcome != "resource_bound" {
			return receipt, storage.ErrPRDispatchHeld
		}
		return receipt, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	var claim storage.PRDispatchClaim
	if err := p.Store.Record(ctx, "product_pr_dispatch", id, &claim); err != nil {
		return receipt, err
	}
	if claim.State != "unknown" || proof.OwnerID != claim.OwnerID || (proof.Disposition != "response_received" && proof.Disposition != "reconciled_after_stop") {
		return receipt, storage.ErrPRReconciliationRequired
	}
	var packet storage.PRPublicationPayload
	if err := p.Store.Record(ctx, "product_pr_publication_payload", id, &packet); err != nil {
		return receipt, err
	}
	if packet.OperationID != id || packet.ProductDigest != claim.Product.Digest {
		return receipt, errors.New("publication packet identity conflict")
	}
	api := p.API(claim.Product.Value, func(context.Context, string, string) error {
		return errors.New("read-only reconciliation forbids writes")
	})
	if api == nil {
		return receipt, errors.New("missing read-only PR transport")
	}
	// A changed default branch does not erase the original operation's identity.
	repo, err := api.Repository(ctx)
	if err != nil {
		return receipt, err
	}
	if repo.ID != claim.Product.Value.WorkItem.RepositoryID {
		return receipt, errors.New("repository identity changed during reconciliation")
	}
	found, err := api.FindPR(ctx, claim.Product.Value.WorkItem.Branch, claim.Product.Value.WorkItem.ID)
	if err != nil {
		return receipt, err
	}
	if found == nil {
		return receipt, storage.ErrPRReconciliationRequired
	}
	// List responses omit fields such as merged. Fetch the full resource before
	// deriving terminal facts or a capacity-release disposition.
	full, err := api.Pull(ctx, found.Number)
	if err != nil {
		return receipt, err
	}
	if full.Number != found.Number || !matched(full, claim) || full.Title != packet.Title || full.Body != github.Marker(claim.Product.Value.WorkItem.ID)+"\n"+packet.Body || !strings.Contains(full.Body, operationMarker(id)) {
		return receipt, storage.ErrPRReconciliationRequired
	}
	return p.Store.BindObservedPRPublication(ctx, id, proof, observation(full, claim, fmt.Sprintf("%s:reconciled", id)))
}
