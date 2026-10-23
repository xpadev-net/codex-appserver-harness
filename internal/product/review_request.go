package product

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type ReviewRequest struct {
	Trigger     string
	RequesterID int64
}

// Gate verifies canonical candidate/validation and the configured, proven Cloud
// request route. A request capability is distinct from native approval capability.
type ReviewRequestGate func(context.Context, storage.ProductTargetSnapshot) (ReviewRequest, error)
type ReviewAPI interface {
	Repository(context.Context) (Repository, error)
	Pull(context.Context, int) (github.PR, error)
	ReviewComments(context.Context, int) ([]github.Comment, error)
	RequestReview(context.Context, int, string, string, int64) (int64, error)
}
type ReviewAPIFactory func(storage.ProductTarget, func(context.Context, string, string) error) ReviewAPI
type ReviewRequester struct {
	Store *storage.KernelStore
	Gate  ReviewRequestGate
	API   ReviewAPIFactory
}
type liveReviewAPI struct{ liveAPI }

func (a liveReviewAPI) ReviewComments(ctx context.Context, n int) ([]github.Comment, error) {
	var comments []github.Comment
	err := a.Client.Pages(ctx, fmt.Sprintf("issues/%d/comments?per_page=100", n), &comments)
	return comments, err
}
func GitHubReviewAPI(p storage.ProductTarget, before func(context.Context, string, string) error) ReviewAPI {
	return liveReviewAPI{liveAPI{github.Client{Repository: p.Repository, RepositoryID: p.WorkItem.RepositoryID, BeforeWrite: before}}}
}
func reviewReceipt(ctx context.Context, k *storage.KernelStore, id string) (storage.PRCompletionReceipt, error) {
	var receipt storage.PRCompletionReceipt
	err := k.Record(ctx, "product_pr_completion", id, &receipt)
	if err == nil && (receipt.OperationID != id || receipt.Outcome != "review_requested" || receipt.ReviewRequest == nil) {
		err = storage.ErrPRDispatchHeld
	}
	return receipt, err
}
func observedRequest(ctx context.Context, api ReviewAPI, claim storage.PRDispatchClaim, packet storage.PRReviewRequestPayload, expectedID int64) (storage.PRReviewRequestObservation, error) {
	var o storage.PRReviewRequestObservation
	comments, err := api.ReviewComments(ctx, claim.Product.Value.PRNumber)
	if err != nil {
		return o, err
	}
	for _, c := range comments {
		if c.User.ID != packet.RequesterID || c.Body != packet.Body() {
			continue
		}
		if c.ID <= 0 || o.CommentID != 0 {
			return o, errors.New("ambiguous review request comments")
		}
		o = storage.PRReviewRequestObservation{ProofID: packet.OperationID + ":comment", RepositoryID: claim.Product.Value.WorkItem.RepositoryID, PRNumber: claim.Product.Value.PRNumber, RoundID: packet.RoundID, CommentID: c.ID, RequesterID: c.User.ID, Body: c.Body, ObservedAt: time.Now().UTC()}
	}
	if o.CommentID == 0 || (expectedID != 0 && o.CommentID != expectedID) {
		return o, storage.ErrPRReconciliationRequired
	}
	return o, nil
}
func (r ReviewRequester) Request(ctx context.Context, id, owner string) (storage.PRCompletionReceipt, error) {
	var receipt storage.PRCompletionReceipt
	if r.Store == nil || r.Gate == nil || r.API == nil {
		return receipt, errors.New("explicit review store, canonical gate and transport required")
	}
	if done, err := reviewReceipt(ctx, r.Store, id); err == nil {
		return done, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	claim, err := r.Store.PreparePRDispatch(ctx, id, owner)
	if err != nil {
		return receipt, err
	}
	request, err := r.Gate(ctx, claim.Product)
	if err != nil {
		return receipt, err
	}
	packet, err := r.Store.SavePRReviewRequestPayload(ctx, id, owner, request.Trigger, request.RequesterID)
	if err != nil {
		return receipt, err
	}
	var marked atomic.Bool
	var api ReviewAPI
	before := func(writeCtx context.Context, method, endpoint string) error {
		if method != "POST" || endpoint != fmt.Sprintf("issues/%d/comments", claim.Product.Value.PRNumber) {
			return errors.New("unexpected review mutation")
		}
		if err := verifyReviewRepository(writeCtx, api, claim.Product.Value, true); err != nil {
			return err
		}
		pr, err := api.Pull(writeCtx, claim.Product.Value.PRNumber)
		if err != nil {
			return err
		}
		if !matched(pr, claim) || pr.State != "open" || pr.Merged || pr.Draft {
			return errors.New("PR changed before review request")
		}
		fresh, err := r.Gate(writeCtx, claim.Product)
		if err != nil {
			return err
		}
		if fresh != request {
			return errors.New("canonical review request facts changed")
		}
		if _, err = r.Store.MarkPRMayHaveSent(writeCtx, id, owner); err != nil {
			return err
		}
		marked.Store(true)
		return nil
	}
	api = r.API(claim.Product.Value, before)
	if api == nil {
		return receipt, errors.New("missing review transport")
	}
	if err = verifyReviewRepository(ctx, api, claim.Product.Value, true); err != nil {
		return receipt, err
	}
	// A pre-existing exact comment can be bound without any write. In all other
	// cases the request call must consume the durable write guard.
	if existing, lookupErr := observedRequest(ctx, api, claim, packet, 0); lookupErr == nil {
		return r.Store.BindObservedPRReviewRequest(ctx, id, storage.PRTransportProof{ID: id + ":existing", OwnerID: owner, Disposition: "not_started"}, existing)
	} else if !errors.Is(lookupErr, storage.ErrPRReconciliationRequired) {
		return receipt, lookupErr
	}
	commentID, err := api.RequestReview(ctx, claim.Product.Value.PRNumber, packet.RoundID, packet.Trigger, packet.RequesterID)
	if err != nil {
		if marked.Load() {
			return receipt, errors.Join(storage.ErrPRReconciliationRequired, err)
		}
		return receipt, err
	}
	if !marked.Load() {
		return receipt, errors.New("transport omitted the review request write guard; reconcile before retry")
	}
	if commentID <= 0 {
		return receipt, storage.ErrPRReconciliationRequired
	}
	o, err := observedRequest(ctx, api, claim, packet, commentID)
	if err != nil {
		return receipt, err
	}
	return r.Store.BindObservedPRReviewRequest(ctx, id, storage.PRTransportProof{ID: id + ":request", OwnerID: owner, Disposition: "response_received"}, o)
}
func verifyReviewRepository(ctx context.Context, api ReviewAPI, p storage.ProductTarget, currentDefault bool) error {
	repo, err := api.Repository(ctx)
	if err != nil {
		return err
	}
	if repo.ID != p.WorkItem.RepositoryID || (currentDefault && (repo.DefaultBranch == "" || repo.DefaultBranch != p.BaseBranch)) {
		return errors.New("review repository identity/default branch changed")
	}
	return nil
}

// The caller proves the old transport stopped. This read-only reconciliation
// accepts saved request facts after scope/policy changes, never another POST.
func (r ReviewRequester) Reconcile(ctx context.Context, id string, proof storage.PRTransportProof) (storage.PRCompletionReceipt, error) {
	var receipt storage.PRCompletionReceipt
	if r.Store == nil || r.API == nil {
		return receipt, errors.New("review store and read transport required")
	}
	if done, err := reviewReceipt(ctx, r.Store, id); err == nil {
		return done, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	var claim storage.PRDispatchClaim
	if err := r.Store.Record(ctx, "product_pr_dispatch", id, &claim); err != nil {
		return receipt, err
	}
	if claim.State != "unknown" || claim.OwnerID != proof.OwnerID || (proof.Disposition != "response_received" && proof.Disposition != "reconciled_after_stop") {
		return receipt, storage.ErrPRReconciliationRequired
	}
	var packet storage.PRReviewRequestPayload
	if err := r.Store.Record(ctx, "product_pr_review_payload", id, &packet); err != nil {
		return receipt, err
	}
	if packet.OperationID != id || packet.ProductDigest != claim.Product.Digest || packet.RoundID != id {
		return receipt, errors.New("review packet identity conflict")
	}
	api := r.API(claim.Product.Value, func(context.Context, string, string) error { return errors.New("read-only review reconciliation") })
	if api == nil {
		return receipt, errors.New("missing read-only review transport")
	}
	if err := verifyReviewRepository(ctx, api, claim.Product.Value, false); err != nil {
		return receipt, err
	}
	o, err := observedRequest(ctx, api, claim, packet, 0)
	if err != nil {
		return receipt, err
	}
	return r.Store.BindObservedPRReviewRequest(ctx, id, proof, o)
}
