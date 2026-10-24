package storage

import (
	"context"
	"fmt"
	"time"
)

// ProductCloudRound is derived from a positively bound request, not model
// output or a caller-selected SHA/round. It grants no approval or merge right.
type ProductCloudRound struct {
	OperationID      string                 `json:"operation_id"`
	Product          ProductTargetSnapshot  `json:"product"`
	Packet           PRReviewRequestPayload `json:"packet"`
	RequestedAt      time.Time              `json:"requested_at"`
	RequestCommentID int64                  `json:"request_comment_id"`
	RequestAttempted bool                   `json:"request_attempted"`
	Resource         PRResourceBinding      `json:"resource"`
	ReservationID    string                 `json:"reservation_id"`
	Digest           string                 `json:"digest"`
}

// ReadProductCloudRound authenticates the original request independently of
// current controls/scope. Historical reads must remain possible after pause or
// revision changes; consumers separately check current authority before effects.
func (k *KernelStore) ReadProductCloudRound(ctx context.Context, id string) (ProductCloudRound, error) {
	var round ProductCloudRound
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return round, err
	}
	defer tx.Rollback()
	intent, state, err := prIntentTx(ctx, tx, id)
	if err != nil {
		return round, err
	}
	if state != "completed" || intent.State != "completed" || intent.Source.Name != "pr_review_request" {
		return round, ErrTaskEvidence
	}
	var claim PRDispatchClaim
	var packet PRReviewRequestPayload
	var receipt PRCompletionReceipt
	for _, r := range []struct {
		kind  string
		value any
	}{{"product_pr_dispatch", &claim}, {"product_pr_review_payload", &packet}, {"product_pr_completion", &receipt}} {
		if err := taskEvidenceRecord(ctx, tx, r.kind, id, r.value); err != nil {
			return round, err
		}
	}
	p := claim.Product.Value
	digest, err := contentDigest(p)
	if err != nil {
		return round, err
	}
	if p.validate() != nil || digest != claim.Product.Digest || digest != intent.ProductDigest || claim.OperationID != id || claim.State != "settled" || claim.OwnerID == "" || p.Target != intent.Source.Target || p.WorkItem.ID != intent.Candidate.WorkItemID || p.Candidate.ID != intent.Candidate.CandidateID || p.Candidate.SHA != intent.Candidate.SHA || p.PRNumber < 1 || packet.OperationID != id || packet.ProductDigest != digest || packet.RoundID != id || packet.RequesterID < 1 || packet.Trigger == "" || receipt.OperationID != id || receipt.Outcome != "review_requested" || receipt.ReviewRequest == nil || receipt.SubscriptionID != intent.SubscriptionID || receipt.Transport.OwnerID != claim.OwnerID || !validPRProofID(receipt.Transport.ID) {
		return round, ErrTaskEvidence
	}
	if receipt.Transport.Disposition != "not_started" && receipt.Transport.Disposition != "response_received" && receipt.Transport.Disposition != "reconciled_after_stop" {
		return round, ErrTaskEvidence
	}
	if (receipt.Transport.Disposition == "not_started") != claim.MayHaveSentAt.IsZero() {
		return round, ErrTaskEvidence
	}
	if !claim.MayHaveSentAt.IsZero() && (claim.MayHaveSentAt.Before(claim.PreparedAt) || claim.MayHaveSentAt.After(receipt.ReviewRequest.ObservedAt)) {
		return round, ErrTaskEvidence
	}
	o := *receipt.ReviewRequest
	expected := PRResourceBinding{ResourceKey: fmt.Sprintf("github:%d:pull:%d", p.WorkItem.RepositoryID, p.PRNumber), RepositoryID: p.WorkItem.RepositoryID, Number: p.PRNumber, WaveID: p.Target.WaveID, WorkItemID: p.WorkItem.ID, HeadBranch: p.WorkItem.Branch}
	if receipt.Resource != expected || o.RepositoryID != expected.RepositoryID || o.PRNumber != expected.Number || o.RoundID != id || o.RequesterID != packet.RequesterID || o.CommentID < 1 || o.Body != packet.Body() || !validPRProofID(o.ProofID) || claim.PreparedAt.IsZero() || o.ObservedAt.IsZero() || o.ObservedAt.Before(claim.PreparedAt) || o.ObservedAt.After(k.clock()) {
		return round, ErrTaskEvidence
	}
	var watch PRSubscriptionIntent
	if err := taskEvidenceRecord(ctx, tx, "product_pr_subscription_intent", intent.SubscriptionID, &watch); err != nil {
		return round, err
	}
	if watch.ID != intent.SubscriptionID || watch.OperationID != id || watch.State != "bound" || watch.Target != p.Target || watch.SessionID != intent.Source.SessionID || watch.RepositoryID != expected.RepositoryID || watch.PRNumber != p.PRNumber || watch.ResourceKey != expected.ResourceKey {
		return round, ErrTaskEvidence
	}
	r, err := loadOperationReservationTx(ctx, tx, claim.ReservationID)
	if err != nil {
		return round, err
	}
	if r.Kind != "cloud_review" || r.ID != id || r.Request.CandidateSHA != p.Candidate.SHA || r.Request.ResourceKey != expected.ResourceKey || r.Request.Target != p.Target || r.SessionID != intent.Source.SessionID {
		return round, ErrTaskEvidence
	}
	// The mutation lease has already been released at request settlement; it
	// must not be required as a live lease by a historical round reader.
	if r.State != "held" && r.State != "released" {
		return round, ErrTaskEvidence
	}
	round = ProductCloudRound{OperationID: id, Product: claim.Product, Packet: packet, Resource: expected, ReservationID: r.ID, RequestedAt: o.ObservedAt, RequestCommentID: o.CommentID, RequestAttempted: !claim.MayHaveSentAt.IsZero()}
	// Observation time is a conservative lower bound for acceptable reviews.
	// A result submitted before acknowledgement remains unaccepted rather than
	// borrowing a different round's approval at the same candidate SHA.
	value := round
	value.Digest = ""
	round.Digest, err = contentDigest(value)
	if err != nil {
		return round, err
	}
	return round, tx.Commit()
}
