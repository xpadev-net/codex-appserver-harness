package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// Controller-owned request text and identity; none are model tool arguments.
type PRReviewRequestPayload struct {
	CloudReviewerIDs []int64 `json:"cloud_reviewer_ids,omitempty"`
	OperationID      string  `json:"operation_id"`
	ProductDigest    string  `json:"product_digest"`
	RoundID          string  `json:"round_id"`
	RequesterID      int64   `json:"requester_id"`
	Trigger          string  `json:"trigger"`
}

func (p PRReviewRequestPayload) Body() string {
	return p.Trigger + "\n\n<!-- harness-review:" + p.RoundID + " -->"
}

func (k *KernelStore) SavePRReviewRequestPayload(ctx context.Context, id, owner, trigger string, requester int64, expected ...[]int64) (PRReviewRequestPayload, error) {
	var packet PRReviewRequestPayload
	if len(expected) > 1 {
		return packet, errors.New("one Cloud actor set required")
	}
	var actors []int64
	if len(expected) == 1 {
		var err error
		actors, err = CanonicalCloudReviewerIDs(expected[0])
		if err != nil {
			return packet, err
		}
	}
	if requester <= 0 || strings.TrimSpace(trigger) == "" || !utf8.ValidString(trigger) || strings.ContainsRune(trigger, '\x00') || !validPRProofID(id) || strings.ContainsAny(id, "<>") {
		return packet, errors.New("verified requester and bounded review trigger required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return packet, err
	}
	defer tx.Rollback()
	intent, state, err := prIntentTx(ctx, tx, id)
	if err != nil {
		return packet, err
	}
	if state != "accepted" || intent.Source.Name != "pr_review_request" {
		return packet, ErrPRDispatchHeld
	}
	var claim PRDispatchClaim
	if err = completionRecord(ctx, tx, "product_pr_dispatch", id, &claim); err != nil {
		return packet, err
	}
	if claim.State != "prepared" || claim.OwnerID != owner {
		return packet, ErrPRDispatchHeld
	}
	if _, _, err = k.preflightPRTx(ctx, tx, intent); err != nil {
		return packet, err
	}
	if err = validatePRClaimTx(ctx, tx, intent, claim); err != nil {
		return packet, err
	}
	packet = PRReviewRequestPayload{CloudReviewerIDs: actors, OperationID: id, ProductDigest: claim.Product.Digest, RoundID: id, RequesterID: requester, Trigger: trigger}
	if len(packet.Body()) > 64<<10 {
		return packet, errors.New("complete review request exceeds body budget")
	}
	var old PRReviewRequestPayload
	err = completionRecord(ctx, tx, "product_pr_review_payload", id, &old)
	if err == nil && !reflect.DeepEqual(old, packet) {
		return packet, errors.New("review request payload is immutable")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return packet, err
	}
	if err = kernelRecordTx(ctx, tx, "product_pr_review_payload", id, intent.Source.SessionID, packet); err != nil {
		return packet, err
	}
	return packet, tx.Commit()
}

// A positively observed request comment acknowledges dispatch, not review
// completion or approval. The Cloud reservation remains held after this receipt.
type PRReviewRequestObservation struct {
	ProofID      string    `json:"proof_id"`
	RepositoryID int64     `json:"repository_id"`
	PRNumber     int       `json:"pr_number"`
	RoundID      string    `json:"round_id"`
	CommentID    int64     `json:"comment_id"`
	RequesterID  int64     `json:"requester_id"`
	Body         string    `json:"body"`
	ObservedAt   time.Time `json:"observed_at"`
}

func (k *KernelStore) BindObservedPRReviewRequest(ctx context.Context, id string, proof PRTransportProof, o PRReviewRequestObservation) (PRCompletionReceipt, error) {
	var receipt PRCompletionReceipt
	o.ObservedAt = o.ObservedAt.UTC()
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return receipt, err
	}
	defer tx.Rollback()
	intent, state, err := prIntentTx(ctx, tx, id)
	if err != nil {
		return receipt, err
	}
	if intent.Source.Name != "pr_review_request" {
		return receipt, ErrPRDispatchHeld
	}
	if state == "completed" {
		if err = completionRecord(ctx, tx, "product_pr_completion", id, &receipt); err != nil {
			return receipt, err
		}
		if receipt.Outcome != "review_requested" || receipt.ReviewRequest == nil || !reflect.DeepEqual(*receipt.ReviewRequest, o) || receipt.Transport != proof {
			return receipt, errors.New("review completion replay conflict")
		}
		return receipt, tx.Commit()
	}
	var claim PRDispatchClaim
	if err = completionRecord(ctx, tx, "product_pr_dispatch", id, &claim); err != nil {
		return receipt, err
	}
	if !((state == "accepted" && claim.State == "prepared" && proof.Disposition == "not_started") || (state == "unknown" && claim.State == "unknown" && (proof.Disposition == "response_received" || proof.Disposition == "reconciled_after_stop"))) || proof.OwnerID != claim.OwnerID || !validPRProofID(proof.ID) {
		return receipt, ErrPRReconciliationRequired
	}
	var packet PRReviewRequestPayload
	if err = completionRecord(ctx, tx, "product_pr_review_payload", id, &packet); err != nil {
		return receipt, err
	}
	p := claim.Product.Value
	if packet.OperationID != id || packet.ProductDigest != claim.Product.Digest || packet.RoundID != id || !validPRProofID(o.ProofID) || o.RepositoryID != p.WorkItem.RepositoryID || o.PRNumber != p.PRNumber || o.RoundID != packet.RoundID || o.CommentID <= 0 || o.RequesterID != packet.RequesterID || o.Body != packet.Body() || o.ObservedAt.IsZero() || o.ObservedAt.Before(claim.PreparedAt) {
		return receipt, errors.New("review request observation differs from saved packet")
	}
	r, err := readPRClaimReservationTx(ctx, tx, intent, claim)
	if err != nil {
		return receipt, err
	}
	if r.State != "held" || r.Kind != "cloud_review" || r.ID != id || r.Request.CandidateSHA != intent.Candidate.SHA || r.Request.ResourceKey != fmt.Sprintf("github:%d:pull:%d", o.RepositoryID, o.PRNumber) {
		return receipt, ErrPRDispatchHeld
	}
	var binding PRResourceBinding
	if err = completionRecord(ctx, tx, "product_work_item_pr", intent.Candidate.WorkItemID, &binding); err != nil {
		return receipt, err
	}
	if binding.RepositoryID != o.RepositoryID || binding.Number != o.PRNumber || binding.WaveID != intent.Source.Target.WaveID || binding.WorkItemID != intent.Candidate.WorkItemID || binding.HeadBranch != p.WorkItem.Branch {
		return receipt, errors.New("review request PR ownership conflict")
	}
	var watch PRSubscriptionIntent
	if err = completionRecord(ctx, tx, "product_pr_subscription_intent", intent.SubscriptionID, &watch); err != nil {
		return receipt, err
	}
	if watch.OperationID != id || watch.SessionID != intent.Source.SessionID || watch.Target != intent.Source.Target || watch.RepositoryID != o.RepositoryID || watch.PRNumber != o.PRNumber {
		return receipt, errors.New("review subscription identity conflict")
	}
	if err = bindPRReceiveSubscriptionTx(ctx, tx, &watch, binding); err != nil {
		return receipt, err
	}
	if err = releasePRMutationTx(ctx, tx, claim); err != nil {
		return receipt, err
	}
	claim.State = "settled"
	if err = kernelRecordTx(ctx, tx, "product_pr_dispatch", id, intent.Source.SessionID, claim); err != nil {
		return receipt, err
	}
	intent.State = "completed"
	if err = kernelRecordTx(ctx, tx, "product_pr_intent", id, intent.Source.SessionID, intent); err != nil {
		return receipt, err
	}
	header, err := json.Marshal(map[string]any{"outcome": "review_requested", "comment_id": o.CommentID, "round_id": o.RoundID, "head_sha": intent.Candidate.SHA})
	if err != nil {
		return receipt, err
	}
	reply, err := json.Marshal(contract.ToolReply{SchemaVersion: 1, State: "completed", OperationID: id, SubscriptionIDs: []string{watch.ID}, ResourceID: binding.ResourceKey, Content: string(header)})
	if err != nil {
		return receipt, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_tool_operations SET state='completed',result=? WHERE id=?", reply, id); err != nil {
		return receipt, err
	}
	receipt = PRCompletionReceipt{OperationID: id, Outcome: "review_requested", Resource: binding, ReviewRequest: &o, Transport: proof, SubscriptionID: watch.ID, NotificationState: "pending"}
	if err = kernelRecordTx(ctx, tx, "product_pr_completion", id, intent.Source.SessionID, receipt); err != nil {
		return receipt, err
	}
	return receipt, tx.Commit()
}

// CanonicalCloudReviewerIDs freezes an optional host-owned provider identity
// set. An absent set preserves legacy requests without granting completion.
func CanonicalCloudReviewerIDs(ids []int64) ([]int64, error) {
	if len(ids) > 128 {
		return nil, errors.New("Cloud reviewer identity set exceeds 128")
	}
	if len(ids) == 0 {
		return nil, nil
	}
	result := slices.Clone(ids)
	slices.Sort(result)
	for i, id := range result {
		if id < 1 || i > 0 && result[i-1] == id {
			return nil, errors.New("unique positive Cloud reviewer IDs required")
		}
	}
	return result, nil
}
