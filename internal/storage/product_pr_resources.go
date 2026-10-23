package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// These facts are supplied only by a trusted transport/reconciler after checking
// the real endpoint, marker and transport owner. No model tool accepts them.
type PRResourceObservation struct {
	ProofID          string    `json:"proof_id"`
	RepositoryID     int64     `json:"repository_id"`
	HeadRepositoryID int64     `json:"head_repository_id"`
	BaseRepositoryID int64     `json:"base_repository_id"`
	Number           int       `json:"number"`
	MarkerWorkItemID string    `json:"marker_work_item_id"`
	HeadSHA          string    `json:"head_sha"`
	HeadBranch       string    `json:"head_branch"`
	BaseBranch       string    `json:"base_branch"`
	State            string    `json:"state"`
	Merged           bool      `json:"merged"`
	Draft            bool      `json:"draft"`
	ObservedAt       time.Time `json:"observed_at"`
}
type PRTransportProof struct {
	ID          string `json:"id"`
	OwnerID     string `json:"owner_id"`
	Disposition string `json:"disposition"` // not_started, response_received, reconciled_after_stop
}
type PRResourceBinding struct {
	ResourceKey  string `json:"resource_key"`
	RepositoryID int64  `json:"repository_id"`
	Number       int    `json:"number"`
	WaveID       string `json:"wave_id"`
	WorkItemID   string `json:"work_item_id"`
	HeadBranch   string `json:"head_branch"`
}
type PRCompletionReceipt struct {
	OperationID       string                      `json:"operation_id"`
	Outcome           string                      `json:"outcome"` // resource_bound, not a validation/approval claim
	Resource          PRResourceBinding           `json:"resource"`
	Observation       PRResourceObservation       `json:"observation"`
	Transport         PRTransportProof            `json:"transport"`
	SubscriptionID    string                      `json:"subscription_id"`
	NotificationState string                      `json:"notification_state"` // durable outbox; delivery wired separately
	ReviewRequest     *PRReviewRequestObservation `json:"review_request,omitempty"`
}

func validPRProofID(value string) bool {
	return strings.TrimSpace(value) != "" && len(value) <= 256 && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}
func validatePRObservation(claim PRDispatchClaim, intent PROperationIntent, o PRResourceObservation, proof PRTransportProof) error {
	p := claim.Product.Value
	if !validPRProofID(o.ProofID) || !validPRProofID(proof.ID) || proof.OwnerID != claim.OwnerID || o.ObservedAt.IsZero() || o.ObservedAt.Before(claim.PreparedAt) || o.RepositoryID != p.WorkItem.RepositoryID || o.HeadRepositoryID != o.RepositoryID || o.BaseRepositoryID != o.RepositoryID || o.Number < 1 || o.MarkerWorkItemID != intent.MarkerWorkItemID || o.HeadSHA != intent.Candidate.SHA || o.HeadBranch != p.WorkItem.Branch || o.BaseBranch != p.BaseBranch || (p.PRNumber > 0 && o.Number != p.PRNumber) || (o.State != "open" && o.State != "closed") || (o.Merged && o.State != "closed") {
		return errors.New("PR observation differs from the admitted immutable publication")
	}
	if claim.State == "prepared" && proof.Disposition == "not_started" {
		return nil
	}
	if claim.State == "unknown" && (proof.Disposition == "response_received" || proof.Disposition == "reconciled_after_stop") {
		return nil
	}
	return errors.New("PR binding requires verified transport quiescence")
}

// BindObservedPRPublication settles only a positive observation. An absent PR is
// not proof that an unknown request was unsent. Scope/pause/cancel changes do not
// erase admitted external facts; the binding keeps its original Wave/session.
// The caller verifies transport completion/group stop before supplying proof.
func (k *KernelStore) BindObservedPRPublication(ctx context.Context, id string, proof PRTransportProof, o PRResourceObservation) (PRCompletionReceipt, error) {
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
	if intent.Source.Name != "pr_publish" {
		return receipt, errors.New("publication binding requires a publication operation")
	}
	if state == "completed" {
		if intent.State != "completed" {
			return receipt, ErrPRDispatchHeld
		}
		if err = completionRecord(ctx, tx, "product_pr_completion", id, &receipt); err != nil {
			return receipt, err
		}
		if receipt.OperationID != id || receipt.Observation != o || receipt.Transport != proof {
			return receipt, errors.New("publication completion replay conflict")
		}
		return receipt, tx.Commit()
	}
	if state != "accepted" && state != "unknown" {
		return receipt, ErrPRDispatchHeld
	}
	var claim PRDispatchClaim
	if err = completionRecord(ctx, tx, "product_pr_dispatch", id, &claim); err != nil {
		return receipt, err
	}
	if (state == "accepted" && claim.State != "prepared") || (state == "unknown" && claim.State != "unknown") {
		return receipt, errors.New("publication dispatch disposition conflict")
	}
	if err = validatePRObservation(claim, intent, o, proof); err != nil {
		return receipt, err
	}
	r, err := readPRClaimReservationTx(ctx, tx, intent, claim)
	if err != nil {
		return receipt, err
	}
	resource := fmt.Sprintf("github:%d:work:%s", o.RepositoryID, intent.Candidate.WorkItemID)
	lifetime, err := loadPRLifetimeSeatTx(ctx, tx, intent.Source.Target.WaveID, resource)
	if err != nil {
		return receipt, err
	}
	if lifetime.ID != claim.PRReservationID || r.ID != lifetime.ID {
		return receipt, errors.New("publication lost its PR lifetime reservation")
	}
	if r.State != "held" && (r.State != "released" || r.Proof == nil || (r.Proof.Disposition != "closed" && r.Proof.Disposition != "merged") || o.State != "closed" || o.ObservedAt.Before(r.ReleasedAt)) {
		return receipt, ErrPRReconciliationRequired
	}
	binding := PRResourceBinding{ResourceKey: fmt.Sprintf("github:%d:pull:%d", o.RepositoryID, o.Number), RepositoryID: o.RepositoryID, Number: o.Number, WaveID: intent.Source.Target.WaveID, WorkItemID: intent.Candidate.WorkItemID, HeadBranch: o.HeadBranch}
	for _, entry := range []struct{ kind, key string }{{"product_pr_resource", binding.ResourceKey}, {"product_work_item_pr", binding.WorkItemID}} {
		var old PRResourceBinding
		e := completionRecord(ctx, tx, entry.kind, entry.key, &old)
		if e == nil && !reflect.DeepEqual(old, binding) {
			return receipt, errors.New("external PR is already bound to another product identity")
		}
		if e != nil && !errors.Is(e, sql.ErrNoRows) {
			return receipt, e
		}
		if err = kernelRecordTx(ctx, tx, entry.kind, entry.key, binding.WaveID, binding); err != nil {
			return receipt, err
		}
	}
	// Update only factual PR identity in the current projection. Preserve a newer
	// candidate, flags and target instead of restoring the source snapshot.
	if err = k.attachCurrentPRNumberTx(ctx, tx, intent, binding); err != nil {
		return receipt, err
	}
	var watch PRSubscriptionIntent
	if err = completionRecord(ctx, tx, "product_pr_subscription_intent", intent.SubscriptionID, &watch); err != nil {
		return receipt, err
	}
	if watch.OperationID != id || watch.SessionID != intent.Source.SessionID || watch.Target != intent.Source.Target || watch.RepositoryID != o.RepositoryID {
		return receipt, errors.New("PR subscription identity conflict")
	}
	if err = bindPRReceiveSubscriptionTx(ctx, tx, &watch, binding); err != nil {
		return receipt, err
	}
	wave, err := loadWave(ctx, tx, binding.WaveID)
	if err != nil {
		return receipt, err
	}
	if err = k.refreshAssignedPRSubscriptionsTx(ctx, tx, wave); err != nil {
		return receipt, err
	}
	if o.State == "closed" && r.State == "held" {
		disposition := "closed"
		if o.Merged {
			disposition = "merged"
		}
		if err = k.releaseOperationCapacityTx(ctx, tx, OperationReleaseProof{OperationID: r.ID, ProofID: o.ProofID, Disposition: disposition, Reason: "trusted PR terminal observation"}); err != nil {
			return receipt, err
		}
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
	header, err := json.Marshal(map[string]any{"outcome": "resource_bound", "repository_id": o.RepositoryID, "pr_number": o.Number, "head_sha": o.HeadSHA, "state": o.State, "merged": o.Merged, "draft": o.Draft})
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
	receipt = PRCompletionReceipt{OperationID: id, Outcome: "resource_bound", Resource: binding, Observation: o, Transport: proof, SubscriptionID: watch.ID, NotificationState: "pending"}
	if err = kernelRecordTx(ctx, tx, "product_pr_completion", id, intent.Source.SessionID, receipt); err != nil {
		return receipt, err
	}
	return receipt, tx.Commit()
}
func releasePRMutationTx(ctx context.Context, tx *sql.Tx, claim PRDispatchClaim) error {
	r, err := tx.ExecContext(ctx, "UPDATE kernel_resource_leases SET state='released' WHERE resource=? AND run_id=? AND generation=? AND state='held'", claim.MutationLease.Resource, claim.OperationID, claim.MutationLease.Generation)
	if err != nil {
		return err
	}
	count, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrPRDispatchHeld
	}
	return nil
}
func (k *KernelStore) attachCurrentPRNumberTx(ctx context.Context, tx *sql.Tx, intent PROperationIntent, binding PRResourceBinding) error {
	var current ProductTargetSnapshot
	err := completionRecord(ctx, tx, "product_target", productTargetID(intent.Source.Target, binding.WorkItemID), &current)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	digest, err := contentDigest(current.Value)
	if err != nil {
		return err
	}
	if digest != current.Digest {
		return errors.New("current product snapshot digest conflict")
	}
	p := current.Value
	if p.WorkItem.ID != binding.WorkItemID || p.WorkItem.RepositoryID != binding.RepositoryID || p.Target.WaveID != binding.WaveID {
		return errors.New("current product ownership differs from external fact")
	}
	if p.WorkItem.Branch != binding.HeadBranch {
		return nil
	}
	if p.PRNumber != 0 && p.PRNumber != binding.Number {
		return errors.New("current product references a different PR")
	}
	if p.PRNumber == binding.Number {
		return nil
	}
	current.Value.PRNumber = binding.Number
	current.Digest, err = contentDigest(current.Value)
	if err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "product_target", productTargetID(p.Target, p.WorkItem.ID), p.Target.WaveID, current); err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, "product_target_observation", current.Digest, p.Target.WaveID, current)
}

func bindPRReceiveSubscriptionTx(ctx context.Context, tx *sql.Tx, watch *PRSubscriptionIntent, binding PRResourceBinding) error {
	watch.ResourceKey, watch.PRNumber, watch.State = binding.ResourceKey, binding.Number, "bound"
	if err := kernelRecordTx(ctx, tx, "product_pr_subscription_intent", watch.ID, watch.SessionID, watch); err != nil {
		return err
	}
	raw, err := json.Marshal(watch)
	if err != nil {
		return err
	}
	updated, err := tx.ExecContext(ctx, "UPDATE kernel_subscriptions SET resource=?,payload=? WHERE id=? AND session_id=?", "external:"+binding.ResourceKey, raw, watch.ID, watch.SessionID)
	if err != nil {
		return err
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("PR receive subscription missing")
	}
	return nil
}
