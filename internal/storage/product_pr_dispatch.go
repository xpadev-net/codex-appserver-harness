package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrPRDispatchHeld = errors.New("PR dispatch is held")
var ErrPRReconciliationRequired = errors.New("PR outcome requires external reconciliation")

// A prepared claim has not authorized an HTTP mutation. The trusted P adapter
// must check its canonical WorkItem/validation/config and then MarkPRMayHaveSent
// in its final BeforeWrite callback. These storage ports perform no external I/O.
type PRDispatchClaim struct {
	OperationID        string                `json:"operation_id"`
	OwnerID            string                `json:"owner_id"`
	State              string                `json:"state"` // prepared, unknown, canceled
	Product            ProductTargetSnapshot `json:"product"`
	ReservationID      string                `json:"reservation_id"`
	ReservationCreated bool                  `json:"reservation_created"`
	MutationLease      ResourceLease         `json:"mutation_lease"`
	PreparedAt         time.Time             `json:"prepared_at"`
	MayHaveSentAt      time.Time             `json:"may_have_sent_at"`
}

func prIntentTx(ctx context.Context, q queryer, id string) (PROperationIntent, string, error) {
	var intent PROperationIntent
	if err := completionRecord(ctx, q, "product_pr_intent", id, &intent); err != nil {
		return intent, "", err
	}
	var state, session string
	var raw []byte
	if err := q.QueryRowContext(ctx, "SELECT state,session_id,payload FROM kernel_tool_operations WHERE id=?", id).Scan(&state, &session, &raw); err != nil {
		return intent, "", err
	}
	var op kernelToolOperation
	if err := json.Unmarshal(raw, &op); err != nil {
		return intent, "", err
	}
	if intent.Source != op || op.ID != id || session != op.SessionID || (op.Name != "pr_publish" && op.Name != "pr_review_request") || intent.SubscriptionID != kernelOperationSubscriptionID(op.SessionID, id) || intent.MarkerWorkItemID != intent.Candidate.WorkItemID {
		return intent, "", errors.New("inconsistent PR operation identity")
	}
	return intent, state, nil
}
func (k *KernelStore) preflightPRTx(ctx context.Context, tx *sql.Tx, intent PROperationIntent) (ProductTargetSnapshot, domain.Wave, error) {
	var saved ProductTargetSnapshot
	w, err := loadWave(ctx, tx, intent.Source.Target.WaveID)
	if err != nil {
		return saved, w, err
	}
	if w.Target() != intent.Source.Target || w.Policy.Stage != "P" || !w.Policy.Product || w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return saved, w, ErrPRDispatchHeld
	}
	b, err := sessionBindingTx(ctx, tx, intent.Source.SessionID)
	if err != nil {
		return saved, w, err
	}
	if b.Closed {
		return saved, w, ErrPRDispatchHeld
	}
	if b.Kind == "orchestrator" {
		if w.OrchestratorSessionID != b.ID {
			return saved, w, ErrPRDispatchHeld
		}
	} else if b.Kind == "task" {
		task, e := loadTask(ctx, tx, b.TaskID)
		if e != nil {
			return saved, w, e
		}
		if task.Target() != w.Target() || task.ParentSessionID != w.OrchestratorSessionID || task.Paused || (task.State != contract.TaskRunning && task.State != contract.TaskWaiting) || !slices.Contains(task.Spec.WorkItemIDs, intent.Candidate.WorkItemID) {
			return saved, w, ErrPRDispatchHeld
		}
	} else {
		return saved, w, ErrPRDispatchHeld
	}
	saved, err = loadProductTargetTx(ctx, tx, w.Target(), intent.Candidate.WorkItemID)
	if err != nil {
		return saved, w, err
	}
	p := saved.Value
	if saved.Digest != intent.ProductDigest || p.WorkItem.Paused || p.WorkItem.MergeHold || p.WorkItem.Phase != domain.Reviewing || p.Candidate.ID != intent.Candidate.CandidateID || p.Candidate.SHA != intent.Candidate.SHA || (intent.Source.Name == "pr_review_request" && p.PRNumber < 1) {
		return saved, w, ErrPRDispatchHeld
	}
	return saved, w, nil
}

// PreparePRDispatch atomically fences a single executor, claims the mutation
// lease and reserves PR/Cloud capacity. Resume does not repeat a possibly sent
// write: unknown claims require a read-only external reconciliation first.
func (k *KernelStore) PreparePRDispatch(ctx context.Context, id, owner string) (PRDispatchClaim, error) {
	var claim PRDispatchClaim
	if strings.TrimSpace(owner) == "" || len(owner) > 256 || !utf8.ValidString(owner) {
		return claim, errors.New("PR dispatch owner required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return claim, err
	}
	defer tx.Rollback()
	intent, state, err := prIntentTx(ctx, tx, id)
	if err != nil {
		return claim, err
	}
	if state == "unknown" {
		return claim, ErrPRReconciliationRequired
	}
	if state != "accepted" {
		return claim, ErrPRDispatchHeld
	}
	p, w, err := k.preflightPRTx(ctx, tx, intent)
	if err != nil {
		return claim, err
	}
	err = completionRecord(ctx, tx, "product_pr_dispatch", id, &claim)
	if err == nil {
		if claim.State != "prepared" || claim.OwnerID != owner {
			return claim, ErrPRDispatchHeld
		}
		if err = validatePRClaimTx(ctx, tx, intent, claim); err != nil {
			return claim, err
		}
		return claim, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return claim, err
	}
	resource := fmt.Sprintf("github:%d:work:%s", p.Value.WorkItem.RepositoryID, p.Value.WorkItem.ID)
	request := OperationCapacityRequest{ID: structureID("product-pr-seat-", w.ID, resource), Kind: "pr", SessionID: w.OrchestratorSessionID, Target: w.Target(), ResourceKey: resource}
	if intent.Source.Name == "pr_review_request" {
		// A registered PR number is not a substitute for accounting its open
		// resource. Bootstrap/binding must retain its PR seat before Cloud work.
		pr, e := loadOperationReservationTx(ctx, tx, request.ID)
		if errors.Is(e, sql.ErrNoRows) {
			return claim, ErrPRDispatchHeld
		}
		if e != nil {
			return claim, e
		}
		if pr.State != "held" || pr.Kind != "pr" || pr.WaveID != w.ID || pr.SessionID != w.OrchestratorSessionID || pr.Request.ResourceKey != resource {
			return claim, ErrPRDispatchHeld
		}
		request.ID, request.Kind, request.ResourceKey, request.CandidateSHA = id, "cloud_review", fmt.Sprintf("github:%d:pull:%d", p.Value.WorkItem.RepositoryID, p.Value.PRNumber), p.Value.Candidate.SHA
	}
	reservation, err := loadOperationReservationTx(ctx, tx, request.ID)
	created := errors.Is(err, sql.ErrNoRows)
	if created {
		reservation, err = k.reserveOperationCapacityTx(ctx, tx, request)
	}
	if err != nil {
		return claim, err
	}
	// PR capacity lives until the PR is closed/merged and can span plan versions.
	// Each write still owns a separate exclusive mutation lease.
	if reservation.State != "held" || reservation.Kind != request.Kind || reservation.WaveID != w.ID || reservation.SessionID != w.OrchestratorSessionID || reservation.Request.ResourceKey != request.ResourceKey || reservation.Request.CandidateSHA != request.CandidateSHA {
		return claim, ErrPRDispatchHeld
	}
	lease, err := claimResourceTx(ctx, tx, "product-pr-mutation:"+resource, id, w.ID)
	if err != nil {
		return claim, err
	}
	claim = PRDispatchClaim{OperationID: id, OwnerID: owner, State: "prepared", Product: p, ReservationID: reservation.ID, ReservationCreated: created, MutationLease: lease, PreparedAt: k.clock()}
	if err = kernelRecordTx(ctx, tx, "product_pr_dispatch", id, intent.Source.SessionID, claim); err != nil {
		return claim, err
	}
	return claim, tx.Commit()
}
func readPRClaimReservationTx(ctx context.Context, tx *sql.Tx, intent PROperationIntent, claim PRDispatchClaim) (OperationReservation, error) {
	var empty OperationReservation
	digest, err := contentDigest(claim.Product.Value)
	if err != nil {
		return empty, err
	}
	if claim.OperationID != intent.Source.ID || claim.Product.Digest != digest || digest != intent.ProductDigest || claim.Product.Value.Target != intent.Source.Target {
		return empty, errors.New("PR dispatch claim identity conflict")
	}
	var owner, state string
	var generation int64
	if err = tx.QueryRowContext(ctx, "SELECT run_id,state,generation FROM kernel_resource_leases WHERE resource=?", claim.MutationLease.Resource).Scan(&owner, &state, &generation); err != nil {
		return empty, err
	}
	if owner != claim.OperationID || state != "held" || generation != claim.MutationLease.Generation {
		return empty, ErrPRDispatchHeld
	}
	r, err := loadOperationReservationTx(ctx, tx, claim.ReservationID)
	if err != nil {
		return empty, err
	}
	if r.WaveID != intent.Source.Target.WaveID {
		return empty, ErrPRDispatchHeld
	}
	return r, nil
}
func validatePRClaimTx(ctx context.Context, tx *sql.Tx, intent PROperationIntent, claim PRDispatchClaim) error {
	r, err := readPRClaimReservationTx(ctx, tx, intent, claim)
	if err != nil {
		return err
	}
	if r.State != "held" {
		return ErrPRDispatchHeld
	}
	return nil
}

// MarkPRMayHaveSent runs only after the P adapter's canonical source/gate checks,
// immediately before its HTTP mutation. It consumes the prepared claim once.
// Even a crash before the actual HTTP call must conservatively retain unknown.
func (k *KernelStore) MarkPRMayHaveSent(ctx context.Context, id, owner string) (PRDispatchClaim, error) {
	var claim PRDispatchClaim
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return claim, err
	}
	defer tx.Rollback()
	intent, state, err := prIntentTx(ctx, tx, id)
	if err != nil {
		return claim, err
	}
	if state == "unknown" {
		return claim, ErrPRReconciliationRequired
	}
	if state != "accepted" {
		return claim, ErrPRDispatchHeld
	}
	if err = completionRecord(ctx, tx, "product_pr_dispatch", id, &claim); err != nil {
		return claim, err
	}
	if claim.State != "prepared" || claim.OwnerID != owner {
		return claim, ErrPRDispatchHeld
	}
	if _, _, err = k.preflightPRTx(ctx, tx, intent); err != nil {
		return claim, err
	}
	if err = validatePRClaimTx(ctx, tx, intent, claim); err != nil {
		return claim, err
	}
	claim.State, claim.MayHaveSentAt = "unknown", k.clock()
	intent.State = "unknown"
	if err = kernelRecordTx(ctx, tx, "product_pr_dispatch", id, intent.Source.SessionID, claim); err != nil {
		return claim, err
	}
	if err = savePRDispositionTx(ctx, tx, intent, "unknown", ""); err != nil {
		return claim, err
	}
	return claim, tx.Commit()
}
func savePRDispositionTx(ctx context.Context, tx *sql.Tx, intent PROperationIntent, state, content string) error {
	if err := kernelRecordTx(ctx, tx, "product_pr_intent", intent.Source.ID, intent.Source.SessionID, intent); err != nil {
		return err
	}
	reply := contract.ToolReply{SchemaVersion: 1, State: state, OperationID: intent.Source.ID, SubscriptionIDs: []string{intent.SubscriptionID}, ResourceID: intent.Candidate.WorkItemID, Content: content}
	raw, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_tool_operations SET state=?,result=? WHERE id=?", state, raw, intent.Source.ID)
	return err
}

// CancelUnsentPRDispatch fences delayed send callbacks even after pause, cancel
// or scope change. It cannot discard unknown requests or release their slots.
func (k *KernelStore) CancelUnsentPRDispatch(ctx context.Context, id, owner, reason string) error {
	if strings.TrimSpace(owner) == "" || len(owner) > 256 || !utf8.ValidString(owner) || strings.TrimSpace(reason) == "" || len(reason) > contract.MaxInline || !utf8.ValidString(reason) {
		return errors.New("unsent PR cancellation requires reason")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	intent, state, err := prIntentTx(ctx, tx, id)
	if err != nil {
		return err
	}
	if state == "unknown" {
		return ErrPRReconciliationRequired
	}
	if state == "completed" && intent.State == "canceled" {
		var audit map[string]string
		if err = completionRecord(ctx, tx, "product_pr_unsent_cancellation", id, &audit); err != nil {
			return err
		}
		if audit["owner_id"] != owner || audit["reason"] != reason {
			return errors.New("PR cancellation replay conflict")
		}
		return tx.Commit()
	}
	if state != "accepted" {
		return ErrPRDispatchHeld
	}
	var claim PRDispatchClaim
	err = completionRecord(ctx, tx, "product_pr_dispatch", id, &claim)
	if err == nil {
		if claim.State != "prepared" || claim.OwnerID != owner {
			return ErrPRDispatchHeld
		}
		reservation, e := readPRClaimReservationTx(ctx, tx, intent, claim)
		if e != nil {
			return e
		}
		if reservation.State != "held" && (reservation.State != "released" || reservation.Proof == nil) {
			return ErrPRDispatchHeld
		}
		if reservation.State == "held" && claim.ReservationCreated && (intent.Source.Name == "pr_review_request" || claim.Product.Value.PRNumber == 0) {
			if err = k.releaseOperationCapacityTx(ctx, tx, OperationReleaseProof{OperationID: claim.ReservationID, ProofID: id + ":unsent", Disposition: "not_started", Reason: reason}); err != nil {
				return err
			}
		}
		result, e := tx.ExecContext(ctx, "UPDATE kernel_resource_leases SET state='released' WHERE resource=? AND run_id=? AND generation=? AND state='held'", claim.MutationLease.Resource, id, claim.MutationLease.Generation)
		if e != nil {
			return e
		}
		count, e := result.RowsAffected()
		if e != nil {
			return e
		}
		if count != 1 {
			return ErrPRDispatchHeld
		}
		claim.State = "canceled"
		if err = kernelRecordTx(ctx, tx, "product_pr_dispatch", id, intent.Source.SessionID, claim); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	intent.State = "canceled"
	if err = savePRDispositionTx(ctx, tx, intent, "completed", "Canceled before external send."); err != nil {
		return err
	}
	var watch PRSubscriptionIntent
	if err = completionRecord(ctx, tx, "product_pr_subscription_intent", intent.SubscriptionID, &watch); err != nil {
		return err
	}
	watch.State = "canceled"
	if err = kernelRecordTx(ctx, tx, "product_pr_subscription_intent", watch.ID, intent.Source.SessionID, watch); err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "product_pr_unsent_cancellation", id, intent.Source.SessionID, map[string]string{"reason": reason, "owner_id": owner}); err != nil {
		return err
	}
	return tx.Commit()
}
