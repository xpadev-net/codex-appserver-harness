package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

var ErrProductSealHeld = errors.New("product candidate sealing requires owner reconciliation")

// ProductSealValue binds a local Git operation to one stopped normal model run.
// No field is a model grant or a verification/approval verdict.
type ProductSealValue struct {
	TaskID         string           `json:"task_id"`
	RunID          string           `json:"run_id"`
	Workspace      ProductWorkspace `json:"workspace"`
	InputDigest    string           `json:"input_digest"`
	OutputDigest   string           `json:"output_digest"`
	SnapshotID     string           `json:"snapshot_id"`
	SnapshotDigest string           `json:"snapshot_digest"`
}
type ProductSealProof struct {
	ProofID string                    `json:"proof_id"`
	Outcome string                    `json:"outcome"` // sealed, failed, not_started; stopped Git owner only
	Reason  string                    `json:"reason"`
	Frozen  *workspace.FrozenCheckout `json:"frozen,omitempty"`
}
type ProductSeal struct {
	ID         string            `json:"id"`
	Value      ProductSealValue  `json:"value"`
	Digest     string            `json:"digest"`
	State      string            `json:"state"` // prepared, launching, sealed, failed, stale
	CreatedAt  time.Time         `json:"created_at"`
	ClaimedAt  time.Time         `json:"claimed_at"`
	FinishedAt time.Time         `json:"finished_at"`
	Proof      *ProductSealProof `json:"proof,omitempty"`
	Candidate  *runner.Candidate `json:"candidate,omitempty"`
}
type ProductSealClaim struct {
	Seal  ProductSeal `json:"seal"`
	Start bool        `json:"start"`
}
type productSealRecord struct {
	Seal   ProductSeal `json:"seal"`
	Digest string      `json:"digest"`
}

func productSealID(taskID, runID string) string { return structureID("product-seal-", taskID, runID) }

func saveProductSealTx(ctx context.Context, tx *sql.Tx, kind string, saved ProductSeal) error {
	digest, err := contentDigest(saved)
	if err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, kind, saved.ID, saved.Value.Workspace.Value.Target.WaveID, productSealRecord{Seal: saved, Digest: digest})
}
func loadProductSeal(ctx context.Context, q queryer, id string) (ProductSeal, error) {
	var record productSealRecord
	if err := taskEvidenceRecord(ctx, q, "product_seal", id, &record); err != nil {
		return record.Seal, err
	}
	saved := record.Seal
	digest, err := contentDigest(saved)
	if err != nil {
		return saved, err
	}
	valueDigest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if record.Digest != digest || saved.Digest != valueDigest || saved.ID != id || id != productSealID(saved.Value.TaskID, saved.Value.RunID) || saved.Value.TaskID == "" || saved.Value.RunID == "" || saved.CreatedAt.IsZero() || saved.Value.Workspace.State != "ready" || saved.Value.Workspace.Value.Request.TaskID != saved.Value.TaskID || !validSourceDigest(saved.Value.InputDigest) || !validSourceDigest(saved.Value.OutputDigest) || !validSourceDigest(saved.Value.SnapshotDigest) || saved.Value.SnapshotID == "" {
		return saved, ErrTaskEvidence
	}
	switch saved.State {
	case "prepared":
		if !saved.ClaimedAt.IsZero() || !saved.FinishedAt.IsZero() || saved.Proof != nil || saved.Candidate != nil {
			return saved, ErrTaskEvidence
		}
	case "launching":
		if saved.ClaimedAt.IsZero() || !saved.FinishedAt.IsZero() || saved.Proof != nil || saved.Candidate != nil {
			return saved, ErrTaskEvidence
		}
	case "sealed", "failed", "stale":
		if saved.FinishedAt.IsZero() || saved.Proof == nil || (saved.ClaimedAt.IsZero() && saved.Proof.Outcome != "not_started") {
			return saved, ErrTaskEvidence
		}
		if (saved.State == "sealed" || saved.State == "stale") && (saved.Proof.Outcome != "sealed" || saved.Candidate == nil) || saved.State == "failed" && (saved.Proof.Outcome != "failed" && saved.Proof.Outcome != "not_started" || saved.Candidate != nil) {
			return saved, ErrTaskEvidence
		}
	default:
		return saved, ErrTaskEvidence
	}
	return saved, nil
}
func (k *KernelStore) ProductSeal(ctx context.Context, taskID, runID string) (ProductSeal, error) {
	return loadProductSeal(ctx, k.DB, productSealID(taskID, runID))
}

// ReserveProductSeal starts no sealing mutation. It saves the immutable run input and
// output binding atomically with Git/destination leases. The normal Task result
// remains unapplied until a later controller candidate adoption step.
func (k *KernelStore) ReserveProductSeal(ctx context.Context, taskID, runID string) (ProductSeal, error) {
	var saved ProductSeal
	if taskID == "" || runID == "" || len(taskID) > contract.MaxInline || len(runID) > contract.MaxInline || !utf8.ValidString(taskID+runID) || strings.ContainsAny(taskID+runID, "\x00\r\n") {
		return saved, errors.New("bounded Task and run identity required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	id := productSealID(taskID, runID)
	saved, err = loadProductSeal(ctx, tx, id)
	if err == nil {
		return saved, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return saved, err
	}
	var history bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_seal_history' AND id=?)", id).Scan(&history); err != nil {
		return saved, err
	}
	if history {
		return saved, ErrTaskEvidence
	}
	value, err := k.productSealEvidenceTx(ctx, tx, taskID, runID)
	if err != nil {
		return saved, err
	}
	v := value.Workspace.Value
	reservation, err := k.reserveOperationCapacityTx(ctx, tx, OperationCapacityRequest{ID: id, Kind: "git", SessionID: v.ParentSessionID, Target: v.Target, Repository: v.Request.Repository, Worktree: v.CWD})
	if err != nil {
		return saved, err
	}
	if reservation.State != "held" {
		return saved, ErrProductSealHeld
	}
	digest, err := contentDigest(value)
	if err != nil {
		return saved, err
	}
	saved = ProductSeal{ID: id, Value: value, Digest: digest, State: "prepared", CreatedAt: k.clock()}
	if err := saveProductSealTx(ctx, tx, "product_seal", saved); err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

func productSealReservationTx(ctx context.Context, tx *sql.Tx, saved ProductSeal) error {
	r, err := loadOperationReservationTx(ctx, tx, saved.ID)
	if err != nil {
		return err
	}
	v := saved.Value.Workspace.Value
	if r.Kind != "git" || r.State != "held" || r.SessionID != v.ParentSessionID || r.Request.Target != v.Target || r.Request.Repository != v.Request.Repository || r.Request.Worktree != v.CWD {
		return ErrProductSealHeld
	}
	return nil
}

func (k *KernelStore) ClaimProductSeal(ctx context.Context, taskID, runID, digest string) (ProductSealClaim, error) {
	var claim ProductSealClaim
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return claim, err
	}
	defer tx.Rollback()
	saved, err := loadProductSeal(ctx, tx, productSealID(taskID, runID))
	if err != nil {
		return claim, err
	}
	claim.Seal = saved
	if saved.Digest != digest {
		return claim, ErrProductSealHeld
	}
	if saved.State != "prepared" {
		return claim, tx.Commit()
	}
	value, err := k.productSealEvidenceTx(ctx, tx, taskID, runID)
	if err != nil {
		return claim, err
	}
	if !reflect.DeepEqual(value, saved.Value) {
		return claim, ErrTaskEvidence
	}
	if err := productSealReservationTx(ctx, tx, saved); err != nil {
		return claim, err
	}
	saved.State, saved.ClaimedAt = "launching", k.clock()
	if err := saveProductSealTx(ctx, tx, "product_seal", saved); err != nil {
		return claim, err
	}
	claim.Seal = saved
	if err := tx.Commit(); err != nil {
		return claim, err
	}
	claim.Start = true
	return claim, nil
}

func (k *KernelStore) FinishProductSeal(ctx context.Context, taskID, runID string, proof ProductSealProof) (ProductSeal, error) {
	var saved ProductSeal
	if !utf8.ValidString(proof.ProofID+proof.Reason) || strings.TrimSpace(proof.ProofID) == "" || strings.TrimSpace(proof.Reason) == "" || len(proof.ProofID) > contract.MaxInline || len(proof.Reason) > contract.MaxInline || strings.ContainsAny(proof.ProofID+proof.Reason, "\x00\r\n") || (proof.Outcome != "sealed" && proof.Outcome != "failed" && proof.Outcome != "not_started") || (proof.Outcome == "sealed") != (proof.Frozen != nil) {
		return saved, errors.New("bounded stopped Git sealing proof required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	saved, err = loadProductSeal(ctx, tx, productSealID(taskID, runID))
	if err != nil {
		return saved, err
	}
	if saved.Proof != nil {
		if !reflect.DeepEqual(*saved.Proof, proof) {
			return saved, errors.New("sealing stopping proof replay conflict")
		}
		return saved, tx.Commit()
	}
	if (saved.State != "launching" || proof.Outcome == "not_started") && (saved.State != "prepared" || proof.Outcome != "not_started") {
		return saved, ErrProductSealHeld
	}
	if err := productSealReservationTx(ctx, tx, saved); err != nil {
		return saved, err
	}
	saved.State = "failed"
	if proof.Outcome == "sealed" {
		v := saved.Value.Workspace.Value
		prepared := saved.Value.Workspace.Proof
		expected := workspace.PinnedCheckout{Repository: v.Request.Repository, CWD: v.CWD, Branch: v.Branch, GitDir: prepared.GitDir, CommonDir: prepared.CommonDir, BaseSHA: v.Request.BaseSHA}
		if proof.Frozen.PinnedCheckout != expected || !fixedSHA(proof.Frozen.SHA) {
			return saved, ErrTaskEvidence
		}
		value, evidenceErr := k.productSealEvidenceTx(ctx, tx, taskID, runID)
		if evidenceErr != nil && !errors.Is(evidenceErr, ErrProductSealHeld) && !errors.Is(evidenceErr, ErrProductChangeReference) && !errors.Is(evidenceErr, ErrTaskEvidence) && !errors.Is(evidenceErr, ErrTaskBinding) && !errors.Is(evidenceErr, sql.ErrNoRows) {
			return saved, evidenceErr
		}
		saved.State = "sealed"
		if evidenceErr != nil || !reflect.DeepEqual(value, saved.Value) {
			saved.State = "stale"
		}
		saved.Candidate = &runner.Candidate{ID: structureID("candidate-", saved.ID, proof.Frozen.SHA), SHA: proof.Frozen.SHA, BaseSHA: v.Request.BaseSHA, ScopeRevision: v.Target.ScopeRevision, PolicyRevision: v.Target.PolicyRevision}
	}
	saved.Proof, saved.FinishedAt = &proof, k.clock()
	disposition := "process_stopped"
	if proof.Outcome == "not_started" {
		disposition = "not_started"
	}
	if err := k.releaseOperationCapacityTx(ctx, tx, OperationReleaseProof{OperationID: saved.ID, ProofID: proof.ProofID, Disposition: disposition, Reason: proof.Reason}); err != nil {
		return saved, err
	}
	if err := saveProductSealTx(ctx, tx, "product_seal", saved); err != nil {
		return saved, err
	}
	if err := saveProductSealTx(ctx, tx, "product_seal_history", saved); err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}
