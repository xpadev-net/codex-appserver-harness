package storage

import (
	"context"
	"errors"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func validateProductSealResolution(ws ProductWorkspace, proof ProductSealProof) error {
	r := proof.Resolution
	if !ws.PreparedBaseConflict() {
		if r != nil {
			return ErrTaskEvidence
		}
		return nil
	}
	if r == nil {
		if proof.Outcome == "sealed" {
			return ErrTaskEvidence
		}
		return nil
	}
	v, prepared := ws.Value, ws.Proof
	expected := workspace.PinnedCheckout{Repository: v.Request.Repository, CWD: v.CWD, Branch: v.Branch, GitDir: prepared.GitDir, CommonDir: prepared.CommonDir, BaseSHA: ws.SealBaseSHA()}
	if r.Source != expected || r.BaseSHA != v.ComparisonBaseSHA || r.InitialHeadSHA != "" && !fixedSHA(r.InitialHeadSHA) || r.MergeHeadSHA != "" && !fixedSHA(r.MergeHeadSHA) {
		return ErrTaskEvidence
	}
	if proof.Outcome == "sealed" {
		if !r.Attempted || r.InitialHeadSHA != prepared.HeadSHA || r.MergeHeadSHA != v.ComparisonBaseSHA || r.Frozen == nil || proof.Frozen == nil || *r.Frozen != *proof.Frozen || r.Failure != nil {
			return ErrTaskEvidence
		}
	} else if r.Frozen != nil {
		return ErrTaskEvidence
	}
	if failure := r.Failure; failure != nil {
		if !r.Attempted || !fixedSHA(failure.HeadSHA) || failure.MergeHeadSHA != "" && !fixedSHA(failure.MergeHeadSHA) || !validBaseMergePaths(failure.Unresolved) {
			return ErrTaskEvidence
		}
	}
	return nil
}

// CheckProductSealBeforeFreeze is the current permission boundary for the
// already claimed, synchronous Git owner. It grants no second start and checks
// actual resource generations as well as the exact saved run/input/output.
func (k *KernelStore) CheckProductSealBeforeFreeze(ctx context.Context, taskID, runID, digest string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	saved, err := loadProductSeal(ctx, tx, productSealID(taskID, runID))
	if err != nil {
		return err
	}
	if saved.State != "launching" || saved.Digest != digest {
		return ErrProductSealHeld
	}
	value, err := k.productSealEvidenceTx(ctx, tx, taskID, runID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(value, saved.Value) {
		return ErrTaskEvidence
	}
	if err := k.productWorkspaceOriginCurrentTx(ctx, tx, saved.Value.Workspace); err != nil {
		return err
	}
	if err := productSealReservationTx(ctx, tx, saved); err != nil {
		return err
	}
	r, err := loadOperationReservationTx(ctx, tx, saved.ID)
	if err != nil {
		return err
	}
	for _, lease := range r.Resources {
		var owner, state string
		var generation int64
		if err := tx.QueryRowContext(ctx, "SELECT run_id,state,generation FROM kernel_resource_leases WHERE resource=?", lease.Resource).Scan(&owner, &state, &generation); err != nil {
			return errors.Join(ErrProductSealHeld, err)
		}
		if owner != saved.ID || state != "held" || generation != lease.Generation {
			return ErrProductSealHeld
		}
	}
	return nil
}
