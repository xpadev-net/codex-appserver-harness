package product

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// CandidateSealer is the synchronous owner of an admitted local Git operation.
// Its caller joins all Git/stopping-proof work before closing the Store. It does
// not apply Task success or publish/project the resulting candidate.
type CandidateSealer struct{ Store *storage.KernelStore }

func (s CandidateSealer) Seal(ctx context.Context, taskID, runID string) (storage.ProductSeal, error) {
	if s.Store == nil {
		return storage.ProductSeal{}, errors.New("configured product sealing store required")
	}
	saved, err := s.Store.ReserveProductSeal(ctx, taskID, runID)
	if err != nil {
		return saved, err
	}
	if saved.State == "sealed" {
		return saved, nil
	}
	claim, err := s.Store.ClaimProductSeal(ctx, taskID, runID, saved.Digest)
	if err != nil {
		return saved, err
	}
	if !claim.Start {
		return claim.Seal, storage.ErrProductSealHeld
	}
	saved = claim.Seal
	v, prepared := saved.Value.Workspace.Value, saved.Value.Workspace.Proof
	proof := storage.ProductSealProof{ProofID: saved.ID + ":stopped", Outcome: "failed", Reason: "local Git sealing failed after owner stopped"}
	sealErr := func() error {
		for _, entry := range []struct {
			path    string
			private bool
		}{{s.Store.Root, true}, {filepath.Join(s.Store.Root, "workspaces"), true}, {v.CWD, true}, {v.Request.Repository, false}} {
			if err := sameUserDirectory(entry.path, entry.private); err != nil {
				return err
			}
		}
		ctx = workspace.WithStoppedCommands(ctx)
		if input := saved.Value.Verification; input != nil {
			if err := verification.CheckChanges(ctx, v.CWD, input.Subject.Value.Candidate.SHA, input.Policy.Value.VerifierPaths); err != nil {
				return err
			}
			if err := input.InputPlan.ValidateFiles(v.CWD); err != nil {
				return err
			}
		}
		pinned := workspace.PinnedCheckout{Repository: v.Request.Repository, CWD: v.CWD, Branch: v.Branch, GitDir: prepared.GitDir, CommonDir: prepared.CommonDir, BaseSHA: saved.Value.Workspace.SealBaseSHA()}
		frozen, err := workspace.FreezePinned(ctx, pinned, "Harness candidate "+saved.ID)
		if err != nil {
			return err
		}
		if input := saved.Value.Verification; input != nil {
			if err := verification.CheckChanges(ctx, v.CWD, input.Subject.Value.Candidate.SHA, input.Policy.Value.VerifierPaths); err != nil {
				return err
			}
			if err := input.InputPlan.ValidateCommit(ctx, v.CWD, frozen.SHA); err != nil {
				return err
			}
			checked := input.CheckProof()
			proof.Verification = &checked
		}
		proof.Outcome, proof.Reason, proof.Frozen = "sealed", "fixed checkout sealed; local Git owner and descendants stopped", &frozen
		return nil
	}()
	if errors.Is(sealErr, workspace.ErrGitStopUnconfirmed) {
		return saved, sealErr
	}
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer stop()
	finished, finishErr := s.Store.FinishProductSeal(cleanup, taskID, runID, proof)
	if finishErr != nil {
		return saved, errors.Join(sealErr, fmt.Errorf("persist sealing stopping proof: %w", finishErr))
	}
	if finished.State == "stale" {
		return finished, storage.ErrProductSealHeld
	}
	return finished, sealErr
}
