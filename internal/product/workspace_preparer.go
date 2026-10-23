package product

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// WorkspacePreparer is the synchronous local Git owner. Its caller retains the
// store until it returns, including bounded stopping-proof persistence on cancel.
// Host configuration maps RepositoryID to the canonical local repository.
type WorkspacePreparer struct{ Store *storage.KernelStore }

func sameUserDirectory(path string, private bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	forbidden := os.FileMode(0022)
	if private {
		forbidden = 0077
	}
	if !info.IsDir() || !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&forbidden != 0 {
		return errors.New("same-user protected workspace directory required")
	}
	return nil
}
func verifyPreparedWorkspace(ctx context.Context, saved storage.ProductWorkspace) (storage.ProductWorkspaceProof, error) {
	v := saved.Value
	proof := storage.ProductWorkspaceProof{ProofID: saved.ID + ":stopped", Outcome: "prepared", Reason: "local Git commands stopped; fixed checkout verified", CWD: v.CWD, Branch: v.Branch}
	if err := workspace.Verify(ctx, v.Request.Repository, v.CWD, v.Branch); err != nil {
		return proof, err
	}
	if err := workspace.Clean(ctx, v.CWD); err != nil {
		return proof, err
	}
	head, err := workspace.Resolve(ctx, v.CWD, "HEAD")
	if err != nil {
		return proof, err
	}
	if head != v.Request.BaseSHA {
		return proof, errors.New("prepared checkout is not the pinned base commit")
	}
	paths := []struct {
		arg string
		out *string
	}{{"--absolute-git-dir", &proof.GitDir}, {"--git-common-dir", &proof.CommonDir}}
	for _, entry := range paths {
		raw, err := workspace.Git(ctx, v.CWD, "rev-parse", "--path-format=absolute", entry.arg)
		if err != nil {
			return proof, err
		}
		real, err := filepath.EvalSymlinks(strings.TrimSpace(raw))
		if err != nil {
			return proof, err
		}
		if err := sameUserDirectory(real, false); err != nil {
			return proof, err
		}
		*entry.out = real
	}
	proof.HeadSHA, proof.Clean = head, true
	return proof, nil
}
func (p WorkspacePreparer) Prepare(ctx context.Context, request storage.ProductWorkspaceRequest) (storage.ProductWorkspace, error) {
	if p.Store == nil {
		return storage.ProductWorkspace{}, errors.New("configured product workspace store required")
	}
	saved, err := p.Store.ReserveProductWorkspace(ctx, request)
	if err != nil {
		return saved, err
	}
	if saved.State == "ready" {
		return saved, nil
	} // Historical read-only replay grants no start.
	claim, err := p.Store.ClaimProductWorkspace(ctx, request.TaskID, saved.Digest)
	if err != nil {
		return saved, err
	}
	if !claim.Start {
		return claim.Workspace, storage.ErrProductWorkspaceHeld
	}
	saved = claim.Workspace
	ctx = workspace.WithStoppedCommands(ctx)
	// All Git subprocesses are synchronous. Git disables hooks, external
	// filters/drivers, fsmonitor, auto maintenance and recursive submodules.
	// A missing final receipt leaves the launching intent/leases held; a replay
	// never runs these commands a second time.
	createErr := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sameUserDirectory(p.Store.Root, true); err != nil {
			return err
		}
		if err := sameUserDirectory(saved.Value.Request.Repository, false); err != nil {
			return err
		}
		parent := filepath.Join(p.Store.Root, "workspaces")
		if err := os.Mkdir(parent, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := sameUserDirectory(parent, true); err != nil {
			return err
		}
		if _, err := os.Lstat(saved.Value.CWD); err == nil {
			return errors.New("workspace destination already exists; explicit reconciliation required")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := workspace.Create(ctx, saved.Value.Request.Repository, saved.Value.CWD, saved.Value.Branch, saved.Value.Request.BaseSHA); err != nil {
			return err
		}
		if err := os.Chmod(saved.Value.CWD, 0700); err != nil {
			return err
		}
		return sameUserDirectory(saved.Value.CWD, true)
	}()
	proof := storage.ProductWorkspaceProof{ProofID: saved.ID + ":stopped", Outcome: "failed", Reason: "local workspace preparation failed after Git owner stopped"}
	if createErr == nil {
		var verified storage.ProductWorkspaceProof
		verified, createErr = verifyPreparedWorkspace(ctx, saved)
		if createErr == nil {
			proof = verified
		}
	}
	if err := ctx.Err(); err != nil {
		createErr = errors.Join(createErr, err)
		proof = storage.ProductWorkspaceProof{ProofID: saved.ID + ":stopped", Outcome: "failed", Reason: "canceled workspace owner stopped"}
	}
	if errors.Is(createErr, workspace.ErrGitStopUnconfirmed) {
		return saved, createErr
	}
	// This is cleanup of an already admitted mutation, not detached new work.
	// The caller joins us before closing the store/controller owner.
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer stop()
	finished, finishErr := p.Store.FinishProductWorkspace(cleanup, request.TaskID, proof)
	if finishErr != nil {
		return saved, errors.Join(createErr, fmt.Errorf("persist workspace stopping proof: %w", finishErr))
	}
	if finished.State == "stale" {
		return finished, storage.ErrProductWorkspaceHeld
	}
	return finished, createErr
}
