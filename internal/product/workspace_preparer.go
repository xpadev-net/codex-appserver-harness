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
type WorkspacePreparer struct {
	Store       *storage.KernelStore
	RefreshBase func(context.Context, string) (storage.ProductBasePlan, error)
}

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
	return verifyPreparedWorkspaceHead(ctx, saved, saved.Value.Request.BaseSHA)
}
func verifyPreparedWorkspaceHead(ctx context.Context, saved storage.ProductWorkspace, expected string) (storage.ProductWorkspaceProof, error) {
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
	if head != expected {
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
	if request.BasePlanID != "" && p.RefreshBase == nil {
		return storage.ProductWorkspace{}, errors.New("fixed-base workspace requires the host source/head reader")
	}
	saved, err := p.Store.ReserveProductWorkspace(ctx, request)
	if err != nil {
		return saved, err
	}
	if saved.State == "ready" {
		return saved, nil
	} // Historical read-only replay grants no start.
	if saved.State != "prepared" {
		return saved, storage.ErrProductWorkspaceHeld
	}
	if request.BasePlanID != "" {
		if err := p.refreshBase(ctx, request); err != nil {
			return saved, err
		}
	}
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
	var fetched *workspace.CommitFetch
	var basePlan *storage.ProductBasePlan
	var merged *workspace.BaseMerge
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
		if request.BasePlanID != "" {
			if err := p.refreshBase(ctx, request); err != nil {
				return err
			}
			plan, err := p.Store.ReadProductBasePlan(ctx, request.BasePlanID)
			if err != nil {
				return err
			}
			basePlan = &plan
			pin, err := workspace.PinRepository(ctx, request.Repository)
			if err != nil {
				return err
			}
			for _, dir := range []string{pin.GitDir, pin.CommonDir} {
				if err := sameUserDirectory(dir, false); err != nil {
					return err
				}
			}
			if err := p.Store.CheckProductWorkspaceBaseFetch(ctx, request.TaskID, saved.Digest, pin); err != nil {
				return err
			}
			observation, err := workspace.FetchPinnedCommit(ctx, pin, plan.Value.Head.Remote, plan.Value.Head.HeadSHA, func(ctx context.Context) error {
				if err := p.refreshBase(ctx, request); err != nil {
					return err
				}
				return p.Store.MarkProductWorkspaceFetchMaySend(ctx, request.TaskID, saved.Digest, pin)
			})
			fetched = &observation
			if err != nil {
				return err
			}
			if err := p.refreshBase(ctx, request); err != nil {
				return err
			}
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
	var conflictReady *storage.ProductWorkspaceProof
	if createErr == nil && basePlan != nil && basePlan.Value.Input.Value.Preparation.Product != nil {
		// The new Task owns a fresh checkout of the original candidate. Its
		// comparison base is the candidate's old base, never its divergent HEAD.
		prepared, err := verifyPreparedWorkspace(ctx, saved)
		createErr = err
		if createErr == nil {
			original := basePlan.Value.Input.Value.Preparation.Product.Value.Candidate
			frozen := workspace.FrozenCheckout{PinnedCheckout: workspace.PinnedCheckout{Repository: request.Repository, CWD: saved.Value.CWD, Branch: saved.Value.Branch, GitDir: prepared.GitDir, CommonDir: prepared.CommonDir, BaseSHA: original.BaseSHA}, SHA: original.SHA}
			createErr = p.refreshBase(ctx, request)
			if createErr == nil {
				createErr = p.Store.CheckProductWorkspaceBaseFetch(ctx, request.TaskID, saved.Digest, fetched.Source)
			}
			if createErr == nil {
				observation, err := workspace.MergePinnedBase(ctx, frozen, basePlan.Value.Head.HeadSHA, func(ctx context.Context) error {
					if err := p.refreshBase(ctx, request); err != nil {
						return err
					}
					return p.Store.MarkProductWorkspaceBaseMergeMaySend(ctx, request.TaskID, saved.Digest, frozen)
				})
				merged, createErr = &observation, err
				if createErr == nil && observation.Outcome == "conflict" {
					if basePlan.Value.Input.Value.Preparation.Task.Spec.ProductBaseConflict == nil {
						createErr = storage.ErrProductWorkspaceHeld
					} else {
						prepared.Clean = false
						prepared.Reason = "fixed merge conflict preserved for the new authenticated fix Task"
						conflictReady = &prepared
					}
				}
			}
		}
	}
	if createErr == nil {
		if conflictReady != nil {
			proof = *conflictReady
		} else {
			var verified storage.ProductWorkspaceProof
			head := request.BaseSHA
			if merged != nil {
				head = merged.HeadSHA
			}
			verified, createErr = verifyPreparedWorkspaceHead(ctx, saved, head)
			if createErr == nil {
				proof = verified
			}
		}
	}
	if createErr == nil && request.BasePlanID != "" {
		createErr = p.refreshBase(ctx, request)
		if createErr != nil {
			proof = storage.ProductWorkspaceProof{ProofID: saved.ID + ":stopped", Outcome: "failed", Reason: "fixed base changed after checkout owner stopped"}
		}
	}
	if err := ctx.Err(); err != nil {
		createErr = errors.Join(createErr, err)
		proof = storage.ProductWorkspaceProof{ProofID: saved.ID + ":stopped", Outcome: "failed", Reason: "canceled workspace owner stopped"}
	}
	proof.Fetch = fetched
	proof.BaseMerge = merged
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

func (p WorkspacePreparer) refreshBase(ctx context.Context, r storage.ProductWorkspaceRequest) error {
	plan, err := p.RefreshBase(ctx, r.TaskID)
	if err != nil {
		return err
	}
	origin := plan.Value.Head.HeadSHA
	if old := plan.Value.Input.Value.Preparation.Product; old != nil {
		origin = old.Value.Candidate.SHA
	}
	if plan.ID != r.BasePlanID || plan.Digest != r.BasePlanDigest || origin != r.BaseSHA || plan.Value.Input.Value.Policy.Value.Repository != r.Repository {
		return storage.ErrProductWorkspaceHeld
	}
	return nil
}
