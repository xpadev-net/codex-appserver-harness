package workspace

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"
)

// BaseMerge is a physical Git observation. A conflict keeps its original HEAD
// and unresolved index for the owning controller to capture and reconcile. It
// grants no candidate adoption, Task success, verification or publication.
type BaseMerge struct {
	Source    FrozenCheckout `json:"source"`
	BaseSHA   string         `json:"base_sha"`
	Attempted bool           `json:"attempted"`
	Outcome   string         `json:"outcome,omitempty"`
	HeadSHA   string         `json:"head_sha,omitempty"`
	Conflicts []string       `json:"conflicts,omitempty"`
}

func mergeIdle(p PinnedCheckout) error {
	for _, name := range []string{"MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "REBASE_HEAD", "rebase-merge", "rebase-apply", "sequencer"} {
		if _, err := os.Lstat(filepath.Join(p.GitDir, name)); err == nil {
			return errors.New("checkout already has an unresolved Git operation")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
func baseMergeSource(ctx context.Context, frozen FrozenCheckout, base string) error {
	if len(frozen.SHA) != 40 || strings.Trim(frozen.SHA, "0123456789abcdef") != "" || len(base) != 40 || strings.Trim(base, "0123456789abcdef") != "" {
		return errors.New("fixed candidate and base commit required")
	}
	pin := RepositoryPin{Repository: frozen.Repository, CommonDir: frozen.CommonDir}
	raw, err := pinnedGit(ctx, frozen.Repository, "rev-parse", "--path-format=absolute", "--absolute-git-dir")
	if err != nil {
		return err
	}
	pin.GitDir = strings.TrimSpace(raw)
	if err := verifyRepositoryPin(ctx, pin); err != nil {
		return err
	}
	// includeIf/gitdir configuration can differ in the linked checkout.
	// Reject implicit promisor transport there before merge reads any object.
	if err := verifyRepositoryPin(ctx, RepositoryPin{Repository: frozen.CWD, GitDir: frozen.GitDir, CommonDir: frozen.CommonDir}); err != nil {
		return err
	}
	head, err := verifyPinned(ctx, frozen.PinnedCheckout)
	if err != nil {
		return err
	}
	if head != frozen.SHA {
		return errors.New("candidate changed before base merge")
	}
	if err := Clean(ctx, frozen.CWD); err != nil {
		return err
	}
	if err := mergeIdle(frozen.PinnedCheckout); err != nil {
		return err
	}
	available, err := pinnedCommitAvailable(ctx, pin, base)
	if err != nil {
		return err
	}
	if !available {
		return errors.New("fixed base commit is unavailable locally")
	}
	// A force-pushed or unrelated base requires an explicit new host decision.
	_, err = pinnedGit(ctx, frozen.CWD, "merge-base", "--is-ancestor", frozen.BaseSHA, base)
	return err
}

// MergePinnedBase attempts an ordinary local merge of the exact new base. The
// caller owns Git/worktree leases and a durable one-use mutation fence, and has
// stopped all users of this checkout. Unknown stop or any error retains that
// ownership; replay decisions belong to the durable controller, not this port.
func MergePinnedBase(ctx context.Context, frozen FrozenCheckout, base string, beforeMerge func(context.Context) error) (BaseMerge, error) {
	out := BaseMerge{Source: frozen, BaseSHA: base}
	if beforeMerge == nil {
		return out, errors.New("host base merge fence required")
	}
	ctx = WithStoppedCommands(ctx)
	if err := baseMergeSource(ctx, frozen, base); err != nil {
		return out, err
	}
	_, ancestorErr := pinnedGit(ctx, frozen.CWD, "merge-base", "--is-ancestor", base, frozen.SHA)
	if ancestorErr == nil {
		out.Outcome, out.HeadSHA = "already_current", frozen.SHA
		return out, ctx.Err()
	}
	var exit *exec.ExitError
	if errors.Is(ancestorErr, ErrGitStopUnconfirmed) || !errors.As(ancestorErr, &exit) || exit.ExitCode() != 1 {
		return out, ancestorErr
	}
	if err := beforeMerge(ctx); err != nil {
		return out, err
	}
	if err := baseMergeSource(ctx, frozen, base); err != nil {
		return out, err
	}
	out.Attempted = true
	// Explicit strategy/options override branch.mergeOptions and repository
	// settings. No hook, filter, rerere, editor, signer or autostash may run here.
	args := []string{"-c", "branch." + frozen.Branch + ".mergeOptions=", "-c", "rerere.enabled=false", "-c", "merge.autoStash=false", "-c", "merge.stat=false", "-c", "merge.log=false", "-c", "merge.verifySignatures=false", "-c", "commit.gpgsign=false", "merge", "--no-commit", "--no-ff", "--no-autostash", "--no-verify", "--no-verify-signatures", "--no-edit", "-s", "ort", "--", base}
	_, mergeErr := pinnedGit(ctx, frozen.CWD, args...)
	if errors.Is(mergeErr, ErrGitStopUnconfirmed) || ctx.Err() != nil {
		return out, errors.Join(mergeErr, ctx.Err())
	}
	if mergeErr != nil {
		if !errors.As(mergeErr, &exit) || exit.ExitCode() != 1 {
			return out, mergeErr
		}
		raw, err := pinnedGit(ctx, frozen.CWD, "diff", "--no-ext-diff", "--no-textconv", "--name-only", "--diff-filter=U", "-z")
		if err != nil {
			return out, errors.Join(mergeErr, err)
		}
		if len(raw) == 0 || len(raw) > 1<<20 || !strings.HasSuffix(raw, "\x00") {
			return out, errors.Join(mergeErr, errors.New("bounded unresolved merge evidence required"))
		}
		paths := strings.Split(strings.TrimSuffix(raw, "\x00"), "\x00")
		if len(paths) > 256 {
			return out, errors.New("base merge conflicts exceed bound")
		}
		for _, p := range paths {
			if !utf8.ValidString(p) || p == "" || p == "." || p == ".." || path.IsAbs(p) || path.Clean(p) != p || strings.HasPrefix(p, "../") {
				return out, errors.New("invalid unresolved merge path")
			}
		}
		slices.Sort(paths)
		out.Conflicts = paths
		head, err := verifyPinned(ctx, frozen.PinnedCheckout)
		if err != nil {
			return out, err
		}
		mergeHead, err := pinnedGit(ctx, frozen.CWD, "rev-parse", "--verify", "MERGE_HEAD^{commit}")
		if err != nil {
			return out, err
		}
		if head != frozen.SHA || strings.TrimSpace(mergeHead) != base {
			return out, errors.New("conflicting merge identity changed")
		}
		out.Outcome, out.HeadSHA = "conflict", head
		return out, ctx.Err()
	}
	// No model text is used as the commit message or as Git arguments.
	if _, err := pinnedGit(ctx, frozen.CWD, "-c", "user.name=Harness", "-c", "user.email=harness@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.editor=false", "commit", "--no-verify", "-m", "Merge fixed product base"); err != nil {
		return out, err
	}
	head, err := verifyPinned(ctx, frozen.PinnedCheckout)
	if err != nil {
		return out, err
	}
	parents, err := pinnedGit(ctx, frozen.CWD, "rev-list", "--parents", "-n", "1", head)
	if err != nil {
		return out, err
	}
	if !slices.Equal(strings.Fields(parents), []string{head, frozen.SHA, base}) {
		return out, errors.New("base merge does not preserve exact candidate and base parents")
	}
	if err := Clean(ctx, frozen.CWD); err != nil {
		return out, err
	}
	if err := mergeIdle(frozen.PinnedCheckout); err != nil {
		return out, err
	}
	out.Outcome, out.HeadSHA = "merged", head
	return out, ctx.Err()
}
