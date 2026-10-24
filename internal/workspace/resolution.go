package workspace

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

// ResolvedBase is stopped local Git evidence, not a quality or approval verdict.
// The host owns staging because workers receive read-only Git metadata.
type ResolvedBase struct {
	Source         PinnedCheckout    `json:"source"`
	BaseSHA        string            `json:"base_sha"`
	InitialHeadSHA string            `json:"initial_head_sha,omitempty"`
	MergeHeadSHA   string            `json:"merge_head_sha,omitempty"`
	Attempted      bool              `json:"attempted"`
	Frozen         *FrozenCheckout   `json:"frozen,omitempty"`
	Failure        *BaseMergeFailure `json:"failure,omitempty"`
}

func resolvedBaseState(ctx context.Context, p PinnedCheckout, base string) (head, mergeHead string, err error) {
	if len(base) != 40 || strings.Trim(base, "0123456789abcdef") != "" {
		return "", "", errors.New("fixed resolution base required")
	}
	pin, err := PinRepository(ctx, p.Repository)
	if err != nil {
		return "", "", err
	}
	if err := verifyRepositoryPin(ctx, pin); err != nil {
		return "", "", err
	}
	if err := verifyRepositoryPin(ctx, RepositoryPin{Repository: p.CWD, GitDir: p.GitDir, CommonDir: p.CommonDir}); err != nil {
		return "", "", err
	}
	available, err := pinnedCommitAvailable(ctx, pin, base)
	if err != nil || !available {
		return "", "", errors.Join(errors.New("fixed resolution base is unavailable locally"), err)
	}
	head, err = verifyPinned(ctx, p)
	if err != nil {
		return "", "", err
	}
	if err := mergeIdleExceptPendingMerge(p, true); err != nil {
		return "", "", err
	}
	path := filepath.Join(p.GitDir, "MERGE_HEAD")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		_, err = pinnedGit(ctx, p.CWD, "merge-base", "--is-ancestor", base, head)
		return head, "", err
	}
	if err != nil {
		return "", "", err
	}
	if !info.Mode().IsRegular() || info.Size() > 41 {
		return "", "", errors.New("one fixed pending merge head required")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	raw, readErr := io.ReadAll(io.LimitReader(f, 42))
	closeErr := f.Close()
	if readErr != nil || closeErr != nil {
		return "", "", errors.Join(readErr, closeErr)
	}
	if string(raw) != base && string(raw) != base+"\n" {
		return "", "", errors.New("pending merge differs from the fixed resolution base")
	}
	return head, base, nil
}

func resolutionMarkers(ctx context.Context, p PinnedCheckout) error {
	// Disable whitespace-style checks; this gate is for leftover Git conflict
	// markers, while independent verification still owns code correctness.
	for _, args := range [][]string{{"diff", "--no-ext-diff", "--no-textconv", "--check"}, {"diff", "--no-ext-diff", "--no-textconv", "--cached", "--check"}} {
		if _, err := pinnedGit(ctx, p.CWD, append([]string{"-c", "core.whitespace=-blank-at-eol,-blank-at-eof,-space-before-tab"}, args...)...); err != nil {
			return err
		}
	}
	return nil
}

// FreezeResolvedBase seals a normally stopped fix's file changes while retaining
// the original candidate and exact new base. The caller owns a durable one-use
// claim and Git/worktree leases; beforeFreeze rechecks its current permission.
// An error never authorizes a retry or an owner/lease release by this port.
func FreezeResolvedBase(ctx context.Context, p PinnedCheckout, base, message string, beforeFreeze func(context.Context) error) (out ResolvedBase, retErr error) {
	out = ResolvedBase{Source: p, BaseSHA: base}
	if beforeFreeze == nil || !utf8.ValidString(message) || len(message) > 4096 || strings.TrimSpace(message) == "" || strings.ContainsRune(message, '\x00') {
		return out, errors.New("host resolution fence and bounded commit message required")
	}
	ctx = WithStoppedCommands(ctx)
	defer func() {
		if retErr == nil || !out.Attempted {
			return
		}
		out.Frozen = nil
		if errors.Is(retErr, ErrGitStopUnconfirmed) {
			return
		}
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		observation, err := observeBaseMergeFailure(WithStoppedCommands(cleanup), p)
		if err != nil {
			retErr = errors.Join(retErr, err)
			return
		}
		out.Failure = &observation
	}()
	var err error
	out.InitialHeadSHA, out.MergeHeadSHA, err = resolvedBaseState(ctx, p, base)
	if err != nil {
		return out, err
	}
	if err := resolutionMarkers(ctx, p); err != nil {
		return out, err
	}
	if err := beforeFreeze(ctx); err != nil {
		return out, err
	}
	head, mergeHead, err := resolvedBaseState(ctx, p, base)
	if err != nil {
		return out, err
	}
	if head != out.InitialHeadSHA || mergeHead != out.MergeHeadSHA {
		return out, errors.New("resolution identity changed after host permission check")
	}
	if err := resolutionMarkers(ctx, p); err != nil {
		return out, err
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if mergeHead != "" {
		out.Attempted = true
		if _, err := pinnedGit(ctx, p.CWD, "add", "--all"); err != nil {
			return out, err
		}
		// Previously untracked files are now visible to the cached check.
		// A rejected file leaves an observed staging failure, never a commit.
		if err := resolutionMarkers(ctx, p); err != nil {
			return out, err
		}
		unmerged, err := unresolvedMergePaths(ctx, p.CWD)
		if err != nil || len(unmerged) != 0 {
			return out, errors.Join(errors.New("resolution retains an unmerged index"), err)
		}
		// Commit even an all-ours resolution whose tree equals the original HEAD.
		if _, err := pinnedGit(ctx, p.CWD, "-c", "user.name=Harness", "-c", "user.email=harness@example.invalid", "-c", "commit.gpgsign=false", "commit", "--no-verify", "-m", message); err != nil {
			return out, err
		}
	} else {
		status, err := pinnedGit(ctx, p.CWD, "status", "--porcelain", "--untracked-files=all")
		if err != nil {
			return out, err
		}
		out.Attempted = status != ""
	}
	var frozen FrozenCheckout
	if out.MergeHeadSHA != "" {
		// The pending merge was just committed. Verify it without allowing a
		// second commit to absorb files changed after that mutation.
		sha, err := verifyPinned(ctx, p)
		if err != nil {
			return out, err
		}
		if err := Clean(ctx, p.CWD); err != nil {
			return out, err
		}
		frozen = FrozenCheckout{PinnedCheckout: p, SHA: sha}
	} else {
		frozen, err = FreezePinned(ctx, p, message)
		if err != nil {
			return out, err
		}
	}
	if err := mergeIdle(p); err != nil {
		return out, err
	}
	if _, err := pinnedGit(ctx, p.CWD, "merge-base", "--is-ancestor", base, frozen.SHA); err != nil {
		return out, err
	}
	if out.MergeHeadSHA != "" {
		parents, err := pinnedGit(ctx, p.CWD, "rev-list", "--parents", "-n", "1", frozen.SHA)
		if err != nil || strings.Join(strings.Fields(parents), " ") != frozen.SHA+" "+out.InitialHeadSHA+" "+base {
			return out, errors.Join(errors.New("resolved merge lost its exact parents"), err)
		}
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	out.Frozen = &frozen
	return out, nil
}
