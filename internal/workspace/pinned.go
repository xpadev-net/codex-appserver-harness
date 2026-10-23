package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"
)

// PinnedCheckout contains host-saved preparation facts, never worker output.
// The caller must hold exclusive Git/worktree leases and prove the worker and
// its descendants stopped before FreezePinned. This API grants no Task success,
// candidate registration, verification, publication or approval.
type PinnedCheckout struct {
	Repository string
	CWD        string
	Branch     string
	GitDir     string
	CommonDir  string
	BaseSHA    string
}

type FrozenCheckout struct {
	PinnedCheckout
	SHA string
}

func pinnedGit(ctx context.Context, cwd string, args ...string) (string, error) {
	return Git(ctx, cwd, append([]string{"--no-replace-objects"}, args...)...)
}

func pinnedDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\x00\r\n") {
		return errors.New("canonical absolute Git directory required")
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if real != path {
		return errors.New("pinned Git directory was redirected")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("pinned Git directory is not a directory")
	}
	return nil
}

func verifyPinned(ctx context.Context, p PinnedCheckout) (string, error) {
	for _, path := range []string{p.Repository, p.CWD, p.GitDir, p.CommonDir} {
		if err := pinnedDirectory(path); err != nil {
			return "", err
		}
	}
	if !regexp.MustCompile(`^harness/[A-Za-z0-9_-]+$`).MatchString(p.Branch) || len(p.BaseSHA) != 40 || strings.Trim(p.BaseSHA, "0123456789abcdef") != "" || !strings.HasPrefix(p.GitDir, p.CommonDir+string(filepath.Separator)+"worktrees"+string(filepath.Separator)) {
		return "", errors.New("fixed linked checkout identity required")
	}
	if _, err := os.Lstat(filepath.Join(p.CommonDir, "info", "grafts")); err == nil {
		return "", errors.New("legacy grafts cannot establish fixed commit ancestry")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	// Same common repository and branch alone are insufficient: a replacement
	// .git directory can imitate both. Compare the exact saved linked git-dir.
	for _, entry := range []struct{ cwd, arg, expected string }{
		{p.Repository, "--git-common-dir", p.CommonDir},
		{p.CWD, "--git-common-dir", p.CommonDir},
		{p.CWD, "--absolute-git-dir", p.GitDir},
	} {
		raw, err := pinnedGit(ctx, entry.cwd, "rev-parse", "--path-format=absolute", entry.arg)
		if err != nil {
			return "", err
		}
		actual, err := filepath.EvalSymlinks(strings.TrimSpace(raw))
		if err != nil {
			return "", err
		}
		if actual != entry.expected {
			return "", errors.New("checkout Git metadata differs from preparation proof")
		}
	}
	branch, err := pinnedGit(ctx, p.CWD, "branch", "--show-current")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(branch) != p.Branch {
		return "", errors.New("checkout branch differs from preparation proof")
	}
	head, err := pinnedGit(ctx, p.CWD, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", err
	}
	head = strings.TrimSpace(head)
	if len(head) != 40 || !shaPattern.MatchString(head) {
		return "", errors.New("fixed commit SHA required")
	}
	if _, err := pinnedGit(ctx, p.CWD, "merge-base", "--is-ancestor", p.BaseSHA, head); err != nil {
		return "", fmt.Errorf("checkout no longer descends from its fixed base: %w", err)
	}
	return head, nil
}

// FreezePinned uses the strict synchronous stopped-command profile, including
// config probes. Missing subprocess stop proof is returned without an apparent
// success receipt; its caller must retain intent/leases for reconciliation.
func FreezePinned(ctx context.Context, p PinnedCheckout, message string) (FrozenCheckout, error) {
	var frozen FrozenCheckout
	if !utf8.ValidString(message) || len(message) > 4096 || strings.TrimSpace(message) == "" || strings.ContainsRune(message, '\x00') {
		return frozen, errors.New("bounded host commit message required")
	}
	ctx = WithStoppedCommands(ctx)
	if _, err := verifyPinned(ctx, p); err != nil {
		return frozen, err
	}
	status, err := pinnedGit(ctx, p.CWD, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return frozen, err
	}
	if status != "" {
		if _, err := pinnedGit(ctx, p.CWD, "add", "--all"); err != nil {
			return frozen, err
		}
		// Identity and signing are host policy. Repository configuration must
		// not start a signing command while controller seals worker changes.
		if _, err := pinnedGit(ctx, p.CWD, "-c", "user.name=Harness", "-c", "user.email=harness@example.invalid", "-c", "commit.gpgsign=false", "commit", "-m", message); err != nil {
			return frozen, err
		}
	}
	sha, err := verifyPinned(ctx, p)
	if err != nil {
		return frozen, err
	}
	status, err = pinnedGit(ctx, p.CWD, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return frozen, err
	}
	if status != "" {
		return frozen, errors.New("frozen checkout has uncommitted changes")
	}
	if err := ctx.Err(); err != nil {
		return frozen, err
	}
	return FrozenCheckout{PinnedCheckout: p, SHA: sha}, nil
}
