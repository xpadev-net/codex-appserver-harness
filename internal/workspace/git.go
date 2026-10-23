// Package workspace owns Git worktrees and candidate commits.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var shaPattern = regexp.MustCompile(`^[0-9a-f]{40,64}$`)

func Git(ctx context.Context, dir string, args ...string) (string, error) {
	safe := []string{"-C", dir, "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "protocol.ext.allow=never", "-c", "gc.auto=0", "-c", "maintenance.auto=false", "-c", "submodule.recurse=false"}
	keys, probeErr := gitOutput(ctx, []string{"-C", dir, "config", "--null", "--name-only", "--get-regexp", `^(filter\..*\.(clean|smudge|process|required)|merge\..*\.driver)$`})
	if stoppedCommands(ctx) && probeErr != nil {
		var exit *exec.ExitError
		if errors.Is(probeErr, ErrGitStopUnconfirmed) || !errors.As(probeErr, &exit) || exit.ExitCode() != 1 {
			return "", fmt.Errorf("git config probe failed: %w", probeErr)
		}
	}
	for _, key := range strings.Split(string(keys), "\x00") {
		if key == "" {
			continue
		}
		value := ""
		if strings.HasSuffix(key, ".required") {
			value = "false"
		}
		if strings.HasPrefix(key, "merge.") {
			value = "false"
		}
		safe = append(safe, "-c", key+"="+value)
	}
	b, err := gitOutput(ctx, append(safe, args...))
	if err != nil {
		return "", fmt.Errorf("git %s failed: %w", args[0], err)
	}
	return string(b), nil
}
func Resolve(ctx context.Context, repo, ref string) (string, error) {
	sha, err := Git(ctx, repo, "rev-parse", "--verify", "--end-of-options", ref+"^{commit}")
	sha = strings.TrimSpace(sha)
	if err == nil && !shaPattern.MatchString(sha) {
		return "", fmt.Errorf("invalid commit SHA")
	}
	return sha, err
}
func Create(ctx context.Context, repo, path, branch, base string) error {
	if !filepath.IsAbs(path) || !regexp.MustCompile(`^harness/[A-Za-z0-9_-]+$`).MatchString(branch) {
		return fmt.Errorf("invalid worktree path or branch")
	}
	sha, err := Resolve(ctx, repo, base)
	if err != nil {
		return err
	}
	args := []string{"worktree", "add"}
	if stoppedCommands(ctx) {
		args = append(args, "--no-checkout")
	}
	args = append(args, "-b", branch, "--", path, sha)
	if _, err = Git(ctx, repo, args...); err != nil {
		return err
	}
	if stoppedCommands(ctx) {
		// Discover conditional branch/gitdir filter settings in the NEW worktree
		// before its first checkout. Source-repository config alone is insufficient.
		_, err = Git(ctx, path, "reset", "--hard", sha)
	}
	return err
}
func Freeze(ctx context.Context, path, message string) (string, error) {
	status, err := Git(ctx, path, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return "", err
	}
	if status != "" {
		if _, err = Git(ctx, path, "add", "--all"); err != nil {
			return "", err
		}
		if _, err = Git(ctx, path, "-c", "core.hooksPath=/dev/null", "commit", "-m", message); err != nil {
			return "", err
		}
	}
	return Resolve(ctx, path, "HEAD")
}
func Clean(ctx context.Context, path string) error {
	s, err := Git(ctx, path, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return err
	}
	if s != "" {
		return fmt.Errorf("worktree has uncommitted changes")
	}
	return nil
}

// Push uses an ordinary fast-forward push and verifies the advertised remote head.
func Push(ctx context.Context, repo, branch, sha string) error {
	if !regexp.MustCompile(`^harness/[A-Za-z0-9_-]+$`).MatchString(branch) || !shaPattern.MatchString(sha) {
		return fmt.Errorf("invalid publication ref")
	}
	if _, err := Git(ctx, repo, "push", "origin", sha+":refs/heads/"+branch); err != nil {
		return err
	}
	out, err := Git(ctx, repo, "ls-remote", "--heads", "origin", "refs/heads/"+branch)
	if err != nil {
		return err
	}
	fields := strings.Fields(out)
	if len(fields) != 2 || fields[0] != sha {
		return fmt.Errorf("remote head mismatch")
	}
	return nil
}

// Verify rejects a replaced .git file or redirected branch before controller writes.
func Verify(ctx context.Context, repo, tree, branch string) error {
	expected, err := Git(ctx, repo, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return err
	}
	actual, err := Git(ctx, tree, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return err
	}
	e, err := filepath.EvalSymlinks(strings.TrimSpace(expected))
	if err != nil {
		return err
	}
	a, err := filepath.EvalSymlinks(strings.TrimSpace(actual))
	if err != nil {
		return err
	}
	if e != a {
		return fmt.Errorf("worktree Git metadata redirected")
	}
	b, err := Git(ctx, tree, "branch", "--show-current")
	if err != nil {
		return err
	}
	if strings.TrimSpace(b) != branch {
		return fmt.Errorf("worktree branch redirected")
	}
	return nil
}
