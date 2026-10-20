// Package workspace owns Git worktrees and candidate commits.
package workspace

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var shaPattern = regexp.MustCompile(`^[0-9a-f]{40,64}$`)

func Git(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	b, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s failed: %w", args[0], err)
	}
	return strings.TrimSpace(string(b)), nil
}
func Resolve(ctx context.Context, repo, ref string) (string, error) {
	sha, err := Git(ctx, repo, "rev-parse", "--verify", "--end-of-options", ref+"^{commit}")
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
	_, err = Git(ctx, repo, "worktree", "add", "-b", branch, "--", path, sha)
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
