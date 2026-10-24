package workspace

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var ErrPublicationUnconfirmed = errors.New("candidate branch publication is unconfirmed")

// BranchPublication is a measured remote-ref fact, never Task success, PR
// creation or approval. On error it is only a partial observation. Attempted
// says the push command was invoked, not that a remote mutation succeeded.
type BranchPublication struct {
	Source    FrozenCheckout
	Remote    string
	Branch    string
	RemoteSHA string
	Attempted bool
}

func validatePublicationRemote(remote string) error {
	if remote == "" || len(remote) > 8192 || strings.ContainsAny(remote, "\x00\r\n\t ") {
		return errors.New("bounded explicit publication remote required")
	}
	if filepath.IsAbs(remote) {
		return pinnedDirectory(remote) // Offline bare-repository transport.
	}
	u, err := url.Parse(remote)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path == "" {
		return errors.New("credential-free HTTPS publication remote required")
	}
	return nil
}

func publicationHead(ctx context.Context, p PinnedCheckout, remote, branch string) (string, error) {
	raw, err := pinnedGit(ctx, p.CWD, "ls-remote", "--heads", "--refs", remote, "refs/heads/"+branch)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return "", nil
	} // Successful read, known absent ref.
	if len(fields) != 2 || fields[1] != "refs/heads/"+branch || len(fields[0]) != 40 || strings.Trim(fields[0], "0123456789abcdef") != "" {
		return "", ErrPublicationUnconfirmed
	}
	return fields[0], nil
}

func publicationSource(ctx context.Context, frozen FrozenCheckout) error {
	head, err := verifyPinned(ctx, frozen.PinnedCheckout)
	if err != nil {
		return err
	}
	if len(frozen.SHA) != 40 || strings.Trim(frozen.SHA, "0123456789abcdef") != "" || head != frozen.SHA {
		return errors.New("publication checkout differs from the sealed Candidate")
	}
	status, err := pinnedGit(ctx, frozen.CWD, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return err
	}
	if status != "" {
		return errors.New("publication checkout changed after sealing")
	}
	return nil
}

func publicationPolicy(ctx context.Context, cwd string) error {
	configured, err := pinnedGit(ctx, cwd, "config", "--null", "--name-only", "--get-regexp", `^(url\..*\.(insteadof|pushinsteadof)|push\.pushoption)$`)
	if err != nil {
		var exit *exec.ExitError
		if errors.Is(err, ErrGitStopUnconfirmed) || !errors.As(err, &exit) || exit.ExitCode() != 1 {
			return err
		}
	} else if configured != "" {
		return errors.New("publication URL rewrite or push option requires host reconciliation")
	}
	return nil
}

// PublishPinned uploads exactly a host-sealed commit using an ordinary push.
// The caller owns Git/worktree and branch mutation leases and supplies a fresh
// one-use authority fence. URL rewrites are rejected; origin and worker-selected
// remotes/refspecs are never used. Every subprocess joins its process group.
func PublishPinned(ctx context.Context, frozen FrozenCheckout, remote, branch string, beforePush func(context.Context) error) (BranchPublication, error) {
	observed := BranchPublication{Source: frozen, Remote: remote, Branch: branch}
	if beforePush == nil || !regexp.MustCompile(`^harness/[A-Za-z0-9_-]+$`).MatchString(branch) {
		return observed, errors.New("fixed publication branch and host send fence required")
	}
	if err := validatePublicationRemote(remote); err != nil {
		return observed, err
	}
	ctx = WithStoppedCommands(ctx)
	if err := publicationSource(ctx, frozen); err != nil {
		return observed, err
	}
	// An explicit URL must not be redirected through either fetch or push URL
	// rewrite rules. Empty insteadOf overrides would themselves match any URL.
	if err := publicationPolicy(ctx, frozen.CWD); err != nil {
		return observed, err
	}
	var err error
	observed.RemoteSHA, err = publicationHead(ctx, frozen.PinnedCheckout, remote, branch)
	if err != nil {
		return observed, err
	}
	if observed.RemoteSHA == frozen.SHA {
		return observed, nil
	}
	if observed.RemoteSHA != "" {
		if _, err := pinnedGit(ctx, frozen.CWD, "merge-base", "--is-ancestor", observed.RemoteSHA, frozen.SHA); err != nil {
			return observed, fmt.Errorf("remote branch is not an ancestor of the Candidate: %w", err)
		}
	}
	if err := beforePush(ctx); err != nil {
		return observed, err
	}
	if err := publicationSource(ctx, frozen); err != nil {
		return observed, err
	}
	if err := publicationPolicy(ctx, frozen.CWD); err != nil {
		return observed, err
	}
	observed.Attempted = true
	if _, err := pinnedGit(ctx, frozen.CWD, "push", "--no-verify", "--no-follow-tags", "--no-mirror", "--recurse-submodules=no", "--signed=no", "--receive-pack=git-receive-pack", remote, frozen.SHA+":refs/heads/"+branch); err != nil {
		return observed, errors.Join(ErrPublicationUnconfirmed, err)
	}
	observed.RemoteSHA, err = publicationHead(ctx, frozen.PinnedCheckout, remote, branch)
	if err != nil || observed.RemoteSHA != frozen.SHA {
		return observed, errors.Join(ErrPublicationUnconfirmed, err)
	}
	return observed, nil
}
