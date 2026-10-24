package runner

import (
	"context"
	"errors"
	"strings"
)

func prepareCheckout(ctx context.Context, s Spec, checkout string, git func(context.Context, string, ...string) (string, error)) (string, error) {
	if _, err := git(ctx, s.Repository, "--no-replace-objects", "-c", "protocol.allow=never", "-c", "protocol.file.allow=always", "clone", "--no-local", "--no-checkout", "--", s.Repository, checkout); err != nil {
		return "", err
	}
	// A host fetch may import the final SHA without updating a branch. A
	// transport clone only carries advertised refs, so explicitly import that
	// exact object from the fixed local repository before checkout. This stage
	// permits only the file transport and never contacts a remote origin.
	if _, err := git(ctx, checkout, "--no-replace-objects", "-c", "protocol.allow=never", "-c", "protocol.file.allow=always", "fetch", "--no-tags", "--no-prune", "--no-write-fetch-head", "--no-auto-gc", "--no-recurse-submodules", "--refmap=", "--", s.Repository, s.Candidate.SHA); err != nil {
		return "", err
	}
	if _, err := git(ctx, checkout, "--no-replace-objects", "-c", "core.hooksPath=/dev/null", "checkout", "--detach", s.Candidate.SHA); err != nil {
		return "", err
	}
	head, err := git(ctx, checkout, "--no-replace-objects", "rev-parse", "--verify", "--end-of-options", "HEAD^{commit}")
	if err != nil {
		return "", err // Preserve unconfirmed-stop/cancellation identity.
	}
	head = strings.TrimSpace(head)
	if head != s.Candidate.SHA {
		return head, errors.New("checkout SHA mismatch")
	}
	status, err := git(ctx, checkout, "--no-replace-objects", "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return head, err
	}
	if status != "" {
		return head, errors.New("runner checkout has uncommitted changes")
	}
	return head, nil
}
