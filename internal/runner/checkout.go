package runner

import (
	"context"
	"errors"
	"strings"
)

func prepareCheckout(ctx context.Context, s Spec, checkout string, git func(context.Context, string, ...string) (string, error)) (string, error) {
	if _, err := git(ctx, s.Repository, "--no-replace-objects", "clone", "--no-local", "--no-checkout", "--", s.Repository, checkout); err != nil {
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
