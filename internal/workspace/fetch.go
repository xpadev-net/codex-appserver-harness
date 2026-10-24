package workspace

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// RepositoryPin is measured by the host before admission. Fetch never uses a
// worker-selected origin, changes the checkout or updates a local branch.
type RepositoryPin struct {
	Repository string `json:"repository"`
	GitDir     string `json:"git_dir"`
	CommonDir  string `json:"common_dir"`
}
type CommitFetch struct {
	Source    RepositoryPin `json:"source"`
	Remote    string        `json:"remote"`
	SHA       string        `json:"sha"`
	Attempted bool          `json:"attempted"`
	Available bool          `json:"available"`
}

func PinRepository(ctx context.Context, repo string) (RepositoryPin, error) {
	pin := RepositoryPin{Repository: repo}
	if err := pinnedDirectory(repo); err != nil {
		return pin, err
	}
	for _, entry := range []struct {
		arg string
		out *string
	}{{"--absolute-git-dir", &pin.GitDir}, {"--git-common-dir", &pin.CommonDir}} {
		raw, err := pinnedGit(ctx, repo, "rev-parse", "--path-format=absolute", entry.arg)
		if err != nil {
			return pin, err
		}
		actual, err := filepath.EvalSymlinks(strings.TrimSpace(raw))
		if err != nil {
			return pin, err
		}
		*entry.out = actual
	}
	return pin, verifyRepositoryPin(ctx, pin)
}
func verifyRepositoryPin(ctx context.Context, pin RepositoryPin) error {
	for _, path := range []string{pin.Repository, pin.GitDir, pin.CommonDir} {
		if err := pinnedDirectory(path); err != nil {
			return err
		}
	}
	for _, entry := range []struct{ arg, expected string }{{"--absolute-git-dir", pin.GitDir}, {"--git-common-dir", pin.CommonDir}} {
		raw, err := pinnedGit(ctx, pin.Repository, "rev-parse", "--path-format=absolute", entry.arg)
		if err != nil {
			return err
		}
		real, err := filepath.EvalSymlinks(strings.TrimSpace(raw))
		if err != nil {
			return err
		}
		if real != entry.expected {
			return errors.New("fetch repository metadata changed")
		}
	}
	if _, err := os.Lstat(filepath.Join(pin.CommonDir, "info", "grafts")); err == nil {
		return errors.New("fetch cannot use legacy grafts")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// A local availability check must not initiate an implicit promisor fetch.
	raw, err := pinnedGit(ctx, pin.Repository, "config", "--null", "--name-only", "--get-regexp", `^(extensions\.partialclone|remote\..*\.(promisor|partialclonefilter))$`)
	if err != nil {
		var exit *exec.ExitError
		if errors.Is(err, ErrGitStopUnconfirmed) || !errors.As(err, &exit) || exit.ExitCode() != 1 {
			return err
		}
	} else if raw != "" {
		return errors.New("partial-clone fetch requires explicit host reconciliation")
	}
	return nil
}
func pinnedCommitAvailable(ctx context.Context, pin RepositoryPin, sha string) (bool, error) {
	kind, err := pinnedGit(ctx, pin.Repository, "cat-file", "-t", sha)
	if err != nil {
		var exit *exec.ExitError
		if !errors.Is(err, ErrGitStopUnconfirmed) && errors.As(err, &exit) && exit.ExitCode() == 128 {
			return false, nil
		}
		return false, err
	}
	if strings.TrimSpace(kind) != "commit" {
		return false, errors.New("fixed fetch SHA is not a commit")
	}
	actual, err := pinnedGit(ctx, pin.Repository, "rev-parse", "--verify", "--end-of-options", sha+"^{commit}")
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(actual) != sha {
		return false, errors.New("fetched commit identity mismatch")
	}
	return true, nil
}

// FetchPinnedCommit only imports objects for an exact SHA. Its caller owns the
// shared Git lease and a durable one-use send fence; every subprocess joins.
// A lost stopping proof leaves the caller's claim/lease held for reconciliation.
func FetchPinnedCommit(ctx context.Context, pin RepositoryPin, remote, sha string, beforeFetch func(context.Context) error) (CommitFetch, error) {
	observed := CommitFetch{Source: pin, Remote: remote, SHA: sha}
	if beforeFetch == nil || len(sha) != 40 || strings.Trim(sha, "0123456789abcdef") != "" {
		return observed, errors.New("fixed fetch SHA and host send fence required")
	}
	if err := validatePublicationRemote(remote); err != nil {
		return observed, err
	}
	ctx = WithStoppedCommands(ctx)
	if err := verifyRepositoryPin(ctx, pin); err != nil {
		return observed, err
	}
	if err := publicationPolicy(ctx, pin.Repository); err != nil {
		return observed, err
	}
	available, err := pinnedCommitAvailable(ctx, pin, sha)
	if err != nil {
		return observed, err
	}
	if available {
		observed.Available = true
		return observed, nil
	}
	if err := beforeFetch(ctx); err != nil {
		return observed, err
	}
	if err := verifyRepositoryPin(ctx, pin); err != nil {
		return observed, err
	}
	if err := publicationPolicy(ctx, pin.Repository); err != nil {
		return observed, err
	}
	if err := validatePublicationRemote(remote); err != nil {
		return observed, err
	}
	observed.Attempted = true
	if _, err := pinnedGit(ctx, pin.Repository, "fetch", "--no-tags", "--no-prune", "--no-write-fetch-head", "--no-auto-gc", "--no-recurse-submodules", "--refmap=", remote, sha); err != nil {
		return observed, err
	}
	if err := verifyRepositoryPin(ctx, pin); err != nil {
		return observed, err
	}
	observed.Available, err = pinnedCommitAvailable(ctx, pin, sha)
	if err == nil && !observed.Available {
		err = errors.New("fetch did not supply fixed commit")
	}
	return observed, err
}

// ValidateCommitFetchRemote checks host transport configuration without network I/O.
func ValidateCommitFetchRemote(remote string) error { return validatePublicationRemote(remote) }
