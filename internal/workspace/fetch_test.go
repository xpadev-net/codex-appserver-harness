package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fetchFixture(t *testing.T) (RepositoryPin, string, string) {
	t.Helper()
	ctx := context.Background()
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(t.TempDir(), "global"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	source := t.TempDir()
	for _, args := range [][]string{{"init", "--initial-branch=trunk"}, {"config", "user.name", "fixture"}, {"config", "user.email", "fixture@example.invalid"}, {"-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "base"}} {
		if _, err := Git(ctx, source, args...); err != nil {
			t.Fatal(err)
		}
	}
	destination := t.TempDir()
	if _, err := Git(ctx, source, "clone", "--no-local", "--", source, destination); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "new.txt"), []byte("new fixed head\n"), 0600); err != nil {
		t.Fatal(err)
	}
	sha, err := Freeze(WithStoppedCommands(ctx), source, "new remote head")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Git(ctx, source, "tag", "unrequested-tag", sha); err != nil {
		t.Fatal(err)
	}
	pin, err := PinRepository(WithStoppedCommands(ctx), destination)
	if err != nil {
		t.Fatal(err)
	}
	return pin, source, sha
}
func TestFetchPinnedCommitImportsExactObjectWithoutRefsOrCheckoutChanges(t *testing.T) {
	ctx := context.Background()
	pin, remote, sha := fetchFixture(t)
	if available, err := pinnedCommitAvailable(ctx, pin, sha); err != nil || available {
		t.Fatal("fixture already contains future head", available, err)
	}
	refs, err := Git(ctx, pin.Repository, "show-ref")
	if err != nil {
		t.Fatal(err)
	}
	head, err := Resolve(ctx, pin.Repository, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Git(ctx, pin.Repository, "remote", "set-url", "origin", filepath.Join(t.TempDir(), "wrong-origin")); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(pin.GitDir, "FETCH_HEAD")
	if err := os.WriteFile(sentinel, []byte("preserved\n"), 0600); err != nil {
		t.Fatal(err)
	}
	dirty := filepath.Join(pin.Repository, "private.txt")
	if err := os.WriteFile(dirty, []byte("keep working tree\n"), 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	result, err := FetchPinnedCommit(ctx, pin, remote, sha, func(context.Context) error { calls++; return nil })
	if err != nil || !result.Attempted || !result.Available || result.SHA != sha || calls != 1 {
		t.Fatal(result, calls, err)
	}
	after, err := Git(ctx, pin.Repository, "show-ref")
	if err != nil || refs != after {
		t.Fatal("fetch changed named refs", after, err)
	}
	afterHead, err := Resolve(ctx, pin.Repository, "HEAD")
	if err != nil || head != afterHead {
		t.Fatal("fetch changed checkout", afterHead, err)
	}
	for name, want := range map[string]string{sentinel: "preserved\n", dirty: "keep working tree\n"} {
		raw, err := os.ReadFile(name)
		if err != nil || string(raw) != want {
			t.Fatal("fetch changed private checkout state", err)
		}
	}
	replay, err := FetchPinnedCommit(ctx, pin, remote, sha, func(context.Context) error { t.Fatal("local object requested new send fence"); return nil })
	if err != nil || replay.Attempted || !replay.Available {
		t.Fatal(replay, err)
	}
}
func TestFetchPinnedCommitRejectsChangedAuthorityAndMetadata(t *testing.T) {
	for _, mode := range []string{"fence", "rewrite", "late_rewrite", "late_remote", "pin", "partial", "credentials", "missing", "tag"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			pin, remote, sha := fetchFixture(t)
			denied := errors.New("host held")
			guard := func(context.Context) error { return nil }
			switch mode {
			case "fence":
				guard = func(context.Context) error { return denied }
			case "rewrite":
				if _, err := Git(ctx, pin.Repository, "config", "url."+remote+".insteadOf", remote); err != nil {
					t.Fatal(err)
				}
			case "late_rewrite":
				guard = func(context.Context) error {
					_, err := Git(ctx, pin.Repository, "config", "url."+remote+".insteadOf", remote)
					return err
				}
			case "late_remote":
				guard = func(context.Context) error {
					if err := os.Rename(remote, remote+"-original"); err != nil {
						return err
					}
					return os.Symlink(remote+"-original", remote)
				}
			case "pin":
				pin.CommonDir = t.TempDir()
			case "partial":
				if _, err := Git(ctx, pin.Repository, "config", "remote.origin.promisor", "true"); err != nil {
					t.Fatal(err)
				}
			case "credentials":
				remote = "https://user:secret@example.invalid/repo.git"
			case "missing":
				sha = strings.Repeat("f", 40)
			case "tag":
				if _, err := Git(ctx, remote, "-c", "tag.gpgsign=false", "tag", "-a", "annotated", "-m", "not a commit", sha); err != nil {
					t.Fatal(err)
				}
				raw, err := Git(ctx, remote, "rev-parse", "refs/tags/annotated")
				if err != nil {
					t.Fatal(err)
				}
				sha = strings.TrimSpace(raw)
			}
			result, err := FetchPinnedCommit(ctx, pin, remote, sha, guard)
			if err == nil || result.Available {
				t.Fatal("held or wrong object became available", result, err)
			}
			if mode != "missing" && mode != "tag" && result.Attempted {
				t.Fatal("invalid authority started fetch", result)
			}
		})
	}
}
