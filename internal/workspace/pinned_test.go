package workspace

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func pinnedFixture(t *testing.T) PinnedCheckout {
	t.Helper()
	ctx := context.Background()
	repo := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"config", "commit.gpgsign", "false"}, {"commit", "--allow-empty", "-m", "base"}} {
		if _, err := Git(ctx, repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	base, err := Resolve(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	p := PinnedCheckout{Repository: repo, CWD: filepath.Join(t.TempDir(), "tree"), Branch: "harness/pinned", BaseSHA: base}
	if err := Create(WithStoppedCommands(ctx), repo, p.CWD, p.Branch, base); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		arg string
		out *string
	}{{"--absolute-git-dir", &p.GitDir}, {"--git-common-dir", &p.CommonDir}} {
		raw, err := Git(ctx, p.CWD, "rev-parse", "--path-format=absolute", entry.arg)
		if err != nil {
			t.Fatal(err)
		}
		*entry.out, err = filepath.EvalSymlinks(strings.TrimSpace(raw))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(p.CWD, "change.txt"), []byte("candidate change\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFreezePinnedSealsChangesWithoutHooksFiltersOrSigning(t *testing.T) {
	ctx := context.Background()
	p := pinnedFixture(t)
	marker := filepath.Join(p.Repository, "EXTERNAL_COMMAND_RAN")
	script := filepath.Join(t.TempDir(), "trap")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntouch '"+marker+"'\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.CommonDir, "hooks", "pre-commit"), []byte("#!/bin/sh\ntouch '"+marker+"'\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"config", "commit.gpgsign", "true"}, {"config", "gpg.program", script}, {"config", "filter.trap.clean", script}, {"config", "filter.trap.required", "true"}} {
		if _, err := Git(ctx, p.Repository, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(p.CWD, ".gitattributes"), []byte("*.txt filter=trap\n"), 0600); err != nil {
		t.Fatal(err)
	}
	frozen, err := FreezePinned(ctx, p, "host candidate")
	if err != nil || frozen.SHA == p.BaseSHA || !shaPattern.MatchString(frozen.SHA) || frozen.PinnedCheckout != p {
		t.Fatal(frozen, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("external controller command ran", err)
	}
	if err := Clean(ctx, p.CWD); err != nil {
		t.Fatal(err)
	}
	data, err := Git(ctx, p.CWD, "show", frozen.SHA+":change.txt")
	if err != nil || data != "candidate change\n" {
		t.Fatal(data, err)
	}
	identity, err := Git(ctx, p.CWD, "log", "-1", "--format=%an <%ae>")
	if err != nil || strings.TrimSpace(identity) != "Harness <harness@example.invalid>" {
		t.Fatal(identity, err)
	}
	again, err := FreezePinned(ctx, p, "read clean current candidate")
	if err != nil || !reflect.DeepEqual(again, frozen) {
		t.Fatal(again, err)
	}
}

func TestFreezePinnedRejectsImitatedCommonDirectoryBeforeWrites(t *testing.T) {
	ctx := context.Background()
	p := pinnedFixture(t)
	fake := t.TempDir()
	for name, value := range map[string]string{"HEAD": "ref: refs/heads/" + p.Branch + "\n", "commondir": p.CommonDir + "\n", "gitdir": filepath.Join(p.CWD, ".git") + "\n"} {
		if err := os.WriteFile(filepath.Join(fake, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(p.CWD, ".git"), []byte("gitdir: "+fake+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Verify(ctx, p.Repository, p.CWD, p.Branch); err != nil {
		t.Fatal("fixture must imitate common directory and branch", err)
	}
	if frozen, err := FreezePinned(ctx, p, "must not commit"); err == nil || frozen.SHA != "" {
		t.Fatal("redirected git-dir accepted", frozen, err)
	}
	head, err := Resolve(ctx, p.Repository, p.Branch)
	if err != nil || head != p.BaseSHA {
		t.Fatal("rejected checkout changed branch", head, err)
	}
	if _, err := os.Stat(filepath.Join(fake, "index")); !os.IsNotExist(err) {
		t.Fatal("rejected checkout was staged", err)
	}
}

func TestFreezePinnedRejectsChangedBranchBaseAndPaths(t *testing.T) {
	for _, mode := range []string{"branch", "base", "replace", "grafts", "path", "message", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			p := pinnedFixture(t)
			originalBase := p.BaseSHA
			message := "host candidate"
			switch mode {
			case "branch":
				if _, err := Git(ctx, p.CWD, "branch", "-m", "harness/redirected"); err != nil {
					t.Fatal(err)
				}
			case "base", "replace":
				for _, args := range [][]string{{"checkout", "--orphan", "unrelated"}, {"commit", "--allow-empty", "-m", "unrelated root"}} {
					if _, err := Git(ctx, p.Repository, args...); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				p.BaseSHA, err = Resolve(ctx, p.Repository, "HEAD")
				if err != nil {
					t.Fatal(err)
				}
				if mode == "replace" {
					if _, err := Git(ctx, p.Repository, "replace", "--graft", originalBase, p.BaseSHA); err != nil {
						t.Fatal(err)
					}
					if _, err := Git(ctx, p.CWD, "merge-base", "--is-ancestor", p.BaseSHA, originalBase); err != nil {
						t.Fatal("fixture must forge normal ancestry", err)
					}
				}
			case "grafts":
				if err := os.WriteFile(filepath.Join(p.CommonDir, "info", "grafts"), []byte(originalBase+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "path":
				alias := filepath.Join(t.TempDir(), "alias")
				if err := os.Symlink(p.CWD, alias); err != nil {
					t.Fatal(err)
				}
				p.CWD = alias
			case "message":
				message = strings.Repeat("x", 4097)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if frozen, err := FreezePinned(ctx, p, message); err == nil || frozen.SHA != "" {
				t.Fatal("invalid pinned checkout sealed", frozen, err)
			}
			head, err := Resolve(context.Background(), p.CWD, "HEAD")
			if err != nil {
				t.Fatal(err)
			}
			if head != originalBase {
				t.Fatal("failed preflight committed changes", head, originalBase)
			}
			index, err := Git(context.Background(), p.CWD, "diff", "--cached", "--name-only")
			if err != nil || index != "" {
				t.Fatal("failed preflight staged changes", index, err)
			}
		})
	}
}
