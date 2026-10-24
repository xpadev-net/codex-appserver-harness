package workspace

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func baseResolutionFixture(t *testing.T) (PinnedCheckout, string, string) {
	t.Helper()
	frozen, base := baseMergeFixture(t, true)
	merged, err := MergePinnedBase(context.Background(), frozen, base, func(context.Context) error { return nil })
	if err != nil || merged.Outcome != "conflict" {
		t.Fatal(merged, err)
	}
	p := frozen.PinnedCheckout
	p.BaseSHA = frozen.SHA // A fix must retain the original candidate as well.
	return p, frozen.SHA, base
}

func TestFreezeResolvedBaseStagesWorkerFilesAndRetainsBothParents(t *testing.T) {
	for _, mode := range []string{"combined", "ours", "delete", "already_committed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			p, original, base := baseResolutionFixture(t)
			path := filepath.Join(p.CWD, "change.txt")
			content := "candidate change plus predecessor merged behavior  \n"
			if mode == "ours" {
				content = "candidate change\n"
			}
			if mode == "delete" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "already_committed" {
				for _, args := range [][]string{{"add", "--all"}, {"commit", "-m", "fixture resolved merge"}} {
					if _, err := Git(ctx, p.CWD, args...); err != nil {
						t.Fatal(err)
					}
				}
			}
			marker := filepath.Join(t.TempDir(), "untrusted-command")
			hooks := t.TempDir()
			for _, hook := range []string{"pre-commit", "commit-msg", "post-commit"} {
				if err := os.WriteFile(filepath.Join(hooks, hook), []byte("#!/bin/sh\ntouch '"+marker+"'\nexit 1\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			for _, args := range [][]string{{"config", "core.hooksPath", hooks}, {"config", "commit.gpgsign", "true"}, {"config", "gpg.program", "/bin/false"}, {"config", "diff.external", "/bin/false"}} {
				if _, err := Git(ctx, p.Repository, args...); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			got, err := FreezeResolvedBase(ctx, p, base, "Resolve fixed base conflict", func(context.Context) error { calls++; return nil })
			if err != nil || got.Frozen == nil || calls != 1 || got.Frozen.PinnedCheckout != p || got.Failure != nil || got.Attempted != (mode != "already_committed") {
				t.Fatal(got, calls, err)
			}
			parents, err := Git(ctx, p.CWD, "rev-list", "--parents", "-n", "1", got.Frozen.SHA)
			if err != nil || !reflect.DeepEqual(strings.Fields(parents), []string{got.Frozen.SHA, original, base}) {
				t.Fatal("resolved merge lost a parent", parents, err)
			}
			if err := Clean(ctx, p.CWD); err != nil {
				t.Fatal(err)
			}
			if err := mergeIdle(p); err != nil {
				t.Fatal(err)
			}
			if mode == "delete" {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("resolved deletion lost", err)
				}
			} else if raw, err := os.ReadFile(path); err != nil || string(raw) != content {
				t.Fatal(string(raw), err)
			}
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("resolution ran a repository command", err)
			}
			if head, err := Resolve(ctx, p.Repository, "HEAD"); err != nil || head != base {
				t.Fatal("resolution moved the host checkout", head, err)
			}
		})
	}
}

func TestFreezeResolvedBaseRejectsUnresolvedOrChangedOwnerBeforeStaging(t *testing.T) {
	for _, mode := range []string{"markers", "unknown_base", "merge_head", "aborted", "other_operation", "partial_clone", "held", "late_markers", "late_head", "cancel", "unknown_stop", "no_fence"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p, original, base := baseResolutionFixture(t)
			path := filepath.Join(p.CWD, "change.txt")
			markers, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if mode != "markers" {
				if err := os.WriteFile(path, []byte("resolved file\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			held := errors.New("current owner held")
			fence := func(context.Context) error {
				switch mode {
				case "held":
					return held
				case "late_markers":
					return os.WriteFile(path, markers, 0600)
				case "late_head":
					_, err := Git(ctx, p.CWD, "reset", "--hard", base)
					return err
				case "cancel":
					cancel()
				case "unknown_stop":
					return ErrGitStopUnconfirmed
				}
				return nil
			}
			switch mode {
			case "unknown_base":
				base = strings.Repeat("a", 40)
			case "merge_head":
				if err := os.WriteFile(filepath.Join(p.GitDir, "MERGE_HEAD"), []byte(original+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "aborted":
				if _, err := Git(ctx, p.CWD, "merge", "--abort"); err != nil {
					t.Fatal(err)
				}
			case "other_operation":
				if err := os.WriteFile(filepath.Join(p.GitDir, "CHERRY_PICK_HEAD"), []byte(base+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "partial_clone":
				if _, err := Git(ctx, p.Repository, "config", "remote.origin.promisor", "true"); err != nil {
					t.Fatal(err)
				}
			case "no_fence":
				fence = nil
			}
			got, err := FreezeResolvedBase(ctx, p, base, "Resolve fixed base conflict", fence)
			if err == nil || got.Attempted || got.Frozen != nil || got.Failure != nil {
				t.Fatal("unresolved/changed source was staged", got, err)
			}
			if mode == "held" && !errors.Is(err, held) {
				t.Fatal(err)
			}
			if mode == "unknown_stop" && !errors.Is(err, ErrGitStopUnconfirmed) {
				t.Fatal(err)
			}
			if mode != "late_head" {
				if head, err := Resolve(context.Background(), p.CWD, "HEAD"); err != nil || head != original {
					t.Fatal("rejected source committed", head, err)
				}
			}
		})
	}
}

func TestFreezeResolvedBaseObservesStoppedPartialMutationWithoutSuccess(t *testing.T) {
	ctx := context.Background()
	p, original, base := baseResolutionFixture(t)
	if err := os.WriteFile(filepath.Join(p.CWD, "change.txt"), []byte("resolved file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	shim := t.TempDir()
	if err := os.WriteFile(filepath.Join(shim, "git"), []byte("#!/bin/sh\nfor arg do\n if [ \"$arg\" = commit ]; then exit 19; fi\ndone\nexec '"+realGit+"' \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
	got, err := FreezeResolvedBase(ctx, p, base, "Resolve fixed base conflict", func(context.Context) error { return nil })
	if err == nil || !got.Attempted || got.Frozen != nil || got.Failure == nil || got.Failure.HeadSHA != original || got.Failure.MergeHeadSHA != base || len(got.Failure.Unresolved) != 0 {
		t.Fatal("staging followed by commit failure lost physical facts", got, err)
	}
}
