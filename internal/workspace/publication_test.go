package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func publicationFixture(t *testing.T) (FrozenCheckout, string) {
	t.Helper()
	// All hostile config in these tests is private; never edit host Git config.
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(t.TempDir(), "global-config"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	p := pinnedFixture(t)
	frozen, err := FreezePinned(context.Background(), p, "fixed Candidate")
	if err != nil {
		t.Fatal(err)
	}
	remote := t.TempDir()
	if _, err := Git(context.Background(), remote, "init", "--bare"); err != nil {
		t.Fatal(err)
	}
	return frozen, remote
}

func TestPublishPinnedUsesOnlyFixedBranchAndFastForward(t *testing.T) {
	ctx := context.Background()
	frozen, remote := publicationFixture(t)
	other := t.TempDir()
	if _, err := Git(ctx, other, "init", "--bare"); err != nil {
		t.Fatal(err)
	}
	if _, err := Git(ctx, frozen.CWD, "remote", "add", "origin", other); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "unexpected-helper")
	hooks := t.TempDir()
	trap := []byte("#!/bin/sh\ntouch '" + marker + "'\nexit 1\n")
	for _, name := range []string{"pre-push", "signer"} {
		if err := os.WriteFile(filepath.Join(hooks, name), trap, 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"config", "core.hooksPath", hooks}, {"config", "gpg.program", filepath.Join(hooks, "signer")}, {"config", "push.gpgSign", "true"}, {"config", "push.followTags", "true"}, {"-c", "tag.gpgsign=false", "tag", "-a", "do-not-publish", "-m", "private tag", frozen.BaseSHA}} {
		if _, err := Git(ctx, frozen.CWD, args...); err != nil {
			t.Fatal(err)
		}
	}
	fences := 0
	guard := func(context.Context) error { fences++; return nil }
	first, err := PublishPinned(ctx, frozen, remote, "harness/product", guard)
	if err != nil || !first.Attempted || first.RemoteSHA != frozen.SHA || fences != 1 {
		t.Fatal(first, fences, err)
	}
	if head, err := publicationHead(WithStoppedCommands(ctx), frozen.PinnedCheckout, other, "harness/product"); err != nil || head != "" {
		t.Fatal("origin was used instead of fixed remote", head, err)
	}
	if tags, err := Git(ctx, remote, "for-each-ref", "--format=%(refname)", "refs/tags/"); err != nil || tags != "" {
		t.Fatal("push followed an unrelated tag", tags, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("publication ran a hook or signer", err)
	}
	again, err := PublishPinned(ctx, frozen, remote, "harness/product", guard)
	if err != nil || again.Attempted || fences != 1 {
		t.Fatal("matching remote head was pushed again", again, fences, err)
	}
	if err := os.WriteFile(filepath.Join(frozen.CWD, "next.txt"), []byte("next Candidate\n"), 0600); err != nil {
		t.Fatal(err)
	}
	next, err := FreezePinned(ctx, frozen.PinnedCheckout, "next Candidate")
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := PublishPinned(ctx, next, remote, "harness/product", guard)
	if err != nil || !advanced.Attempted || advanced.RemoteSHA != next.SHA || fences != 2 {
		t.Fatal("fast-forward did not publish", advanced, fences, err)
	}
}

func TestPublishPinnedRejectsChangedSourceAndAuthorityBeforePush(t *testing.T) {
	for _, mode := range []string{"guard", "dirty", "head", "rewrite", "rewrite_in_guard", "push_option", "credentials"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			frozen, remote := publicationFixture(t)
			rejected := errors.New("permission changed")
			guard := func(context.Context) error { return nil }
			switch mode {
			case "guard":
				guard = func(context.Context) error { return rejected }
			case "dirty":
				if err := os.WriteFile(filepath.Join(frozen.CWD, "late.txt"), []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "head":
				if _, err := Git(ctx, frozen.CWD, "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "unexpected later commit"); err != nil {
					t.Fatal(err)
				}
			case "rewrite":
				if _, err := Git(ctx, frozen.CWD, "config", "url."+t.TempDir()+".pushInsteadOf", remote); err != nil {
					t.Fatal(err)
				}
			case "rewrite_in_guard":
				guard = func(context.Context) error {
					_, err := Git(ctx, frozen.CWD, "config", "url."+t.TempDir()+".insteadOf", remote)
					return err
				}
			case "push_option":
				if _, err := Git(ctx, frozen.CWD, "config", "push.pushOption", "unrelated-server-action"); err != nil {
					t.Fatal(err)
				}
			case "credentials":
				remote = "https://secret@github.com/owner/repo.git"
			}
			observed, err := PublishPinned(ctx, frozen, remote, "harness/product", guard)
			if err == nil || observed.Attempted {
				t.Fatal("invalid publication invoked push", observed, err)
			}
			if mode == "guard" && !errors.Is(err, rejected) {
				t.Fatal(err)
			}
		})
	}
}

func TestPublishPinnedRefusesDivergedRemoteWithoutForce(t *testing.T) {
	ctx := context.Background()
	frozen, remote := publicationFixture(t)
	if _, err := Git(ctx, frozen.Repository, "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "independent remote history"); err != nil {
		t.Fatal(err)
	}
	diverged, err := Resolve(ctx, frozen.Repository, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Git(ctx, frozen.Repository, "push", remote, diverged+":refs/heads/harness/product"); err != nil {
		t.Fatal(err)
	}
	called := false
	observed, err := PublishPinned(ctx, frozen, remote, "harness/product", func(context.Context) error { called = true; return nil })
	if err == nil || observed.Attempted || called {
		t.Fatal("diverged remote reached send fence", observed, err)
	}
	head, err := publicationHead(WithStoppedCommands(ctx), frozen.PinnedCheckout, remote, "harness/product")
	if err != nil || head != diverged {
		t.Fatal("publication rewrote remote history", head, err)
	}
}
