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
	"time"
)

func baseMergeFixture(t *testing.T, conflict bool) (FrozenCheckout, string) {
	t.Helper()
	ctx := context.Background()
	p := pinnedFixture(t)
	frozen, err := FreezePinned(ctx, p, "original implementation")
	if err != nil {
		t.Fatal(err)
	}
	name := "upstream.txt"
	if conflict {
		name = "change.txt"
	}
	if err := os.WriteFile(filepath.Join(p.Repository, name), []byte("predecessor merged behavior\n"), 0600); err != nil {
		t.Fatal(err)
	}
	base, err := Freeze(ctx, p.Repository, "predecessor merge")
	if err != nil {
		t.Fatal(err)
	}
	return frozen, base
}

func TestMergePinnedBasePreservesHistoryAndSuppressesRepositoryCommands(t *testing.T) {
	ctx := context.Background()
	frozen, base := baseMergeFixture(t, false)
	marker := filepath.Join(t.TempDir(), "external-command")
	script := filepath.Join(t.TempDir(), "trap")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntouch '"+marker+"'\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	hooks := t.TempDir()
	for _, hook := range []string{"pre-merge-commit", "pre-commit", "commit-msg", "post-merge", "post-commit"} {
		if err := os.WriteFile(filepath.Join(hooks, hook), []byte("#!/bin/sh\ntouch '"+marker+"'\nexit 1\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{
		{"config", "core.hooksPath", hooks},
		{"config", "branch." + frozen.Branch + ".mergeOptions", "-s ours --autostash"},
		{"config", "commit.gpgsign", "true"}, {"config", "gpg.program", script},
		{"config", "merge.verifySignatures", "true"}, {"config", "merge.autoStash", "true"},
	} {
		if _, err := Git(ctx, frozen.Repository, args...); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	got, err := MergePinnedBase(ctx, frozen, base, func(context.Context) error { calls++; return nil })
	if err != nil || calls != 1 || !got.Attempted || got.Outcome != "merged" || got.HeadSHA == frozen.SHA {
		t.Fatal(got, calls, err)
	}
	for name, want := range map[string]string{"change.txt": "candidate change\n", "upstream.txt": "predecessor merged behavior\n"} {
		raw, err := os.ReadFile(filepath.Join(frozen.CWD, name))
		if err != nil || string(raw) != want {
			t.Fatal(name, string(raw), err)
		}
	}
	parents, err := Git(ctx, frozen.CWD, "rev-list", "--parents", "-n", "1", got.HeadSHA)
	if err != nil || !reflect.DeepEqual(strings.Fields(parents), []string{got.HeadSHA, frozen.SHA, base}) {
		t.Fatal(parents, err)
	}
	source, err := Resolve(ctx, frozen.Repository, "HEAD")
	if err != nil || source != base {
		t.Fatal("base branch moved", source, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("repository command executed", err)
	}
	next := frozen
	next.SHA = got.HeadSHA
	again, err := MergePinnedBase(ctx, next, base, func(context.Context) error { t.Fatal("current base consumed another fence"); return nil })
	if err != nil || again.Attempted || again.Outcome != "already_current" || again.HeadSHA != got.HeadSHA {
		t.Fatal(again, err)
	}
	if _, err := MergePinnedBase(ctx, frozen, base, func(context.Context) error { return nil }); err == nil {
		t.Fatal("old candidate reused after merge")
	}
}

func TestMergePinnedBaseKeepsConflictsWithoutClaimingSuccess(t *testing.T) {
	ctx := context.Background()
	frozen, base := baseMergeFixture(t, true)
	got, err := MergePinnedBase(ctx, frozen, base, func(context.Context) error { return nil })
	if err != nil || got.Outcome != "conflict" || !got.Attempted || got.HeadSHA != frozen.SHA || !reflect.DeepEqual(got.Conflicts, []string{"change.txt"}) {
		t.Fatal(got, err)
	}
	raw, err := os.ReadFile(filepath.Join(frozen.CWD, "change.txt"))
	if err != nil || !strings.Contains(string(raw), "<<<<<<< HEAD") || !strings.Contains(string(raw), "predecessor merged behavior") {
		t.Fatal(string(raw), err)
	}
	if _, err := MergePinnedBase(ctx, frozen, base, func(context.Context) error { t.Fatal("conflict replay consumed another fence"); return nil }); err == nil {
		t.Fatal("unresolved merge replayed")
	}
}

func TestMergePinnedBaseRejectsChangedOrUntrustedInputsBeforeMutation(t *testing.T) {
	for _, mode := range []string{"symbolic_base", "unknown_base", "divergent_base", "candidate_changed", "dirty", "partial_clone", "checkout_partial_clone", "busy", "held", "late_dirty", "late_head", "cancel", "no_fence"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			frozen, base := baseMergeFixture(t, false)
			held := errors.New("source/owner changed")
			calls := 0
			fence := func(context.Context) error {
				calls++
				switch mode {
				case "held":
					return held
				case "late_dirty":
					return os.WriteFile(filepath.Join(frozen.CWD, "late.txt"), []byte("late"), 0600)
				case "late_head":
					_, err := Git(ctx, frozen.CWD, "reset", "--hard", base)
					return err
				}
				return nil
			}
			switch mode {
			case "symbolic_base":
				base = "HEAD"
			case "unknown_base":
				base = strings.Repeat("a", 40)
			case "divergent_base":
				var err error
				base, err = Git(ctx, frozen.Repository, "commit-tree", "HEAD^{tree}", "-m", "unrelated root")
				if err != nil {
					t.Fatal(err)
				}
				base = strings.TrimSpace(base)
			case "checkout_partial_clone":
				config := filepath.Join(t.TempDir(), "promisor-config")
				if err := os.WriteFile(config, []byte("[remote \"origin\"]\n promisor = true\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := Git(ctx, frozen.Repository, "config", "includeIf.onbranch:harness/**.path", config); err != nil {
					t.Fatal(err)
				}
				raw, err := Git(ctx, frozen.CWD, "config", "--get", "remote.origin.promisor")
				if err != nil || strings.TrimSpace(raw) != "true" {
					t.Fatal("fixture did not enable checkout-only partial clone", raw, err)
				}
			case "candidate_changed":
				frozen.SHA = base
			case "dirty":
				if err := os.WriteFile(filepath.Join(frozen.CWD, "dirty.txt"), []byte("dirty"), 0600); err != nil {
					t.Fatal(err)
				}
			case "partial_clone":
				if _, err := Git(ctx, frozen.Repository, "config", "remote.origin.promisor", "true"); err != nil {
					t.Fatal(err)
				}
			case "busy":
				if err := os.WriteFile(filepath.Join(frozen.GitDir, "CHERRY_PICK_HEAD"), []byte(base), 0600); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				var stop context.CancelFunc
				ctx, stop = context.WithCancel(ctx)
				stop()
			case "no_fence":
				fence = nil
			}
			got, err := MergePinnedBase(ctx, frozen, base, fence)
			if err == nil || got.Attempted || got.Outcome != "" {
				t.Fatal("invalid input mutated checkout", got, err)
			}
			if mode == "held" && !errors.Is(err, held) {
				t.Fatal("lost fence failure", err)
			}
			if mode != "held" && mode != "late_dirty" && mode != "late_head" && calls != 0 {
				t.Fatal("invalid input consumed fence", calls)
			}
		})
	}
}

func TestMergePinnedBaseDoesNotExecuteAttributeDrivers(t *testing.T) {
	ctx := context.Background()
	frozen, base := baseMergeFixture(t, true)
	marker := filepath.Join(t.TempDir(), "attribute-driver-ran")
	script := filepath.Join(t.TempDir(), "trap")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntouch '"+marker+"'\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"config", "merge.trap.driver", script}, {"config", "filter.trap.clean", script}, {"config", "filter.trap.smudge", script}, {"config", "filter.trap.required", "true"}} {
		if _, err := Git(ctx, frozen.Repository, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(frozen.CWD, ".gitattributes"), []byte("change.txt merge=trap filter=trap\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var err error
	frozen, err = FreezePinned(ctx, frozen.PinnedCheckout, "candidate attribute policy")
	if err != nil {
		t.Fatal(err)
	}
	got, err := MergePinnedBase(ctx, frozen, base, func(context.Context) error { return nil })
	if err != nil || got.Outcome != "conflict" || got.HeadSHA != frozen.SHA {
		t.Fatal(got, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("merge/filter driver executed", err)
	}
}

func TestMergePinnedBaseCancellationNeverReportsCompletedMerge(t *testing.T) {
	frozen, base := baseMergeFixture(t, false)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	started := filepath.Join(bin, "merge-started")
	script := "#!/bin/sh\ncase \"$*\" in\n *' merge --no-commit '*) : > \"$BASE_MERGE_TEST_STARTED\"; /bin/sleep 30; exit 0 ;;\nesac\nexec '" + realGit + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	t.Setenv("BASE_MERGE_TEST_STARTED", started)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := os.Stat(started); err == nil {
					cancel()
					return
				}
			}
		}
	}()
	got, err := MergePinnedBase(ctx, frozen, base, func(context.Context) error { return nil })
	cancel()
	<-joined
	if _, err := os.Stat(started); err != nil {
		t.Fatal("did not reach actual mutation command", err)
	}
	if !errors.Is(err, context.Canceled) || !got.Attempted || got.Outcome != "" || got.HeadSHA != "" {
		t.Fatal("cancel became completed merge", got, err)
	}
	head, err := Resolve(context.Background(), frozen.CWD, "HEAD")
	if err != nil || head != frozen.SHA {
		t.Fatal("canceled fixture changed HEAD", head, err)
	}
}

func TestMergePinnedBaseNonConflictFailureReturnsReconciliationObservation(t *testing.T) {
	frozen, base := baseMergeFixture(t, false)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	// Actually stage the complete merge, then lose the successful exit. This
	// leaves no unmerged paths, but its index/worktree/MERGE_HEAD did change.
	script := "#!/bin/sh\ncase \"$*\" in\n *' merge --no-commit '*) '" + realGit + "' \"$@\" || exit $?; exit 1 ;;\nesac\nexec '" + realGit + "' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	got, err := MergePinnedBase(context.Background(), frozen, base, func(context.Context) error { return nil })
	if !errors.Is(err, ErrBaseMergeUnsettled) || !got.Attempted || got.Outcome != "" || got.Failure == nil {
		t.Fatal("partial mutation disappeared or became success", got, err)
	}
	f := got.Failure
	if f.HeadSHA != frozen.SHA || f.MergeHeadSHA != base || f.Clean || len(f.Unresolved) != 0 {
		t.Fatal("lost stopped partial merge state", f)
	}
	if _, err := MergePinnedBase(context.Background(), frozen, base, func(context.Context) error { t.Fatal("partial merge replayed"); return nil }); err == nil {
		t.Fatal("partial merge did not hold")
	}
}
