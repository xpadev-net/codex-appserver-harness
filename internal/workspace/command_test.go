package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func TestStoppedGitCommandsJoinOrphansBeforeReturningOutput(t *testing.T) {
	bin := t.TempDir()
	childFile := filepath.Join(bin, "child")
	script := `#!/bin/sh
case "$*" in
 *'config --null --name-only --get-regexp'*) exit 1 ;;
esac
/bin/sleep 30 &
printf '%s' "$!" > "$GIT_STOP_TEST_CHILD"
printf 'fixed output\n'
exit 0
`
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("GIT_STOP_TEST_CHILD", childFile)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	output, err := Git(WithStoppedCommands(ctx), bin, "rev-parse", "HEAD")
	if err != nil || output != "fixed output\n" {
		t.Fatal(output, err)
	}
	raw, err := os.ReadFile(childFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	state, _, _, statErr := process.ReadStat(pid)
	if statErr == nil && state != "Z" && state != "X" {
		t.Fatal("Git descendant survived successful owner proof", pid, state)
	}
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		t.Fatal(statErr)
	}
}
