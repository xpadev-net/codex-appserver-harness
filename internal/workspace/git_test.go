package workspace

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCandidateWorktree(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "harness@example.invalid"}, {"config", "user.name", "Harness"}, {"commit", "--allow-empty", "-m", "base"}} {
		if _, err := Git(ctx, repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "tree")
	if err := Create(ctx, repo, path, "harness/test", "HEAD"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "file"), []byte("content"), 0600); err != nil {
		t.Fatal(err)
	}
	sha, err := Freeze(ctx, path, "candidate")
	if err != nil || !shaPattern.MatchString(sha) {
		t.Fatal(sha, err)
	}
	if err = Clean(ctx, path); err != nil {
		t.Fatal(err)
	}
}
