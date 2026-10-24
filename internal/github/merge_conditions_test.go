package github

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResourceConditionsUsesBoundedReadOnlyCommandAndRetainsUnknown(t *testing.T) {
	dir := t.TempDir()
	payload := filepath.Join(dir, "payload")
	args := filepath.Join(dir, "args")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$MERGE_CONDITIONS_ARGS\"\n/bin/cat \"$MERGE_CONDITIONS_PAYLOAD\"\n"
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("MERGE_CONDITIONS_ARGS", args)
	t.Setenv("MERGE_CONDITIONS_PAYLOAD", payload)
	client := Client{Repository: "owner/repo", RepositoryID: 17}
	for _, body := range []string{`{"mergeStateStatus":"CLEAN"}`, `{"mergeStateStatus":"UNKNOWN"}`, `{}`, `invalid`, strings.Repeat("x", MaxResourceReadBytes+1)} {
		if err := os.WriteFile(payload, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		state, err := client.ResourceConditions(context.Background(), 42)
		if body == `{"mergeStateStatus":"CLEAN"}` {
			if err != nil || state != "CLEAN" {
				t.Fatal(state, err)
			}
		} else if state == "CLEAN" {
			t.Fatal("unknown became ready", body)
		}
	}
	raw, err := os.ReadFile(args)
	if err != nil || strings.TrimSpace(string(raw)) != "pr view 42 --repo owner/repo --json mergeStateStatus" {
		t.Fatal(string(raw), err)
	}
	if _, err := client.ResourceConditions(context.Background(), 0); err == nil {
		t.Fatal("invalid PR number")
	}
	if err := os.WriteFile(payload, []byte(`{"mergeStateStatus":"CLEAN"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ResourceConditions(WithResourceReadBudget(context.Background(), 1), 42); err == nil {
		t.Fatal("aggregate budget ignored")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.ResourceConditions(ctx, 42); err == nil {
		t.Fatal("canceled read accepted")
	}
}
