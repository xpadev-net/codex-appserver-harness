package github

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResourceReadsBoundOutputEvenThroughCopyAndRequireCompletePages(t *testing.T) {
	var output boundedResourceOutput
	if _, err := io.Copy(&output, strings.NewReader(strings.Repeat("x", MaxResourceReadBytes+1))); err == nil || output.buffer.Len() > MaxResourceReadBytes {
		t.Fatal("aggregate output cap bypassed", output.buffer.Len(), err)
	}
	dir := t.TempDir()
	payload := filepath.Join(dir, "payload")
	args := filepath.Join(dir, "args")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$RESOURCE_TEST_ARGS\"\n/bin/cat \"$RESOURCE_TEST_PAYLOAD\"\n"
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("RESOURCE_TEST_ARGS", args)
	t.Setenv("RESOURCE_TEST_PAYLOAD", payload)
	c := Client{Repository: "owner/repo", RepositoryID: 17}
	for _, test := range []struct {
		body, field string
		count       int
		valid       bool
	}{{`[[{"id":1}],[{"id":2}]]`, "", 2, true}, {`[{"check_runs":[{"id":3}]},{"check_runs":[]}]`, "check_runs", 1, true}, {`[[]]`, "", 0, true}, {`[]`, "", 0, false}, {`null`, "", 0, false}, {`[null]`, "", 0, false}, {`[{}]`, "check_runs", 0, false}, {`[{"check_runs":null}]`, "check_runs", 0, false}} {
		if err := os.WriteFile(payload, []byte(test.body), 0600); err != nil {
			t.Fatal(err)
		}
		items, err := c.ResourcePages(context.Background(), "pulls/42/comments?per_page=100", test.field)
		if (err == nil) != test.valid || (err == nil && len(items) != test.count) {
			t.Fatal(test, items, err)
		}
	}
	wire, err := os.ReadFile(args)
	if err != nil || !strings.Contains(string(wire), "--method GET --paginate --slurp repos/owner/repo/") {
		t.Fatal(string(wire), err)
	}
	if err = os.WriteFile(payload, []byte(strings.Repeat("x", MaxResourceReadBytes+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ResourceGET(context.Background(), "pulls/42"); err == nil {
		t.Fatal("oversized subprocess output accepted")
	}
}

func TestFailedResourcePageDecodingConsumesSharedWireBudgetBeforeNextCommand(t *testing.T) {
	dir := t.TempDir()
	payload := filepath.Join(dir, "payload")
	calls := filepath.Join(dir, "calls")
	if err := os.WriteFile(payload, []byte(strings.Repeat("x", 100)), 0600); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nprintf 'call\\n' >> \"$RESOURCE_TEST_CALLS\"\n/bin/cat \"$RESOURCE_TEST_PAYLOAD\"\n"
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("RESOURCE_TEST_CALLS", calls)
	t.Setenv("RESOURCE_TEST_PAYLOAD", payload)
	ctx := WithResourceReadBudget(context.Background(), 400)
	c := Client{Repository: "owner/repo", RepositoryID: 17}
	for i := 0; i < 6; i++ {
		if _, err := c.ResourcePages(ctx, "issues/42/comments", ""); err == nil {
			t.Fatal("malformed response accepted")
		}
	}
	wire, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(wire), "call\n") != 4 {
		t.Fatal("failed reads bypassed shared budget", string(wire), err)
	}
}

func TestNestedResourceBudgetsChargeFailedChildAndBlockSiblings(t *testing.T) {
	parent := WithResourceReadBudget(context.Background(), 10)
	first := WithResourceReadBudget(parent, 2)
	budget := first.Value(resourceBudgetKey{}).(*resourceReadBudget)
	if err := budget.charge(6); err == nil {
		t.Fatal("child limit was ignored")
	}
	sibling := WithResourceReadBudget(parent, 100)
	shared := sibling.Value(resourceBudgetKey{}).(*resourceReadBudget)
	if !shared.available() {
		t.Fatal("failed child charged bytes it did not consume")
	}
	if err := shared.charge(4); err != nil {
		t.Fatal(err)
	}
	if shared.available() || WithResourceReadBudget(parent, 100).Value(resourceBudgetKey{}).(*resourceReadBudget).available() {
		t.Fatal("new child reset exhausted parent")
	}
}
