package product

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestIssuePoolMetadataPageAndItemCheckpointSurviveReopen(t *testing.T) {
	numbers := make([]int, 70)
	for i := range numbers {
		numbers[i] = i + 1
	}
	p, _ := issuePoolFixture(t, numbers...)
	ctx := context.Background()
	// Previous admissions are retained, but release controller capacity using
	// the real operator path so the next new source can be admitted.
	for number := 1; number <= github.IssuePoolPageSize; number++ {
		wave, err := p.Admitter.Admit(ctx, number)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Admitter.Store.ApplyOperatorControl(ctx, storage.OperatorControl{Key: fmt.Sprintf("close-fixture-%d", number), Target: wave.Target(), Action: "cancel", Reason: "release fixture capacity"}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := p.Scan(ctx)
	if err != nil || len(first.Admitted) != 1 {
		t.Fatal(first, err)
	}
	var cursor issuePoolCursor
	if err := p.Admitter.Store.Record(ctx, "product_issue_pool_cursor", "17", &cursor); err != nil || cursor.After != "n:32" || cursor.LastNumber != 33 || cursor.Group != 0 {
		t.Fatal("metadata page/item checkpoint missing", cursor, err)
	}
	path := p.Admitter.Store.Root + "/state.db"
	p.Admitter.Store.Close()
	p.Admitter.Store, err = storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Admitter.Store.Close()
	second, err := p.Scan(ctx)
	if err != nil || len(second.Admitted) != 1 {
		t.Fatal(second, err)
	}
	wave, err := p.Admitter.Store.Wave(ctx, second.Admitted[0])
	if err != nil || wave.SourceID != "r17-i34" {
		t.Fatal("resumed metadata page skipped unread Issue", wave, err)
	}
}

func TestIssuePoolRechecksPriorityAgainstFreshCompleteLabels(t *testing.T) {
	p, api := issuePoolFixture(t, 1)
	p.PriorityLabels = []string{"P0"}
	issue := api.dependencies["issues/1"]
	label := issue.Labels[0]
	label.Name = "P0"
	issue.Labels = append(issue.Labels, label)
	api.dependencies["issues/1"] = issue // Metadata has not observed this edit.
	first, err := p.Scan(context.Background())
	if err != nil || len(first.Admitted) != 0 || len(first.Held) != 0 {
		t.Fatal("stale metadata selected wrong priority group", first, err)
	}
	assertNoIssueAdmission(t, p.Admitter)
	setPoolItem(t, api, 0, func(issue *github.Issue) {
		label := issue.Labels[0]
		label.Name = "P0"
		issue.Labels = append(issue.Labels, label)
	})
	second, err := p.Scan(context.Background())
	if err != nil || len(second.Admitted) != 1 {
		t.Fatal("fresh priority group was not reconsidered", second, err)
	}
}

func TestIssuePoolIntakesLargeBodyCollectionThroughMetadataPages(t *testing.T) {
	p, api := issuePoolFixture(t)
	p.PriorityLabels, p.Attempts = []string{"P0"}, 1
	items := make([]github.Issue, 64)
	for i := range items {
		items[i] = api.issue
		items[i].Number = i + 1
		items[i].Body += "\n" + strings.Repeat("large request context ", 4096)
	}
	label := items[63].Labels[0]
	label.Name = "P0"
	items[63].Labels = append(items[63].Labels, label)
	legacy, err := json.Marshal([][]github.Issue{items})
	if err != nil || len(legacy) <= github.MaxResourceReadBytes {
		t.Fatal("large collection regression not exercised", len(legacy), err)
	}
	dir := t.TempDir()
	files := map[string][]byte{
		"meta":  []byte(`{"id":17,"full_name":"owner/repo"}`),
		"high":  issueMetadataJSON(t, items[63:], false, "p0:64"),
		"first": issueMetadataJSON(t, items[:32], true, "n:32"),
		"next":  issueMetadataJSON(t, items[32:], false, "n:64"),
	}
	for _, issue := range items {
		raw, err := json.Marshal(issue)
		if err != nil {
			t.Fatal(err)
		}
		files[fmt.Sprintf("issue-%d", issue.Number)] = raw
	}
	for name, raw := range files {
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$ISSUE_PAGE_TEST_CALLS"
case "$*" in
 'api --method POST graphql --input -')
  request=$(/bin/cat)
  case "$request" in
   *'"labels":["ready","P0"]'*) /bin/cat "$ISSUE_PAGE_TEST_DIR/high" ;;
   *'"after":"n:32"'*) /bin/cat "$ISSUE_PAGE_TEST_DIR/next" ;;
   *) /bin/cat "$ISSUE_PAGE_TEST_DIR/first" ;;
  esac ;;
 'api --method GET repos/owner/repo') /bin/cat "$ISSUE_PAGE_TEST_DIR/meta" ;;
 'api --method GET repos/owner/repo/issues/'*)
  for argument do endpoint=$argument; done
  number="${endpoint##*/}"
  printf '%s\n' "$number" >> "$ISSUE_PAGE_TEST_BODIES"
  /bin/cat "$ISSUE_PAGE_TEST_DIR/issue-$number" ;;
 *) exit 49 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("ISSUE_PAGE_TEST_DIR", dir)
	t.Setenv("ISSUE_PAGE_TEST_CALLS", filepath.Join(dir, "calls"))
	t.Setenv("ISSUE_PAGE_TEST_BODIES", filepath.Join(dir, "bodies"))
	p.Admitter.API = github.Client{Repository: "owner/repo", RepositoryID: 17}
	first, err := p.Scan(context.Background())
	if err != nil || len(first.Admitted) != 1 {
		t.Fatal("large body collection blocked intake", first, err)
	}
	wave, err := p.Admitter.Store.Wave(context.Background(), first.Admitted[0])
	if err != nil || wave.SourceID != "r17-i64" {
		t.Fatal("priority on a later metadata page lost", wave, err)
	}
	second, err := p.Scan(context.Background())
	if err != nil || len(second.Admitted) != 1 {
		t.Fatal(second, err)
	}
	wave, err = p.Admitter.Store.Wave(context.Background(), second.Admitted[0])
	if err != nil || wave.SourceID != "r17-i1" {
		t.Fatal("numeric order after high priority lost", wave, err)
	}
	bodies, err := os.ReadFile(filepath.Join(dir, "bodies"))
	if err != nil || string(bodies) != "64\n64\n1\n1\n" {
		t.Fatal("pool enumerated unselected Issue bodies", string(bodies), err)
	}
	var runs int
	if err := p.Admitter.Store.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("metadata intake started a model", runs, err)
	}
}
