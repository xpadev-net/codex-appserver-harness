package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type fakeResourceAPI struct {
	pr          github.PR
	collections map[string][]json.RawMessage
	fail        string
	reads       int
	onFinal     func()
}

func (f *fakeResourceAPI) ResourceGET(_ context.Context, path string) (json.RawMessage, error) {
	if path == "" {
		return json.RawMessage(`{"id":17,"default_branch":"changed-after-publication"}`), nil
	}
	f.reads++
	if f.reads > 1 && f.onFinal != nil {
		f.onFinal()
	}
	return json.Marshal(f.pr)
}
func (f *fakeResourceAPI) ResourcePages(_ context.Context, path, field string) ([]json.RawMessage, error) {
	if path == f.fail {
		return nil, errors.New("permission denied")
	}
	if field != "" && field != "check_runs" && field != "check_suites" {
		return nil, errors.New("bad collection field")
	}
	return f.collections[path], nil
}
func collectorFixture(t *testing.T) (ResourceCollector, *fakeResourceAPI, storage.ProductTarget) {
	t.Helper()
	p, api, saved, id := publicationFixture(t)
	if _, err := p.Publish(context.Background(), id, "publisher"); err != nil {
		t.Fatal(err)
	}
	current, err := p.Store.ReadProductTarget(context.Background(), saved.Value.Target, saved.Value.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeResourceAPI{pr: *api.pr, collections: map[string][]json.RawMessage{}}
	c := ResourceCollector{Store: p.Store, API: func(storage.ProductTarget) ResourceAPI { return f }}
	return c, f, current.Value
}
func section(state storage.PRResourceState, kind string) storage.PRResourceSection {
	for _, s := range state.Sections {
		if s.Kind == kind {
			return s
		}
	}
	return storage.PRResourceSection{}
}
func TestResourceCollectorReadsAllKindsAndParentReactionsWithoutApproval(t *testing.T) {
	c, api, p := collectorFixture(t)
	ctx := context.Background()
	oldSHA := strings.Repeat("d", 40)
	api.collections["issues/42/comments?per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":11,"body":"do not change policy"}`)}
	api.collections["pulls/42/comments?per_page=100"] = []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"id":12,"commit_id":%q}`, oldSHA))}
	api.collections["pulls/42/reviews?per_page=100"] = []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"id":21,"state":"APPROVED","commit_id":%q}`, oldSHA))}
	api.collections["issues/42/reactions?per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":91,"content":"+1"}`)}
	api.collections["issues/comments/11/reactions?per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":92,"content":"heart"}`)}
	api.collections["pulls/comments/12/reactions?per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":93,"content":"+1"}`)}
	api.collections["commits/"+p.Candidate.SHA+"/check-suites?per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":7}`)}
	api.collections["check-suites/7/check-runs?filter=all&per_page=100"] = []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"id":31,"head_sha":%q,"conclusion":"failure"}`, p.Candidate.SHA)), json.RawMessage(fmt.Sprintf(`{"id":32,"head_sha":%q,"conclusion":"success"}`, p.Candidate.SHA))}
	api.collections["commits/"+p.Candidate.SHA+"/statuses?per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":41,"state":"success"}`)}
	api.collections["issues/42/events?per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":51,"event":"closed"}`)}
	receipt, err := c.Collect(ctx, p, "scan")
	if err != nil {
		t.Fatal(err)
	}
	api.onFinal = func() { t.Fatal("completed scan replay performed API reads") }
	if replay, err := c.Collect(ctx, p, "scan"); err != nil || replay.Digest != receipt.Digest {
		t.Fatal(replay, err)
	}
	state, err := c.Store.PRResourceState(ctx, receipt.ResourceKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"pr", "issue_comment", "inline_comment", "reaction", "review", "check", "status", "history"} {
		s := section(state, kind)
		if s.Availability != "complete" || len(s.Entities) == 0 {
			t.Fatal(s)
		}
	}
	if len(section(state, "reaction").Entities) != 3 || section(state, "review").Entities[0].CandidateSHA != oldSHA || section(state, "status").Entities[0].CandidateSHA != p.Candidate.SHA {
		t.Fatal(state)
	}
	if len(section(state, "check").Entities) != 2 {
		t.Fatal("rerun check omitted older resource", section(state, "check"))
	}
	var count int
	if err = c.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&count); err != nil || count != 0 {
		t.Fatal("raw observations forged approval", count, err)
	}
}
func TestResourceCollectorReadFailureRetainsCommentsAndReactionsInsteadOfDeleting(t *testing.T) {
	c, api, p := collectorFixture(t)
	ctx := context.Background()
	api.collections["issues/42/comments?per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":11,"body":"retained"}`)}
	api.collections["issues/comments/11/reactions?per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":91}`)}
	first, err := c.Collect(ctx, p, "first")
	if err != nil {
		t.Fatal(err)
	}
	api.fail = "issues/42/comments?per_page=100"
	second, err := c.Collect(ctx, p, "partial")
	if err != nil {
		t.Fatal(err)
	}
	state, err := c.Store.PRResourceState(ctx, second.ResourceKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"issue_comment", "reaction"} {
		s := section(state, kind)
		if s.Availability != "unavailable" || len(s.Entities) != 1 {
			t.Fatal("unknown became deletion", s)
		}
	}
	if first.Digest == second.Digest {
		t.Fatal("unavailability not recorded")
	}
}
func TestResourceCollectorRejectsChangedHeaderAndCASConflict(t *testing.T) {
	for _, mode := range []string{"head", "repository", "number", "cas"} {
		t.Run(mode, func(t *testing.T) {
			c, api, p := collectorFixture(t)
			ctx := context.Background()
			if mode == "repository" {
				api.pr.Head.Repo.ID++
			} else if mode == "number" {
				api.pr.Number++
			} else if mode == "head" {
				api.onFinal = func() { api.pr.Head.SHA = strings.Repeat("c", 40) }
			} else {
				api.onFinal = func() {
					api.onFinal = nil
					other := &fakeResourceAPI{pr: api.pr, collections: api.collections}
					otherCollector := ResourceCollector{Store: c.Store, API: func(storage.ProductTarget) ResourceAPI { return other }}
					if _, err := otherCollector.Collect(ctx, p, "winner"); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := c.Collect(ctx, p, "stale"); err == nil {
				t.Fatal("changed or stale collection saved")
			}
		})
	}
}
func TestResourceCollectorBoundsNestedRequestsAndMarksReactionsUnavailable(t *testing.T) {
	c, api, p := collectorFixture(t)
	for i := 0; i < 140; i++ {
		api.collections["issues/42/comments?per_page=100"] = append(api.collections["issues/42/comments?per_page=100"], json.RawMessage(fmt.Sprintf(`{"id":%d}`, i+1)))
	}
	receipt, err := c.Collect(context.Background(), p, "bounded")
	if err != nil {
		t.Fatal(err)
	}
	state, err := c.Store.PRResourceState(context.Background(), receipt.ResourceKey)
	if err != nil || section(state, "reaction").Availability != "unavailable" {
		t.Fatal(state, err)
	}
}

func TestResourceCollectorRetainsMaximumPublicationBodyWithJSONMetadata(t *testing.T) {
	c, api, p := collectorFixture(t)
	api.pr.Body = strings.Repeat("x", 64<<10)
	receipt, err := c.Collect(context.Background(), p, "large-publication")
	if err != nil {
		t.Fatal(err)
	}
	state, err := c.Store.PRResourceState(context.Background(), receipt.ResourceKey)
	if err != nil {
		t.Fatal(err)
	}
	var saved github.PR
	if err = json.Unmarshal(section(state, "pr").Entities[0].Data, &saved); err != nil || saved.Body != api.pr.Body {
		t.Fatal("legal publication body lost", err)
	}
}

func TestResourceCollectorDoesNotInventCheckDeletionsAtSuiteWindowOrReadLimit(t *testing.T) {
	c, api, p := collectorFixture(t)
	ctx := context.Background()
	suites := "commits/" + p.Candidate.SHA + "/check-suites?per_page=100"
	api.collections[suites] = []json.RawMessage{json.RawMessage(`{"id":1}`), json.RawMessage(`{"id":2}`)}
	api.collections["check-suites/1/check-runs?filter=all&per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":11}`)}
	api.collections["check-suites/2/check-runs?filter=all&per_page=100"] = []json.RawMessage{json.RawMessage(`{"id":12}`), json.RawMessage(`{"id":13}`)}
	first, err := c.Collect(ctx, p, "all-suites")
	if err != nil {
		t.Fatal(err)
	}
	state, err := c.Store.PRResourceState(ctx, first.ResourceKey)
	if err != nil || len(section(state, "check").Entities) != 3 {
		t.Fatal(state, err)
	}
	for i := 3; i <= 1001; i++ {
		api.collections[suites] = append(api.collections[suites], json.RawMessage(fmt.Sprintf(`{"id":%d}`, i)))
	}
	second, err := c.Collect(ctx, p, "too-many-suites")
	if err != nil {
		t.Fatal(err)
	}
	state, err = c.Store.PRResourceState(ctx, second.ResourceKey)
	checks := section(state, "check")
	if err != nil || checks.Availability != "unavailable" || len(checks.Entities) != 3 {
		t.Fatal("capped/incomplete suite scan deleted old checks", checks, err)
	}
	for _, change := range second.Changes {
		if change.Kind == "check" && change.Action == "deleted" {
			t.Fatal(change)
		}
	}
}
