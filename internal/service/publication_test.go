package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func TestPRBodyPublishesCurrentValidationAndSafeReferences(t *testing.T) {
	c, gh, _, _ := setup(t)
	ctx := context.Background()
	gh.approved = false
	drive(t, c, 8)
	w, _ := c.Store.Get(ctx, "w")
	candidate, _ := c.candidate(ctx, w)
	results, err := c.results(ctx, candidate, w)
	if err != nil || len(results) != 1 {
		t.Fatal(results, err)
	}
	result := results[0]
	for _, want := range []string{"| `unit` | 1 | passed |", candidate.SHA, result.JobID, "harness-journal:work-items/w/validation/" + result.JobID, "harness inspect"} {
		if !strings.Contains(gh.pr.Body, want) {
			t.Fatalf("missing %q in %s", want, gh.pr.Body)
		}
	}
	if strings.Contains(gh.pr.Body, result.LogPath) || strings.Contains(gh.pr.Body, c.Config.Root) {
		t.Fatal("private path published", gh.pr.Body)
	}
}

func TestPRBodyUpdatesEvidenceWithCandidate(t *testing.T) {
	c, gh, w, _ := pendingPRUpdate(t)
	ctx := context.Background()
	oldSHA := gh.pr.Head.SHA
	oldBody := gh.pr.Body
	candidate, _ := c.candidate(ctx, w)
	results, err := c.results(ctx, candidate, w)
	if err != nil || len(results) != 1 {
		t.Fatal(results, err)
	}
	result := results[0]
	result.LogPath = "/controller-private/credentials/runner.log"
	result.Environment = "secret environment"
	result.Reason = "secret diagnostic"
	if err = c.Store.Put(ctx, "validation", result.JobID, w.ID, result); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 2)
	if gh.pr.Head.SHA != candidate.SHA || gh.pr.Body == oldBody || strings.Contains(gh.pr.Body, oldSHA) {
		t.Fatal("old candidate evidence retained", gh.pr.Body)
	}
	for _, want := range []string{candidate.SHA, result.JobID, "harness-journal:work-items/w/validation/" + result.JobID} {
		if !strings.Contains(gh.pr.Body, want) {
			t.Fatal("missing updated evidence", want, gh.pr.Body)
		}
	}
	for _, private := range []string{result.LogPath, result.Environment, result.Reason} {
		if strings.Contains(gh.pr.Body, private) {
			t.Fatal("private evidence published", private)
		}
	}
}

func TestPublicationSelectsLatestFormalProfilesDeterministically(t *testing.T) {
	candidate := runner.Candidate{ID: "current", SHA: strings.Repeat("a", 40), ScopeRevision: 1, PolicyRevision: 1}
	w := domain.WorkItem{ID: "work/item", Issue: 1, ScopeRevision: 1, PolicyRevision: 1}
	result := runner.Result{JobID: "current-job", Candidate: candidate, ProfileID: "unit", ProfileRevision: 3, Status: "passed", TestedSHA: candidate.SHA, Started: time.Now()}
	old := result
	old.JobID = "old-job"
	old.Status = "failed"
	old.Started = result.Started.Add(-time.Second)
	unrelated := result
	unrelated.JobID = "other-candidate"
	unrelated.Candidate.ID = "old"
	unrelated.Started = result.Started.Add(time.Second)
	exploration := result
	exploration.JobID = "exploratory-job"
	exploration.ProfileID = "exploration"
	body := publicationBody(w, verification.Scope{}, candidate, []string{"unit"}, []runner.Result{old, result, unrelated, exploration})
	if !strings.Contains(body, "| `unit` | 3 | passed |") || !strings.Contains(body, "harness-journal:work-items/work%2Fitem/validation/current-job") {
		t.Fatal(body)
	}
	for _, excluded := range []string{old.JobID, unrelated.JobID, exploration.JobID} {
		if strings.Contains(body, excluded) {
			t.Fatal("noncurrent/formal evidence included", excluded, body)
		}
	}
	if reordered := publicationBody(w, verification.Scope{}, candidate, []string{"unit"}, []runner.Result{exploration, unrelated, result, old}); reordered != body {
		t.Fatal("journal ordering changed publication content")
	}
}
