package service

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func reopenAdoptionStore(t *testing.T, c *Controller) {
	t.Helper()
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	c.Store, err = storage.Open(filepath.Join(c.Config.Root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Store.Close() })
}

func TestAdoptChangedRequirementsAfterFullMerge(t *testing.T) {
	for _, change := range []string{"issue", "policy"} {
		t.Run(change, func(t *testing.T) {
			c, gh, agents, jobs := setup(t)
			ctx := context.Background()
			jobs.failFirst = true // Retained repair, token and job budgets are nonzero.
			var previous domain.WorkItem
			for range 30 {
				drive(t, c, 1)
				previous, _ = c.Store.Get(ctx, "w")
				if previous.Phase == domain.Merged {
					break
				}
			}
			if previous.Phase != domain.Merged || gh.issue.State != "open" || previous.RepairRounds == 0 {
				t.Fatal("bad full-merge setup", previous, gh.issue.State)
			}
			var scope, oldScope verification.Scope
			if err := c.Store.Record(ctx, "scope", previous.ID, &scope); err != nil {
				t.Fatal(err)
			}
			if len(scope.Remaining) != 0 {
				t.Fatal("expected full coverage")
			}
			oldScope = scope
			var oldPolicy string
			c.Store.Record(ctx, "policy_snapshot", previous.ID, &oldPolicy)
			if change == "issue" {
				gh.issue.Body += "\nupdated acceptance requirements"
			} else {
				c.Config.Verification.Revision++
				profile := c.Config.Verification.Profiles["unit"]
				profile.Revision++
				c.Config.Verification.Profiles["unit"] = profile
				if err := c.ensurePolicy(ctx); err != nil {
					t.Fatal(err)
				}
			}
			drive(t, c, 1)
			state := finalState(t, c)
			if state.Classification != "scope_or_policy_conflict" || !strings.Contains(state.RequiredAction, "adopt --config CONFIG --id w --file") {
				t.Fatal(state)
			}
			reopenAdoptionStore(t, c)
			beforeJobs, err := c.jobAttempts(ctx, previous.ID)
			if err != nil {
				t.Fatal(err)
			}
			beforeMetrics, err := c.Metrics(ctx, previous.ID)
			if err != nil {
				t.Fatal(err)
			}
			scope.Revision++
			scope.OriginalRequest = gh.issue.Body
			if err = c.AdoptContinuation(ctx, previous, scope, "accept updated requirements/policy"); err != nil {
				t.Fatal(err)
			}
			next, err := c.Store.Get(ctx, "w-remaining")
			if err != nil || next.Phase != domain.Ready || next.Candidate != "" || next.RepairRounds != previous.RepairRounds || next.PolicyRevision != c.Config.Verification.Revision {
				t.Fatal(next, err)
			}
			state = finalState(t, c)
			if state.WorkItemID != next.ID || state.RecoveryID != next.ID || state.ScopeRevision != scope.Revision || state.Candidate.ID != "" || state.State != "waiting" {
				t.Fatal("final owner not transferred", state)
			}
			var claim string
			if err = c.Store.DB.QueryRowContext(ctx, "SELECT work_item_id FROM issue_claims WHERE repository_id=1 AND issue=1").Scan(&claim); err != nil || claim != next.ID {
				t.Fatal(claim, err)
			}
			var baseline BudgetBaseline
			if err = c.Store.Record(ctx, "budget_baseline", next.ID, &baseline); err != nil || baseline.ValidationJobs != beforeJobs || baseline.ObservedTokens != beforeMetrics["observed_tokens"].(int64) {
				t.Fatal("lost budget", baseline, err)
			}
			for _, kind := range []string{"candidate", "plan", "review_round", "role_thread", "final_failure"} {
				if records, err := c.Store.Records(ctx, kind, next.ID); err != nil || len(records) != 0 {
					t.Fatal("old evidence inherited", kind, records, err)
				}
			}
			reopenAdoptionStore(t, c)
			calls := agents.calls
			gh.approved = false
			drive(t, c, 20)
			if gh.merges != 1 || gh.closed != 0 || agents.calls != calls+2 {
				t.Fatal("new implementation/verification/approval missing", gh.merges, gh.closed, agents.calls, calls)
			}
			gh.approved = true
			drive(t, c, 15)
			if state = finalState(t, c); state.State != "completed" || state.WorkItemID != next.ID || gh.merges != 2 || gh.closed != 1 {
				t.Fatal(state, gh.merges, gh.closed)
			}
			actual, err := c.Store.Get(ctx, previous.ID)
			if err != nil || !reflect.DeepEqual(actual, previous) {
				t.Fatal("merged history changed", actual, previous, err)
			}
			var actualScope verification.Scope
			c.Store.Record(ctx, "scope", previous.ID, &actualScope)
			var actualPolicy string
			c.Store.Record(ctx, "policy_snapshot", previous.ID, &actualPolicy)
			if !reflect.DeepEqual(actualScope, oldScope) || actualPolicy != oldPolicy {
				t.Fatal("old scope/policy changed", actualScope, actualPolicy)
			}
			subject, err := c.Store.FinalAcceptanceSubject(ctx, previous)
			if err != nil || subject.ID != next.ID {
				t.Fatal("lineage does not recognize new final owner", subject, err)
			}
			history, err := c.Store.Records(ctx, "parent_final_history", previous.ID)
			if err != nil || len(history) != 1 {
				t.Fatal("old conflict lost", history, err)
			}
		})
	}
}
