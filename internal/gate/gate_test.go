package gate

import (
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"testing"
	"time"
)

func fixture() Snapshot {
	c := runner.Candidate{ID: "c", SHA: "head", BaseSHA: "base", ScopeRevision: 1, PolicyRevision: 1}
	w := domain.WorkItem{ID: "w", RepositoryID: 1, Phase: domain.Reviewing, Candidate: c.ID, ScopeRevision: 1, PolicyRevision: 1}
	p := github.PR{State: "open"}
	p.Head.SHA = c.SHA
	p.Head.Repo.ID = 1
	p.Base.SHA = c.BaseSHA
	p.Base.Ref = "configured"
	p.Base.Repo.ID = 1
	v := policy.Verification{Revision: 1, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"true"}, TimeoutSeconds: 1, SuccessPattern: "PASS"}}}
	scope := verification.Scope{Revision: 1, OriginalRequest: "requirements", Criteria: []verification.Criterion{{ID: "AC", Requirement: "behavior"}}, Included: []string{"AC"}}
	plan := verification.Plan{SchemaVersion: 1, WorkItemID: "w", ScopeRevision: 1, PolicyRevision: 1, InputCandidateID: "c", Revision: 1, CriteriaTests: []verification.Mapping{{CriterionID: "AC", TestRef: "test", ExpectationBasis: "AC", ProfileID: "unit"}}, OnSuccess: "publish_pr"}
	zero := 0
	r := runner.Result{Candidate: c, TestedSHA: c.SHA, ProfileID: "unit", ProfileRevision: 1, Status: "passed", ExitCode: &zero, Started: time.Now(), Finished: time.Now(), LogPath: "evidence"}
	return Snapshot{WorkItem: w, Candidate: c, PR: p, BaseBranch: "configured", Scope: scope, Plan: plan, Verification: v, Results: []runner.Result{r}, Review: review.Decision{Approved: true}, RepositoryConditions: "CLEAN", CurrentScopeRevision: 1, CurrentPolicyRevision: 1, ObservedAt: time.Now()}
}
func TestGateFailsClosed(t *testing.T) {
	s := fixture()
	if reasons := Reasons(s); len(reasons) > 0 {
		t.Fatal(reasons)
	}
	for name, change := range map[string]func(*Snapshot){"held": func(s *Snapshot) { s.WorkItem.MergeHold = true }, "cancel": func(s *Snapshot) { s.WorkItem.Phase = domain.Canceled }, "old head": func(s *Snapshot) { s.PR.Head.SHA = "old" }, "base": func(s *Snapshot) { s.PR.Base.SHA = "changed" }, "scope": func(s *Snapshot) { s.CurrentScopeRevision = 2 }, "policy": func(s *Snapshot) { s.CurrentPolicyRevision = 2 }, "empty tests": func(s *Snapshot) { s.Results = nil }, "no approval": func(s *Snapshot) { s.Review.Approved = false }, "rules unknown": func(s *Snapshot) { s.RepositoryConditions = "UNKNOWN" }, "residual": func(s *Snapshot) { s.Scope.Remaining = []string{"future"} }, "stale": func(s *Snapshot) { s.ObservedAt = time.Now().Add(-time.Hour) }} {
		t.Run(name, func(t *testing.T) {
			s := fixture()
			change(&s)
			if len(Reasons(s)) == 0 {
				t.Fatal("unsafe merge permitted")
			}
		})
	}
}
func TestCheckOriginAndMergeSHA(t *testing.T) {
	s := fixture()
	s.RequiredChecks = []RequiredCheck{{Name: "ci", AppID: 9, AllowedConclusions: []string{"success"}}}
	check := github.Check{ID: 1, Name: "ci", HeadSHA: "merge", Status: "completed", Conclusion: "success"}
	check.App.ID = 9
	s.Checks = []github.Check{check}
	if len(Reasons(s)) == 0 {
		t.Fatal("unknown merge sha accepted")
	}
	s.CI = CIRevision{HeadSHA: "head", BaseSHA: "base", TestedSHA: "merge", Verified: true}
	if reasons := Reasons(s); len(reasons) > 0 {
		t.Fatal(reasons)
	}
	s.Checks[0].App.ID = 10
	if len(Reasons(s)) == 0 {
		t.Fatal("wrong app accepted")
	}
}
