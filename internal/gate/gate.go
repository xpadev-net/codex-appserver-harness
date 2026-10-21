// Package gate centralizes merge eligibility. All facts are freshly reconciled by
// the merge handler; event arrival order and agent success claims are insufficient.
package gate

import (
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"time"
)

type RequiredCheck struct {
	Name               string   `json:"name"`
	AppID              int64    `json:"app_id"`
	AllowedConclusions []string `json:"allowed_conclusions"`
}
type CIRevision struct {
	HeadSHA   string
	BaseSHA   string
	TestedSHA string
	Verified  bool
}
type Snapshot struct {
	UnresolvedFindings    int
	CI                    CIRevision
	WorkItem              domain.WorkItem
	Candidate             runner.Candidate
	PR                    github.PR
	BaseBranch            string
	Scope                 verification.Scope
	Plan                  verification.Plan
	Verification          policy.Verification
	Results               []runner.Result
	Review                review.Decision
	RepositoryConditions  string
	Checks                []github.Check
	RequiredChecks        []RequiredCheck
	ResidualPersisted     bool
	CurrentScopeRevision  int
	CurrentPolicyRevision int
	ObservedAt            time.Time
}

func Reasons(s Snapshot) []string {
	reasons := []string{}
	add := func(condition bool, reason string) {
		if !condition {
			reasons = append(reasons, reason)
		}
	}
	w := s.WorkItem
	c := s.Candidate
	add(!w.Terminal() && !w.MergeHold, "operator prohibits merge")
	add(w.Phase == domain.Reviewing || w.Phase == domain.MergePending, "workflow is not ready for merge")
	add(w.Candidate == c.ID && w.ScopeRevision == c.ScopeRevision && w.PolicyRevision == c.PolicyRevision, "work item candidate changed")
	add(s.PR.State == "open" && !s.PR.Draft && !s.PR.Merged, "PR not open and ready")
	add(s.PR.Head.SHA == c.SHA && s.PR.Head.Repo.ID == w.RepositoryID && s.PR.Base.Repo.ID == w.RepositoryID, "remote candidate/repository mismatch")
	add(s.PR.Base.Ref == s.BaseBranch && s.PR.Base.SHA == c.BaseSHA, "base changed")
	add(s.Scope.Revision == s.CurrentScopeRevision && c.ScopeRevision == s.CurrentScopeRevision && c.PolicyRevision == s.CurrentPolicyRevision, "scope or policy superseded")
	add(s.Plan.Validate(w.ID, c, s.Scope, s.Verification) == nil && verification.PassedPlan(c, s.Verification, s.Plan, s.Results), "required verification incomplete")
	add(s.UnresolvedFindings == 0, "review feedback remains unresolved")
	add(s.Review.Approved && !s.Review.Blocking, "explicit review approval missing or blocked")
	add(s.RepositoryConditions == "CLEAN", "repository rules/checks/conversations not satisfied")
	add(len(s.Scope.Remaining) == 0 || s.ResidualPersisted, "residual work not persisted")
	add(!s.ObservedAt.IsZero() && time.Since(s.ObservedAt) >= 0 && time.Since(s.ObservedAt) < time.Minute, "merge facts stale")
	target := s.Candidate.SHA
	if s.CI.TestedSHA != "" {
		add(s.CI.Verified && s.CI.HeadSHA == s.Candidate.SHA && s.CI.BaseSHA == s.Candidate.BaseSHA, "CI SHA mapping unknown or stale")
		target = s.CI.TestedSHA
	}
	for _, req := range s.RequiredChecks {
		latest := LatestCheck(s.Checks, req, target)
		ok := false
		if latest != nil && latest.Status == "completed" {
			for _, allowed := range req.AllowedConclusions {
				if latest.Conclusion == allowed {
					ok = true
				}
			}
		}
		add(req.Name != "" && req.AppID > 0 && ok, "required CI check missing or unsuccessful: "+req.Name)
	}
	return reasons
}

// LatestCheck shares the exact issuer/name/SHA selection used for merge gates.
func LatestCheck(checks []github.Check, req RequiredCheck, target string) *github.Check {
	var latest *github.Check
	for i := range checks {
		c := &checks[i]
		if c.Name == req.Name && c.App.ID == req.AppID && c.HeadSHA == target && (latest == nil || c.ID > latest.ID) {
			latest = c
		}
	}
	return latest
}
