// Package verification validates independent test plans and routes evidence-based repairs.
package verification

import (
	"context"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
	"path"
	"strings"
)

type Criterion struct {
	ID          string `json:"id"`
	Requirement string `json:"requirement"`
}
type Scope struct {
	Revision         int         `json:"revision"`
	OriginalRequest  string      `json:"original_request"`
	Criteria         []Criterion `json:"criteria"`
	Included         []string    `json:"included"`
	PreviouslyMerged []string    `json:"previously_merged"`
	Remaining        []string    `json:"remaining"`
}

func (s Scope) Validate() error {
	if s.Revision < 1 || strings.TrimSpace(s.OriginalRequest) == "" || len(s.Criteria) == 0 || len(s.Included) == 0 {
		return errors.New("incomplete scope")
	}
	known := map[string]bool{}
	for _, c := range s.Criteria {
		if c.ID == "" || c.Requirement == "" || known[c.ID] {
			return errors.New("invalid criterion")
		}
		known[c.ID] = true
	}
	seen := map[string]bool{}
	for _, id := range append(append(append([]string{}, s.Included...), s.Remaining...), s.PreviouslyMerged...) {
		if !known[id] || seen[id] {
			return errors.New("scope overlap or unknown criterion")
		}
		seen[id] = true
	}
	if len(seen) != len(known) {
		return errors.New("scope drops requirements")
	}
	return nil
}

type Mapping struct {
	CriterionID      string `json:"criterion_id"`
	TestRef          string `json:"test_ref"`
	ExpectationBasis string `json:"expectation_basis"`
	ProfileID        string `json:"profile_id"`
}
type Plan struct {
	SchemaVersion      int       `json:"schema_version"`
	WorkItemID         string    `json:"work_item_id"`
	ScopeRevision      int       `json:"scope_revision"`
	PolicyRevision     int       `json:"policy_revision"`
	InputCandidateID   string    `json:"input_candidate_id"`
	Revision           int       `json:"plan_revision"`
	CriteriaTests      []Mapping `json:"criteria_tests"`
	RequestedProfiles  []string  `json:"requested_profiles"`
	UnverifiedCriteria []string  `json:"unverified_criteria"`
	Blockers           []string  `json:"blockers"`
	OnSuccess          string    `json:"on_success"`
}

func DecodePlan(data []byte, id string, c runner.Candidate, scope Scope, v policy.Verification) (Plan, error) {
	var p Plan
	if err := contract.DecodeStrict(data, &p); err != nil {
		return p, err
	}
	return p, p.Validate(id, c, scope, v)
}
func (p Plan) Validate(id string, c runner.Candidate, s Scope, v policy.Verification) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if err := v.Validate(); err != nil {
		return err
	}
	if p.SchemaVersion != 1 || p.WorkItemID != id || p.ScopeRevision != s.Revision || p.ScopeRevision != c.ScopeRevision || p.PolicyRevision != v.Revision || p.PolicyRevision != c.PolicyRevision || p.InputCandidateID != c.ID || p.Revision < 1 || p.OnSuccess != "publish_pr" {
		return errors.New("plan identity/revision mismatch")
	}
	if _, err := v.ExecutionSet(p.RequestedProfiles); err != nil {
		return err
	}
	included := set(s.Included)
	covered := map[string]bool{}
	for _, m := range p.CriteriaTests {
		if !included[m.CriterionID] || strings.TrimSpace(m.TestRef) == "" || strings.TrimSpace(m.ExpectationBasis) == "" {
			return errors.New("invalid criterion mapping")
		}
		if _, ok := v.Profiles[m.ProfileID]; !ok {
			return errors.New("unknown mapping profile")
		}
		covered[m.CriterionID] = true
	}
	unverified := map[string]bool{}
	for _, id := range p.UnverifiedCriteria {
		if !included[id] || unverified[id] {
			return errors.New("invalid unverified criterion")
		}
		unverified[id] = true
	}
	for id := range included {
		if !covered[id] && !unverified[id] {
			return errors.New("missing criterion disposition")
		}
	}
	return nil
}
func (p Plan) Ready() bool { return len(p.UnverifiedCriteria) == 0 && len(p.Blockers) == 0 }

// Profiles includes policy requirements plus every profile referenced by a mapping.
func (p Plan) Profiles(v policy.Verification) ([]string, error) {
	requested := append([]string{}, p.RequestedProfiles...)
	for _, m := range p.CriteriaTests {
		requested = append(requested, m.ProfileID)
	}
	return v.ExecutionSet(requested)
}
func set(ids []string) map[string]bool {
	m := map[string]bool{}
	for _, id := range ids {
		m[id] = true
	}
	return m
}

type Assessment struct {
	SchemaVersion    int      `json:"schema_version"`
	WorkItemID       string   `json:"work_item_id"`
	CandidateID      string   `json:"candidate_id"`
	ScopeRevision    int      `json:"scope_revision"`
	Classification   string   `json:"classification"`
	EvidenceIDs      []string `json:"evidence_ids"`
	Reason           string   `json:"reason"`
	ExpectationBasis string   `json:"expectation_basis"`
	TestChanges      []string `json:"test_changes"`
}

func (a Assessment) Route(id string, c runner.Candidate, evidenceIDs []string) (string, error) {
	if a.SchemaVersion != 1 || a.WorkItemID != id || a.CandidateID != c.ID || a.ScopeRevision != c.ScopeRevision || len(a.EvidenceIDs) == 0 || strings.TrimSpace(a.Reason) == "" || strings.TrimSpace(a.ExpectationBasis) == "" {
		return "", errors.New("assessment missing identity or evidence")
	}
	known := set(evidenceIDs)
	seen := map[string]bool{}
	for _, e := range a.EvidenceIDs {
		if !known[e] || seen[e] {
			return "", errors.New("invalid evidence reference")
		}
		seen[e] = true
	}
	switch a.Classification {
	case "implementation_defect":
		return "implementation", nil
	case "test_defect":
		return "verification", nil
	case "infrastructure_failure":
		return "controller", nil
	case "requirement_ambiguity", "inconclusive":
		return "blocked", nil
	}
	return "", errors.New("unknown failure classification")
}

// CheckChanges compares against the controller's saved implementation SHA, so commits,
// deletions, staged/unstaged edits, and newly created files all undergo the same policy.
// Allowed entries are path prefixes ending in / or exact paths; no agent-defined glob.
func CheckChanges(ctx context.Context, dir, base string, allowed []string) error {
	sha, err := workspace.Resolve(ctx, dir, base)
	if err != nil {
		return err
	}
	tracked, err := workspace.Git(ctx, dir, "diff", "--name-only", "-z", "--no-renames", sha, "--")
	if err != nil {
		return err
	}
	untracked, err := workspace.Git(ctx, dir, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return err
	}
	for _, name := range append(strings.Split(tracked, "\x00"), strings.Split(untracked, "\x00")...) {
		if name == "" {
			continue
		}
		if path.Clean(name) != name || strings.HasPrefix(name, "../") || path.IsAbs(name) {
			return errors.New("invalid changed path")
		}
		ok := false
		for _, rule := range allowed {
			if rule != "" && (name == rule || (strings.HasSuffix(rule, "/") && strings.HasPrefix(name, rule))) {
				ok = true
			}
		}
		if !ok {
			return fmt.Errorf("verification modified disallowed path: %s", name)
		}
	}
	return nil
}

// CanRepair does not reset counts on candidate or role changes.
func CanRepair(total, limit int) bool { return total >= 0 && limit > 0 && total < limit }

func PassedPlan(c runner.Candidate, v policy.Verification, p Plan, results []runner.Result) bool {
	if !p.Ready() || p.InputCandidateID != c.ID || p.ScopeRevision != c.ScopeRevision || p.PolicyRevision != c.PolicyRevision {
		return false
	}
	profiles, err := p.Profiles(v)
	if err != nil {
		return false
	}
	v.RequiredProfiles = profiles
	return runner.Passed(c, v, results)
}

// Rebind preserves test design while invalidating all execution evidence for a new candidate.
func Rebind(p Plan, id string, c runner.Candidate, s Scope, v policy.Verification) (Plan, error) {
	p.InputCandidateID = c.ID
	if err := p.Validate(id, c, s, v); err != nil {
		return Plan{}, err
	}
	return p, nil
}
