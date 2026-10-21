package verification

import (
	"context"
	"encoding/json"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
	"os"
	"path/filepath"
	"testing"
)

func fixtures() (Scope, runner.Candidate, policy.Verification, Plan) {
	s := Scope{Revision: 1, OriginalRequest: "implement acceptance", Criteria: []Criterion{{"AC1", "reject duplicates"}, {"AC2", "future behavior"}}, Included: []string{"AC1"}, Remaining: []string{"AC2"}}
	c := runner.Candidate{ID: "c", ScopeRevision: 1, PolicyRevision: 1}
	v := policy.Verification{Revision: 1, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"true"}, TimeoutSeconds: 1}}}
	p := Plan{SchemaVersion: 1, WorkItemID: "w", ScopeRevision: 1, PolicyRevision: 1, InputCandidateID: "c", Revision: 1, CriteriaTests: []Mapping{{"AC1", "test::duplicates", "Issue AC1", "unit"}}, RequestedProfiles: []string{}, UnverifiedCriteria: []string{}, Blockers: []string{}, OnSuccess: "publish_pr"}
	return s, c, v, p
}
func TestPlanCoverageAndBounds(t *testing.T) {
	s, c, v, p := fixtures()
	b, _ := json.Marshal(p)
	if _, err := DecodePlan(b, "w", c, s, v); err != nil {
		t.Fatal(err)
	}
	p.CriteriaTests = nil
	if err := p.Validate("w", c, s, v); err == nil {
		t.Fatal("missing coverage accepted")
	}
	p.UnverifiedCriteria = []string{"AC1"}
	if err := p.Validate("w", c, s, v); err != nil || p.Ready() {
		t.Fatal("unverified accepted ready", err)
	}
	p.UnverifiedCriteria = []string{"AC2"}
	if err := p.Validate("w", c, s, v); err == nil {
		t.Fatal("residual mixed into plan")
	}
	s.Remaining = nil
	if err := s.Validate(); err == nil {
		t.Fatal("requirements dropped")
	}
}
func TestAssessmentRoutesAndBudget(t *testing.T) {
	_, c, _, _ := fixtures()
	for cls, role := range map[string]string{"implementation_defect": "implementation", "test_defect": "verification", "infrastructure_failure": "controller", "requirement_ambiguity": "blocked", "inconclusive": "blocked"} {
		a := Assessment{SchemaVersion: 1, WorkItemID: "w", CandidateID: c.ID, ScopeRevision: 1, Classification: cls, EvidenceIDs: []string{"job"}, Reason: "failure", ExpectationBasis: "AC1", TestChanges: []string{}}
		got, err := a.Route("w", c, []string{"job"})
		if err != nil || got != role {
			t.Fatal(got, err)
		}
		a.EvidenceIDs = []string{"invented"}
		if _, err = a.Route("w", c, []string{"job"}); err == nil {
			t.Fatal("invented evidence")
		}
	}
	if !CanRepair(2, 3) || CanRepair(3, 3) {
		t.Fatal("repair budget")
	}
}
func TestVerificationChangePolicy(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	for _, a := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"config", "commit.gpgsign", "false"}, {"commit", "--allow-empty", "-m", "base"}} {
		if _, err := workspace.Git(ctx, repo, a...); err != nil {
			t.Fatal(err)
		}
	}
	os.Mkdir(filepath.Join(repo, "tests"), 0700)
	os.WriteFile(filepath.Join(repo, "tests", "new_test.go"), []byte("test"), 0600)
	if err := CheckChanges(ctx, repo, "HEAD", []string{"tests/"}); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(repo, "product.go"), []byte("code"), 0600)
	if err := CheckChanges(ctx, repo, "HEAD", []string{"tests/"}); err == nil {
		t.Fatal("product change accepted")
	}
	os.Remove(filepath.Join(repo, "product.go"))
	os.Mkdir(filepath.Join(repo, " tests"), 0700)
	os.WriteFile(filepath.Join(repo, " tests", "product.go"), []byte("code"), 0600)
	if err := CheckChanges(ctx, repo, "HEAD", []string{"tests/"}); err == nil {
		t.Fatal("leading whitespace bypass")
	}
}
func TestPlanReuseAndStrictNull(t *testing.T) {
	s, c, v, p := fixtures()
	c.ID = "new"
	rebound, err := Rebind(p, "w", c, s, v)
	if err != nil || rebound.Revision != p.Revision {
		t.Fatal(rebound, err)
	}
	c.ScopeRevision = 2
	if _, err = Rebind(p, "w", c, s, v); err == nil {
		t.Fatal("stale scope reused")
	}
	b, _ := json.Marshal(p)
	var raw map[string]any
	json.Unmarshal(b, &raw)
	raw["blockers"] = []any{nil}
	b, _ = json.Marshal(raw)
	if _, err = DecodePlan(b, "w", c, s, v); err == nil {
		t.Fatal("null reason accepted")
	}
}

func TestValidateCommitUsesImmutableRegularFiles(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"config", "commit.gpgsign", "false"}} {
		if _, err := workspace.Git(ctx, repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(repo, "test"), []byte("original"), 0600)
	os.Symlink("test", filepath.Join(repo, "link"))
	sha, err := workspace.Freeze(ctx, repo, "tests")
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(filepath.Join(repo, "test"))
	os.WriteFile(filepath.Join(repo, "untracked"), []byte("new test"), 0600)
	for _, tc := range []struct {
		name  string
		valid bool
	}{{"test::case", true}, {"untracked", false}, {"link", false}, {"missing", false}} {
		p := Plan{CriteriaTests: []Mapping{{TestRef: tc.name}}}
		if err := p.ValidateCommit(ctx, repo, sha); (err == nil) != tc.valid {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}
}
