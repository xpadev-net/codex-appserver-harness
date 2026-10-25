package worker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestProductLocalReviewOutputRequiresTrustedVersionAndBinding(t *testing.T) {
	b := SessionBinding{Kind: "task", TaskID: "review", PhaseID: "phase", Target: contract.Target{WaveID: "wave", ScopeRevision: 1, PlanRevision: 1, PolicyRevision: 1}}
	r := contract.TaskResult{SchemaVersion: 4, Target: b.Target, TaskID: b.TaskID, PhaseID: b.PhaseID, Outcome: "result", Artifacts: []string{"report"}, CriterionResults: []contract.CriterionResult{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}, Review: &contract.ReviewResult{TargetDigest: "candidate", Verdict: "clean", Findings: []contract.ReviewFinding{}}, ProductLocalReview: &contract.ProductLocalReview{ScopeDigest: strings.Repeat("a", 64), Criteria: []contract.CriterionResult{{CriterionID: "local", EvidenceIDs: []string{"report"}, Status: "claimed", Basis: "independent code review"}}}}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateProductLocalReviewTaskOutput(raw, b); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTaskOutput(raw, b); err == nil {
		t.Fatal("ordinary v1 profile accepted scoped review")
	}
	b.TaskID = "another-task"
	if err := ValidateProductLocalReviewTaskOutput(raw, b); err == nil {
		t.Fatal("scoped review escaped Task binding")
	}
}
