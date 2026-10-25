package contract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProductLocalReviewSeparatesLocalIDsFromWaveAssignment(t *testing.T) {
	target := Target{WaveID: "wave", ScopeRevision: 1, PlanRevision: 1, PolicyRevision: 1}
	result := TaskResult{SchemaVersion: 4, Target: target, TaskID: "review", PhaseID: "phase", Outcome: "result", Artifacts: []string{"report"}, CriterionResults: []CriterionResult{{CriterionID: "AC", EvidenceIDs: []string{"report"}, Status: "claimed", Basis: "Wave mapping"}}, WorkItemRefs: []CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}, Review: &ReviewResult{TargetDigest: "candidate", Verdict: "clean", Findings: []ReviewFinding{}}, ProductLocalReview: &ProductLocalReview{ScopeDigest: strings.Repeat("a", 64), Criteria: []CriterionResult{{CriterionID: "AC", EvidenceIDs: []string{"local-report"}, Status: "claimed", Basis: "different local meaning"}, {CriterionID: "parse-left", EvidenceIDs: []string{"local-report"}, Status: "claimed", Basis: "local behavior"}}}}
	if err := result.ValidateAssignment(target, "review", "phase", []string{"AC"}); err != nil {
		t.Fatal("qualified local IDs must not become invented Wave IDs", err)
	}
	for _, mode := range []string{"legacy", "missing", "bad_digest", "duplicate", "unsupported", "missing_evidence"} {
		t.Run(mode, func(t *testing.T) {
			raw, _ := json.Marshal(result)
			var bad TaskResult
			if err := DecodeStrict(raw, &bad); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "legacy":
				bad.SchemaVersion = 1
			case "missing":
				bad.ProductLocalReview = nil
			case "bad_digest":
				bad.ProductLocalReview.ScopeDigest = "unknown"
			case "duplicate":
				bad.ProductLocalReview.Criteria[1].CriterionID = "AC"
			case "unsupported":
				bad.ProductLocalReview.Criteria[0].Status = "success"
			case "missing_evidence":
				bad.ProductLocalReview.Criteria[0].EvidenceIDs = []string{}
			}
			if err := bad.Validate(); err == nil {
				t.Fatal("invalid local review accepted", mode)
			}
		})
	}
	for _, schema := range []map[string]any{TaskResultSchema(), ProductTaskResultSchema(), ProductVerificationTaskResultSchema()} {
		if _, exists := schema["properties"].(map[string]any)["product_local_review"]; exists {
			t.Fatal("old role schema changed")
		}
	}
	if ProductLocalReviewTaskResultSchema()["properties"].(map[string]any)["schema_version"].(map[string]any)["const"] != 4 {
		t.Fatal("unversioned scoped review profile")
	}
}
