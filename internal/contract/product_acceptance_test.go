package contract

import (
	"fmt"
	"strings"
	"testing"
)

func productAcceptanceFixture() ProductAcceptance {
	return ProductAcceptance{CoverageDigest: strings.Repeat("a", 64), FinalPlanID: "final-plan", FinalPlanDigest: strings.Repeat("b", 64), FinalVerificationDigest: strings.Repeat("c", 64), CriterionIDs: []string{"AC"}}
}
func TestProductAcceptanceContractKeepsGenericAndProductEvidenceExclusive(t *testing.T) {
	p := productAcceptanceFixture()
	d := MainDecision{Target: Target{"wave", 1, 1, 1}, Action: "accept", ProductAcceptance: &p, Summary: "accept original criteria using exact final evidence"}
	for _, mode := range []string{"valid", "both", "none", "plan", "wait", "report", "remaining", "digest", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			copy := d
			proposal := p
			copy.ProductAcceptance = &proposal
			switch mode {
			case "both":
				copy.Acceptance = &ArtifactAcceptance{Outputs: []string{"artifact"}, Evaluations: []ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: "artifact", ReviewTaskID: "review"}}}
			case "none":
				copy.ProductAcceptance = nil
			case "plan", "wait", "report":
				copy.Action = mode
			case "remaining":
				copy.Remaining = []string{"unfinished"}
			case "digest":
				proposal.FinalVerificationDigest = "pass"
			case "duplicate":
				proposal.CriterionIDs = []string{"AC", "AC"}
			}
			err := (MainResult{SchemaVersion: 1, SessionID: "main", Decisions: []MainDecision{copy}}).Validate()
			if (err == nil) != (mode == "valid") {
				t.Fatal(mode, err)
			}
		})
	}
	for i := 1; i < 256; i++ {
		p.CriterionIDs = append(p.CriterionIDs, fmt.Sprintf("AC%d", i))
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	p.CriterionIDs = append(p.CriterionIDs, "overflow")
	if p.Validate() == nil {
		t.Fatal("criterion list exceeded Wave bound")
	}
	fields := ProductAcceptanceSchema()["properties"].(map[string]any)
	if fields["criterion_ids"].(map[string]any)["maxItems"] != 256 {
		t.Fatal("schema/validation mismatch")
	}
	if MainResultSchema()["properties"] == nil {
		t.Fatal("product acceptance not in main schema")
	}
}
