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
	mainFields := MainResultSchema()["properties"].(map[string]any)["decisions"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	field, ok := mainFields["product_acceptance"].(map[string]any)
	if !ok {
		t.Fatal("product acceptance field absent")
	}
	alternatives, ok := field["anyOf"].([]any)
	if !ok || len(alternatives) != 2 {
		t.Fatal("product acceptance must permit typed evidence or null")
	}
	object, ok := alternatives[0].(map[string]any)
	if !ok {
		t.Fatal("product acceptance object schema absent")
	}
	properties, ok := object["properties"].(map[string]any)
	if !ok {
		t.Fatal("product acceptance properties absent")
	}
	criteria, ok := properties["criterion_ids"].(map[string]any)
	if !ok || criteria["minItems"] != 1 || criteria["maxItems"] != 256 {
		t.Fatal("main output lost criterion bounds")
	}
	for _, name := range []string{"coverage_digest", "final_plan_digest", "final_verification_digest"} {
		digest, ok := properties[name].(map[string]any)
		if !ok || digest["pattern"] != "^[0-9a-f]{64}$" {
			t.Fatal("main output lost digest constraint", name)
		}
	}
	null, ok := alternatives[1].(map[string]any)
	if !ok || null["type"] != "null" {
		t.Fatal("main output cannot omit product acceptance for generic decisions")
	}
}
