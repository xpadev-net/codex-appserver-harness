package contract

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestArtifactAcceptanceRejectsAmbiguousOrUnboundedReferences(t *testing.T) {
	for _, mode := range []string{"empty", "duplicate_output", "duplicate_evaluation", "unlisted_output", "too_many", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			p := ArtifactAcceptance{Outputs: []string{"output"}, Evaluations: []ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: "output", ReviewTaskID: "independent"}}}
			switch mode {
			case "empty":
				p.Outputs = nil
			case "duplicate_output":
				p.Outputs = append(p.Outputs, "output")
			case "duplicate_evaluation":
				p.Evaluations = append(p.Evaluations, p.Evaluations[0])
			case "unlisted_output":
				p.Evaluations[0].ArtifactID = "different"
			case "too_many":
				p.Evaluations = make([]ArtifactCriterionEvaluation, 1025)
			case "oversized":
				p.Evaluations[0].ReviewTaskID = string(make([]byte, 1<<17))
			}
			if p.Validate() == nil {
				t.Fatal("invalid references accepted")
			}
		})
	}
}

func TestArtifactAcceptanceBoundsMatchSchemaAndStrictDecoder(t *testing.T) {
	p := ArtifactAcceptance{Outputs: []string{"output"}, Evaluations: []ArtifactCriterionEvaluation{}}
	for i := 0; i < MaxArtifactEvaluations; i++ {
		p.Evaluations = append(p.Evaluations, ArtifactCriterionEvaluation{CriterionID: fmt.Sprint(i), ArtifactID: "output", ReviewTaskID: "review"})
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ArtifactAcceptance
	if err = DecodeStrict(raw, &decoded); err != nil {
		t.Fatal("advertised maximum cannot be decoded", err)
	}
	if ArtifactAcceptanceSchema()["properties"].(map[string]any)["evaluations"].(map[string]any)["maxItems"] != MaxArtifactEvaluations {
		t.Fatal("schema evaluation limit differs")
	}
	p.Evaluations = append(p.Evaluations, ArtifactCriterionEvaluation{CriterionID: "overflow", ArtifactID: "output", ReviewTaskID: "review"})
	raw, err = json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if p.Validate() == nil || DecodeStrict(raw, &decoded) == nil {
		t.Fatal("oversized evaluations accepted")
	}
}
