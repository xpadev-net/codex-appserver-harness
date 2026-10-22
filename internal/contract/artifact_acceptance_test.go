package contract

import "testing"

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
