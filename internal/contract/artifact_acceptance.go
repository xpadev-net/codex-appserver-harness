package contract

import (
	"encoding/json"
	"errors"
)

const MaxArtifactEvaluations = 256

type ArtifactCriterionEvaluation struct {
	CriterionID  string `json:"criterion_id"`
	ArtifactID   string `json:"artifact_id"`
	ReviewTaskID string `json:"review_task_id"`
}

// ArtifactAcceptance names the proposed final outputs and their independent
// evaluations. These are references to facts for the controller to verify.
type ArtifactAcceptance struct {
	Outputs     []string                      `json:"outputs"`
	Evaluations []ArtifactCriterionEvaluation `json:"evaluations"`
}

func (p ArtifactAcceptance) Validate() error {
	if len(p.Outputs) == 0 || len(p.Outputs) > 128 || !validIDs(p.Outputs) || len(p.Evaluations) == 0 || len(p.Evaluations) > MaxArtifactEvaluations {
		return errors.New("bounded output/evaluation references required")
	}
	outputs := map[string]bool{}
	for _, id := range p.Outputs {
		if !inline(id) || outputs[id] {
			return errors.New("invalid or duplicate output")
		}
		outputs[id] = true
	}
	seen := map[ArtifactCriterionEvaluation]bool{}
	for _, evaluation := range p.Evaluations {
		if !inline(evaluation.CriterionID) || !inline(evaluation.ReviewTaskID) || !outputs[evaluation.ArtifactID] || seen[evaluation] {
			return errors.New("invalid or duplicate artifact evaluation")
		}
		seen[evaluation] = true
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	if len(raw) > 64<<10 {
		return errors.New("artifact acceptance exceeds 64 KiB")
	}
	return nil
}

func ArtifactAcceptanceSchema() map[string]any {
	schema := Schema(ArtifactAcceptance{})
	fields := schema["properties"].(map[string]any)
	fields["outputs"].(map[string]any)["minItems"] = 1
	fields["outputs"].(map[string]any)["maxItems"] = 128
	fields["evaluations"].(map[string]any)["minItems"] = 1
	fields["evaluations"].(map[string]any)["maxItems"] = MaxArtifactEvaluations
	return schema
}
