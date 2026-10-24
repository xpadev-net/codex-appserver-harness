package contract

import (
	"errors"
	"regexp"
)

// ProductAcceptance explicitly names the merged coverage and final execution
// evidence consumed by the parent. Only the controller can verify/complete it.
type ProductAcceptance struct {
	CoverageDigest          string   `json:"coverage_digest"`
	FinalPlanID             string   `json:"final_plan_id"`
	FinalPlanDigest         string   `json:"final_plan_digest"`
	FinalVerificationDigest string   `json:"final_verification_digest"`
	CriterionIDs            []string `json:"criterion_ids"`
}

func (p ProductAcceptance) Validate() error {
	digest := regexp.MustCompile(`^[0-9a-f]{64}$`)
	if !digest.MatchString(p.CoverageDigest) || !digest.MatchString(p.FinalPlanDigest) || !digest.MatchString(p.FinalVerificationDigest) || !inline(p.FinalPlanID) || len(p.CriterionIDs) < 1 || len(p.CriterionIDs) > 256 || !validIDs(p.CriterionIDs) {
		return errors.New("bounded product acceptance evidence references required")
	}
	seen := map[string]bool{}
	for _, id := range p.CriterionIDs {
		if !inline(id) || seen[id] {
			return errors.New("unique original criterion references required")
		}
		seen[id] = true
	}
	return nil
}
func ProductAcceptanceSchema() map[string]any {
	schema := Schema(ProductAcceptance{})
	fields := schema["properties"].(map[string]any)
	for _, name := range []string{"coverage_digest", "final_plan_digest", "final_verification_digest"} {
		fields[name].(map[string]any)["pattern"] = "^[0-9a-f]{64}$"
	}
	fields["criterion_ids"].(map[string]any)["minItems"] = 1
	fields["criterion_ids"].(map[string]any)["maxItems"] = 256
	return schema
}
