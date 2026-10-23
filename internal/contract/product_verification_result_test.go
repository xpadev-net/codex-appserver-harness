package contract

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func productVerificationResultFixture() TaskResult {
	r := productTaskResultFixture()
	r.SchemaVersion = 3
	r.Artifacts = []string{"artifact-plan"}
	r.ProductVerification = &ProductVerificationReference{SubjectID: "subject", SubjectDigest: strings.Repeat("b", 64), PlanArtifactID: r.Artifacts[0]}
	return r
}
func TestProductVerificationResultIsPlanAndChangeIntentWithoutSelfEvidence(t *testing.T) {
	for _, mode := range []string{"valid", "blocked", "waiting", "legacy", "work_output", "missing_plan", "missing_subject", "digest", "self_evidence", "self_review", "self_candidate", "blocked_ref"} {
		t.Run(mode, func(t *testing.T) {
			r := productVerificationResultFixture()
			switch mode {
			case "blocked", "waiting":
				r.Outcome = mode
				r.ProductChanges, r.ProductVerification = []ProductChangeReference{}, nil
				if mode == "blocked" {
					r.Blockers = []string{"need refined test expectation"}
				} else {
					r.SubscriptionIDs = []string{"watch"}
				}
			case "legacy":
				r.SchemaVersion, r.ProductChanges = 1, nil
			case "work_output":
				r.SchemaVersion = 2
			case "missing_plan":
				r.Artifacts = []string{}
			case "missing_subject":
				r.ProductVerification = nil
			case "digest":
				r.ProductVerification.SubjectDigest = "unproven"
			case "self_evidence":
				r.Verification = &VerificationReference{PlanID: "model", EvidenceIDs: []string{"self-passed"}}
			case "self_review":
				r.Review = &ReviewResult{TargetDigest: "self", Verdict: "clean", Findings: []ReviewFinding{}}
			case "self_candidate":
				r.WorkItemRefs = []CandidateReference{{WorkItemID: "work", CandidateID: "invented", SHA: strings.Repeat("a", 40)}}
			case "blocked_ref":
				r.Outcome, r.Blockers, r.ProductChanges = "blocked", []string{"not finished"}, []ProductChangeReference{}
			}
			if (r.Validate() == nil) != (mode == "valid" || mode == "blocked" || mode == "waiting") {
				t.Fatal(mode, r.Validate())
			}
		})
	}
}
func TestProductVerificationSchemaKeepsPriorSchemasAndHasNoNewSHASlot(t *testing.T) {
	for name, schema := range map[string]any{"task-result-v1.json": TaskResultSchema(), "product-task-result-v2.json": ProductTaskResultSchema(), "product-verification-task-result-v3.json": ProductVerificationTaskResultSchema()} {
		data, err := json.MarshalIndent(schema, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		saved, err := os.ReadFile("../../schemas/handoffs/" + name)
		if err != nil || string(saved) != string(append(data, '\n')) {
			t.Fatal("versioned schema drift", name, err)
		}
	}
	properties := StrictOutputSchema(ProductVerificationTaskResultSchema())["properties"].(map[string]any)
	if properties["schema_version"].(map[string]any)["const"] != 3 || properties["review"].(map[string]any)["type"] != "null" || properties["verification"].(map[string]any)["type"] != "null" {
		t.Fatal(properties)
	}
	ref := properties["product_verification"].(map[string]any)["anyOf"].([]any)[0].(map[string]any)["properties"].(map[string]any)
	if len(ref) != 3 || ref["sha"] != nil || ref["evidence_ids"] != nil {
		t.Fatal("model assigned candidate/evidence", ref)
	}
	for _, schema := range []map[string]any{TaskResultSchema(), ProductTaskResultSchema()} {
		if _, ok := schema["properties"].(map[string]any)["product_verification"]; ok {
			t.Fatal("verification output leaked into earlier schema")
		}
	}
}
