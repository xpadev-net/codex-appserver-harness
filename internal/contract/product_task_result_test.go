package contract

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func productTaskResultFixture() TaskResult {
	return TaskResult{SchemaVersion: 2, Target: Target{WaveID: "wave", ScopeRevision: 1, PlanRevision: 1, PolicyRevision: 1}, TaskID: "task", PhaseID: "phase", Outcome: "result", Artifacts: []string{}, CriterionResults: []CriterionResult{{CriterionID: "AC", Status: "unverified", Basis: "code changes are ready for independent validation", EvidenceIDs: []string{}}}, WorkItemRefs: []CandidateReference{}, ProductChanges: []ProductChangeReference{{WorkItemID: "work", WorkspaceID: "workspace", WorkspaceDigest: strings.Repeat("a", 64)}}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}}
}
func TestProductTaskResultVersionSeparatesChangesFromCandidateAndQualityClaims(t *testing.T) {
	for _, mode := range []string{"valid", "legacy", "missing", "multiple", "digest", "foreign_sha", "review", "verification", "blocked_changes", "blocked"} {
		t.Run(mode, func(t *testing.T) {
			result := productTaskResultFixture()
			switch mode {
			case "legacy":
				result.SchemaVersion = 1
			case "missing":
				result.ProductChanges = nil
			case "multiple":
				result.ProductChanges = append(result.ProductChanges, result.ProductChanges[0])
			case "digest":
				result.ProductChanges[0].WorkspaceDigest = "unverified"
			case "foreign_sha":
				result.WorkItemRefs = []CandidateReference{{WorkItemID: "work", CandidateID: "self-claimed", SHA: strings.Repeat("b", 40)}}
			case "review":
				result.Review = &ReviewResult{TargetDigest: "self-grade", Verdict: "clean", Findings: []ReviewFinding{}}
			case "verification":
				result.Verification = &VerificationReference{PlanID: "self-claimed", EvidenceIDs: []string{"model"}}
			case "blocked_changes":
				result.Outcome = "blocked"
				result.Blockers = []string{"not ready"}
			case "blocked":
				result.Outcome = "blocked"
				result.Blockers = []string{"not ready"}
				result.ProductChanges = []ProductChangeReference{}
			}
			if (result.Validate() == nil) != (mode == "valid" || mode == "blocked") {
				t.Fatal(mode, result.Validate())
			}
		})
	}
}
func TestProductTaskResultSchemaKeepsExistingVersionOneBytes(t *testing.T) {
	generated, err := json.MarshalIndent(TaskResultSchema(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile("../../schemas/handoffs/task-result-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(append(generated, '\n')) != string(saved) {
		t.Fatal("legacy TaskResult schema changed")
	}
	if _, exists := TaskResultSchema()["properties"].(map[string]any)["product_changes"]; exists {
		t.Fatal("v2 output leaked into generic schema")
	}
	generic := productTaskResultFixture()
	generic.SchemaVersion = 1
	generic.ProductChanges = nil
	generic.Artifacts = []string{"artifact"}
	data, err := json.Marshal(generic)
	if err != nil || strings.Contains(string(data), "product_changes") {
		t.Fatal(string(data), err)
	}
	schema := StrictOutputSchema(ProductTaskResultSchema())
	props := schema["properties"].(map[string]any)
	if props["schema_version"].(map[string]any)["const"] != 2 || props["work_item_refs"].(map[string]any)["maxItems"] != 0 || props["review"].(map[string]any)["type"] != "null" {
		t.Fatal(schema)
	}
	change := props["product_changes"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	if len(change) != 3 || change["sha"] != nil || change["candidate_id"] != nil {
		t.Fatal("model schema manufactures candidate identity", change)
	}
}
