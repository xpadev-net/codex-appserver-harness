package control

import (
	"context"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"path/filepath"
	"testing"
)

func TestKernelSourceUsesProviderOutputSchemas(t *testing.T) {
	for _, kind := range []string{"orchestrator", "task"} {
		schema, validate, err := sourceContract(kind)
		if err != nil || validate == nil {
			t.Fatal(kind, err)
		}
		object := schema.(map[string]any)
		if kind == "orchestrator" {
			object = object["properties"].(map[string]any)["decisions"].(map[string]any)["items"].(map[string]any)
		}
		required := object["required"].([]string)
		expected := "review"
		if kind == "orchestrator" {
			expected = "acceptance"
		}
		found := false
		for _, name := range required {
			found = found || name == expected
		}
		if !found {
			t.Fatal("native schema missing optional key", kind, required)
		}
	}
	// The provider form must not alter the versioned generic contract.
	if contract.MainResultSchema() == nil {
		t.Fatal("generic main schema unavailable")
	}
}

func TestKernelSourceSelectsProductOutputVersionFromTrustedTaskRole(t *testing.T) {
	ctx := context.Background()
	k, err := storage.OpenKernel(filepath.Join(t.TempDir(), "controller", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.CreateOrchestrator(ctx, "main", "configured", t.TempDir(), "read-only"); err != nil {
		t.Fatal(err)
	}
	w, err := k.CreateProductWave(ctx, storage.ProductWaveRequest{Wave: storage.WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "product-output", OriginalRequest: "implement behavior", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "preserve behavior"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: domain.DefaultKernelLimits()}}, WorkItems: []storage.ProductWorkItemDefinition{{ID: "work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}, {ID: "other", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}})
	if err != nil {
		t.Fatal(err)
	}
	work := contract.TaskSpec{ID: "implementation", Kind: "work", Instruction: "implement behavior", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{"work"}}
	review := work
	review.ID, review.Kind, review.Instruction = "independent-review", "review", "independently review behavior"
	multi := work
	multi.ID, multi.WorkItemIDs = "multi-work", []string{"work", "other"}
	verify := work
	verify.ID, verify.Kind = "independent-verification", "verification"
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "implementation", Tasks: []contract.TaskSpec{work, review, multi, verify}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{work.ID, review.ID, multi.ID, verify.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
	receipt, err := k.AdoptPlan(ctx, w.Target(), "product-output-role", plan)
	if err != nil {
		t.Fatal(err)
	}
	source := NewKernelSource(k)
	for _, id := range receipt.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		b, err := k.SessionBinding(ctx, task.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		schema, validate, err := source.bindingContract(ctx, b)
		if err != nil || validate == nil {
			t.Fatal(err)
		}
		props := schema.(map[string]any)["properties"].(map[string]any)
		expected := 1
		if task.Spec.Kind == "work" && len(task.Spec.WorkItemIDs) == 1 {
			expected = 2
		}
		if task.Spec.Kind == "verification" && len(task.Spec.WorkItemIDs) == 1 {
			expected = 3
		}
		if props["schema_version"].(map[string]any)["const"] != expected {
			t.Fatal(task.Spec.Kind, props["schema_version"])
		}
		_, hasChanges := props["product_changes"]
		if hasChanges != (expected == 2 || expected == 3) {
			t.Fatal("product profile leaked across trusted roles", task.Spec.Kind, props)
		}
	}
}
