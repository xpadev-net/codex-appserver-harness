package control

import (
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
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
