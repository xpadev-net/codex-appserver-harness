// Run from the repository root to regenerate the versioned kernel schemas.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func main() {
	documents := map[string]any{"main-result-v1.json": contract.MainResultSchema(), "task-result-v1.json": contract.TaskResultSchema(), "product-task-result-v2.json": contract.ProductTaskResultSchema(), "wave-plan-v1.json": contract.PlanSchema(), "kernel-tools-v1.json": map[string]any{"schema_version": 1, "orchestrator": contract.KernelTools("orchestrator"), "task": contract.KernelTools("task")}}
	for name, document := range documents {
		b, err := json.MarshalIndent(document, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join("schemas/handoffs", name), append(b, '\n'), 0644)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
