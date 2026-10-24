package contract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProductBaseConflictPlanReferenceIsOptionalAndBounded(t *testing.T) {
	plan := planFixture()
	encoded, err := json.Marshal(plan)
	if err != nil || strings.Contains(string(encoded), "product_base_conflict") {
		t.Fatal("legacy plan encoding changed", string(encoded), err)
	}
	ref := ProductBaseConflictReference{TaskID: "old-task", WorkspaceID: "old-workspace", WorkspaceDigest: strings.Repeat("a", 64), ProofDigest: strings.Repeat("b", 64)}
	for _, mode := range []string{"valid", "work", "no_work_item", "task", "workspace", "digest", "proof"} {
		t.Run(mode, func(t *testing.T) {
			p := planFixture()
			r := ref
			task := &p.Phases[0].Tasks[0]
			task.Kind, task.WorkItemIDs, task.ProductBaseConflict = "fix", []string{"product"}, &r
			switch mode {
			case "work":
				task.Kind = "work"
			case "no_work_item":
				task.WorkItemIDs = nil
			case "task":
				r.TaskID = ""
			case "workspace":
				r.WorkspaceID = "bad\nworkspace"
			case "digest":
				r.WorkspaceDigest = strings.Repeat("z", 64)
			case "proof":
				r.ProofDigest = "missing"
			}
			if err := p.Validate(); (mode == "valid") != (err == nil) {
				t.Fatal(mode, err)
			}
		})
	}
}
