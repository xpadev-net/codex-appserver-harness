package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestWaveInspectionPagesHistoryAndSummarizesRunsWithoutBodies(t *testing.T) {
	ctx := context.Background()
	k, w, _, _, _ := acceptanceFixture(t, nil)
	next := ""
	phases := ""
	tasks := map[string]bool{}
	phaseIDs := map[string]bool{}
	for i := 0; i < 2; i++ {
		view, err := k.InspectWave(ctx, w.ID, next, phases, 1)
		if err != nil || len(view.Tasks) != 1 || len(view.Phases) != 1 {
			t.Fatal(view, err)
		}
		task := view.Tasks[0]
		if tasks[task.Task.ID] || !task.Current || task.Session.LastRun == nil || task.Session.LastRun.FinalBytes <= 0 || task.Session.LastRun.Budget == nil || len(task.Artifacts) == 0 {
			t.Fatal(task)
		}
		tasks[task.Task.ID] = true
		phaseIDs[view.Phases[0].ID] = true
		next = view.NextTaskAfter
		phases = view.NextPhaseAfter
		raw, err := json.Marshal(view)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), `"final":`) || strings.Contains(string(raw), `"criterion_results":`) {
			t.Fatal("inspection copied raw model result")
		}
	}
	if next != "" || phases != "" || len(tasks) != 2 || len(phaseIDs) != 2 {
		t.Fatal(next, phases, tasks, phaseIDs)
	}
	var attempts, mainTurns int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings").Scan(&attempts); err != nil || attempts != 2 {
		t.Fatal(attempts, err)
	}
	if err := k.DB.QueryRowContext(ctx, "SELECT coalesce(sum(amount),0) FROM kernel_budget_ledger WHERE kind='main_turn'").Scan(&mainTurns); err != nil || mainTurns != 0 {
		t.Fatal(mainTurns, err)
	}
}
