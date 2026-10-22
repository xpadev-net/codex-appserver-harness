package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func parentFixPlan(batches, parallel int, prefix string) contract.PlanProposal {
	p := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{}, Templates: []contract.FixReviewTemplate{}}
	for i := 0; i < batches; i++ {
		phase := contract.PhaseSpec{ID: fmt.Sprintf("%s-phase-%d", prefix, i), Tasks: []contract.TaskSpec{}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{}}, FailurePolicy: "drain_and_block"}
		for j := 0; j < parallel; j++ {
			id := fmt.Sprintf("%s-fix-%d-%d", prefix, i, j)
			phase.Tasks = append(phase.Tasks, contract.TaskSpec{ID: id, Kind: "fix", Instruction: "fix the observed defect", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}})
			phase.Completion.RequiredTaskIDs = append(phase.Completion.RequiredTaskIDs, id)
		}
		p.Phases = append(p.Phases, phase)
	}
	return p
}
func TestParentFixPlanReservesParallelBatchesAndReplaysAfterRestart(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.Policy.Limits.FixRounds = 2
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	p := parentFixPlan(2, 2, "initial")
	receipt, err := k.AdoptPlan(ctx, w.Target(), "parent-fix", p)
	if err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.FixRounds != 2 || current.TasksCreated != 4 || current.Replans != 0 {
		t.Fatal(current, err)
	}
	var charged int
	if err = k.DB.QueryRowContext(ctx, "SELECT coalesce(sum(amount),0) FROM kernel_budget_ledger WHERE wave_id=? AND kind='fix_round'", w.ID).Scan(&charged); err != nil || charged != 2 {
		t.Fatal(charged, err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.AdoptPlan(ctx, w.Target(), "parent-fix", p)
	if err != nil || !reflect.DeepEqual(receipt, replay) {
		t.Fatal(replay, err)
	}
	if _, err = k.AdoptPlan(ctx, current.Target(), "extra-fix", parentFixPlan(1, 1, "next")); !errors.Is(err, ErrPlanAdmission) {
		t.Fatal("exhausted fix budget admitted", err)
	}
	unchanged, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(current, unchanged) {
		t.Fatal(unchanged, err)
	}
	tasks, err := k.Tasks(ctx, w.ID)
	if err != nil || len(tasks) != 4 {
		t.Fatal(tasks, err)
	}
	for _, task := range tasks {
		if task.State != contract.TaskReady {
			t.Fatal("rejected plan retired old task", task)
		}
	}
}
func TestParentFixBudgetSharesTemplateChargesAndAuditRollback(t *testing.T) {
	for _, mode := range []string{"template_consumed", "audit_failure", "replan_ledger"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, request := waveStoreFixture(t)
			request.Policy.Limits.FixRounds = 1
			w, err := k.CreateWave(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture()); err != nil {
				t.Fatal(err)
			}
			w, err = k.Wave(ctx, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "template_consumed" {
				// Existing template reservations update this same cumulative Wave value.
				w.FixRounds = 1
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err = saveWave(ctx, tx, w); err != nil {
					t.Fatal(err)
				}
				if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,'template:fixture','fix_round',1,0,'{}')", w.ID); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "audit_failure" {
				if _, err = k.DB.ExecContext(ctx, `CREATE TRIGGER fail_parent_fix_charge BEFORE INSERT ON kernel_budget_ledger WHEN NEW.kind='fix_round' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
					t.Fatal(err)
				}
			}
			_, err = k.AdoptPlan(ctx, w.Target(), "fix", parentFixPlan(1, 1, "next"))
			if mode == "replan_ledger" {
				if err != nil {
					t.Fatal(err)
				}
				var fixes, replans int
				if err = k.DB.QueryRowContext(ctx, "SELECT coalesce(sum(CASE WHEN kind='fix_round' THEN amount ELSE 0 END),0),coalesce(sum(CASE WHEN kind='replan' THEN amount ELSE 0 END),0) FROM kernel_budget_ledger WHERE wave_id=?", w.ID).Scan(&fixes, &replans); err != nil || fixes != 1 || replans != 1 {
					t.Fatal(fixes, replans, err)
				}
			} else {
				if err == nil {
					t.Fatal("unsafe plan admitted")
				}
				actual, err := k.Wave(ctx, w.ID)
				if err != nil || !reflect.DeepEqual(actual, w) {
					t.Fatal(actual, err)
				}
				tasks, err := k.Tasks(ctx, w.ID)
				if err != nil || len(tasks) != 2 {
					t.Fatal(tasks, err)
				}
				for _, task := range tasks {
					if task.State != contract.TaskReady {
						t.Fatal(task)
					}
				}
			}
		})
	}
}

func TestCompletedFixCarryDoesNotReserveAnotherRound(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.Policy.Limits.FixRounds = 1
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	p := parentFixPlan(1, 1, "first")
	first, err := k.AdoptPlan(ctx, w.Target(), "initial-fix", p)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, first.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	completeTemplateTask(t, k, task, "fixed artifact", "", nil)
	replay, err := k.AdoptPlan(ctx, first.Target, "carry-fix", p)
	if err != nil || len(replay.CarriedTaskIDs) != 1 || len(replay.CreatedTaskIDs) != 0 {
		t.Fatal(replay, err)
	}
	actual, err := k.Wave(ctx, w.ID)
	if err != nil || actual.FixRounds != 1 || actual.Replans != 1 {
		t.Fatal(actual, err)
	}
}
func TestAutomaticAndParentFixPlansUseOneCumulativeCounter(t *testing.T) {
	ctx := context.Background()
	k, w, review, _ := templateFixture(t, false, []contract.FixReviewTemplate{kernelTemplate()}, []string{"artifact_defect"}, func(p *contract.PlanProposal) { p.Phases[0].Tasks[0].Kind = "fix" })
	before, err := k.Wave(ctx, w.ID)
	if err != nil || before.FixRounds != 1 {
		t.Fatal(before, err)
	}
	expansion, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID})
	if err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.FixRounds != 2 || current.Replans != 0 {
		t.Fatal(current, err)
	}
	if _, err = k.ExpandReviewTemplates(ctx, expansion.Request); err != nil {
		t.Fatal(err)
	}
	var charges int
	if err = k.DB.QueryRowContext(ctx, "SELECT sum(amount) FROM kernel_budget_ledger WHERE wave_id=? AND kind='fix_round'", w.ID).Scan(&charges); err != nil || charges != 2 {
		t.Fatal(charges, err)
	}
}
