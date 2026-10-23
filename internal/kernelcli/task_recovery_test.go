package kernelcli

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestKernelCLIAuthorizesExactTaskRetryAndRejectsStaleDecision(t *testing.T) {
	root, w := controlWave(t)
	ctx := context.Background()
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	spec := contract.TaskSpec{ID: "work", Kind: "work", Instruction: "research", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{}}
	plan := contract.PlanProposal{SchemaVersion: 1, Templates: []contract.FixReviewTemplate{}, Phases: []contract.PhaseSpec{{ID: "work", Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"}}}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	// Controlled persisted input blocker before any model launch. Production
	// runtime-to-blocked and exact same-thread retry are covered in control tests.
	task.State = contract.TaskBlocked
	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.ExecContext(ctx, "UPDATE kernel_tasks SET state=?,payload=? WHERE id=?", task.State, raw, task.ID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, task.SessionID, []contract.Target{task.Target()})
	if err != nil {
		t.Fatal(err)
	}
	request := map[string]any{"expected": task, "run_id": "retry-one", "snapshot": snapshot, "reason": "operator resolved persisted input blocker"}
	file := operatorFile(t, request)
	first, err := cliCall(t, "retry-task", "--root", root, "--file", file)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := cliCall(t, "retry-task", "--root", root, "--file", file)
	if err != nil || !bytes.Equal(first, replay) {
		t.Fatal(string(replay), err)
	}
	pending, err := k.PendingTaskRecovery(ctx, task.ID)
	if err != nil || pending == nil || pending.RunID != "retry-one" || pending.SnapshotID != snapshot.ID {
		t.Fatal(pending, err)
	}
	current, err := k.Task(ctx, task.ID)
	if err != nil || current.Attempts != 0 || current.State != contract.TaskBlocked {
		t.Fatal("authorization launched retry", current, err)
	}
	request["reason"] = "conflicting retry"
	if _, err = cliCall(t, "retry-task", "--root", root, "--file", operatorFile(t, request)); err == nil {
		t.Fatal("conflicting decision accepted")
	}
	request["run_id"] = "stale-retry"
	task.Attempts++
	request["expected"] = task
	if _, err = cliCall(t, "retry-task", "--root", root, "--file", operatorFile(t, request)); err == nil {
		t.Fatal("stale Task accepted")
	}
}
