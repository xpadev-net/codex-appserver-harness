package storage

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func productMergeSuccessorFixture(t *testing.T, merged bool) (*KernelStore, domain.Wave, domain.Task, ProductMerge) {
	t.Helper()
	ctx := context.Background()
	k, job := knownCoverageFixture(t)
	if merged {
		if _, _, err := k.RecordProductMergedWorkItem(ctx, job.ID); err != nil {
			t.Fatal(err)
		}
	}
	w, err := k.Wave(ctx, job.Value.Product.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", ProductWorkItemDefinition{ID: "successor", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := k.DB.QueryRow("SELECT payload FROM kernel_plans WHERE wave_id=? AND revision=?", w.ID, w.PlanRevision).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var old PlanAdmission
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatal(err)
	}
	last := old.Plan.Phases[len(old.Plan.Phases)-1].Tasks[0]
	successor := contract.TaskSpec{ID: "successor-work", Kind: "work", Instruction: "implement after predecessor merge", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: last.ID}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{"successor"}}
	old.Plan.Phases = append(old.Plan.Phases, contract.PhaseSpec{ID: "successor-phase", Tasks: []contract.TaskSpec{successor}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{successor.ID}}, FailurePolicy: "drain_and_block"})
	admitted, err := k.AdoptPlan(ctx, w.Target(), "append-successor", old.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(admitted.CreatedTaskIDs) != 1 {
		t.Fatal(admitted)
	}
	task, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, task, job
}
func TestProductSuccessorWaitsForAuthenticatedMergeBeforePreparation(t *testing.T) {
	ctx := context.Background()
	k, w, task, job := productMergeSuccessorFixture(t, true)
	receipt, err := k.ReadProductMergedWorkItem(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merged_work_item' AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, task.ID); !errors.Is(err, ErrProductMergeDependencyHeld) {
		t.Fatal("completed predecessor allowed preparation without receipt", err)
	}
	var workspaces int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_workspace' AND json_extract(payload,'$.workspace.value.request.task_id')=?", task.ID).Scan(&workspaces); err != nil || workspaces != 0 {
		t.Fatal(workspaces, err)
	}
	if err := k.Put(ctx, "product_merged_work_item", job.ID, w.ID, receipt); err != nil {
		t.Fatal(err)
	}
	prepared, err := k.ReadProductTaskPreparation(ctx, task.ID)
	if err != nil || prepared.Task.ID != task.ID || prepared.Registration.Value.Definition.ID != "successor" {
		t.Fatal("known merge did not release successor", prepared, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.MainTurns != w.MainTurns || current.TasksCreated != w.TasksCreated || current.State == domain.WaveCompleted {
		t.Fatal("merge dependency spent model or completed parent", current, err)
	}
	policy, err := k.ReadProductVerificationPolicy(ctx, w.Target(), 17)
	if err != nil {
		t.Fatal(err)
	}
	base, err := workspace.Resolve(ctx, policy.Value.Repository, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	config := ProductModelRuntime{Model: binding.Model, ToolProfile: "product_v2", RuntimeDigest: strings.Repeat("a", 64), Repositories: []ProductModelRepository{{Name: "owner/repo", BaseBranch: "master", InitialSHA: base, Policy: policy.Value, Runner: ProductRunnerRuntime{Root: filepath.Join(k.Root, "successor-runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}}}}
	if err := k.RegisterIsolatedRuntime(config.RuntimeDigest, config.Model, config.ToolProfile); err != nil {
		t.Fatal(err)
	}
	revoke, err := k.BindProductModelRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	defer revoke()
	request := ProductWorkspaceRequest{TaskID: task.ID, RepositoryID: 17, Repository: policy.Value.Repository, BaseSHA: base}
	reserved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	// Loss of a receipt's immutable proof must immediately hold new preparation.
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merged_work_item_history' AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, task.ID); !errors.Is(err, ErrProductMergeDependencyHeld) {
		t.Fatal("lost merge proof allowed successor", err)
	}
	claim, err := k.ClaimProductWorkspace(ctx, task.ID, reserved.Digest)
	if !errors.Is(err, ErrProductMergeDependencyHeld) || claim.Start {
		t.Fatal("late lost proof admitted Git owner", claim, err)
	}
	set, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
	if err != nil || len(set.Tasks) != 0 || len(set.Held) != 1 || set.Held[0].Reason != "product_merge_dependency" {
		t.Fatal("model dispatch bypassed lost merge dependency", set, err)
	}
}

func TestProductMergeDependencyDoesNotRequireSameWorkItemToMerge(t *testing.T) {
	ctx := context.Background()
	k, job := knownCoverageFixture(t)
	w, err := k.Wave(ctx, job.Value.Product.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := k.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	var prior domain.Task
	for _, task := range tasks {
		if task.Spec.Kind == "review" {
			prior = task
		}
	}
	if prior.ID == "" {
		t.Fatal("missing completed review fixture")
	}
	for _, kind := range []string{"fix", "verification", "review"} {
		task := domain.Task{Spec: contract.TaskSpec{Kind: kind, WorkItemIDs: []string{job.Value.Product.Value.WorkItem.ID}, DependsOn: []contract.TaskDependency{{TaskID: prior.LocalID}}}}
		tx, err := k.kernelTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = k.productMergeDependenciesTx(ctx, tx, w, task)
		tx.Rollback()
		if err != nil {
			t.Fatal("same candidate workflow required merge", kind, err)
		}
	}
}

func TestProductSuccessorCompletedTasksWithoutMergeRemainHeld(t *testing.T) {
	ctx := context.Background()
	k, _, task, _ := productMergeSuccessorFixture(t, false)
	if _, err := k.ReadProductTaskPreparation(ctx, task.ID); !errors.Is(err, ErrProductMergeDependencyHeld) {
		t.Fatal("Task completion/native response without authenticated projection released successor", err)
	}
}
