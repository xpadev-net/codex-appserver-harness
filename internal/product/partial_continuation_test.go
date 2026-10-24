package product

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Real local quality and the registered HTTP child are exercised upstream;
// native approval, CI/rules and the remote merge response remain fixtures.
func checkPartialMergeContinuation(t *testing.T, e Executor, g KernelMergeGate, saved storage.ProductTargetSnapshot, requestID string) {
	t.Helper()
	ctx := context.Background()
	k := e.Store
	tasks, err := k.Tasks(ctx, saved.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	var successor domain.Task
	for _, task := range tasks {
		if task.Spec.ID == "remaining-work" {
			successor = task
		}
	}
	if successor.ID == "" || successor.SessionID == "" || successor.Attempts != 0 || successor.State != contract.TaskReady {
		t.Fatal("fresh continuation Task missing", successor)
	}
	for _, task := range tasks {
		if task.ID != successor.ID && task.SessionID == successor.SessionID {
			t.Fatal("continuation reused an earlier session", task)
		}
	}
	before, err := k.Wave(ctx, saved.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, successor.ID); !errors.Is(err, storage.ErrProductMergeDependencyHeld) {
		t.Fatal("completed review bypassed merge dependency", err)
	}
	if _, err := k.ProductWorkspace(ctx, successor.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("continuation prepared Git before merge", err)
	}
	job, err := k.PrepareProductMergeHTTP(ctx, requestID, "kernel-http-owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	checkMergeHTTPWrapper(t, g, saved, requestID, "success")
	merged, err := k.ReadProductMerge(ctx, job.ID)
	if err != nil || merged.State != "merged" || merged.HTTPStop == nil {
		t.Fatal("native response lacked a stopped owner", merged, err)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, successor.ID); !errors.Is(err, storage.ErrProductMergeDependencyHeld) {
		t.Fatal("unrecorded native response released continuation", err)
	}
	receipt, changed, err := k.RecordProductMergedWorkItem(ctx, job.ID)
	if err != nil || !changed || receipt.Value.Projection == nil {
		t.Fatal(receipt, changed, err)
	}
	if again, changed, err := k.RecordProductMergedWorkItem(ctx, job.ID); err != nil || changed || !reflect.DeepEqual(again, receipt) {
		t.Fatal("merge recording replay changed facts", again, changed, err)
	}
	coverage, err := k.ProductCoverage(ctx, before.Target())
	if err != nil || len(coverage.Value.Merged) != 1 || !reflect.DeepEqual(coverage.Value.Residual, []string{"extra"}) || !reflect.DeepEqual(coverage.Value.UnmergedWorkItems, []string{"remaining"}) {
		t.Fatal("partial merge lost residual work", coverage, err)
	}
	prepared, err := k.ReadProductTaskPreparation(ctx, successor.ID)
	if err != nil || prepared.Task.ID != successor.ID || prepared.Task.SessionID != successor.SessionID || prepared.Registration.Value.Definition.ID != "remaining" {
		t.Fatal("known partial merge did not release preparation", prepared, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merged_work_item' AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, successor.ID); !errors.Is(err, storage.ErrProductMergeDependencyHeld) {
		t.Fatal("lost receipt authorized continuation", err)
	}
	if err := k.Put(ctx, "product_merged_work_item", job.ID, before.ID, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, successor.ID); err != nil {
		t.Fatal(err)
	}
	after, err := k.Wave(ctx, before.ID)
	if err != nil || after.State == domain.WaveCompleted || after.MainTurns != before.MainTurns || after.Replans != before.Replans || after.TasksCreated != before.TasksCreated || after.FixRounds != before.FixRounds || after.Deadline != before.Deadline {
		t.Fatal("partial merge accepted parent or reset budgets", after, before, err)
	}
	afterBudget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil || !reflect.DeepEqual(afterBudget, budget) {
		t.Fatal("merge/continuation changed operation budget", afterBudget, budget, err)
	}
	current, err := k.Task(ctx, successor.ID)
	if err != nil || !reflect.DeepEqual(current, successor) {
		t.Fatal("metadata preparation restarted continuation", current, err)
	}
	if _, err := k.ProductWorkspace(ctx, successor.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("read-only preparation created checkout", err)
	}
}
