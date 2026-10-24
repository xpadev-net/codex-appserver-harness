package product

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func TestProductInvalidStoppedResultSettlesTaskAndNotifiesWithoutGit(t *testing.T) {
	for _, mode := range []string{"json", "schema", "task", "phase", "target", "foreign_criterion", "missing_criterion", "workspace", "workspace_digest", "wrong_profile"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			preparer, request, _, task := workspacePreparerFixture(t)
			ready, err := preparer.Prepare(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			k := preparer.Store
			expected := contract.TaskFailed
			task, run := stoppedProductWriterRunFixture(t, k, ready, task, func(run *contract.SessionRun) {
				var result contract.TaskResult
				if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "schema":
					result.SchemaVersion = 99
				case "task":
					result.TaskID = "foreign-task"
				case "phase":
					result.PhaseID = "foreign-phase"
				case "target":
					result.Target.PlanRevision++
				case "foreign_criterion":
					result.CriterionResults[0].CriterionID = "foreign"
				case "missing_criterion":
					result.CriterionResults = nil
				case "workspace":
					result.ProductChanges[0].WorkspaceID = "foreign-workspace"
				case "workspace_digest":
					result.ProductChanges[0].WorkspaceDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				case "wrong_profile":
					a, err := k.ImportArtifact(ctx, storage.ArtifactImport{SessionID: task.SessionID, Target: task.Target(), Key: "generic-output", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: task.ID}}, []byte("generic artifact cannot stand in for a product candidate"))
					if err != nil {
						t.Fatal(err)
					}
					result.SchemaVersion, result.ProductChanges, result.Artifacts = 1, nil, []string{a.ID}
					expected = contract.TaskBlocked
				}
				raw, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				run.Final = string(raw)
				if mode == "json" {
					run.Final = "{invalid JSON"
				}
			})
			head, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD")
			if err != nil {
				t.Fatal(err)
			}
			index, err := workspace.Git(ctx, ready.Value.CWD, "diff", "--cached", "--binary")
			if err != nil {
				t.Fatal(err)
			}
			repo := executorRepositoryFixture(ready.Value.Request)
			e := Executor{Store: k, Repositories: map[int64]ExecutionRepository{repo.Policy.RepositoryID: repo}}
			worker := NewExecutionWorker(e, time.Second)
			_, applyErr := worker.task(ctx, task.ID)
			if expected == contract.TaskFailed && applyErr != nil || expected == contract.TaskBlocked && !errors.Is(applyErr, storage.ErrTaskResultReferences) {
				t.Fatal("unexpected result settlement", applyErr)
			}
			current, err := k.Task(ctx, task.ID)
			if err != nil || current.State != expected || current.ID != task.ID || current.SessionID != task.SessionID || current.Attempts != task.Attempts || current.ResultID != "" {
				t.Fatal("invalid result did not settle original Task", current, err)
			}
			if expected == contract.TaskFailed {
				first, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID)
				if err != nil || first.State != expected {
					t.Fatal(first, err)
				}
				if again, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID); err != nil || !reflect.DeepEqual(first, again) {
					t.Fatal("failure replay changed receipt", again, err)
				}
			}
			if _, err := k.ReconcileSessionTemplates(ctx, task.ParentSessionID); err != nil {
				t.Fatal(err)
			}
			all, err := k.PendingWakeNotifications(ctx, task.ParentSessionID, 128)
			if err != nil {
				t.Fatal(err)
			}
			var notices []storage.Notification
			for _, n := range all {
				if n.TaskID == task.ID && n.Kind == "parent_attention" {
					notices = append(notices, n)
				}
			}
			if len(notices) != 1 || notices[0].TaskState != expected {
				t.Fatal("parent has no bounded exception", notices)
			}
			if err := k.AcknowledgeNotification(ctx, notices[0].SessionID, notices[0].ID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReconcileSessionTemplates(ctx, task.ParentSessionID); err != nil {
				t.Fatal(err)
			}
			after, err := k.PendingWakeNotifications(ctx, task.ParentSessionID, 128)
			if err != nil || len(after) != len(all) {
				t.Fatal("poll/ACK repeated parent cause", after, err)
			}
			afterHead, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD")
			if err != nil || afterHead != head {
				t.Fatal("invalid result committed", afterHead, err)
			}
			afterIndex, err := workspace.Git(ctx, ready.Value.CWD, "diff", "--cached", "--binary")
			if err != nil || afterIndex != index {
				t.Fatal("invalid result staged files", afterIndex, err)
			}
			var sealed, models int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_seal','product_candidate','product_task_candidate')").Scan(&sealed); err != nil || sealed != 0 {
				t.Fatal("invalid output admitted Git/candidate", sealed, err)
			}
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&models); err != nil || models != 1 {
				t.Fatal("invalid output restarted model", models, err)
			}
		})
	}
}
