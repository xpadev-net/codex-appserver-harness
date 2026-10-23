package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func readyProductWorkspaceFixture(t *testing.T) (*KernelStore, ProductWorkspace) {
	t.Helper()
	ctx := context.Background()
	k, _, task, request := productWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	saved, err = k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved))
	if err != nil {
		t.Fatal(err)
	}
	return k, saved
}
func TestProductChangeInputIsTaskLocalAndCarriesOnlyItsGrantedWorkspace(t *testing.T) {
	ctx := context.Background()
	k, saved := readyProductWorkspaceFixture(t)
	w, err := k.Wave(ctx, saved.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	own := registrationManifest(t, k, saved.Value.SessionID, w.Target())
	expected := contract.ProductChangeReference{WorkItemID: saved.Value.WorkItemID, WorkspaceID: saved.ID, WorkspaceDigest: saved.Digest}
	if own.ProductWorkspace == nil || own.ProductWorkspace.Reference != expected || own.ProductWorkspace.BaseSHA != saved.Value.Request.BaseSHA || own.ProductWorkspace.Branch != saved.Value.Branch {
		t.Fatal(own.ProductWorkspace)
	}
	parent := registrationManifest(t, k, w.OrchestratorSessionID, w.Target())
	if parent.ProductWorkspace != nil {
		t.Fatal("parent received Task workspace output identity", parent.ProductWorkspace)
	}
	tasks, err := k.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.Spec.Kind == "review" {
			view := registrationManifest(t, k, task.SessionID, w.Target())
			if view.ProductWorkspace != nil {
				t.Fatal("review role received implementation output identity", view.ProductWorkspace)
			}
		}
	}
	snapshot, err := k.BuildSessionInput(ctx, saved.Value.SessionID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(inputContext(t, snapshot).Instructions, "schema_version=2") || !strings.Contains(inputContext(t, snapshot).Instructions, "controller owns Git commits") {
		t.Fatal("product output contract missing from saved instructions")
	}
	if _, err := k.DB.Exec("UPDATE records SET payload='broken registration' WHERE kind='product_work_item_registration'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.BuildSessionInput(ctx, saved.Value.SessionID, []contract.Target{w.Target()}); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("corrupt source evidence produced a new product input", err)
	}
}
func TestProductChangeReferenceRequiresCurrentOwnedWorkspaceWithoutGrantingCandidate(t *testing.T) {
	ctx := context.Background()
	k, saved := readyProductWorkspaceFixture(t)
	w, err := k.Wave(ctx, saved.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, saved.Value.Request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	ref := contract.ProductChangeReference{WorkItemID: saved.Value.WorkItemID, WorkspaceID: saved.ID, WorkspaceDigest: saved.Digest}
	for _, mode := range []string{"valid", "peer_workspace", "foreign_work", "digest", "parent"} {
		t.Run(mode, func(t *testing.T) {
			change, actor := ref, b
			switch mode {
			case "peer_workspace":
				change.WorkspaceID = productWorkspaceID("peer-task")
			case "foreign_work":
				change.WorkItemID = "peer-work"
			case "digest":
				change.WorkspaceDigest = strings.Repeat("f", 64)
			case "parent":
				actor.ID = w.OrchestratorSessionID
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			ready, err := k.productChangeReferenceTx(ctx, tx, actor, w, task, change)
			if mode == "valid" {
				if err != nil || !ready {
					t.Fatal(ready, err)
				}
			} else if !errors.Is(err, ErrProductChangeReference) {
				t.Fatal("unowned reference accepted", mode, ready, err)
			}
		})
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := k.productChangeReferenceTx(canceled, tx, b, w, task, ref); !errors.Is(err, context.Canceled) || errors.Is(err, ErrProductChangeReference) {
		tx.Rollback()
		t.Fatal("operational failure became invalid model data", err)
	}
	tx.Rollback()
	var runs, candidates int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal(runs, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_target','product_candidate')").Scan(&candidates); err != nil || candidates != 0 {
		t.Fatal("change declaration minted candidate", candidates, err)
	}
}
func TestProductChangeSchemaCannotReclassifyGenericKRun(t *testing.T) {
	ctx := context.Background()
	k, b, run, _ := recordedTaskResultFixture(t, func(result *contract.TaskResult) {
		result.SchemaVersion = 2
		result.WorkItemRefs = []contract.CandidateReference{}
		result.ProductChanges = []contract.ProductChangeReference{{WorkItemID: "foreign-product", WorkspaceID: "unowned", WorkspaceDigest: strings.Repeat("a", 64)}}
	})
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	result, err := k.ApplyTaskResult(ctx, b.ID, run.ID)
	if err != nil || result.State != contract.TaskFailed || !strings.Contains(result.Reason, "trusted P work/fix") {
		t.Fatal("generic run self-classified as a product output", result, err)
	}
}
