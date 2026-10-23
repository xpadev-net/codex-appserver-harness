package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestFixedProductInputStillChecksArtifactAndDependencyEvidence(t *testing.T) {
	for _, mode := range []string{"artifact", "dependency"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, request := productWorkspaceFixture(t)
			artifact := mustImport(t, k, ArtifactImport{SessionID: w.OrchestratorSessionID, Target: w.Target(), Key: "fixed-input", Kind: "note", ContentType: "text/plain; charset=utf-8", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "trusted host"}}, "original input")
			task.Spec.InputArtifactIDs = []string{artifact.ID}
			task.Spec.DependsOn = []contract.TaskDependency{{TaskID: "review"}}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			task.InputDigest, err = k.taskInputDigestTx(ctx, tx, w, task.Spec)
			if err == nil {
				err = saveTask(ctx, tx, task)
			}
			if err == nil {
				_, err = tx.Exec("UPDATE kernel_plan_bindings SET input_digest=? WHERE task_id=?", task.InputDigest, task.ID)
			}
			if err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			saved, err := k.ReserveProductWorkspace(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
				t.Fatal(err)
			}
			if _, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved)); err != nil {
				t.Fatal(err)
			}
			if _, err := productTaskDigest(t, k, w, task.Spec); err != nil {
				t.Fatal(err)
			}
			if mode == "artifact" {
				if err := os.WriteFile(filepath.Join(k.Root, "objects", artifact.ID), []byte("changed bytes"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				prior, err := loadTask(ctx, tx, structureID("task-", w.ID, "review"))
				if err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				prior.State, prior.ResultID = contract.TaskCompleted, "unproven-result"
				if err := saveTask(ctx, tx, prior); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := productTaskDigest(t, k, w, task.Spec); err == nil {
				t.Fatal("fixed workspace bypassed changed input evidence", mode)
			}
		})
	}
}

func productTaskDigest(t *testing.T, k *KernelStore, w domain.Wave, spec contract.TaskSpec) (string, error) {
	t.Helper()
	tx, err := k.kernelTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	return k.taskInputDigestTx(context.Background(), tx, w, spec)
}

func TestFixedProductInputSurvivesCandidateAdvanceAndReopen(t *testing.T) {
	for _, kind := range []string{"work", "fix"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, request := productWorkspaceFixture(t)
			if kind == "fix" {
				task.Spec.Kind = kind
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				task.InputDigest, err = k.taskInputDigestTx(ctx, tx, w, task.Spec)
				if err == nil {
					err = saveTask(ctx, tx, task)
				}
				if err == nil {
					_, err = tx.Exec("UPDATE kernel_plan_bindings SET input_digest=? WHERE task_id=?", task.InputDigest, task.ID)
				}
				if err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			original := task.InputDigest
			saved, err := k.ReserveProductWorkspace(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
				t.Fatal(err)
			}
			proof := workspaceProofFixture(t, k, saved)
			ready, err := k.FinishProductWorkspace(ctx, task.ID, proof)
			if err != nil {
				t.Fatal(err)
			}
			task, err = k.Task(ctx, task.ID)
			if err != nil || task.InputDigest == original || ready.Value.InputDigest != original || task.Attempts != 0 {
				t.Fatal(task, ready, err)
			}
			var bound string
			if err := k.DB.QueryRow("SELECT input_digest FROM kernel_plan_bindings WHERE task_id=? AND revision=?", task.ID, w.PlanRevision).Scan(&bound); err != nil || bound != task.InputDigest {
				t.Fatal(bound, task.InputDigest, err)
			}
			manifest := registrationManifest(t, k, task.SessionID, w.Target())
			if len(manifest.Tasks) != 1 || manifest.Tasks[0].InputDigest != bound {
				t.Fatal(manifest.Tasks)
			}
			review := task.Spec
			review.Kind = "review"
			review.ID = "unbound-review"
			beforeReview, err := productTaskDigest(t, k, w, review)
			if err != nil {
				t.Fatal(err)
			}
			if err := k.Put(ctx, "wave_work_item", "work", w.ID, map[string]any{"target": w.Target(), "candidate_id": "new-candidate"}); err != nil {
				t.Fatal(err)
			}
			got, err := productTaskDigest(t, k, w, task.Spec)
			if err != nil || got != bound {
				t.Fatal("own candidate invalidated fixed input", got, bound, err)
			}
			afterReview, err := productTaskDigest(t, k, w, review)
			if err != nil || afterReview == beforeReview {
				t.Fatal("legacy review input was frozen", err)
			}
			root := k.Root
			k.Close()
			k, err = OpenExistingKernel(filepath.Join(root, "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			again, err := k.FinishProductWorkspace(ctx, task.ID, proof)
			if err != nil || !reflect.DeepEqual(again, ready) {
				t.Fatal(again, err)
			}
			got, err = productTaskDigest(t, k, w, task.Spec)
			if err != nil || got != bound {
				t.Fatal(got, bound, err)
			}
		})
	}
}

func TestFixedProductInputProofLossCannotFallBackToMutableCandidate(t *testing.T) {
	for _, mode := range []string{"missing_head", "corrupt_head", "missing_history", "corrupt_history", "registration", "scope", "policy", "membership_scope"} {
		t.Run(mode, func(t *testing.T) {
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
			switch mode {
			case "missing_head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_workspace'")
			case "corrupt_head":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_workspace'")
			case "missing_history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_workspace_history'")
			case "corrupt_history":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_workspace_history'")
			case "registration":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_work_item_registration'")
			case "scope":
				w.ScopeRevision++
			case "policy":
				w.Policy.Revision++
			case "membership_scope":
				target := w.Target()
				target.ScopeRevision++
				err = k.Put(ctx, "wave_work_item", "work", w.ID, map[string]any{"target": target, "candidate_id": "other"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := productTaskDigest(t, k, w, task.Spec); !errors.Is(err, ErrTaskEvidence) {
				t.Fatal("lost proof accepted", err)
			}
		})
	}
}

func TestFixedProductInputBindingFailureRollsBackWorkspaceAndPermissions(t *testing.T) {
	ctx := context.Background()
	k, _, task, request := productWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_fixed_input BEFORE UPDATE ON kernel_plan_bindings BEGIN SELECT RAISE(FAIL,'fixed input rollback'); END"); err != nil {
		t.Fatal(err)
	}
	proof := workspaceProofFixture(t, k, saved)
	if _, err := k.FinishProductWorkspace(ctx, task.ID, proof); err == nil || !strings.Contains(err.Error(), "fixed input rollback") {
		t.Fatal(err)
	}
	current, err := k.ProductWorkspace(ctx, task.ID)
	if err != nil || current.State != "launching" || current.Proof != nil {
		t.Fatal(current, err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "read-only" {
		t.Fatal(b, err)
	}
	reservation, err := k.OperationReservation(ctx, saved.ID)
	if err != nil || reservation.State != "held" {
		t.Fatal(reservation, err)
	}
	unchanged, err := k.Task(ctx, task.ID)
	if err != nil || unchanged.InputDigest != task.InputDigest {
		t.Fatal(unchanged, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_fixed_input"); err != nil {
		t.Fatal(err)
	}
	if ready, err := k.FinishProductWorkspace(ctx, task.ID, proof); err != nil || ready.State != "ready" {
		t.Fatal(ready, err)
	}
}
