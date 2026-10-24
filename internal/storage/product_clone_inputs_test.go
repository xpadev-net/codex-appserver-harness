package storage

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductTemplateFutureWriterCapturesRepairedCandidate(t *testing.T) {
	for _, kind := range []string{"work", "fix"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, w, original, final, clone, _ := productFutureWriterFixture(t, kind, true)
			before := clone.InputDigest
			finishProductRepairWorkFixture(t, k, w, original, clone, strings.Repeat("9", 40))
			clone, err := k.Task(ctx, clone.ID)
			if err != nil || clone.State != contract.TaskCompleted || clone.InputDigest == before {
				t.Fatal("cloned writer did not capture and apply the repaired input", clone, err)
			}
			workspace, err := k.ProductWorkspace(ctx, clone.ID)
			if err != nil || workspace.Value.Request.BaseSHA != final.Value.Candidate.SHA {
				t.Fatal("cloned writer used superseded Candidate", workspace, err)
			}
			replayed, err := k.ReserveProductWorkspace(ctx, workspace.Value.Request)
			if err != nil || !reflect.DeepEqual(workspace, replayed) {
				t.Fatal("workspace replay changed the frozen input", replayed, err)
			}
		})
	}
}

func productFutureWriterFixture(t *testing.T, kind string, completeReviews bool) (*KernelStore, domain.Wave, ProductVerificationPlanSnapshot, ProductVerificationPlanSnapshot, domain.Task, TemplateExpansion) {
	t.Helper()
	ctx := context.Background()
	k, w, original, runtime, review := productTemplateFixture(t, "implementation_defect", func(plan *contract.PlanProposal) {
		future := contract.TaskSpec{ID: "future-writer", Kind: kind, Instruction: "continue against the independently reviewed Candidate", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: plan.Phases[1].Tasks[0].ID}, {TaskID: "quality-review"}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{"work"}}
		plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: "future-write", Tasks: []contract.TaskSpec{future}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{future.ID}}, FailurePolicy: "drain_and_block"})
	})
	expansion, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID})
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	fix, err := k.Task(ctx, structureID("task-", w.ID, expansion.Product.Repairs[0]))
	if err != nil {
		t.Fatal(err)
	}
	finishProductRepairWorkFixture(t, k, w, original, fix, strings.Repeat("e", 40))
	verifier, err := k.Task(ctx, structureID("task-", w.ID, expansion.Product.Verifier))
	if err != nil {
		t.Fatal(err)
	}
	final := finishProductRepairVerifierFixture(t, k, w, original, verifier)
	completeProductRunnerFixture(t, k, final, runtime, "unit", "passed")
	for _, id := range expansion.Product.Replacements[review.ID] {
		if !completeReviews {
			continue
		}
		next, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		next, run := seedProductQualityReview(t, k, w, final, next)
		if _, err := k.ApplyTaskResult(ctx, next.SessionID, run.ID); err != nil {
			t.Fatal(err)
		}
	}
	clone, err := k.Task(ctx, expansion.Product.FutureClones[structureID("task-", w.ID, "future-writer")])
	if err != nil {
		t.Fatal(err)
	}
	return k, w, original, final, clone, expansion
}

func TestProductTemplateFutureWriterRejectsStaleOrUnprovenInput(t *testing.T) {
	for _, mode := range []string{"old_base", "missing_history", "missing_clone", "wrong_clone", "used_thread", "pending_review"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, original, final, clone, expansion := productFutureWriterFixture(t, "work", mode != "pending_review")
			sealed, err := k.ProductSeal(ctx, original.Value.TaskID, original.Value.RunID)
			if err != nil {
				t.Fatal(err)
			}
			request := sealed.Value.Workspace.Value.Request
			request.TaskID, request.BaseSHA = clone.ID, final.Value.Candidate.SHA
			switch mode {
			case "old_base":
				request.BaseSHA = original.Value.Candidate.SHA
			case "missing_history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_template_expansion_history' AND id=?", expansion.Key)
			case "missing_clone":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='template_future_clone' AND id=?", clone.ID)
			case "wrong_clone":
				tx, e := k.kernelTx(ctx)
				if e != nil {
					t.Fatal(e)
				}
				err = kernelRecordTx(ctx, tx, "template_future_clone", clone.ID, w.ID, templateTaskClone{OriginalID: "unrelated", ExpansionKey: expansion.Key})
				if err == nil {
					err = tx.Commit()
				} else {
					tx.Rollback()
				}
			case "used_thread":
				_, err = k.DB.Exec("UPDATE kernel_sessions SET thread_id='already-used-future-thread' WHERE id=?", clone.SessionID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReserveProductWorkspace(ctx, request); err == nil {
				t.Fatal("stale or unproven writer input was admitted")
			}
			after, err := k.Task(ctx, clone.ID)
			if err != nil || !reflect.DeepEqual(after, clone) {
				t.Fatal("refusal changed Task input", after, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_workspace' AND id=?", productWorkspaceID(clone.ID)).Scan(&count); err != nil || count != 0 {
				t.Fatal("refusal saved a workspace", count, err)
			}
		})
	}
}
