package storage

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productSubjectFixture(t *testing.T, kind string, configure ...func(*contract.PlanProposal)) (*KernelStore, domain.Wave, domain.Task, ProductTaskCandidateSnapshot) {
	t.Helper()
	ctx := context.Background()
	k, w, producer, run := productSealFixture(t, func(plan *contract.PlanProposal) {
		plan.Phases[1].Tasks[0].Kind = kind
		plan.Phases[1].Tasks[0].WorkItemIDs = []string{"work"}
		for _, apply := range configure {
			apply(plan)
		}
	})
	seal, err := k.ReserveProductSeal(ctx, producer.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductSeal(ctx, producer.ID, run.ID, seal.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductSeal(ctx, producer.ID, run.ID, sealProofFixture(seal)); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.AdoptProductTaskResult(ctx, producer.SessionID, run.ID, "master")
	if err != nil {
		t.Fatal(err)
	}
	source, err := k.ReadProductTaskCandidate(ctx, receipt.ResultID)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := k.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.Spec.Kind == kind {
			return k, w, task, source
		}
	}
	t.Fatal("subject Task missing")
	return nil, w, domain.Task{}, source
}

func TestProductSubjectFixesDeclaredCandidateAndMaterializesUnusedInput(t *testing.T) {
	for _, kind := range []string{"review", "verification"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, source := productSubjectFixture(t, kind)
			origin, err := productTaskDigest(t, k, w, task.Spec)
			if err != nil {
				t.Fatal(err)
			}
			saved, err := k.BindProductCandidateSubject(ctx, task.ID)
			if err != nil || saved.Value.Candidate != source.Value.Candidate || saved.Value.OriginInputDigest != origin || len(saved.Value.Sources) != 1 || !reflect.DeepEqual(saved.Value.Sources[0], source) {
				t.Fatal(saved, err)
			}
			candidateDigest, err := contentDigest(source.Value.Candidate)
			if err != nil || saved.Value.CandidateDigest != candidateDigest || saved.Digest == candidateDigest {
				t.Fatal(saved, err)
			}
			current, err := k.Task(ctx, task.ID)
			if err != nil || current.Attempts != 0 || current.State != contract.TaskReady || current.InputDigest == task.InputDigest || current.InputDigest == origin {
				t.Fatal(current, err)
			}
			var bound string
			if err := k.DB.QueryRow("SELECT input_digest FROM kernel_plan_bindings WHERE task_id=?", task.ID).Scan(&bound); err != nil || bound != current.InputDigest {
				t.Fatal(bound, err)
			}
			own := registrationManifest(t, k, task.SessionID, w.Target())
			if own.ProductSubject == nil || !reflect.DeepEqual(*own.ProductSubject, saved) || len(own.ProductSubjects) != 1 || own.ProductWorkspace != nil {
				t.Fatal(own.ProductSubject, own.ProductSubjects)
			}
			parent := registrationManifest(t, k, w.OrchestratorSessionID, w.Target())
			if parent.ProductSubject != nil || len(parent.ProductSubjects) != 1 {
				t.Fatal("parent obtained own-role output identity", parent.ProductSubject, parent.ProductSubjects)
			}
			snapshot, err := k.BuildSessionInput(ctx, task.SessionID, []contract.Target{w.Target()})
			if err != nil || !strings.Contains(inputContext(t, snapshot).Instructions, "product_subject") || kind == "review" && !strings.Contains(inputContext(t, snapshot).Instructions, "review.target_digest") {
				t.Fatal(err)
			}
			p, err := k.ReadProductTarget(ctx, w.Target(), "work")
			if err != nil {
				t.Fatal(err)
			}
			next := p.Value
			next.Candidate.ID, next.Candidate.SHA = "advanced-candidate", strings.Repeat("d", 40)
			next.WorkItem.Candidate = next.Candidate.ID
			next.WorkItem.Version++
			if _, err := k.SetProductTarget(ctx, p.Digest, next); err != nil {
				t.Fatal(err)
			}
			got, err := productTaskDigest(t, k, w, task.Spec)
			if err != nil || got != bound {
				t.Fatal("current membership retargeted fixed subject", got, bound, err)
			}
			root := k.Root
			k.Close()
			k, err = OpenExistingKernel(filepath.Join(root, "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			again, err := k.BindProductCandidateSubject(ctx, task.ID)
			if err != nil || !reflect.DeepEqual(again, saved) {
				t.Fatal("retry rebound subject", again, err)
			}
			b, err := k.SessionBinding(ctx, task.SessionID)
			if err != nil || b.ThreadID != "" || b.Sandbox != "read-only" {
				t.Fatal("subject binding granted execution/write", b, err)
			}
			ready, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
			if err != nil || len(ready.Tasks) != 0 || len(ready.Held) == 0 || ready.Held[0].Reason != "product_readiness_unavailable" {
				t.Fatal("subject bypassed P readiness", ready, err)
			}
		})
	}
}

func TestProductSubjectProofLossCannotFallBackToMutableMembership(t *testing.T) {
	for _, mode := range []string{"head", "history", "corrupt_head", "corrupt_history", "producer", "candidate", "registration"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, _ := productSubjectFixture(t, "review")
			if _, err := k.BindProductCandidateSubject(ctx, task.ID); err != nil {
				t.Fatal(err)
			}
			var err error
			switch mode {
			case "head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_candidate_subject'")
			case "history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_candidate_subject_history'")
			case "corrupt_head":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_candidate_subject'")
			case "corrupt_history":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_candidate_subject_history'")
			case "producer":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_task_candidate'")
			case "candidate":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_candidate'")
			case "registration":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_work_item_registration'")
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := productTaskDigest(t, k, w, task.Spec); err == nil {
				t.Fatal("lost fixed input fell back", mode)
			}
			if _, err := k.BuildSessionInput(ctx, task.SessionID, []contract.Target{w.Target()}); err == nil {
				t.Fatal("lost proof produced executable input", mode)
			}
		})
	}
}

func TestProductSubjectBindingRollbackDoesNotPartiallyRewriteTask(t *testing.T) {
	for _, stage := range []string{"history", "plan"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			k, _, task, _ := productSubjectFixture(t, "review")
			ddl := "CREATE TRIGGER fail_subject BEFORE INSERT ON records WHEN NEW.kind='product_candidate_subject_history' BEGIN SELECT RAISE(FAIL,'subject rollback'); END"
			if stage == "plan" {
				ddl = "CREATE TRIGGER fail_subject BEFORE UPDATE ON kernel_plan_bindings BEGIN SELECT RAISE(FAIL,'subject rollback'); END"
			}
			if _, err := k.DB.Exec(ddl); err != nil {
				t.Fatal(err)
			}
			if _, err := k.BindProductCandidateSubject(ctx, task.ID); err == nil || !strings.Contains(err.Error(), "subject rollback") {
				t.Fatal(err)
			}
			current, err := k.Task(ctx, task.ID)
			if err != nil || !reflect.DeepEqual(current, task) {
				t.Fatal("Task changed after failed bind", current, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_candidate_subject','product_candidate_subject_history')").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial fixed subject", count, err)
			}
			if _, err := k.DB.Exec("DROP TRIGGER fail_subject"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.BindProductCandidateSubject(ctx, task.ID); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProductSubjectRequiresDeclaredCurrentCandidateAndUnusedSession(t *testing.T) {
	for _, mode := range []string{"no_source", "candidate_changed", "thread", "attempt", "pause", "work_item_pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, _ := productSubjectFixture(t, "review")
			var err error
			switch mode {
			case "no_source", "attempt":
				if mode == "no_source" {
					task.Spec.DependsOn = []contract.TaskDependency{}
				} else {
					task.Attempts = 1
				}
				tx, openErr := k.kernelTx(ctx)
				if openErr != nil {
					t.Fatal(openErr)
				}
				err = saveTask(ctx, tx, task)
				if err == nil {
					err = tx.Commit()
				} else {
					tx.Rollback()
				}
			case "thread":
				b, readErr := k.SessionBinding(ctx, task.SessionID)
				if readErr != nil {
					t.Fatal(readErr)
				}
				b.ThreadID = "already-used-thread"
				raw, marshalErr := json.Marshal(b)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				_, err = k.DB.Exec("UPDATE kernel_sessions SET payload=?,thread_id=? WHERE id=?", raw, b.ThreadID, b.ID)
			case "candidate_changed", "work_item_pause":
				p, readErr := k.ReadProductTarget(ctx, w.Target(), "work")
				if readErr != nil {
					t.Fatal(readErr)
				}
				next := p.Value
				if mode == "candidate_changed" {
					next.Candidate.ID, next.Candidate.SHA = "other-candidate", strings.Repeat("d", 40)
					next.WorkItem.Candidate = next.Candidate.ID
				} else {
					next.WorkItem.Paused = true
				}
				next.WorkItem.Version++
				_, err = k.SetProductTarget(ctx, p.Digest, next)
			case "pause":
				_, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "subject-pause", Target: w.Target(), Action: "pause", Reason: "hold new input binding"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.BindProductCandidateSubject(ctx, task.ID); err == nil {
				t.Fatal("invalid subject admission", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_candidate_subject'").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
		})
	}
}

func TestProductSubjectWaitsForAppliedProducerAndDoesNotAcceptWorkRole(t *testing.T) {
	ctx := context.Background()
	k, w, producer, _ := productWorkspaceFixture(t)
	tasks, err := k.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		_, err := k.BindProductCandidateSubject(ctx, task.ID)
		if err == nil {
			t.Fatal("unapplied producer permitted subject", task.ID)
		}
		if task.ID == producer.ID && !errors.Is(err, ErrProductSubjectHeld) {
			t.Fatal("work role selected review subject", err)
		}
	}
}

func TestProductSubjectUsesExplicitlyCarriedProducerAndSourceResultContext(t *testing.T) {
	for _, mode := range []string{"dependency", "source_result"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, _, source := productSubjectFixture(t, "review")
			plan := storagePlanFixture()
			plan.Phases[0].Tasks[0].WorkItemIDs = []string{"work"}
			plan.Phases[1].ID = "next-evaluation"
			review := &plan.Phases[1].Tasks[0]
			review.ID, review.WorkItemIDs = "next-review", []string{"work"}
			plan.Phases[1].Completion.RequiredTaskIDs = []string{review.ID}
			if mode == "source_result" {
				review.DependsOn = []contract.TaskDependency{}
				review.SourceResultIDs = []string{source.Value.ResultID}
			}
			admitted, err := k.AdoptPlan(ctx, w.Target(), "subject-replan", plan)
			if err != nil || len(admitted.CarriedTaskIDs) != 1 || len(admitted.CreatedTaskIDs) != 1 {
				t.Fatal(admitted, err)
			}
			saved, err := k.BindProductCandidateSubject(ctx, admitted.CreatedTaskIDs[0])
			if err != nil || saved.Value.Candidate != source.Value.Candidate || saved.Value.Target != admitted.Target || saved.Value.Sources[0].Value.Target.PlanRevision == saved.Value.Target.PlanRevision {
				t.Fatal("carried source was rebound or refused", saved, err)
			}
		})
	}
}
