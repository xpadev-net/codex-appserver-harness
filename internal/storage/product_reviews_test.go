package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productReviewFixture(t *testing.T, verdict string) (*KernelStore, domain.Wave, domain.Task, contract.SessionRun, ProductCandidateSubjectSnapshot) {
	t.Helper()
	ctx := context.Background()
	k, w, task, request, subject := reviewWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	ready, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved))
	if err != nil {
		t.Fatal(err)
	}
	task, err = k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	report := mustImport(t, k, ArtifactImport{SessionID: task.SessionID, Target: task.Target(), Key: "review-report", Kind: "review_report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: task.ID}}, "independent review observations")
	_, _, task, run := seedProductSealRun(t, k, w, task, ready, func(result *contract.TaskResult) {
		result.SchemaVersion = 1
		result.ProductChanges = nil
		result.Artifacts = []string{report.ID}
		result.Review = &contract.ReviewResult{TargetDigest: subject.Value.CandidateDigest, Verdict: verdict, Findings: []contract.ReviewFinding{}}
		if verdict == "findings" {
			result.Review.Findings = []contract.ReviewFinding{{ID: "implementation-defect", Classification: "implementation_defect", TargetDigest: subject.Value.CandidateDigest, Basis: "AC expectation", Description: "observed implementation defect"}}
		}
	})
	return k, w, task, run, subject
}

func TestProductReviewAppliesIndependentBoundReportWithoutQualityApproval(t *testing.T) {
	for _, verdict := range []string{"clean", "findings", "inconclusive"} {
		t.Run(verdict, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run, subject := productReviewFixture(t, verdict)
			before, err := k.ReadProductTarget(ctx, w.Target(), "work")
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID)
			if err != nil || receipt.State != contract.TaskCompleted {
				t.Fatal(receipt, err)
			}
			review, err := k.ReadProductTaskReview(ctx, receipt.ResultID)
			if err != nil || review.Value.SubjectDigest != subject.Digest || review.Value.Candidate != subject.Value.Candidate || review.Value.Review.TargetDigest != subject.Value.CandidateDigest || review.Value.Review.Verdict != verdict {
				t.Fatal(review, err)
			}
			after, err := k.ReadProductTarget(ctx, w.Target(), "work")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("report changed candidate/phase/approval", after, err)
			}
			again, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID)
			if err != nil || !reflect.DeepEqual(receipt, again) {
				t.Fatal(again, err)
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			input, readErr := k.taskResultInputTx(ctx, tx, w, receipt.ResultID)
			tx.Rollback()
			if readErr != nil || input.ProductReview == nil || input.ProductReview.Digest != review.Digest || input.ProductCandidate != nil {
				t.Fatal(input, readErr)
			}
			var observed contract.SessionRun
			if err := k.Record(ctx, "agent_run", run.ID, &observed); err != nil || observed.Final != run.Final {
				t.Fatal("review raw final changed", err)
			}
		})
	}
}

func TestProductReviewRejectsWrongTargetAndHoldsMissingRunEvidence(t *testing.T) {
	for _, mode := range []string{"subject_digest", "baseline", "snapshot", "budget", "manifest", "subject_history", "workspace_history", "writable", "candidate", "pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run, subject := productReviewFixture(t, "clean")
			switch mode {
			case "subject_digest":
				var result contract.TaskResult
				if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
					t.Fatal(err)
				}
				result.Review.TargetDigest = subject.Digest
				raw, err := marshalTaskResultFixture(result)
				if err != nil {
					t.Fatal(err)
				}
				run.Final = string(raw)
				fallthrough
			case "baseline":
				if mode == "baseline" {
					run.BaselineTurnIDs = []string{run.TurnID}
				}
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := kernelRecordTx(ctx, tx, "agent_run", run.ID, task.SessionID, run); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "snapshot":
				if _, err := k.DB.Exec("UPDATE kernel_snapshots SET data='{}' WHERE id=(SELECT snapshot_id FROM kernel_run_bindings WHERE run_id=?)", run.ID); err != nil {
					t.Fatal(err)
				}
			case "budget":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='run_budget' AND id=?", run.ID); err != nil {
					t.Fatal(err)
				}
			case "manifest":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='session_input_manifest' AND json_extract(payload,'$.session_id')=?", task.SessionID); err != nil {
					t.Fatal(err)
				}
			case "subject_history":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_candidate_subject_history' AND id=?", subject.ID); err != nil {
					t.Fatal(err)
				}
			case "workspace_history":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_workspace_history' AND id=?", productWorkspaceID(task.ID)); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if _, err := k.DB.Exec("UPDATE kernel_sessions SET payload=json_set(payload,'$.sandbox','workspace-write') WHERE id=?", task.SessionID); err != nil {
					t.Fatal(err)
				}
			case "candidate", "pause":
				p, err := k.ReadProductTarget(ctx, w.Target(), "work")
				if err != nil {
					t.Fatal(err)
				}
				if mode == "pause" {
					p.Value.WorkItem.Paused = true
				} else {
					p.Value.Candidate.ID = "replacement"
					p.Value.Candidate.SHA = strings.Repeat("e", 40)
					p.Value.WorkItem.Candidate = p.Value.Candidate.ID
				}
				if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
					t.Fatal(err)
				}
			}
			receipt, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID)
			if mode == "subject_digest" {
				if err != nil || receipt.State != contract.TaskFailed {
					t.Fatal("wrong review target accepted", receipt, err)
				}
			} else if !errors.Is(err, ErrTaskResultReferences) || receipt.State != contract.TaskBlocked {
				t.Fatal("missing/stale proof accepted", receipt, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_task_review','product_task_review_history')").Scan(&count); err != nil || count != 0 {
				t.Fatal("rejected report persisted", count, err)
			}
		})
	}
}

func TestProductReviewApplicationRollbackAndProofLoss(t *testing.T) {
	for _, mode := range []string{"rollback", "head", "history"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run, _ := productReviewFixture(t, "clean")
			if mode == "rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_review BEFORE INSERT ON records WHEN NEW.kind='product_task_review_history' BEGIN SELECT RAISE(FAIL,'review rollback'); END"); err != nil {
					t.Fatal(err)
				}
				if _, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID); err == nil || !strings.Contains(err.Error(), "review rollback") {
					t.Fatal(err)
				}
				current, err := k.Task(ctx, task.ID)
				if err != nil || !reflect.DeepEqual(current, task) {
					t.Fatal("partial review completion", current, err)
				}
				if _, err := k.DB.Exec("DROP TRIGGER fail_review"); err != nil {
					t.Fatal(err)
				}
			}
			receipt, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID)
			if err != nil || receipt.State != contract.TaskCompleted {
				t.Fatal(receipt, err)
			}
			if mode == "rollback" {
				return
			}
			kind := "product_task_review"
			if mode == "history" {
				kind = "product_task_review_history"
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=?", kind); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductTaskReview(ctx, receipt.ResultID); err == nil {
				t.Fatal("lost review proof accepted")
			}
			if _, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID); err == nil {
				t.Fatal("replay ignored proof loss")
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, readErr := k.taskResultInputTx(ctx, tx, w, receipt.ResultID)
			tx.Rollback()
			if readErr == nil {
				t.Fatal("source input ignored proof loss")
			}
		})
	}
}
