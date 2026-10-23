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
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

func priorProductWorkspaceFixture(t *testing.T) (*KernelStore, domain.Wave, domain.Task, ProductWorkspaceRequest, ProductTargetSnapshot) {
	t.Helper()
	ctx := context.Background()
	k, w, task, request := productWorkspaceFixture(t)
	p := ProductTarget{Target: w.Target(), Repository: "owner/repo", BaseBranch: "master", PRNumber: 17, WorkItem: domain.WorkItem{ID: "work", RepositoryID: 17, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, Version: 4, Attempt: 3, RepairRounds: 2, Phase: domain.Reviewing, Branch: "harness/published", Candidate: "previous-candidate", Worktree: "/old-worktree", MergeHold: true, CreatedAt: k.clock(), Reason: "old findings", LastCompletion: &domain.Event{Kind: "local_review_completed", Candidate: "previous-candidate"}}, Candidate: runner.Candidate{ID: "previous-candidate", SHA: request.BaseSHA, BaseSHA: request.BaseSHA, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision}}
	saved, err := k.SetProductTarget(ctx, "", p)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	task.InputDigest, err = k.taskInputDigestTx(ctx, tx, w, task.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveTask(ctx, tx, task); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE kernel_plan_bindings SET input_digest=? WHERE task_id=?", task.InputDigest, task.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return k, w, task, request, saved
}

func TestProductCandidateAdoptionPreservesPublishedBranchPRMergeHoldAndCounters(t *testing.T) {
	ctx := context.Background()
	k, w, task, request, prior := priorProductWorkspaceFixture(t)
	ready, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, ready.Digest); err != nil {
		t.Fatal(err)
	}
	ready, err = k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, ready))
	if err != nil {
		t.Fatal(err)
	}
	task, err = k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, task, run := seedProductSealRun(t, k, w, task, ready)
	saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil {
		t.Fatal("merge hold incorrectly stopped local sealing", err)
	}
	if _, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, sealProofFixture(saved)); err != nil {
		t.Fatal(err)
	}
	if receipt, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "master"); err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal(receipt, err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil || p.Value.PRNumber != prior.Value.PRNumber || p.Value.WorkItem.Branch != prior.Value.WorkItem.Branch || p.Value.WorkItem.Version != prior.Value.WorkItem.Version+1 || p.Value.WorkItem.Attempt != prior.Value.WorkItem.Attempt || p.Value.WorkItem.RepairRounds != prior.Value.WorkItem.RepairRounds || !p.Value.WorkItem.MergeHold || p.Value.WorkItem.Phase != domain.Verifying || p.Value.WorkItem.LastCompletion != nil || p.Value.WorkItem.Reason != "" || p.Value.WorkItem.Worktree != ready.Value.CWD {
		t.Fatal(p, err)
	}
}

func TestProductWorkItemPauseFencesWorkspaceAndSealing(t *testing.T) {
	ctx := context.Background()
	k, w, task, request, p := priorProductWorkspaceFixture(t)
	next := p.Value
	next.WorkItem.Paused = true
	p, err := k.SetProductTarget(ctx, p.Digest, next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductWorkspace(ctx, request); !errors.Is(err, ErrProductWorkspaceHeld) {
		t.Fatal("paused WorkItem prepared", err)
	}
	next.WorkItem.Paused = false
	p, err = k.SetProductTarget(ctx, p.Digest, next)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, ready.Digest); err != nil {
		t.Fatal(err)
	}
	ready, err = k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, ready))
	if err != nil {
		t.Fatal(err)
	}
	task, err = k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, task, run := seedProductSealRun(t, k, w, task, ready)
	next.WorkItem.Paused = true
	if _, err := k.SetProductTarget(ctx, p.Digest, next); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSeal(ctx, task.ID, run.ID); !errors.Is(err, ErrProductSealHeld) {
		t.Fatal("paused WorkItem sealed", err)
	}
}

func sealedProductFixture(t *testing.T) (*KernelStore, domain.Wave, domain.Task, contract.SessionRun, ProductSeal) {
	t.Helper()
	ctx := context.Background()
	k, w, task, run := productSealFixture(t)
	saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	sealed, err := k.FinishProductSeal(ctx, task.ID, run.ID, sealProofFixture(saved))
	if err != nil || sealed.State != "sealed" {
		t.Fatal(sealed, err)
	}
	return k, w, task, run, sealed
}

func TestProductCandidateAdoptionCompletesTaskAtomicallyWithoutQualityOrRawRewrite(t *testing.T) {
	ctx := context.Background()
	k, w, task, run, sealed := sealedProductFixture(t)
	if _, err := k.ApplyTaskResult(ctx, task.SessionID, run.ID); !errors.Is(err, ErrTaskResultReferences) {
		t.Fatal("generic completion adopted candidate", err)
	}
	receipt, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "master")
	if err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal(receipt, err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil || p.Value.Candidate != *sealed.Candidate || p.Value.WorkItem.Phase != domain.Verifying || p.Value.PRNumber != 0 || p.Value.WorkItem.Worktree != sealed.Value.Workspace.Value.CWD {
		t.Fatal(p, err)
	}
	current, err := k.Task(ctx, task.ID)
	if err != nil || current.InputDigest != task.InputDigest || current.Attempts != 1 || current.ResultID != receipt.ResultID {
		t.Fatal(current, err)
	}
	digest, err := productTaskDigest(t, k, w, task.Spec)
	if err != nil || digest != task.InputDigest {
		t.Fatal("candidate invalidated its originating input", digest, err)
	}
	sidecar, err := k.ReadProductTaskCandidate(ctx, receipt.ResultID)
	if err != nil || sidecar.Value.Candidate != *sealed.Candidate || sidecar.Value.OutputDigest != bytesDigest([]byte(run.Final)) {
		t.Fatal(sidecar, err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	input, inputErr := k.taskResultInputTx(ctx, tx, w, receipt.ResultID)
	_, resultErr := k.completedTaskResultTx(ctx, tx, w, current)
	tx.Rollback()
	if inputErr != nil || resultErr != nil || input.ProductCandidate == nil || !reflect.DeepEqual(*input.ProductCandidate, sidecar) || input.Result.SchemaVersion != 2 || len(input.Result.WorkItemRefs) != 0 {
		t.Fatal(input, inputErr, resultErr)
	}
	var observed contract.SessionRun
	if err := k.Record(ctx, "agent_run", run.ID, &observed); err != nil || observed.Final != run.Final {
		t.Fatal("model final rewritten", err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	again, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "master")
	if err != nil || !reflect.DeepEqual(again, receipt) {
		t.Fatal(again, err)
	}
	if _, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "other-base"); err == nil {
		t.Fatal("replay changed trusted base")
	}
	var applications, projections int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='task_result_application'").Scan(&applications); err != nil || applications != 1 {
		t.Fatal(applications, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_candidate'").Scan(&projections); err != nil || projections != 1 {
		t.Fatal(projections, err)
	}
}

func TestProductCandidateSealingCannotBypassInvalidResultApplication(t *testing.T) {
	for _, mode := range []string{"criterion", "artifact"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, task, run := productSealFixture(t)
			var result contract.TaskResult
			if err := contract.DecodeStrict([]byte(run.Final), &result); err != nil {
				t.Fatal(err)
			}
			if mode == "criterion" {
				result.CriterionResults[0].CriterionID = "outside-assignment"
			} else {
				result.Artifacts = []string{"missing-artifact"}
			}
			raw, err := marshalTaskResultFixture(result)
			if err != nil {
				t.Fatal(err)
			}
			run.Final = string(raw)
			raw, err = json.Marshal(run)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("UPDATE records SET payload=? WHERE kind='agent_run' AND id=?", raw, run.ID); err != nil {
				t.Fatal(err)
			}
			saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
				t.Fatal(claim, err)
			}
			if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, sealProofFixture(saved)); err != nil {
				t.Fatal(err)
			}
			receipt, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "master")
			if err != nil || receipt.State != contract.TaskFailed {
				t.Fatal("sealed commit bypassed model result validation", receipt, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_target','product_candidate','product_task_candidate')").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
		})
	}
}

func TestProductCandidateAdoptionFailureRollsBackProjectionAndTask(t *testing.T) {
	for _, stage := range []string{"sidecar", "completion"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run, _ := sealedProductFixture(t)
			ddl := "CREATE TRIGGER fail_adoption BEFORE INSERT ON records WHEN NEW.kind='product_task_candidate' BEGIN SELECT RAISE(FAIL,'adoption rollback'); END"
			if stage == "completion" {
				ddl = "CREATE TRIGGER fail_adoption BEFORE UPDATE ON kernel_plan_bindings WHEN NEW.source_result_id<>'' BEGIN SELECT RAISE(FAIL,'adoption rollback'); END"
			}
			if _, err := k.DB.Exec(ddl); err != nil {
				t.Fatal(err)
			}
			if _, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "master"); err == nil || !strings.Contains(err.Error(), "adoption rollback") {
				t.Fatal(err)
			}
			current, err := k.Task(ctx, task.ID)
			if err != nil || !reflect.DeepEqual(current, task) {
				t.Fatal("partial Task completion", current, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_target','product_candidate','product_task_candidate','task_result_application')").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial candidate adoption", count, err)
			}
			var applied bool
			if err := k.DB.QueryRow("SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&applied); err != nil || applied {
				t.Fatal(applied, err)
			}
			digest, err := productTaskDigest(t, k, w, task.Spec)
			if err != nil || digest != task.InputDigest {
				t.Fatal(digest, err)
			}
			if _, err := k.DB.Exec("DROP TRIGGER fail_adoption"); err != nil {
				t.Fatal(err)
			}
			if receipt, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "master"); err != nil || receipt.State != contract.TaskCompleted {
				t.Fatal(receipt, err)
			}
		})
	}
}

func TestProductCandidateAdoptionRejectsMissingEvidenceAndConcurrentOrigin(t *testing.T) {
	for _, mode := range []string{"head", "history", "origin", "pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run, _ := sealedProductFixture(t)
			var err error
			switch mode {
			case "head":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_seal'")
			case "history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_seal_history'")
			case "origin":
				err = k.Put(ctx, "wave_work_item", "work", w.ID, map[string]any{"target": w.Target(), "candidate_id": "newer-candidate"})
			case "pause":
				_, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "adoption-pause", Target: w.Target(), Action: "pause", Reason: "pause publication"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "master"); err == nil {
				t.Fatal("unavailable candidate adopted")
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_target','product_candidate','product_task_candidate')").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
		})
	}
}

func TestProductCandidateSidecarLossOrResultCorruptionInvalidatesEvidence(t *testing.T) {
	for _, mode := range []string{"sidecar", "history", "candidate", "result"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run, _ := sealedProductFixture(t)
			receipt, err := k.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "master")
			if err != nil {
				t.Fatal(err)
			}
			kind := map[string]string{"sidecar": "product_task_candidate", "history": "product_seal_history", "candidate": "product_candidate"}[mode]
			if mode == "result" {
				var result contract.TaskResult
				if err := k.Record(ctx, "task_result", receipt.ResultID, &result); err != nil {
					t.Fatal(err)
				}
				result.CriterionResults[0].Basis = "corrupt stored evidence"
				raw, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				_, err = k.DB.Exec("UPDATE records SET payload=? WHERE kind='task_result' AND id=?", raw, receipt.ResultID)
				if err != nil {
					t.Fatal(err)
				}
			} else if _, err := k.DB.Exec("DELETE FROM records WHERE kind=?", kind); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductTaskCandidate(ctx, receipt.ResultID); err == nil {
				t.Fatal("missing evidence accepted")
			}
			current, err := k.Task(ctx, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = k.completedTaskResultTx(ctx, tx, w, current)
			tx.Rollback()
			if err == nil {
				t.Fatal("corrupt product completion remained usable")
			}
		})
	}
}
