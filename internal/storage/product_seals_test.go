package storage

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// Storage fixtures author trusted stopping/SQL facts only; physical checkout
// verification is independently exercised by product/workspace integration tests.
func productSealFixture(t *testing.T, configure ...func(*contract.PlanProposal)) (*KernelStore, domain.Wave, domain.Task, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	k, saved := readyProductWorkspaceFixture(t, configure...)
	w, err := k.Wave(ctx, saved.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, saved.Value.Request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	return seedProductSealRun(t, k, w, task, saved)
}

func seedProductSealRun(t *testing.T, k *KernelStore, w domain.Wave, task domain.Task, saved ProductWorkspace, configure ...func(*contract.TaskResult)) (*KernelStore, domain.Wave, domain.Task, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	b.ThreadID, b.PreparationID = structureID("test-thread-", task.ID, "seal"), structureID("test-process-", task.ID, "seal")
	result := contract.TaskResult{SchemaVersion: 2, Target: task.Target(), TaskID: task.ID, PhaseID: task.PhaseID, Outcome: "result", ProductChanges: []contract.ProductChangeReference{{WorkItemID: saved.Value.WorkItemID, WorkspaceID: saved.ID, WorkspaceDigest: saved.Digest}}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "unverified", EvidenceIDs: []string{}, Basis: "implementation ready for independent verification"}}}
	for _, apply := range configure {
		apply(&result)
	}
	raw, err := marshalTaskResultFixture(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: structureID("test-run-", task.ID, "seal"), ThreadID: b.ThreadID, TurnID: "product-turn", State: "completed", BaselineKnown: true, BaselineTurnIDs: []string{}, Started: time.Now().UTC(), Final: string(raw)}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	braw, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE kernel_sessions SET payload=?,thread_id=?,active_run_id=?,process_stopped=1,creating=0 WHERE id=?", braw, b.ThreadID, run.ID, b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO kernel_run_bindings(run_id,session_id,snapshot_id,thread_id,state,process_stopped,applied) VALUES(?,?,?,?,'completed',1,0)", run.ID, b.ID, snapshot.ID, b.ThreadID); err != nil {
		t.Fatal(err)
	}
	task.State, task.Attempts = contract.TaskRunning, 1
	if err := saveTask(ctx, tx, task); err != nil {
		t.Fatal(err)
	}
	if err := kernelRecordTx(ctx, tx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	budget := RunBudgetBinding{RunID: run.ID, SessionID: b.ID, ThreadID: b.ThreadID, TaskID: task.ID, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest, Targets: []contract.Target{w.Target()}}
	if err := kernelRecordTx(ctx, tx, "run_budget", run.ID, b.ID, budget); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return k, w, task, run
}

func sealProofFixture(saved ProductSeal) ProductSealProof {
	v, proof := saved.Value.Workspace.Value, saved.Value.Workspace.Proof
	return ProductSealProof{ProofID: "git-stopped", Outcome: "sealed", Reason: "test host sealed fixed checkout and proved Git stopped", Frozen: &workspace.FrozenCheckout{PinnedCheckout: workspace.PinnedCheckout{Repository: v.Request.Repository, CWD: v.CWD, Branch: v.Branch, GitDir: proof.GitDir, CommonDir: proof.CommonDir, BaseSHA: v.Request.BaseSHA}, SHA: strings.Repeat("c", 40)}}
}

func TestProductSealInputMayIncludeGrantedDependencyMetadata(t *testing.T) {
	ctx := context.Background()
	k, _, task, run := productSealFixture(t, func(plan *contract.PlanProposal) {
		setup := plan.Phases[0].Tasks[0]
		setup.ID, setup.WorkItemIDs = "setup", []string{}
		plan.Phases[0].Tasks = append(plan.Phases[0].Tasks, setup)
		plan.Phases[0].Completion.RequiredTaskIDs = append(plan.Phases[0].Completion.RequiredTaskIDs, setup.ID)
		plan.Phases[0].Tasks[0].DependsOn = []contract.TaskDependency{{TaskID: setup.ID}}
	})
	if saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID); err != nil || saved.State != "prepared" {
		t.Fatal(saved, err)
	}
}

func TestProductSealNotStartedAndFailedReceiptsCannotGrantAnotherStart(t *testing.T) {
	for _, outcome := range []string{"not_started", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			k, _, task, run := productSealFixture(t)
			saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "failed" {
				if claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
					t.Fatal(claim, err)
				}
			}
			failed, err := k.FinishProductSeal(ctx, task.ID, run.ID, ProductSealProof{ProofID: "stopped", Outcome: outcome, Reason: "trusted owner stopped or never started"})
			if err != nil || failed.State != "failed" || failed.Candidate != nil || (outcome == "not_started" && !failed.ClaimedAt.IsZero()) {
				t.Fatal(failed, err)
			}
			if claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || claim.Start {
				t.Fatal("failed receipt restarted", claim, err)
			}
		})
	}
}

func TestProductSealOneUseReopenAndProofDoNotApplyTaskOrProjectCandidate(t *testing.T) {
	ctx := context.Background()
	k, w, task, run := productSealFixture(t)
	saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil || saved.State != "prepared" {
		t.Fatal(saved, err)
	}
	var group sync.WaitGroup
	claims := make([]ProductSealClaim, 2)
	errs := make([]error, 2)
	for i := range claims {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			claims[i], errs[i] = k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest)
		}(i)
	}
	group.Wait()
	starts := 0
	for i := range claims {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if claims[i].Start {
			starts++
		}
	}
	if starts != 1 {
		t.Fatal("sealing started more than once", claims)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	again, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest)
	if err != nil || again.Start || again.Seal.State != "launching" {
		t.Fatal(again, err)
	}
	reservation, err := k.OperationReservation(ctx, saved.ID)
	if err != nil || reservation.State != "held" {
		t.Fatal("unknown owner released leases", reservation, err)
	}
	if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, ProductSealProof{ProofID: "false-not-started", Outcome: "not_started", Reason: "cannot relabel launching"}); err == nil {
		t.Fatal("launching relabeled not-started")
	}
	proof := sealProofFixture(saved)
	sealed, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof)
	if err != nil || sealed.State != "sealed" || sealed.Candidate == nil || sealed.Candidate.SHA != proof.Frozen.SHA {
		t.Fatal(sealed, err)
	}
	replay, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof)
	if err != nil || !reflect.DeepEqual(replay, sealed) {
		t.Fatal(replay, err)
	}
	reservation, err = k.OperationReservation(ctx, saved.ID)
	if err != nil || reservation.State != "released" {
		t.Fatal(reservation, err)
	}
	current, err := k.Task(ctx, task.ID)
	if err != nil || !reflect.DeepEqual(current, task) {
		t.Fatal("seal applied Task", current, task, err)
	}
	var persisted contract.SessionRun
	if err := k.Record(ctx, "agent_run", run.ID, &persisted); err != nil || persisted.Final != run.Final {
		t.Fatal("seal rewrote raw final", persisted, err)
	}
	var applied, projections int
	if err := k.DB.QueryRow("SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&applied); err != nil || applied != 0 {
		t.Fatal(applied, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_target','product_candidate','task_result_application')").Scan(&projections); err != nil || projections != 0 {
		t.Fatal("seal bypassed adoption/quality", projections, err)
	}
	digest, err := productTaskDigest(t, k, w, task.Spec)
	if err != nil || digest != task.InputDigest {
		t.Fatal(digest, task.InputDigest, err)
	}
}

func TestProductSealAdmissionRequiresStoppedNormalBoundRunAndOriginalInput(t *testing.T) {
	for _, mode := range []string{"not_stopped", "session_live", "applied", "wrong_active", "baseline", "baseline_unknown", "output", "snapshot", "manifest", "workspace_history", "origin", "pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, run := productSealFixture(t)
			var err error
			switch mode {
			case "not_stopped":
				_, err = k.DB.Exec("UPDATE kernel_run_bindings SET process_stopped=0")
			case "session_live":
				_, err = k.DB.Exec("UPDATE kernel_sessions SET process_stopped=0 WHERE id=?", task.SessionID)
			case "applied":
				_, err = k.DB.Exec("UPDATE kernel_run_bindings SET applied=1")
			case "wrong_active":
				_, err = k.DB.Exec("UPDATE kernel_sessions SET active_run_id='peer-run' WHERE id=?", task.SessionID)
			case "baseline", "baseline_unknown", "output":
				if mode == "baseline" {
					run.BaselineTurnIDs = []string{run.TurnID}
				}
				if mode == "baseline_unknown" {
					run.BaselineKnown = false
				}
				if mode == "output" {
					run.Final = strings.ReplaceAll(run.Final, "product_changes", "foreign_changes")
				}
				var raw []byte
				raw, err = json.Marshal(run)
				if err == nil {
					_, err = k.DB.Exec("UPDATE records SET payload=? WHERE kind='agent_run' AND id=?", raw, run.ID)
				}
			case "snapshot":
				_, err = k.DB.Exec("UPDATE kernel_snapshots SET data='{}'")
			case "manifest":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='session_input_manifest'")
			case "workspace_history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_workspace_history'")
			case "origin":
				err = k.Put(ctx, "wave_work_item", "work", w.ID, map[string]any{"target": w.Target(), "candidate_id": "newer-candidate"})
			case "pause":
				_, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "seal-pause", Target: w.Target(), Action: "pause", Reason: "operator stopped sealing"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReserveProductSeal(ctx, task.ID, run.ID); err == nil {
				t.Fatal("invalid run admitted", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_seal'").Scan(&count); err != nil || count != 0 {
				t.Fatal("invalid admission saved intent", count, err)
			}
		})
	}
}

func TestProductSealCommitFailureRetainsLaunchAndStaleProofIsHistoryOnly(t *testing.T) {
	ctx := context.Background()
	k, w, task, run := productSealFixture(t)
	saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_seal_claim BEFORE UPDATE ON records WHEN NEW.kind='product_seal' BEGIN SELECT RAISE(FAIL,'claim rollback'); END"); err != nil {
		t.Fatal(err)
	}
	claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest)
	if err == nil || claim.Start {
		t.Fatal("failed commit granted start", claim, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_seal_claim"); err != nil {
		t.Fatal(err)
	}
	if claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	proof := sealProofFixture(saved)
	bad := proof
	changed := *proof.Frozen
	changed.GitDir += "-replaced"
	bad.Frozen = &changed
	if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, bad); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("wrong checkout proof accepted", err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_seal_history BEFORE INSERT ON records WHEN NEW.kind='product_seal_history' BEGIN SELECT RAISE(FAIL,'history rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof); err == nil || !strings.Contains(err.Error(), "history rollback") {
		t.Fatal(err)
	}
	current, err := k.ProductSeal(ctx, task.ID, run.ID)
	reservation, leaseErr := k.OperationReservation(ctx, saved.ID)
	if err != nil || leaseErr != nil || current.State != "launching" || current.Proof != nil || reservation.State != "held" {
		t.Fatal("partial receipt saved", current, reservation, err, leaseErr)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_seal_history"); err != nil {
		t.Fatal(err)
	}
	if err := k.Put(ctx, "wave_work_item", "work", w.ID, map[string]any{"target": w.Target(), "candidate_id": "newer-candidate"}); err != nil {
		t.Fatal(err)
	}
	stale, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof)
	if err != nil || stale.State != "stale" || stale.Candidate == nil {
		t.Fatal("stale physical commit lost", stale, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_seal'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSeal(ctx, task.ID, run.ID); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("lost head recreated launch", err)
	}
}
