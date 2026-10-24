package product

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// A real private temp Git checkout, with trusted normal/stopped model facts
// seeded in SQLite. This is no evidence of real model dispatch or P acceptance.
func candidateSealerFixture(t *testing.T, configure ...func(*contract.PlanProposal)) (CandidateSealer, storage.ProductWorkspace, domain.Task, contract.SessionRun) {
	t.Helper()
	return candidateSealerFixtureWithAdmission(t, nil, configure...)
}

func candidateSealerFixtureWithAdmission(t *testing.T, request *storage.ProductWaveRequest, configure ...func(*contract.PlanProposal)) (CandidateSealer, storage.ProductWorkspace, domain.Task, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	p, workspaceRequest, _, task := workspacePreparerFixtureWithAdmission(t, request, configure...)
	ready, err := p.Prepare(ctx, workspaceRequest)
	if err != nil {
		t.Fatal(err)
	}
	task, run := stoppedProductWriterRunFixture(t, p.Store, ready, task)
	return CandidateSealer{Store: p.Store}, ready, task, run
}

// The structured result/run/stop are synthetic; checkout and sealing are real.
func stoppedProductWriterRunFixture(t *testing.T, k *storage.KernelStore, ready storage.ProductWorkspace, task domain.Task, configure ...func(*contract.SessionRun)) (domain.Task, contract.SessionRun) {
	t.Helper()
	ctx := context.Background()
	w, err := k.Wave(ctx, task.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	task, err = k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	result := contract.TaskResult{SchemaVersion: 2, Target: task.Target(), TaskID: task.ID, PhaseID: task.PhaseID, Outcome: "result", ProductChanges: []contract.ProductChangeReference{{WorkItemID: ready.Value.WorkItemID, WorkspaceID: ready.ID, WorkspaceDigest: ready.Digest}}, Artifacts: []string{}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "unverified", EvidenceIDs: []string{}, Basis: "await independent verification"}}}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	b.ThreadID, b.PreparationID = "test-product-thread", "test-product-preparation"
	run := contract.SessionRun{ID: "test-product-run", ThreadID: b.ThreadID, TurnID: "test-product-turn", State: "completed", BaselineKnown: true, BaselineTurnIDs: []string{}, Started: time.Now().UTC(), Final: string(raw)}
	for _, apply := range configure {
		apply(&run)
	}
	task.State, task.Attempts = contract.TaskRunning, 1
	budget := storage.RunBudgetBinding{RunID: run.ID, SessionID: b.ID, TaskID: task.ID, ThreadID: b.ThreadID, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest, Targets: []contract.Target{w.Target()}}
	tx, err := k.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	braw, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	traw, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE kernel_sessions SET payload=?,thread_id=?,active_run_id=?,process_stopped=1,creating=0 WHERE id=?", braw, b.ThreadID, run.ID, b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE kernel_tasks SET state=?,payload=? WHERE id=?", task.State, traw, task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO kernel_run_bindings(run_id,session_id,snapshot_id,thread_id,state,process_stopped,applied) VALUES(?,?,?,?,'completed',1,0)", run.ID, b.ID, snapshot.ID, b.ThreadID); err != nil {
		t.Fatal(err)
	}
	for _, record := range []struct {
		kind  string
		value any
	}{{"agent_run", run}, {"run_budget", budget}} {
		raw, err := json.Marshal(record.value)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", record.kind, run.ID, b.ID, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ready.Value.CWD, "implementation.txt"), []byte("implemented behavior\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return task, run
}

func TestCandidateSealerConnectsStoppedRunToFixedCommitWithoutApplyingQuality(t *testing.T) {
	ctx := context.Background()
	s, ready, task, run := candidateSealerFixture(t)
	sealed, err := s.Seal(ctx, task.ID, run.ID)
	if err != nil || sealed.State != "sealed" || sealed.Candidate == nil || sealed.Candidate.SHA == ready.Value.Request.BaseSHA {
		t.Fatal(sealed, err)
	}
	head, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD")
	if err != nil || head != sealed.Candidate.SHA {
		t.Fatal(head, err)
	}
	if err := workspace.Clean(ctx, ready.Value.CWD); err != nil {
		t.Fatal(err)
	}
	again, err := s.Seal(ctx, task.ID, run.ID)
	if err != nil || !reflect.DeepEqual(again, sealed) {
		t.Fatal(again, err)
	}
	current, err := s.Store.Task(ctx, task.ID)
	if err != nil || !reflect.DeepEqual(current, task) {
		t.Fatal("local seal applied quality", current, err)
	}
	var observed contract.SessionRun
	if err := s.Store.Record(ctx, "agent_run", run.ID, &observed); err != nil || observed.Final != run.Final {
		t.Fatal("raw final was rewritten", err)
	}
	var projections int
	if err := s.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_target','product_candidate','task_result_application')").Scan(&projections); err != nil || projections != 0 {
		t.Fatal("local seal became acceptance", projections, err)
	}
	receipt, err := s.Store.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "trunk")
	if err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal(receipt, err)
	}
	p, err := s.Store.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
	if err != nil || p.Value.Candidate.SHA != head || p.Value.WorkItem.Phase != domain.Verifying || p.Value.WorkItem.Issue != 42 || p.Value.PRNumber != 0 {
		t.Fatal("local adoption supplied quality or lost source", p, err)
	}
	if err := s.Store.Record(ctx, "agent_run", run.ID, &observed); err != nil || observed.Final != run.Final {
		t.Fatal("adoption rewrote raw final", err)
	}
}

func TestCandidateSealerDoesNotRetryUnknownOrFailedPersistence(t *testing.T) {
	for _, mode := range []string{"unknown", "save_failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s, ready, task, run := candidateSealerFixture(t)
			saved, err := s.Store.ReserveProductSeal(ctx, task.ID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "unknown" {
				if claim, err := s.Store.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
					t.Fatal(claim, err)
				}
			} else {
				if _, err := s.Store.DB.Exec("CREATE TRIGGER fail_seal_proof BEFORE INSERT ON records WHEN NEW.kind='product_seal_history' BEGIN SELECT RAISE(FAIL,'sealing proof persistence failed'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Seal(ctx, task.ID, run.ID); err == nil {
				t.Fatal("unknown owner/persistence failure succeeded")
			}
			current, err := s.Store.ProductSeal(ctx, task.ID, run.ID)
			if err != nil || current.State != "launching" || current.Proof != nil {
				t.Fatal(current, err)
			}
			reservation, err := s.Store.OperationReservation(ctx, saved.ID)
			if err != nil || reservation.State != "held" {
				t.Fatal("unknown Git released", reservation, err)
			}
			head, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD")
			if err != nil || (mode == "unknown" && head != ready.Value.Request.BaseSHA) || (mode == "save_failure" && head == ready.Value.Request.BaseSHA) {
				t.Fatal(head, err)
			}
			if _, err := s.Seal(ctx, task.ID, run.ID); !errors.Is(err, storage.ErrProductSealHeld) {
				t.Fatal("launching operation retried", err)
			}
			after, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD")
			if err != nil || after != head {
				t.Fatal("retry changed physical commit", after, head, err)
			}
		})
	}
}
