package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func waveCompletionFixture(t *testing.T) (*KernelStore, domain.Wave, contract.SessionBinding, contract.SessionRun, contract.MainResult) {
	t.Helper()
	ctx := context.Background()
	k, w, _, _, p := acceptanceFixture(t, nil)
	b, run, _ := reserveBudgetMain(t, k, "final-main", w.Target())
	result := contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{{Target: w.Target(), Action: "accept", Acceptance: &p, Summary: "The original criterion is covered by the independent evaluation of the final output.", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}}}}
	run = saveMainCompletion(t, k, b, run, result, true)
	if _, err := k.Subscribe(ctx, SubscriptionRequest{SessionID: b.ID, Target: w.Target(), Kind: "wave_terminal", Key: "watch-completion"}); err != nil {
		t.Fatal(err)
	}
	return k, w, b, run, result
}
func TestMainAcceptanceCompletesWaveWithProofNotificationAndReplay(t *testing.T) {
	ctx := context.Background()
	k, w, b, run, _ := waveCompletionFixture(t)
	applied, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || applied.State != "applied" || len(applied.Completions) != 1 {
		t.Fatal(applied, err)
	}
	completed, err := k.Wave(ctx, w.ID)
	if err != nil || completed.State != domain.WaveCompleted || completed.CompletedAt.IsZero() || completed.MainTurns != 1 {
		t.Fatal(completed, err)
	}
	var proof WaveCompletion
	if err = k.Record(ctx, "wave_completion", applied.Completions[0].ID, &proof); err != nil || !reflect.DeepEqual(proof, applied.Completions[0]) {
		t.Fatal(proof, err)
	}
	var original contract.SessionRun
	if err = k.Record(ctx, "agent_run", run.ID, &original); err != nil || !reflect.DeepEqual(original, run) {
		t.Fatal("completion rewrote physical evidence", original, err)
	}
	notifications, err := k.Notifications(ctx, b.ID, 0, 100)
	if err != nil || len(notifications) != 1 || notifications[0].Kind != "wave_terminal" || notifications[0].WaveState != domain.WaveCompleted {
		t.Fatal(notifications, err)
	}
	state, err := k.SessionDispatchState(ctx, b.ID)
	if err != nil || state.Run != nil || !state.ProcessStopped {
		t.Fatal(state, err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || !reflect.DeepEqual(replay, applied) {
		t.Fatal(replay, err)
	}
	if _, err = k.ReconcileNotifications(ctx, 100); err != nil {
		t.Fatal(err)
	}
	after, err := k.Notifications(ctx, b.ID, 0, 100)
	if err != nil || len(after) != 1 {
		t.Fatal(after, err)
	}
}
func TestMainAcceptanceRejectsMissingProofAndWaitsForPhysicalAndOperationSettlement(t *testing.T) {
	for _, mode := range []string{"bad_mapping", "paused", "direction", "deadline", "process_live", "run_unstopped", "pending_operation", "unknown_run", "event_failure", "receipt_failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, b, run, result := waveCompletionFixture(t)
			switch mode {
			case "bad_mapping":
				result.Decisions[0].Acceptance.Evaluations[0].CriterionID = "invented"
				raw, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				// Simulate the actual final returned by this normal main completion;
				// ordinary Put correctly prohibits rewriting an already completed run.
				run.Final = string(raw)
				raw, err = json.Marshal(run)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = k.DB.ExecContext(ctx, "UPDATE records SET payload=? WHERE kind='agent_run' AND id=?", raw, run.ID); err != nil {
					t.Fatal(err)
				}
			case "paused", "direction", "deadline":
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "paused" {
					w.Paused = true
				}
				if mode == "direction" {
					w.PendingDirectionID = "queued"
				}
				if mode == "deadline" {
					w.Deadline = k.clock()
				}
				if err = saveWave(ctx, tx, w); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "process_live":
				if _, err := k.DB.ExecContext(ctx, "UPDATE kernel_sessions SET process_stopped=0 WHERE id=?", b.ID); err != nil {
					t.Fatal(err)
				}
			case "run_unstopped":
				if _, err := k.DB.ExecContext(ctx, "UPDATE kernel_run_bindings SET process_stopped=0 WHERE run_id=?", run.ID); err != nil {
					t.Fatal(err)
				}
			case "pending_operation":
				raw, err := json.Marshal(kernelToolOperation{ID: "uncertain", SessionID: b.ID, Target: w.Target()})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = k.DB.ExecContext(ctx, `INSERT INTO kernel_tool_operations(id,session_id,thread_id,turn_id,call_id,name,argument_digest,state,payload) VALUES('uncertain',?,'thread','turn','call','artifact_publish','digest','unknown',?)`, b.ID, raw); err != nil {
					t.Fatal(err)
				}
			case "unknown_run":
				run.State = "unknown"
				raw, err := json.Marshal(run)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = k.DB.ExecContext(ctx, "UPDATE records SET payload=? WHERE kind='agent_run' AND id=?", raw, run.ID); err != nil {
					t.Fatal(err)
				}
				if _, err = k.DB.ExecContext(ctx, "UPDATE kernel_run_bindings SET state='unknown' WHERE run_id=?", run.ID); err != nil {
					t.Fatal(err)
				}
			case "event_failure":
				if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_wave_completion_event BEFORE INSERT ON kernel_events WHEN NEW.kind='wave_completed' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
					t.Fatal(err)
				}
			case "receipt_failure":
				if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_wave_completion_receipt BEFORE INSERT ON records WHEN NEW.kind='main_result_application' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
					t.Fatal(err)
				}
			}
			applied, err := k.ApplyMainResult(ctx, b.ID, run.ID)
			if mode == "pending_operation" && !errors.Is(err, ErrMainResultHeld) {
				t.Fatal(err)
			}
			if mode == "direction" {
				if err != nil || applied.State != "archived" {
					t.Fatal(applied, err)
				}
			} else if err == nil {
				t.Fatal("unsupported completion applied", mode)
			}
			wave, err := k.Wave(ctx, w.ID)
			if err != nil || wave.State == domain.WaveCompleted || !wave.CompletedAt.IsZero() {
				t.Fatal(wave, err)
			}
			var count int
			if err = k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='wave_completion'").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
			notes, err := k.Notifications(ctx, b.ID, 0, 100)
			if err != nil || len(notes) != 0 {
				t.Fatal("rolled-back completion notified", notes, err)
			}
		})
	}
}

func TestMainAcceptanceWaitsForLiveRecoveryOwnerThenCompletesWithoutNewRun(t *testing.T) {
	ctx := context.Background()
	k, w, b, run, _ := waveCompletionFixture(t)
	binding, err := k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	recovery := managedReserve(t, k, binding, "late-recovery", run.ID, "recovery")
	managedStart(t, k, recovery, 99)
	if _, err = k.ApplyMainResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("live recovery owner did not hold completion")
	}
	if err = k.ConfirmManagedProcessStopped(ctx, ManagedStopProof{OwnerID: "late-recovery", Disposition: "group_stopped", Identity: managedIdentity(99)}); err != nil {
		t.Fatal(err)
	}
	applied, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || len(applied.Completions) != 1 {
		t.Fatal(applied, err)
	}
	wave, err := k.Wave(ctx, w.ID)
	if err != nil || wave.MainTurns != 1 || wave.State != domain.WaveCompleted {
		t.Fatal(wave, err)
	}
}
func TestMainAcceptanceMultipleWavesCommitOrRollbackTogether(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "second_event_failure"}[fail], func(t *testing.T) {
			ctx := context.Background()
			k, request := waveStoreFixture(t)
			waves := []domain.Wave{}
			decisions := []contract.MainDecision{}
			targets := []contract.Target{}
			for _, source := range []string{"first", "second"} {
				request.SourceID = source
				w, err := k.CreateWave(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				admission, err := k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture())
				if err != nil {
					t.Fatal(err)
				}
				work, err := k.Task(ctx, admission.CreatedTaskIDs[0])
				if err != nil {
					t.Fatal(err)
				}
				artifact := completeTemplateTask(t, k, work, "output for "+source, "", nil)
				review, err := k.Task(ctx, admission.CreatedTaskIDs[1])
				if err != nil {
					t.Fatal(err)
				}
				completeTemplateTask(t, k, review, "independent evaluation", artifact.Digest, nil)
				proposal := contract.ArtifactAcceptance{Outputs: []string{artifact.ID}, Evaluations: []contract.ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: artifact.ID, ReviewTaskID: review.ID}}}
				decisions = append(decisions, contract.MainDecision{Target: w.Target(), Action: "accept", Acceptance: &proposal, Summary: "all required work independently evaluated", SubscriptionIDs: []string{}, Remaining: []string{}, Blockers: []string{}})
				waves = append(waves, w)
				targets = append(targets, w.Target())
			}
			b, run, _ := reserveBudgetMain(t, k, "shared-final", targets...)
			saveMainCompletion(t, k, b, run, contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: decisions}, true)
			if fail {
				if _, err := k.DB.ExecContext(ctx, `CREATE TRIGGER fail_second_wave_completion BEFORE INSERT ON kernel_events WHEN NEW.kind='wave_completed' AND EXISTS(SELECT 1 FROM kernel_events WHERE kind='wave_completed') BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
					t.Fatal(err)
				}
			}
			applied, err := k.ApplyMainResult(ctx, b.ID, run.ID)
			if fail {
				if err == nil {
					t.Fatal("second event failure accepted")
				}
			} else if err != nil || len(applied.Completions) != 2 {
				t.Fatal(applied, err)
			}
			for _, w := range waves {
				actual, err := k.Wave(ctx, w.ID)
				if err != nil || (actual.State == domain.WaveCompleted) == fail {
					t.Fatal(actual, err)
				}
			}
			state, err := k.SessionDispatchState(ctx, b.ID)
			if err != nil {
				t.Fatal(err)
			}
			if fail && (state.Run == nil || state.Run.ID != run.ID) || !fail && state.Run != nil {
				t.Fatal("partial main disposition", state)
			}
		})
	}
}

func TestMainAcceptanceWaitsForRunnerAndGitReservationRelease(t *testing.T) {
	for _, kind := range []string{"runner", "git"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, w, b, run, _ := waveCompletionFixture(t)
			request := OperationCapacityRequest{ID: "outstanding-" + kind, Kind: kind, SessionID: b.ID, Target: w.Target()}
			if kind == "runner" {
				request.Worktree = t.TempDir()
			} else {
				request.Repository = operationRepo(t)
			}
			reservation, err := k.ReserveOperationCapacity(ctx, request)
			if err != nil || reservation.State != "held" {
				t.Fatal(reservation, err)
			}
			if _, err = k.ApplyMainResult(ctx, b.ID, run.ID); !errors.Is(err, ErrMainResultHeld) {
				t.Fatal("unsettled operation permitted completion", err)
			}
			if err = k.ReleaseOperationCapacity(ctx, OperationReleaseProof{OperationID: request.ID, ProofID: "unknown-response", Disposition: "unknown", Reason: "outcome is still unresolved"}); err == nil {
				t.Fatal("unknown operation released")
			}
			if _, err = k.ApplyMainResult(ctx, b.ID, run.ID); !errors.Is(err, ErrMainResultHeld) {
				t.Fatal("unknown response permitted completion", err)
			}
			releaseOperation(t, k, request.ID, "process_stopped")
			result, err := k.ApplyMainResult(ctx, b.ID, run.ID)
			if err != nil || len(result.Completions) != 1 {
				t.Fatal(result, err)
			}
			actual, err := k.Wave(ctx, w.ID)
			if err != nil || actual.State != domain.WaveCompleted || actual.MainTurns != 1 {
				t.Fatal(actual, err)
			}
		})
	}
}

func TestMainAcceptanceClassifiesReferenceHoldsButPreservesIOFailures(t *testing.T) {
	for _, missingObject := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_reference", true: "io_failure"}[missingObject], func(t *testing.T) {
			ctx := context.Background()
			k, _, b, run, result := waveCompletionFixture(t)
			if missingObject {
				if err := os.Remove(filepath.Join(k.Root, "objects", result.Decisions[0].Acceptance.Outputs[0])); err != nil {
					t.Fatal(err)
				}
			} else {
				result.Decisions[0].Acceptance.Outputs = []string{"nonexistent"}
				result.Decisions[0].Acceptance.Evaluations[0].ArtifactID = "nonexistent"
				raw, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				run.Final = string(raw)
				raw, err = json.Marshal(run)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = k.DB.ExecContext(ctx, "UPDATE records SET payload=? WHERE kind='agent_run' AND id=?", raw, run.ID); err != nil {
					t.Fatal(err)
				}
			}
			_, err := k.ApplyMainResult(ctx, b.ID, run.ID)
			if missingObject {
				if !errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrMainResultHeld) || errors.Is(err, ErrMainResultInvalid) {
					t.Fatal("OS error misclassified", err)
				}
			} else if !errors.Is(err, ErrMainResultHeld) {
				t.Fatal("reference error retained local flight", err)
			}
		})
	}
}
