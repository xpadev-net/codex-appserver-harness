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

func acceptanceFixture(t *testing.T, findings []string) (*KernelStore, domain.Wave, domain.Task, domain.Artifact, contract.ArtifactAcceptance) {
	t.Helper()
	k, w, review, artifact := templateFixture(t, false, []contract.FixReviewTemplate{kernelTemplate()}, findings)
	var err error
	review, err = k.Task(context.Background(), review.ID)
	if err != nil {
		t.Fatal(err)
	}
	proposal := contract.ArtifactAcceptance{Outputs: []string{artifact.ID}, Evaluations: []contract.ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: artifact.ID, ReviewTaskID: review.ID}}}
	return k, w, review, artifact, proposal
}
func TestArtifactAcceptanceProvesIndependentCurrentOutputsWithoutCompletingWave(t *testing.T) {
	ctx := context.Background()
	k, w, _, artifact, p := acceptanceFixture(t, nil)
	proof, err := k.CheckArtifactAcceptance(ctx, w.Target(), p)
	if err != nil || proof.Digest == "" || proof.ArtifactDigests[artifact.ID] != artifact.Digest || len(proof.ResultDigests) != 2 {
		t.Fatal(proof, err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.State != domain.WaveRunning || !current.CompletedAt.IsZero() {
		t.Fatal("proof completed Wave", current, err)
	}
	var fakeApproval int
	if err = k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&fakeApproval); err != nil || fakeApproval != 0 {
		t.Fatal(fakeApproval, err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.CheckArtifactAcceptance(ctx, w.Target(), p)
	if err != nil || !reflect.DeepEqual(proof, replay) {
		t.Fatal(replay, err)
	}
}
func TestArtifactAcceptanceHoldsMissingIndependentOrCurrentEvidence(t *testing.T) {
	for _, mode := range []string{"self_review", "foreign_criterion", "missing_output_evaluation", "unapplied", "unstopped", "live_owner", "writable_review", "forged_result", "remaining", "corrupt_artifact", "stale_target", "new_scope", "missing_proof"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, review, artifact, p := acceptanceFixture(t, nil)
			switch mode {
			case "self_review":
				p.Evaluations[0].ReviewTaskID = artifact.TaskID
			case "foreign_criterion":
				p.Evaluations[0].CriterionID = "not-original"
			case "missing_output_evaluation":
				resultArtifact := completeExtraArtifactForAcceptance(t, k, review)
				p.Outputs = append(p.Outputs, resultArtifact.ID)
			case "unapplied":
				if _, err := k.DB.ExecContext(ctx, "UPDATE kernel_run_bindings SET applied=0 WHERE session_id=?", review.SessionID); err != nil {
					t.Fatal(err)
				}
			case "unstopped":
				if _, err := k.DB.ExecContext(ctx, "UPDATE kernel_run_bindings SET process_stopped=0 WHERE session_id=?", review.SessionID); err != nil {
					t.Fatal(err)
				}
			case "live_owner":
				if _, err := k.DB.ExecContext(ctx, "UPDATE kernel_sessions SET process_stopped=0 WHERE id=?", review.SessionID); err != nil {
					t.Fatal(err)
				}
			case "writable_review":
				binding, err := k.SessionBinding(ctx, review.SessionID)
				if err != nil {
					t.Fatal(err)
				}
				binding.Sandbox = "workspace-write"
				raw, err := json.Marshal(binding)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = k.DB.ExecContext(ctx, "UPDATE kernel_sessions SET payload=? WHERE id=?", raw, binding.ID); err != nil {
					t.Fatal(err)
				}
			case "forged_result", "remaining":
				var r contract.TaskResult
				if err := k.Record(ctx, "task_result", review.ResultID, &r); err != nil {
					t.Fatal(err)
				}
				if mode == "remaining" {
					r.Remaining = []string{"unchecked requirement"}
				} else {
					r.Review.TargetDigest = "different"
				}
				if err := k.Put(ctx, "task_result", review.ResultID, review.ID, r); err != nil {
					t.Fatal(err)
				}
			case "corrupt_artifact":
				if err := os.WriteFile(filepath.Join(k.Root, "objects", artifact.ID), []byte("tampered"), 0600); err != nil {
					t.Fatal(err)
				}
			case "stale_target":
				w.PlanRevision++
			case "new_scope":
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				w.ScopeRevision++
				if err = saveWave(ctx, tx, w); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
			case "missing_proof":
				if _, err := k.DB.ExecContext(ctx, "DELETE FROM records WHERE kind='task_result_application' AND work_item_id=?", review.ID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.CheckArtifactAcceptance(ctx, w.Target(), p); err == nil {
				t.Fatal("unsupported acceptance succeeded", mode)
			}
		})
	}
}

// Review evidence itself is a real committed output, but is not an independently
// evaluated producer output. Adding it to the declared final output set fails.
func completeExtraArtifactForAcceptance(t *testing.T, k *KernelStore, review domain.Task) domain.Artifact {
	t.Helper()
	var result contract.TaskResult
	if err := k.Record(context.Background(), "task_result", review.ResultID, &result); err != nil {
		t.Fatal(err)
	}
	a, err := k.Artifact(context.Background(), result.Artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func TestArtifactAcceptanceDistinguishesFindingsAndCleanReplacement(t *testing.T) {
	ctx := context.Background()
	k, w, review, _, p := acceptanceFixture(t, []string{"artifact_defect"})
	if _, err := k.CheckArtifactAcceptance(ctx, w.Target(), p); !errors.Is(err, ErrArtifactAcceptanceHeld) {
		t.Fatal(err)
	}
	expansion, err := k.ExpandReviewTemplates(ctx, TemplateExpansionRequest{Target: w.Target(), ReviewTaskID: review.ID})
	if err != nil {
		t.Fatal(err)
	}
	var fixed domain.Artifact
	var clean domain.Task
	for _, id := range expansion.Admission.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if task.Spec.Kind == "fix" {
			fixed = completeTemplateTask(t, k, task, "fixed output", "", nil)
		} else {
			completeTemplateTask(t, k, task, "independent clean evaluation", fixed.Digest, nil)
			clean = task
		}
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	next := contract.ArtifactAcceptance{Outputs: []string{fixed.ID}, Evaluations: []contract.ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: fixed.ID, ReviewTaskID: clean.ID}}}
	proof, err := k.CheckArtifactAcceptance(ctx, w.Target(), next)
	if err != nil || proof.ArtifactDigests[fixed.ID] != fixed.Digest {
		t.Fatal(proof, err)
	}
	p.Evaluations[0].ReviewTaskID = clean.ID
	if _, err = k.CheckArtifactAcceptance(ctx, w.Target(), p); err == nil {
		t.Fatal("replacement review accepted old defective artifact")
	}
}

func acceptanceTwoCriteriaFixture(t *testing.T, mutate func(*contract.TaskResult)) (*KernelStore, domain.Wave, contract.ArtifactAcceptance) {
	t.Helper()
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.Criteria = append(request.Criteria, domain.WaveCriterion{ID: "BC", Requirement: "independently substantiate the second requirement"})
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	for i := range plan.Phases {
		plan.Phases[i].Tasks[0].Criteria = []string{"AC", "BC"}
	}
	admission, err := k.AdoptPlan(ctx, w.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	work, err := k.Task(ctx, admission.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	output := completeTemplateTask(t, k, work, "output covering both original requirements", "", nil)
	review, err := k.Task(ctx, admission.CreatedTaskIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, review.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	b = reserveSessionFixture(t, k, b)
	if err = k.BindSessionThread(ctx, b, "independent-review-thread"); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	evidence := mustImport(t, k, ArtifactImport{SessionID: b.ID, Target: b.Target, Key: "evaluation", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: review.ID}}, "independent evaluation")
	result := contract.TaskResult{SchemaVersion: 1, Target: b.Target, TaskID: review.ID, PhaseID: review.PhaseID, Outcome: "result", Artifacts: []string{evidence.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "checked the first requirement", EvidenceIDs: []string{evidence.ID}}, {CriterionID: "BC", Status: "claimed", Basis: "checked the second requirement", EvidenceIDs: []string{evidence.ID}}}, Review: &contract.ReviewResult{TargetDigest: output.Digest, Verdict: "clean", Findings: []contract.ReviewFinding{}}}
	if mutate != nil {
		mutate(&result)
	}
	recordAndStopTaskResult(t, k, b, "review-run", result)
	applied, err := k.ApplyTaskResult(ctx, b.ID, "review-run")
	if err != nil || applied.State != contract.TaskCompleted {
		t.Fatal(applied, err)
	}
	return k, w, contract.ArtifactAcceptance{Outputs: []string{output.ID}, Evaluations: []contract.ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: output.ID, ReviewTaskID: review.ID}, {CriterionID: "BC", ArtifactID: output.ID, ReviewTaskID: review.ID}}}
}
func TestArtifactAcceptanceRequiresAllCriteriaAndNoOutstandingWork(t *testing.T) {
	for _, mode := range []string{"complete", "missing_criterion", "unverified", "remaining", "blockers", "inconclusive"} {
		t.Run(mode, func(t *testing.T) {
			k, w, p := acceptanceTwoCriteriaFixture(t, func(r *contract.TaskResult) {
				switch mode {
				case "unverified":
					r.CriterionResults[1].Status = "unverified"
				case "remaining":
					r.Remaining = []string{"second check still needs confirmation"}
				case "blockers":
					r.Blockers = []string{"expected-value source unavailable"}
				case "inconclusive":
					r.Review.Verdict = "inconclusive"
				}
			})
			if mode == "missing_criterion" {
				p.Evaluations = p.Evaluations[:1]
			}
			_, err := k.CheckArtifactAcceptance(context.Background(), w.Target(), p)
			if mode == "complete" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrArtifactAcceptanceHeld) {
				t.Fatal("incomplete evaluation accepted", mode, err)
			}
		})
	}
}

func TestArtifactAcceptanceRetainsExplicitlyCarriedReviewAfterRestart(t *testing.T) {
	ctx := context.Background()
	k, w, review, _, p := acceptanceFixture(t, nil)
	admission, err := k.AdoptPlan(ctx, w.Target(), "carry-completed-work-and-review", storagePlanFixture())
	if err != nil || len(admission.CarriedTaskIDs) != 2 {
		t.Fatal(admission, err)
	}
	current, err := k.SessionBinding(ctx, review.SessionID)
	if err != nil || current.Target.PlanRevision != 1 {
		t.Fatal("carry rewrote reviewer history", current, err)
	}
	proof, err := k.CheckArtifactAcceptance(ctx, admission.Target, p)
	if err != nil || proof.Target.PlanRevision != 2 {
		t.Fatal(proof, err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.CheckArtifactAcceptance(ctx, admission.Target, p)
	if err != nil || !reflect.DeepEqual(replay, proof) {
		t.Fatal(replay, err)
	}
	if _, err = k.CheckArtifactAcceptance(ctx, w.Target(), p); !errors.Is(err, ErrArtifactAcceptanceHeld) {
		t.Fatal("old plan accepted as current", err)
	}
}
