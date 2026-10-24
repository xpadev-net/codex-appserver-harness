package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestReviewInputIncludesCurrentMachineEvidenceWithoutPrivateRunnerData(t *testing.T) {
	for _, status := range []string{"passed", "failed", "not_run", "prepared", "launching", "lost_terminal", "corrupt_job"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			k, w, plan, runtime, review, _ := productQualityFixture(t)
			snapshot, err := k.BuildSessionInput(ctx, review.SessionID, []contract.Target{w.Target()})
			if err != nil {
				t.Fatal(err)
			}
			before := storedManifest(t, k, snapshot)
			if before.ProductMachineVerification == nil || before.ProductMachineVerification.Passed || before.ProductMachineVerification.Complete || before.ProductMachineVerification.Jobs[0].State != "not_run" {
				t.Fatal("pre-run input claimed success")
			}
			old, err := json.Marshal(before)
			if err != nil {
				t.Fatal(err)
			}
			finished := status == "passed" || status == "failed"
			if status == "prepared" || status == "launching" {
				job, err := k.ReserveProductRunnerJob(ctx, plan.ID, "unit", runtime)
				if err != nil {
					t.Fatal(err)
				}
				if status == "launching" {
					if _, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest); err != nil {
						t.Fatal(err)
					}
				}
			}
			if finished || status == "lost_terminal" || status == "corrupt_job" {
				result := status
				if status == "lost_terminal" || status == "corrupt_job" {
					result = "passed"
				}
				job := completeProductRunnerFixture(t, k, plan, runtime, "unit", result)
				if status == "corrupt_job" {
					if _, err := k.DB.Exec("UPDATE records SET payload='{' WHERE kind='product_runner_job' AND id=?", job.ID); err != nil {
						t.Fatal(err)
					}

				}
				if status == "lost_terminal" {
					if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_runner_job_terminal' AND id=?", job.ID); err != nil {
						t.Fatal(err)
					}
				}
			}
			after := registrationManifest(t, k, review.SessionID, w.Target())
			if status == "lost_terminal" || status == "corrupt_job" {
				if after.ProductMachineVerification != nil || after.UnavailableProductMachineVerification != "machine_evidence_unavailable" {
					t.Fatal("lost proof fell back to pass")
				}
				return
			}
			input := after.ProductMachineVerification
			if input == nil || input.Candidate != plan.Value.Candidate || input.Plan.ID != plan.ID || input.Plan.Digest != plan.Digest || input.Passed != (status == "passed") || input.Complete != finished {
				t.Fatal("wrong fixed candidate observations", input)
			}
			if len(input.Jobs) != 1 || input.Jobs[0].ProfileID != "unit" || input.Digest == "" || input.EvidenceDigest == "" {
				t.Fatal("missing exact profile audit")
			}
			if finished && (input.Jobs[0].Status != status || input.Jobs[0].TestedSHA != plan.Value.Candidate.SHA || input.Jobs[0].CompletionDigest == "" || input.Jobs[0].LogDigest == "") {
				t.Fatal("missing stopped runner receipt")
			}
			if !finished && (input.Jobs[0].State != status || input.Jobs[0].Status != "" || input.Jobs[0].TestedSHA != "" || input.Jobs[0].CompletionDigest != "") {
				t.Fatal("unfinished job became a result")
			}
			raw, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			for _, private := range []string{k.Root, runtime.Root, "log_path", "environment", "argv", "fixture failure"} {
				if strings.Contains(string(raw), private) {
					t.Fatal("private runner data leaked")
				}
			}
			same, err := json.Marshal(storedManifest(t, k, snapshot))
			if err != nil || string(old) != string(same) {
				t.Fatal("historical input mutated")
			}
			task, err := k.Task(ctx, plan.Value.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := k.SessionBinding(ctx, task.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			verifier, err := k.sessionManifestTx(ctx, tx, binding, w)
			tx.Rollback()
			if err != nil {
				t.Fatal(err)
			}
			if verifier.ProductMachineVerification != nil || verifier.UnavailableProductMachineVerification != "" {
				t.Fatal("review-only summary leaked to verifier")
			}
			// No runner/model/effect is admitted by reading the summary.
			var completions int
			if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='product_wave_completion'").Scan(&completions); err != nil || completions != 0 {
				t.Fatal("input completed P")
			}
		})
	}
}

func TestReviewMachineInputPropagatesDatabaseErrors(t *testing.T) {
	ctx := context.Background()
	k, _, plan, runtime, review, _ := productQualityFixture(t)
	completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
	subject, err := k.ReadProductCandidateSubject(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("ALTER TABLE kernel_dispatch_intents RENAME TO unavailable_dispatch_intents"); err != nil {
		t.Fatal(err)
	}
	summary, unavailable, err := k.productReviewMachineInputTx(ctx, tx, subject)
	if err == nil || summary != nil || unavailable {
		t.Fatal("database error was hidden as ordinary unavailable evidence", summary, unavailable, err)
	}
}
