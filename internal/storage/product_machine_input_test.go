package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestReviewInputIncludesCurrentMachineEvidenceWithoutPrivateRunnerData(t *testing.T) {
	for _, status := range []string{"passed", "failed", "pending", "lost_terminal"} {
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
			if status != "pending" {
				result := status
				if status == "lost_terminal" {
					result = "passed"
				}
				job := completeProductRunnerFixture(t, k, plan, runtime, "unit", result)
				if status == "lost_terminal" {
					if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_runner_job_terminal' AND id=?", job.ID); err != nil {
						t.Fatal(err)
					}
				}
			}
			after := registrationManifest(t, k, review.SessionID, w.Target())
			if status == "lost_terminal" {
				if after.ProductMachineVerification != nil || after.UnavailableProductMachineVerification != "machine_evidence_unavailable" {
					t.Fatal("lost proof fell back to pass")
				}
				return
			}
			input := after.ProductMachineVerification
			if input == nil || input.Candidate != plan.Value.Candidate || input.Plan.ID != plan.ID || input.Plan.Digest != plan.Digest || input.Passed != (status == "passed") || input.Complete != (status != "pending") {
				t.Fatal("wrong fixed candidate observations", input)
			}
			if len(input.Jobs) != 1 || input.Jobs[0].ProfileID != "unit" || input.Digest == "" || input.EvidenceDigest == "" {
				t.Fatal("missing exact profile audit")
			}
			if status != "pending" && (input.Jobs[0].Status != status || input.Jobs[0].TestedSHA != plan.Value.Candidate.SHA || input.Jobs[0].CompletionDigest == "" || input.Jobs[0].LogDigest == "") {
				t.Fatal("missing stopped runner receipt")
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
