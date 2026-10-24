package storage

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productParentRun(t *testing.T, k *KernelStore, targets ...contract.Target) (contract.SessionBinding, contract.SessionRun, contract.InputSnapshot) {
	t.Helper()
	ctx := context.Background()
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	var active string
	if err := k.DB.QueryRow("SELECT active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != "" {
		var old contract.SessionRun
		if err := k.Record(ctx, "agent_run", active, &old); err != nil {
			t.Fatal(err)
		}
		old.State = "completed"
		if old.TurnID == "" {
			old.TurnID = "prior-product-turn"
		}
		if err := k.Put(ctx, "agent_run", old.ID, b.ID, old); err != nil {
			t.Fatal(err)
		}
		if err := k.MarkSessionStopped(ctx, b.ID, old.ID); err != nil {
			t.Fatal(err)
		}
		if err := k.ArchiveSessionRun(ctx, b.ID, old.ID, "fixture prior publication turn finished"); err != nil {
			t.Fatal(err)
		}
	}
	b = prepareWakeSession(t, k, b.ID)
	snapshot, err := k.BuildSessionInput(ctx, b.ID, targets)
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "product-final-parent", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err := k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal(err)
	}
	return b, run, snapshot
}
func productAcceptanceResult(t *testing.T, k *KernelStore, b contract.SessionBinding, target contract.Target) contract.MainResult {
	t.Helper()
	evidence, err := k.ReadProductFinalVerificationEvidence(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	criteria := []string{}
	w, err := k.Wave(context.Background(), target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	for _, criterion := range w.Criteria {
		criteria = append(criteria, criterion.ID)
	}
	p := contract.ProductAcceptance{CoverageDigest: evidence.Value.CoverageDigest, FinalPlanID: evidence.Value.Plan.ID, FinalPlanDigest: evidence.Value.Plan.Digest, FinalVerificationDigest: evidence.Digest, CriterionIDs: criteria}
	return contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{{Target: target, Action: "accept", ProductAcceptance: &p, Summary: "all original criteria covered by merged work and exact fixed-base final verification", Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}}}}
}
func finishProductCloudForParent(t *testing.T, k *KernelStore, plan ProductFinalPlan) {
	t.Helper()
	ctx := context.Background()
	for _, repo := range plan.Value.Repositories {
		for mergeID := range repo.MergeReceipts {
			merge, err := k.ReadProductMerge(ctx, mergeID)
			if err != nil {
				t.Fatal(err)
			}
			round, err := k.ReadProductCloudRound(ctx, merge.Value.RequestID)
			if err != nil {
				t.Fatal(err)
			}
			report := contract.NewReport(round.Product.Value.Repository)
			report.RepositoryID, report.DefaultBranch = repo.RepositoryID, repo.BaseBranch
			for _, name := range []string{"cloud_request", "review_sha"} {
				report.Capabilities[name] = contract.Capability{State: contract.Verified, Evidence: "fixture mapped original provider terminal response"}
			}
			value := ProductCloudCompletionValue{OperationID: round.OperationID, RoundDigest: round.Digest, ProofID: "parent-cloud-terminal", Report: report, Reviews: []ProductNativeReviewFact{{ID: 101, ReviewerID: 91, State: "APPROVED", SHA: round.Product.Value.Candidate.SHA, SubmittedAt: k.clock()}}, ReadAt: k.clock()}
			if _, err := k.RecordProductCloudReviewCompleted(ctx, value); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestProductParentAcceptanceCompletesOnlyNormalStoppedExactInputAndReplays(t *testing.T) {
	ctx := context.Background()
	k, plan, _ := completeFinalEvidenceFixture(t, "passed")
	finishProductCloudForParent(t, k, plan)
	b, run, snapshot := productParentRun(t, k, plan.Value.Target)
	result := productAcceptanceResult(t, k, b, plan.Value.Target)
	run = saveMainCompletion(t, k, b, run, result, true)
	applied, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || applied.State != "applied" || len(applied.ProductCompletions) != 1 || len(applied.Completions) != 0 {
		t.Fatal(applied, err)
	}
	completion := applied.ProductCompletions[0]
	if completion.Proof.SnapshotID != snapshot.ID || completion.Proof.SnapshotDigest != snapshot.Digest || completion.Proof.Manifest.ID == "" || completion.Proof.PlanDigest == "" || completion.Proof.Selection.ID != plan.ID {
		t.Fatal("lost actual main input/fresh plan binding", completion)
	}
	w, err := k.Wave(ctx, plan.Value.Target.WaveID)
	if err != nil || w.State != domain.WaveCompleted || w.CompletedAt.IsZero() {
		t.Fatal(w, err)
	}
	var proof ProductWaveCompletion
	if err := k.Record(ctx, "product_wave_completion", completion.ID, &proof); err != nil || !reflect.DeepEqual(proof, completion) {
		t.Fatal(proof, err)
	}
	var generic int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='wave_completion'").Scan(&generic); err != nil || generic != 0 {
		t.Fatal("product acceptance used a generic artifact proof", generic, err)
	}
	replay, err := k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || !reflect.DeepEqual(replay, applied) {
		t.Fatal("completion reapplied", replay, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err = k.ApplyMainResult(ctx, b.ID, run.ID)
	if err != nil || !reflect.DeepEqual(replay, applied) {
		t.Fatal("completion lost after reopen", replay, err)
	}
}

func TestProductParentAcceptanceHoldsChangedInputsUnknownWorkAndRollback(t *testing.T) {
	for _, mode := range []string{"live", "unknown", "wrong_digest", "criterion", "pause", "stale_observation", "new_candidate", "new_head", "cloud_capacity", "runner_capacity", "pending_operation", "managed_process", "grant_loss", "grant_foreign", "manifest_loss", "manifest_corrupt", "event_failure", "receipt_failure", "proof_failure", "failed_final"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			status := "passed"
			if mode == "failed_final" {
				status = "failed"
			}
			k, plan, _ := completeFinalEvidenceFixture(t, status)
			if mode != "cloud_capacity" {
				finishProductCloudForParent(t, k, plan)
			}
			target := plan.Value.Target
			b, run, snapshot := productParentRun(t, k, target)
			result := productAcceptanceResult(t, k, b, target)
			if mode == "wrong_digest" {
				result.Decisions[0].ProductAcceptance.FinalVerificationDigest = strings.Repeat("0", 64)
			}
			if mode == "criterion" {
				result.Decisions[0].ProductAcceptance.CriterionIDs = []string{"other"}
			}
			run = saveMainCompletion(t, k, b, run, result, mode != "live")
			switch mode {
			case "unknown":
				run.State = "unknown"
				raw, _ := json.Marshal(run)
				if _, err := k.DB.Exec("UPDATE records SET payload=? WHERE kind='agent_run' AND id=?", raw, run.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := k.DB.Exec("UPDATE kernel_run_bindings SET state='unknown' WHERE run_id=?", run.ID); err != nil {
					t.Fatal(err)
				}
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-final-parent", Target: target, Action: "pause", Reason: "hold final acceptance"}); err != nil {
					t.Fatal(err)
				}
			case "stale_observation":
				now := k.clock().Add(31 * time.Second)
				k.Now = func() time.Time { return now }
			case "new_candidate":
				p, err := k.ReadProductTarget(ctx, target, "work")
				if err != nil {
					t.Fatal(err)
				}
				next := p.Value
				next.WorkItem.Version++
				next.Candidate.ID = "next-after-final"
				next.WorkItem.Candidate = next.Candidate.ID
				if _, err := k.SetProductTarget(ctx, p.Digest, next); err != nil {
					t.Fatal(err)
				}
			case "new_head":
				heads := append([]ProductFinalHeadObservation{}, plan.Admission...)
				heads[0].HeadSHA = strings.Repeat("b", 40)
				heads[0].ObservedAt = k.clock()
				if _, err := k.PrepareProductFinalPlan(ctx, target, plan.Value.CoverageDigest, heads); err != nil {
					t.Fatal(err)
				}
			case "runner_capacity":
				if _, err := k.ReserveOperationCapacity(ctx, OperationCapacityRequest{ID: "unresolved-final-work", Kind: "runner", SessionID: b.ID, Target: target, Worktree: filepath.Join(k.Root, "unknown-final-work")}); err != nil {
					t.Fatal(err)
				}
			case "pending_operation":
				raw, _ := json.Marshal(kernelToolOperation{ID: "uncertain-final", SessionID: b.ID, Target: target})
				if _, err := k.DB.Exec(`INSERT INTO kernel_tool_operations(id,session_id,thread_id,turn_id,call_id,name,argument_digest,state,payload) VALUES('uncertain-final',?,'thread','turn','call','pr_publish','digest','unknown',?)`, b.ID, raw); err != nil {
					t.Fatal(err)
				}
			case "managed_process":
				if err := k.Put(ctx, "managed_process", "unconfirmed-final-child", target.WaveID, map[string]any{"state": "unknown", "request": map[string]any{"binding": map[string]string{"id": b.ID}}}); err != nil {
					t.Fatal(err)
				}
			case "grant_loss":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='session_input_references' AND id=?", snapshot.ID); err != nil {
					t.Fatal(err)
				}
			case "grant_foreign":
				var grant inputReferenceGrant
				if err := k.Record(ctx, "session_input_references", snapshot.ID, &grant); err != nil {
					t.Fatal(err)
				}
				grant.SessionID = "foreign"
				if err := k.Put(ctx, "session_input_references", snapshot.ID, b.ID, grant); err != nil {
					t.Fatal(err)
				}
			case "manifest_loss", "manifest_corrupt":
				ref := inputContext(t, snapshot).Waves[0].Manifest
				query := "DELETE FROM records WHERE kind='session_input_manifest' AND id=?"
				if mode == "manifest_corrupt" {
					query = "UPDATE records SET payload='{}' WHERE kind='session_input_manifest' AND id=?"
				}
				if _, err := k.DB.Exec(query, ref.ID); err != nil {
					t.Fatal(err)
				}
			case "event_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_product_final_event BEFORE INSERT ON kernel_events WHEN NEW.kind='wave_completed' BEGIN SELECT RAISE(FAIL,'final event rollback'); END"); err != nil {
					t.Fatal(err)
				}
			case "receipt_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_product_final_receipt BEFORE INSERT ON records WHEN NEW.kind='main_result_application' BEGIN SELECT RAISE(FAIL,'final receipt rollback'); END"); err != nil {
					t.Fatal(err)
				}
			case "proof_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_product_final_proof BEFORE INSERT ON records WHEN NEW.kind='product_wave_completion' BEGIN SELECT RAISE(FAIL,'final proof rollback'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if applied, err := k.ApplyMainResult(ctx, b.ID, run.ID); err == nil {
				t.Fatal("unverified product parent completed", mode, applied)
			}
			w, err := k.Wave(ctx, target.WaveID)
			if err != nil || w.State == domain.WaveCompleted || !w.CompletedAt.IsZero() {
				t.Fatal("held final acceptance changed Wave", w, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_wave_completion','wave_completion')").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial completion proof", count, err)
			}
			if err := k.DB.QueryRow("SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&count); err != nil || count != 0 {
				t.Fatal("held completion applied main run", count, err)
			}
		})
	}
}
func TestProductParentAcceptanceCannotConsumeFinalFactsProducedAfterItsInput(t *testing.T) {
	ctx := context.Background()
	k, plan, _, runtime := finalRunnerFixture(t)
	finishProductCloudForParent(t, k, plan)
	b, run, snapshot := productParentRun(t, k, plan.Value.Target)
	job, err := k.ReserveProductFinalRunnerJob(ctx, plan.ID, 17, "unit", runtime)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.ClaimProductRunnerJob(ctx, job.ID, job.Digest)
	if err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	if _, err := k.FinishProductRunnerJob(ctx, job.ID, job.Digest, productRunnerCompletionFixture(claim.Job)); err != nil {
		t.Fatal(err)
	}
	result := productAcceptanceResult(t, k, b, plan.Value.Target)
	run = saveMainCompletion(t, k, b, run, result, true)
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); !errors.Is(err, ErrProductAcceptanceHeld) {
		t.Fatal("current result replaced actual pending input", err)
	}
	var old savedInputManifest
	if err := k.Record(ctx, "session_input_manifest", inputContext(t, snapshot).Waves[0].Manifest.ID, &old); err != nil {
		t.Fatal(err)
	}
	var manifest sessionManifest
	if err := json.Unmarshal(old.Data, &manifest); err != nil || manifest.ProductFinalVerification.Value.Passed {
		t.Fatal("old input was rewritten", err)
	}
}
func TestProductParentAcceptanceNeverUsesGenericKProof(t *testing.T) {
	ctx := context.Background()
	k, plan, _ := completeFinalEvidenceFixture(t, "passed")
	finishProductCloudForParent(t, k, plan)
	b, run, _ := productParentRun(t, k, plan.Value.Target)
	result := contract.MainResult{SchemaVersion: 1, SessionID: b.ID, Decisions: []contract.MainDecision{{Target: plan.Value.Target, Action: "accept", Acceptance: &contract.ArtifactAcceptance{Outputs: []string{"output"}, Evaluations: []contract.ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: "output", ReviewTaskID: "review"}}}, Summary: "generic proof must not complete a product Wave"}}}
	run = saveMainCompletion(t, k, b, run, result, true)
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("generic acceptance completed product Wave")
	}
	k2, w, _, _, _ := acceptanceFixture(t, nil)
	b2, run2, _ := reserveBudgetMain(t, k2, "product-on-generic", w.Target())
	proposal := contract.ProductAcceptance{CoverageDigest: strings.Repeat("a", 64), FinalPlanID: "final", FinalPlanDigest: strings.Repeat("b", 64), FinalVerificationDigest: strings.Repeat("c", 64), CriterionIDs: []string{"AC"}}
	result = contract.MainResult{SchemaVersion: 1, SessionID: b2.ID, Decisions: []contract.MainDecision{{Target: w.Target(), Action: "accept", ProductAcceptance: &proposal, Summary: "product proof must not complete generic Wave"}}}
	run2 = saveMainCompletion(t, k2, b2, run2, result, true)
	if _, err := k2.ApplyMainResult(ctx, b2.ID, run2.ID); err == nil {
		t.Fatal("product acceptance completed generic Wave")
	}
}

func TestProductParentAcceptanceValidatesWholeMixedBatchBeforeAnyCompletion(t *testing.T) {
	ctx := context.Background()
	k, plan, _ := completeFinalEvidenceFixture(t, "passed")
	finishProductCloudForParent(t, k, plan)
	generic, err := k.CreateWave(ctx, WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "mixed-generic", OriginalRequest: "an independently reviewed generic result", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "verified report"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "K", Limits: domain.DefaultKernelLimits()}})
	if err != nil {
		t.Fatal(err)
	}
	b, run, _ := productParentRun(t, k, plan.Value.Target, generic.Target())
	result := productAcceptanceResult(t, k, b, plan.Value.Target)
	result.Decisions = append(result.Decisions, contract.MainDecision{Target: generic.Target(), Action: "accept", Acceptance: &contract.ArtifactAcceptance{Outputs: []string{"missing"}, Evaluations: []contract.ArtifactCriterionEvaluation{{CriterionID: "AC", ArtifactID: "missing", ReviewTaskID: "missing-review"}}}, Summary: "this proposal lacks independent evidence"})
	run = saveMainCompletion(t, k, b, run, result, true)
	if _, err := k.ApplyMainResult(ctx, b.ID, run.ID); err == nil {
		t.Fatal("mixed batch accepted missing generic evidence")
	}
	for _, id := range []string{plan.Value.Target.WaveID, generic.ID} {
		w, err := k.Wave(ctx, id)
		if err != nil || w.State == domain.WaveCompleted {
			t.Fatal("mixed batch partially completed", w, err)
		}
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('wave_completion','product_wave_completion')").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}
