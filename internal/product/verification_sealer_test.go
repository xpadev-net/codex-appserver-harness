package product

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func seedVerificationCompletedRun(t *testing.T, k *storage.KernelStore, task domain.Task, result contract.TaskResult) contract.SessionRun {
	t.Helper()
	ctx := context.Background()
	snapshot, err := k.BuildSessionInput(ctx, task.SessionID, []contract.Target{task.Target()})
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	b.ThreadID, b.PreparationID = "verification-thread-"+task.ID, "verification-preparation-"+task.ID
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := contract.DecodeStrict(raw, &contract.TaskResult{}); err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "verification-run-" + task.ID, ThreadID: b.ThreadID, TurnID: "verification-turn-" + task.ID, State: "completed", BaselineKnown: true, BaselineTurnIDs: []string{}, Started: time.Now().UTC(), Final: string(raw)}
	task.State, task.Attempts = contract.TaskRunning, 1
	budget := storage.RunBudgetBinding{RunID: run.ID, SessionID: b.ID, TaskID: task.ID, ThreadID: b.ThreadID, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest, Targets: []contract.Target{task.Target()}}
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
	return run
}

func TestProductVerificationSealerChecksRealDiffAndTrackedPlanBeforeReceipt(t *testing.T) {
	for _, mode := range []string{"success", "runner_cancel", "runner_retry", "host_executor", "publication", "publication_push", "publication_noop", "publication_unknown", "publication_partial", "publication_stopped", "publication_metadata", "publication_cancel", "publication_retry_twice", "publication_fence", "publication_pause", "product_change", "missing_test"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s, implementation, producer, run := candidateSealerFixture(t, func(plan *contract.PlanProposal) {
				plan.Phases[1].Tasks[0].Kind = "verification"
				if strings.HasPrefix(mode, "publication") {
					verifier := plan.Phases[1].Tasks[0]
					review := contract.TaskSpec{ID: "publication-review", Kind: "review", Instruction: "independently evaluate the fixed code/tests against original AC", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: verifier.ID}}, InputArtifactIDs: []string{}, WorkItemIDs: append([]string{}, verifier.WorkItemIDs...)}
					plan.Phases = append(plan.Phases, contract.PhaseSpec{ID: "local-quality", Tasks: []contract.TaskSpec{review}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{review.ID}}, FailurePolicy: "drain_and_block"})
				}
			})
			input, err := s.Seal(ctx, producer.ID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Store.AdoptProductTaskResult(ctx, producer.SessionID, run.ID, "trunk"); err != nil {
				t.Fatal(err)
			}
			tasks, err := s.Store.Tasks(ctx, producer.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			var task domain.Task
			for _, candidate := range tasks {
				if candidate.Spec.Kind == "verification" {
					task = candidate
				}
			}
			subject, err := s.Store.BindProductCandidateSubject(ctx, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			profileScript := "grep -qx 'implemented behavior' implementation.txt && test -f tests/behavior_test.go && printf PASS"
			if mode == "runner_cancel" {
				profileScript = "sleep 30; printf PASS"
			}
			if _, err := s.Store.RegisterProductVerificationPolicy(ctx, task.Target(), storage.ProductVerificationPolicy{WaveID: task.WaveID, PolicyRevision: task.PolicyRevision, RepositoryID: implementation.Value.Request.RepositoryID, Repository: implementation.Value.Request.Repository, VerifierPaths: []string{"tests/"}, Verification: policy.Verification{Revision: task.PolicyRevision, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"sh", "-c", profileScript}, TimeoutSeconds: 30, SuccessPattern: "PASS"}}}}); err != nil {
				t.Fatal(err)
			}
			var ready storage.ProductWorkspace
			var host Executor
			if mode == "host_executor" || strings.HasPrefix(mode, "publication") {
				repo := executorRepositoryFixture(implementation.Value.Request)
				pinned, readErr := s.Store.ReadProductVerificationPolicy(ctx, task.Target(), repo.Policy.RepositoryID)
				if readErr != nil {
					t.Fatal(readErr)
				}
				repo.Policy = pinned.Value
				host = Executor{Store: s.Store, Repositories: map[int64]ExecutionRepository{repo.Policy.RepositoryID: repo}}
				ready, err = host.Prepare(ctx, task.ID)
			} else {
				ready, err = (WorkspacePreparer{Store: s.Store}).Prepare(ctx, storage.ProductWorkspaceRequest{TaskID: task.ID, RepositoryID: implementation.Value.Request.RepositoryID, Repository: implementation.Value.Request.Repository, BaseSHA: input.Candidate.SHA})
			}
			if err != nil {
				t.Fatal(err)
			}
			task, err = s.Store.Task(ctx, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(ready.Value.CWD, "tests"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(ready.Value.CWD, "tests", "behavior_test.go"), []byte("package tests\nimport \"testing\"\nfunc TestBehavior(t *testing.T) {}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "product_change" {
				if err := os.WriteFile(filepath.Join(ready.Value.CWD, "implementation.txt"), []byte("unauthorized product change\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			plan := verification.Plan{SchemaVersion: 1, WorkItemID: ready.Value.WorkItemID, ScopeRevision: task.ScopeRevision, PolicyRevision: task.PolicyRevision, InputCandidateID: input.Candidate.ID, Revision: 1, CriteriaTests: []verification.Mapping{{CriterionID: "AC", TestRef: "tests/behavior_test.go::TestBehavior", ExpectationBasis: "original AC requirement", ProfileID: "unit"}}, RequestedProfiles: []string{}, UnverifiedCriteria: []string{}, Blockers: []string{}, OnSuccess: "publish_pr"}
			if mode == "missing_test" {
				plan.CriteriaTests[0].TestRef = "tests/missing_test.go::TestMissing"
			}
			raw, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			a, err := s.Store.ImportArtifact(ctx, storage.ArtifactImport{SessionID: task.SessionID, Target: task.Target(), Key: "verification-plan", Kind: "verification_plan", ContentType: "application/json", Origin: domain.ArtifactOrigin{Kind: "task", Reference: task.ID}}, raw)
			if err != nil {
				t.Fatal(err)
			}
			result := contract.TaskResult{SchemaVersion: 3, Target: task.Target(), TaskID: task.ID, PhaseID: task.PhaseID, Outcome: "result", Artifacts: []string{a.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "unverified", Basis: "ready for formal runner", EvidenceIDs: []string{}}}, WorkItemRefs: []contract.CandidateReference{}, ProductChanges: []contract.ProductChangeReference{{WorkItemID: ready.Value.WorkItemID, WorkspaceID: ready.ID, WorkspaceDigest: ready.Digest}}, ProductVerification: &contract.ProductVerificationReference{SubjectID: subject.ID, SubjectDigest: subject.Digest, PlanArtifactID: a.ID}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}}
			verificationRun := seedVerificationCompletedRun(t, s.Store, task, result)
			sealed, err := s.Seal(ctx, task.ID, verificationRun.ID)
			if mode == "product_change" || mode == "missing_test" {
				if err == nil || sealed.State != "failed" || sealed.Candidate != nil {
					t.Fatal("invalid verification changes sealed", mode, sealed, err)
				}
				head, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD")
				if err != nil || head != input.Candidate.SHA {
					t.Fatal("rejected verification committed", head, err)
				}
				return
			}
			if err != nil || sealed.State != "sealed" || sealed.Candidate.SHA == input.Candidate.SHA || sealed.Candidate.BaseSHA != input.Candidate.BaseSHA || sealed.VerificationPlan == nil || sealed.VerificationPlan.InputCandidateID != sealed.Candidate.ID {
				t.Fatal(sealed, err)
			}
			if err := sealed.VerificationPlan.ValidateCommit(workspace.WithStoppedCommands(ctx), ready.Value.CWD, sealed.Candidate.SHA); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(s.Store.Root, "objects", a.ID))
			if err != nil || string(data) != string(raw) {
				t.Fatal("plan artifact rewritten", err)
			}
			var observed contract.SessionRun
			if err := s.Store.Record(ctx, "agent_run", verificationRun.ID, &observed); err != nil || observed.Final != verificationRun.Final {
				t.Fatal("raw final rewritten", err)
			}
			p, err := s.Store.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
			if err != nil || p.Value.Candidate != *input.Candidate {
				t.Fatal("local sealing bypassed host adoption", p, err)
			}
			var receipt storage.ResultApplication
			if mode == "host_executor" || strings.HasPrefix(mode, "publication") {
				receipt, err = host.Apply(ctx, task.ID, verificationRun.ID)
			} else {
				receipt, err = s.Store.AdoptProductVerificationTaskResult(ctx, task.SessionID, verificationRun.ID, "trunk")
			}
			if err != nil || receipt.State != contract.TaskCompleted {
				t.Fatal(receipt, err)
			}
			sidecar, err := s.Store.ReadProductTaskCandidate(ctx, receipt.ResultID)
			if err != nil || sidecar.Value.Candidate != *sealed.Candidate || sidecar.Value.VerificationPlan == nil {
				t.Fatal(sidecar, err)
			}
			canonical, err := s.Store.ReadProductVerificationPlan(ctx, sidecar.Value.VerificationPlan.ID)
			if err != nil || canonical.Value.Candidate != *sealed.Candidate || canonical.Value.Plan.InputCandidateID != sealed.Candidate.ID {
				t.Fatal(canonical, err)
			}
			p, err = s.Store.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
			if err != nil || p.Value.Candidate != *sealed.Candidate || p.Value.WorkItem.Phase != domain.Verifying {
				t.Fatal("adoption invented runner pass", p, err)
			}
			if strings.HasPrefix(mode, "publication") {
				checkProductExecutorVerification(t, s.Store, canonical)
				checkKernelPublicationGate(t, host, canonical, mode)
			} else if mode == "host_executor" {
				checkProductExecutorVerification(t, s.Store, canonical)
			} else if mode == "runner_retry" {
				checkProductRunnerRetryExecution(t, s.Store, canonical)
			} else {
				checkProductRunnerExecution(t, s.Store, canonical, mode == "runner_cancel")
			}
		})
	}
}
