package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

type productWaveIssueAPI struct{ issue github.Issue }

func (a productWaveIssueAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch path {
	case "":
		return json.Marshal(map[string]any{"id": 17, "full_name": "owner/repo"})
	case "issues/42":
		return json.Marshal(a.issue)
	default:
		return nil, errors.New("unexpected product Wave source read")
	}
}

func (productWaveIssueAPI) ResourcePages(context.Context, string, string) ([]json.RawMessage, error) {
	return nil, errors.New("single Issue source must not enumerate the pool")
}

func TestProductWaveAdmissionsReachTheSameExecutorReadiness(t *testing.T) {
	for _, source := range []string{"direct", "issue_pool"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			k, main, err := openKernel(ctx, cliRoot(t))
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()

			config, runtime := productModelRuntimeFixture(t, k, main.Model)
			repo := config.Repositories[0].Policy.Repository
			for _, args := range [][]string{{"init", "--initial-branch=trunk"}, {"config", "user.name", "product Wave integration"}, {"config", "user.email", "product-wave@example.invalid"}, {"-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "base"}} {
				if _, err := workspace.Git(ctx, repo, args...); err != nil {
					t.Fatal(err)
				}
			}
			config.Repositories[0].InitialSHA, err = workspace.Resolve(ctx, repo, "HEAD")
			if err != nil {
				t.Fatal(err)
			}

			var wave domain.Wave
			definition := storage.ProductWorkItemDefinition{ID: "r17-i42", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}
			if source == "direct" {
				wave, err = k.CreateProductWave(ctx, storage.ProductWaveRequest{Wave: storage.WaveRequest{OrchestratorSessionID: main.SessionID, Source: "direct", SourceID: "direct-integration", OriginalRequest: "implement behavior", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "preserve behavior"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: main.Limits}}, WorkItems: []storage.ProductWorkItemDefinition{definition}})
			} else {
				var body = "implement behavior\n\x60\x60\x60harness-scope\n" + `{"revision":1,"original_request":"source","criteria":[{"id":"AC","requirement":"preserve behavior"}],"included":["AC"],"remaining":[],"previously_merged":[]}` + "\n\x60\x60\x60"
				issue := github.Issue{Number: 42, Title: "product request", State: "open", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Body: body}
				issue.Labels = append(issue.Labels, struct {
					Name string `json:"name"`
				}{Name: "ready"})
				admitter := product.IssueAdmitter{Store: k, API: productWaveIssueAPI{issue: issue}, Policy: product.IssueSourcePolicy{Repository: "owner/repo", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}, SessionID: main.SessionID, Limits: main.Limits}
				wave, err = admitter.Admit(ctx, 42)
			}
			if err != nil {
				t.Fatal("source admission failed", err)
			}
			wantSource, wantSourceID := "direct", "direct-integration"
			if source == "issue_pool" {
				wantSource, wantSourceID = "github", "r17-i42"
			}
			if wave.Source != wantSource || wave.SourceID != wantSourceID || wave.Policy.Stage != "P" || !wave.Policy.Product {
				t.Fatal("admission did not create a product Wave", wave)
			}

			taskSpec := contract.TaskSpec{ID: "implementation", Kind: "work", Instruction: "implement behavior", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{definition.ID}}
			plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "implementation", Tasks: []contract.TaskSpec{taskSpec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{taskSpec.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
			admitted, err := k.AdoptPlan(ctx, wave.Target(), "product-wave-integration-plan", plan)
			if err != nil || len(admitted.CreatedTaskIDs) != 1 {
				t.Fatal("could not plan the source WorkItem", admitted, err)
			}

			pipeline, err := loadProductExecutorPipeline(operatorFile(t, config), k, runtime, false)
			if err != nil || pipeline.Worker == nil {
				t.Fatal("product executor pipeline failed to load", err)
			}
			before, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: wave.Target()})
			if err != nil || len(before.Tasks) != 0 {
				t.Fatal("source admission alone made a Task model-ready", before, err)
			}

			runCtx, cancel := context.WithCancel(ctx)
			errorsSeen := make(chan error, 8)
			done := make(chan error, 1)
			go func() { done <- pipeline.Worker(runCtx, k, func() {}, func(err error) { errorsSeen <- err }) }()
			deadline := time.Now().Add(10 * time.Second)
			for {
				select {
				case err := <-errorsSeen:
					cancel()
					<-done
					t.Fatal("product executor reported an error", err)
				default:
				}
				ready, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: wave.Target()})
				if err != nil {
					cancel()
					<-done
					t.Fatal(err)
				}
				if len(ready.Tasks) == 1 {
					if ready.Tasks[0].Task.ID != admitted.CreatedTaskIDs[0] {
						cancel()
						<-done
						t.Fatal("executor admitted the wrong Task", ready)
					}
					break
				}
				if time.Now().After(deadline) {
					cancel()
					<-done
					t.Fatal("source-admitted Task never reached executor readiness", ready)
				}
				time.Sleep(10 * time.Millisecond)
			}
			workspaceState, err := k.ProductWorkspace(ctx, admitted.CreatedTaskIDs[0])
			if err != nil || workspaceState.State != "ready" {
				cancel()
				<-done
				t.Fatal("runtime-ready Task lacks an executor workspace", workspaceState, err)
			}
			runID := seedStoppedProductRun(t, k, wave, admitted.CreatedTaskIDs[0], workspaceState)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal("product worker did not stop cleanly", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("product executor did not join")
			}
			after, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: wave.Target()})
			if err != nil || len(after.Tasks) != 0 {
				t.Fatal("stopped executor retained product readiness", after, err)
			}
			executor := product.Executor{Store: k, Repositories: map[int64]product.ExecutionRepository{17: config.Repositories[0]}}
			if _, err := executor.Apply(ctx, admitted.CreatedTaskIDs[0], runID); err != nil {
				t.Fatal("runtime-ready Wave's normally stopped result could not be sealed", err)
			}
			applied, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
			if err != nil || applied.State != contract.TaskCompleted {
				t.Fatal("stopped product result was not atomically adopted", applied, err)
			}
			candidate, err := k.ReadProductTaskCandidate(ctx, applied.ResultID)
			if err != nil || candidate.Value.RunID != runID || candidate.Value.Target.WaveID != wave.ID {
				t.Fatal("runtime-ready executor did not seal the Wave candidate", candidate, err)
			}
			workItem, err := k.ReadProductTarget(ctx, wave.Target(), definition.ID)
			if err != nil || workItem.Value.Candidate.SHA != candidate.Value.Candidate.SHA || workItem.Value.WorkItem.Phase != domain.Verifying {
				t.Fatal("candidate adoption did not update its registered WorkItem", workItem, err)
			}
		})
	}
}

// This fixture supplies a synthetic stopped model result while preserving the
// real executor readiness, Git workspace, candidate seal, and SQLite adoption path.
func seedStoppedProductRun(t *testing.T, k *storage.KernelStore, wave domain.Wave, taskID string, ready storage.ProductWorkspace) string {
	t.Helper()
	ctx := context.Background()
	task, err := k.Task(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, binding.ID, []contract.Target{wave.Target()})
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
	binding.ThreadID, binding.PreparationID = "integration-thread-"+task.ID, "integration-preparation-"+task.ID
	runID := "integration-run-" + task.ID
	run := contract.SessionRun{ID: runID, ThreadID: binding.ThreadID, TurnID: "integration-turn", State: "completed", BaselineKnown: true, BaselineTurnIDs: []string{}, Started: time.Now().UTC(), Final: string(raw)}
	task.State, task.Attempts = contract.TaskRunning, 1
	budget := storage.RunBudgetBinding{RunID: run.ID, SessionID: binding.ID, TaskID: task.ID, ThreadID: binding.ThreadID, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest, Targets: []contract.Target{wave.Target()}}
	tx, err := k.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	bindingRaw, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	taskRaw, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE kernel_sessions SET payload=?,thread_id=?,active_run_id=?,process_stopped=1,creating=0 WHERE id=?", bindingRaw, binding.ThreadID, run.ID, binding.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE kernel_tasks SET state=?,payload=? WHERE id=?", task.State, taskRaw, task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO kernel_run_bindings(run_id,session_id,snapshot_id,thread_id,state,process_stopped,applied) VALUES(?,?,?,?,'completed',1,0)", run.ID, binding.ID, snapshot.ID, binding.ThreadID); err != nil {
		t.Fatal(err)
	}
	for _, record := range []struct {
		kind  string
		value any
	}{{"agent_run", run}, {"run_budget", budget}} {
		payload, err := json.Marshal(record.value)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", record.kind, run.ID, binding.ID, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ready.Value.CWD, "implementation.txt"), []byte("implemented behavior\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return run.ID
}
