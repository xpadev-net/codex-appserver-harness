package product

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func existingBaseExecutorFixture(t *testing.T, mode string) (Executor, domain.Task, *finalReadFixture, string, string) {
	t.Helper()
	ctx := context.Background()
	preparer, request, w, originalTask := workspacePreparerFixture(t)
	k := preparer.Store
	repo := executorRepositoryFixture(request)
	remote := t.TempDir()
	if _, err := workspace.Git(ctx, request.Repository, "clone", "--no-local", "--", request.Repository, remote); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"config", "user.name", "base integration"}, {"config", "user.email", "base-integration@example.invalid"}} {
		if _, err := workspace.Git(ctx, remote, args...); err != nil {
			t.Fatal(err)
		}
	}
	changedPath := "candidate.txt"
	if mode == "conflict" {
		changedPath = "base.txt"
	}
	if err := os.WriteFile(filepath.Join(request.Repository, changedPath), []byte("original candidate change\n"), 0600); err != nil {
		t.Fatal(err)
	}
	candidateSHA, err := workspace.Freeze(workspace.WithStoppedCommands(ctx), request.Repository, "original candidate")
	if err != nil {
		t.Fatal(err)
	}
	head := repo.InitialSHA
	if mode != "already_current" {
		path := "predecessor.txt"
		if mode == "conflict" {
			path = "base.txt"
		}
		if err := os.WriteFile(filepath.Join(remote, path), []byte("concurrent merged predecessor\n"), 0600); err != nil {
			t.Fatal(err)
		}
		head, err = workspace.Freeze(workspace.WithStoppedCommands(ctx), remote, "merged predecessor")
		if err != nil {
			t.Fatal(err)
		}
	}
	candidate := runner.Candidate{ID: "original-candidate", SHA: candidateSHA, BaseSHA: repo.InitialSHA, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision}
	product := storage.ProductTarget{Target: w.Target(), Repository: repo.Name, BaseBranch: repo.BaseBranch, WorkItem: domain.WorkItem{ID: w.SourceID, RepositoryID: 17, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, Version: 1, Phase: domain.Reviewing, Candidate: candidate.ID, Branch: "harness/original", Worktree: request.Repository}, Candidate: candidate}
	original, err := k.SetProductTarget(ctx, "", product)
	if err != nil {
		t.Fatal(err)
	}
	spec := originalTask.Spec
	spec.ID, spec.Kind = "updated-base-fix", "fix"
	review := contract.TaskSpec{ID: "updated-base-review", Kind: "review", Instruction: "independently review the updated candidate", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: spec.ID}}, InputArtifactIDs: []string{}, WorkItemIDs: []string{w.SourceID}}
	proposal := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "updated-base-implementation", Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"}, {ID: "updated-base-evaluation", Tasks: []contract.TaskSpec{review}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{review.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "later-base-task", proposal)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	product.Target = task.Target()
	if _, err := k.SetProductTarget(ctx, original.Digest, product); err != nil {
		t.Fatal(err)
	}
	api := &finalReadFixture{repository: Repository{ID: 17, Name: repo.Name, DefaultBranch: repo.BaseBranch, CloneURL: remote}, head: head, merge: repo.InitialSHA}
	e := Executor{Store: k, Repositories: map[int64]ExecutionRepository{17: repo}, Runner: Runner{Store: k}}
	e.BasePlanner = &BasePlanner{Executor: e, Source: func(context.Context, storage.ProductTargetSnapshot) error { return nil }, API: func(storage.ProductTarget) ResourceAPI { return api }}
	return e, task, api, candidateSHA, head
}

func TestExistingBaseExecutorImportsAndOrdinarilyMergesOriginalCandidate(t *testing.T) {
	for _, mode := range []string{"merged", "already_current", "conflict", "hold_before_merge", "head_moves_after_merge", "unknown_stop"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			e, task, api, originalSHA, baseSHA := existingBaseExecutorFixture(t, mode)
			sourceCalls := 0
			moved := false
			blocked := errors.New("fixture source held before merge")
			e.BasePlanner.Source = func(ctx context.Context, _ storage.ProductTargetSnapshot) error {
				sourceCalls++
				cwd := filepath.Join(e.Store.Root, "workspaces", task.SessionID)
				if _, err := os.Stat(cwd); err != nil {
					return nil
				}
				if mode == "hold_before_merge" {
					return blocked
				}
				if mode == "unknown_stop" {
					return workspace.ErrGitStopUnconfirmed
				}
				if mode == "head_moves_after_merge" && !moved {
					head, err := workspace.Resolve(ctx, cwd, "HEAD")
					if err != nil {
						return err
					}
					if head != originalSHA {
						moved = true
						if err := os.WriteFile(filepath.Join(api.repository.CloneURL, "later.txt"), []byte("later default advance\n"), 0600); err != nil {
							return err
						}
						next, err := workspace.Freeze(workspace.WithStoppedCommands(ctx), api.repository.CloneURL, "later default advance")
						api.head = next
						return err
					}
				}
				return nil
			}
			worker := NewExecutionWorker(e, time.Second)
			changed, prepareErr := worker.task(ctx, task.ID)
			if !changed && mode != "unknown_stop" {
				t.Fatal("preparation/exception was not reconciled", prepareErr)
			}
			result, err := e.Store.ProductWorkspace(ctx, task.ID)
			if err != nil || result.Value.Request.BaseSHA != originalSHA || result.Value.ComparisonBaseSHA != baseSHA {
				t.Fatal("candidate and new comparison base conflated", result, err)
			}
			switch mode {
			case "unknown_stop":
				if !errors.Is(prepareErr, workspace.ErrGitStopUnconfirmed) || result.State != "launching" || result.Proof != nil {
					t.Fatal("unknown owner became stopped", result, prepareErr)
				}
			case "hold_before_merge":
				if !errors.Is(prepareErr, blocked) || result.State != "failed" || result.Proof.BaseMerge != nil || result.BaseMergeAt != nil {
					t.Fatal("held source merged", result, prepareErr)
				}
			case "conflict":
				if !errors.Is(prepareErr, storage.ErrProductWorkspaceHeld) || result.State != "failed" || result.Proof.BaseMerge == nil || result.Proof.BaseMerge.Outcome != "conflict" || result.BaseMergeAt == nil {
					t.Fatal("conflict became ready", result, prepareErr)
				}
				mergeHead, err := workspace.Git(ctx, result.Value.CWD, "rev-parse", "MERGE_HEAD")
				data, readErr := os.ReadFile(filepath.Join(result.Value.CWD, "base.txt"))
				if err != nil || strings.TrimSpace(mergeHead) != baseSHA || readErr != nil || !strings.Contains(string(data), "original candidate change") || !strings.Contains(string(data), "concurrent merged predecessor") {
					t.Fatal("conflict contents or identities lost", mergeHead, err, readErr)
				}
			case "head_moves_after_merge":
				if !errors.Is(prepareErr, storage.ErrProductWorkspaceHeld) || result.State != "failed" || result.Proof.BaseMerge == nil || result.Proof.BaseMerge.Outcome != "merged" || !moved {
					t.Fatal("new HEAD erased successful historical merge", result, prepareErr)
				}
			default:
				if prepareErr != nil || result.State != "ready" || result.Proof.BaseMerge == nil || result.Proof.BaseMerge.Outcome != mode {
					t.Fatal(result, prepareErr)
				}
				if mode == "merged" {
					parents, err := workspace.Git(ctx, result.Value.CWD, "rev-list", "--parents", "-n", "1", "HEAD")
					if err != nil || strings.Join(strings.Fields(parents), " ") != result.Proof.HeadSHA+" "+originalSHA+" "+baseSHA {
						t.Fatal("ordinary merge lost original parent", parents, err)
					}
					for path, expected := range map[string]string{"candidate.txt": "original candidate change\n", "predecessor.txt": "concurrent merged predecessor\n"} {
						data, err := os.ReadFile(filepath.Join(result.Value.CWD, path))
						if err != nil || string(data) != expected {
							t.Fatal(path, string(data), err)
						}
					}
				} else if result.BaseMergeAt != nil || result.Proof.HeadSHA != originalSHA {
					t.Fatal("already-current checkout consumed mutation fence", result)
				}
				// The public manifest receives fixed SHA facts, without private Git
				// paths, remotes, commands or the full stopping proof.
				input, err := e.Store.BuildSessionInput(ctx, task.SessionID, []contract.Target{task.Target()})
				if err != nil || len(input.Input) == 0 {
					t.Fatal(input, err)
				}
			}
			reservation, err := e.Store.OperationReservation(ctx, result.ID)
			if err != nil || (mode == "unknown_stop") != (reservation.State == "held") {
				t.Fatal("owner stopping/capacity mismatch", reservation, err)
			}
			before := sourceCalls
			if _, err := worker.task(ctx, task.ID); err != nil && !errors.Is(err, storage.ErrProductWorkspaceHeld) {
				t.Fatal(err)
			}
			if sourceCalls != before {
				t.Fatal("settled/unknown workspace repeated source/Git", sourceCalls, before)
			}
			again, err := e.Store.ProductWorkspace(ctx, task.ID)
			if err != nil || !reflect.DeepEqual(result, again) {
				t.Fatal("poll rewrote merge observation", again, err)
			}
			repo := e.Repositories[17].Policy.Repository
			if head, err := workspace.Resolve(ctx, repo, "HEAD"); err != nil || head != originalSHA {
				t.Fatal("controller changed original checkout", head, err)
			}
			var runs int
			if err := e.Store.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
				t.Fatal("base reconciliation started model", runs, err)
			}
		})
	}
}

func TestExistingBaseCandidateAdoptionKeepsNewComparisonAndNeedsVerification(t *testing.T) {
	ctx := context.Background()
	e, task, _, originalSHA, baseSHA := existingBaseExecutorFixture(t, "merged")
	ready, err := e.Prepare(ctx, task.ID)
	if err != nil || ready.State != "ready" || ready.Proof.BaseMerge == nil {
		t.Fatal(ready, err)
	}
	task, run := stoppedProductWriterRunFixture(t, e.Store, ready, task)
	applied, err := e.Apply(ctx, task.ID, run.ID)
	if err != nil || applied.State != contract.TaskCompleted {
		t.Fatal(applied, err)
	}
	product, err := e.Store.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
	if err != nil || product.Value.Candidate.BaseSHA != baseSHA || product.Value.Candidate.SHA == originalSHA || product.Value.Candidate.SHA == ready.Proof.HeadSHA || product.Value.WorkItem.Phase != domain.Verifying || product.Value.WorkItem.Version != 2 {
		t.Fatal("new candidate inherited obsolete comparison or quality", product, err)
	}
	for _, ancestor := range []string{originalSHA, baseSHA, ready.Proof.HeadSHA} {
		if _, err := workspace.Git(ctx, ready.Value.CWD, "merge-base", "--is-ancestor", ancestor, product.Value.Candidate.SHA); err != nil {
			t.Fatal("new candidate lost ordinary merge history", ancestor, err)
		}
	}
	sealed, err := e.Store.ProductSeal(ctx, task.ID, run.ID)
	if err != nil || sealed.State != "sealed" || sealed.Candidate.BaseSHA != baseSHA || sealed.Value.Workspace.Digest != ready.Digest {
		t.Fatal("sealing/adoption lost fixed workspace", sealed, err)
	}
	if repeated, err := e.Apply(ctx, task.ID, run.ID); err != nil || !reflect.DeepEqual(repeated, applied) {
		t.Fatal("adoption repeated candidate mutation", repeated, err)
	}
}

func TestExistingBaseCandidateCannotDiscardPreparedMergeHistory(t *testing.T) {
	ctx := context.Background()
	e, task, _, originalSHA, _ := existingBaseExecutorFixture(t, "merged")
	ready, err := e.Prepare(ctx, task.ID)
	if err != nil || ready.State != "ready" {
		t.Fatal(ready, err)
	}
	// Simulate a worker undoing the host's merge before reporting a result.
	if _, err := workspace.Git(ctx, ready.Value.CWD, "reset", "--hard", originalSHA); err != nil {
		t.Fatal(err)
	}
	task, run := stoppedProductWriterRunFixture(t, e.Store, ready, task)
	if _, err := e.Apply(ctx, task.ID, run.ID); err == nil {
		t.Fatal("discarded default base adopted as a new candidate")
	}
	product, err := e.Store.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
	if err != nil || product.Value.Candidate.SHA != originalSHA || product.Value.WorkItem.Version != 1 {
		t.Fatal("failed ancestry check mutated candidate", product, err)
	}
	sealed, err := e.Store.ProductSeal(ctx, task.ID, run.ID)
	if err != nil || sealed.State != "failed" || sealed.Candidate != nil {
		t.Fatal("discarded merge was sealed", sealed, err)
	}
}
