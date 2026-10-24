package product

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// This local remote fixture supplies an actually existing ordinary merge SHA
// to the simulated HTTP response; it makes no claim of a real GitHub merge.
func physicalContinuationMerge(t *testing.T, e Executor, old storage.ProductTargetSnapshot) (string, string) {
	t.Helper()
	ctx := context.Background()
	repo := e.Repositories[old.Value.WorkItem.RepositoryID]
	remote := t.TempDir()
	if _, err := workspace.Git(ctx, repo.Policy.Repository, "clone", "--no-local", "--", repo.Policy.Repository, remote); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"config", "user.name", "continuation fixture"}, {"config", "user.email", "continuation@example.invalid"}, {"config", "core.hooksPath", "/dev/null"}, {"-c", "commit.gpgsign=false", "merge", "--no-ff", "--no-edit", "--", old.Value.Candidate.SHA}} {
		if _, err := workspace.Git(ctx, remote, args...); err != nil {
			t.Fatal(err)
		}
	}
	sha, err := workspace.Resolve(ctx, remote, "HEAD")
	if err != nil || sha == old.Value.Candidate.SHA {
		t.Fatal("ordinary remote merge missing", sha, err)
	}
	parents, err := workspace.Git(ctx, remote, "rev-list", "--parents", "-n", "1", sha)
	if err != nil || len(strings.Fields(parents)) != 3 {
		t.Fatal("remote merge lacks two parents", parents, err)
	}
	return remote, sha
}

func checkVersionedPhysicalCandidate(t *testing.T, e Executor, w domain.Wave, node storage.ProductContinuation, remote, mergeSHA string, source IssueSourceObserver) {
	t.Helper()
	ctx := context.Background()
	k := e.Store
	if node.Value.ParentMergeSHA != mergeSHA {
		t.Fatal("lineage lost physical merge", node.Value.ParentMergeSHA, mergeSHA)
	}
	before := w
	oldTasks, err := k.Tasks(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	work := contract.TaskSpec{ID: "continued-implementation", Kind: "work", Instruction: "implement fresh version from the known merged base", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{node.Value.Request.Definition.ID}}
	verify := contract.TaskSpec{ID: "continued-verification", Kind: "verification", Instruction: "independently verify the new candidate", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: work.ID}}, InputArtifactIDs: []string{}, WorkItemIDs: append([]string{}, work.WorkItemIDs...)}
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "continued-work", Tasks: []contract.TaskSpec{work}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{work.ID}}, FailurePolicy: "drain_and_block"}, {ID: "continued-quality", Tasks: []contract.TaskSpec{verify}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{verify.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
	admission, err := k.AdoptPlan(ctx, w.Target(), "physical-continuation-plan", plan)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admission.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, old := range oldTasks {
		if old.ID == task.ID || old.SessionID == task.SessionID {
			t.Fatal("continuation reused a completed Task/session", old, task)
		}
	}
	repo := e.Repositories[node.Value.Request.Definition.RepositoryID]
	api := &finalReadFixture{repository: Repository{ID: repo.Policy.RepositoryID, Name: repo.Name, DefaultBranch: repo.BaseBranch, CloneURL: remote}, head: mergeSHA, merge: repo.InitialSHA}
	sourceGate := KernelSourceGate{Store: k, Policies: map[int64]IssueSourcePolicy{repo.Policy.RepositoryID: source.Policy}, API: func(storage.ProductTarget) ResourceAPI { return source.API }}
	e.BasePlanner = &BasePlanner{Executor: e, Source: sourceGate.Check, API: func(storage.ProductTarget) ResourceAPI { return api }}
	prepared, err := e.Prepare(ctx, task.ID)
	if err != nil || prepared.Proof == nil || prepared.Proof.HeadSHA != mergeSHA || prepared.Value.Request.BasePlanID == "" {
		t.Fatal("new Task did not import actual merged base", prepared, err)
	}
	head, err := workspace.Resolve(ctx, prepared.Value.CWD, "HEAD")
	if err != nil || head != mergeSHA {
		t.Fatal("physical checkout is not fixed merged SHA", head, err)
	}
	parentContent, err := os.ReadFile(filepath.Join(prepared.Value.CWD, "implementation.txt"))
	if err != nil || string(parentContent) != "implemented behavior\n" {
		t.Fatal("merged implementation missing from new checkout", string(parentContent), err)
	}
	task, run := stoppedProductWriterRunFixture(t, k, prepared, task)
	if err := os.WriteFile(filepath.Join(prepared.Value.CWD, "implementation.txt"), []byte("continued behavior needs fresh independent verification\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Apply(ctx, task.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	product, err := k.ReadProductTarget(ctx, w.Target(), node.Value.Request.Definition.ID)
	if err != nil || product.Value.Candidate.BaseSHA != mergeSHA || product.Value.Candidate.SHA == mergeSHA || product.Value.Candidate.SHA == node.Value.ParentCandidateSHA || product.Value.WorkItem.Phase != domain.Verifying || product.Value.PRNumber != 0 {
		t.Fatal("new work reused old candidate/PR/quality", product, err)
	}
	if publication, err := e.PublicationGate(ctx, product); err == nil || publication.Title != "" {
		t.Fatal("old quality authorized new candidate publication", publication, err)
	}
	completed, err := k.Task(ctx, task.ID)
	if err != nil || completed.State != contract.TaskCompleted || completed.Attempts != 1 {
		t.Fatal(completed, err)
	}
	verifier, err := k.Task(ctx, admission.CreatedTaskIDs[1])
	if err != nil || verifier.State != contract.TaskReady || verifier.Attempts != 0 || verifier.SessionID == task.SessionID {
		t.Fatal("new verifier was reused or already executed", verifier, err)
	}
	verificationWorkspace, err := e.Prepare(ctx, verifier.ID)
	if err != nil || verificationWorkspace.Proof == nil || verificationWorkspace.Proof.HeadSHA != product.Value.Candidate.SHA || verificationWorkspace.Value.CWD == prepared.Value.CWD {
		t.Fatal("new verifier did not prepare the new fixed candidate", verificationWorkspace, err)
	}
	subject, err := k.ReadProductCandidateSubject(ctx, verifier.ID)
	if err != nil || subject.Value.Candidate != product.Value.Candidate || len(subject.Value.Sources) != 1 || subject.Value.Sources[0].Value.VerificationPlan != nil {
		t.Fatal("new verifier inherited an old candidate or plan", subject, err)
	}
	oldOrigin, err := k.ReadProductContinuation(ctx, node.Value.Request.Definition.ID)
	if err != nil || !reflect.DeepEqual(oldOrigin, node) {
		t.Fatal("candidate rewrote original lineage", oldOrigin, err)
	}
	if w.State == domain.WaveCompleted || w.MainTurns != before.MainTurns || w.Deadline != before.Deadline || w.TasksCreated != before.TasksCreated+len(admission.CreatedTaskIDs) || w.Replans < before.Replans {
		t.Fatal("new work reset budget or accepted Wave", w, before)
	}
}
