package product

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func checkKernelPublicationGate(t *testing.T, executor Executor, plan storage.ProductVerificationPlanSnapshot) {
	t.Helper()
	ctx := context.Background()
	k := executor.Store
	tasks, err := k.Tasks(ctx, plan.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	var review domain.Task
	for _, task := range tasks {
		if task.Spec.ID == "publication-review" {
			review = task
		}
	}
	ready, err := executor.Prepare(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	review, err = k.Task(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := k.ReadProductCandidateSubject(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	report, err := k.ImportArtifact(ctx, storage.ArtifactImport{SessionID: review.SessionID, Target: review.Target(), Key: "publication-review", Kind: "review_report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: review.ID}}, []byte("original AC independently evaluated against exact implementation and tests"))
	if err != nil {
		t.Fatal(err)
	}
	result := contract.TaskResult{SchemaVersion: 1, Target: review.Target(), TaskID: review.ID, PhaseID: review.PhaseID, Outcome: "result", Artifacts: []string{report.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "claimed", Basis: "original AC checked in fixed code/tests", EvidenceIDs: []string{report.ID}}}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}, Review: &contract.ReviewResult{TargetDigest: subject.Value.CandidateDigest, Verdict: "clean", Findings: []contract.ReviewFinding{}}}
	run := seedVerificationCompletedRun(t, k, review, result)
	if _, err := executor.Apply(ctx, review.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	before, err := k.ReadProductTarget(ctx, review.Target(), ready.Value.WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.PublicationGate(ctx, before); err == nil {
		t.Fatal("unprepared quality granted publication")
	}
	worker := NewExecutionWorker(executor, 1)
	changed, err := worker.task(ctx, review.ID)
	if err != nil || !changed {
		t.Fatal("worker did not admit complete quality", changed, err)
	}
	saved, err := k.ReadProductPublicationReadiness(ctx, review.Target(), ready.Value.WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := executor.PublicationGate(ctx, saved.Value.Product)
	if err != nil || publication.Title == "" || !strings.Contains(publication.Body, plan.Value.Candidate.SHA) || !strings.Contains(publication.Body, saved.Value.Quality.Digest) || strings.Contains(publication.Body, "grep -qx") {
		t.Fatal(publication, err)
	}
	if changed, err := worker.task(ctx, review.ID); err != nil || changed {
		t.Fatal("worker repeated quality admission", changed, err)
	}
	again, err := executor.PublicationGate(ctx, saved.Value.Product)
	if err != nil || !reflect.DeepEqual(publication, again) {
		t.Fatal("publication body was not stable", again, err)
	}
	repo := executor.Repositories[saved.Value.Product.Value.WorkItem.RepositoryID]
	profile := repo.Policy.Verification.Profiles["unit"]
	profile.Argv = []string{"sh", "-c", "printf changed-policy"}
	repo.Policy.Verification.Profiles["unit"] = profile
	executor.Repositories[repo.Policy.RepositoryID] = repo
	if changed, err := executor.PublicationGate(ctx, saved.Value.Product); err == nil || changed.Title != "" {
		t.Fatal("changed host policy retained publication authority", changed, err)
	}
}
