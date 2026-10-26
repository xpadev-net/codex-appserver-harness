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

func checkKernelPublicationGate(t *testing.T, executor Executor, plan storage.ProductVerificationPlanSnapshot, mode string) {
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
	if mode == "publication_residual" {
		if _, err := k.DB.Exec("CREATE TRIGGER fail_partial_ready BEFORE INSERT ON records WHEN NEW.kind='product_publication_readiness' BEGIN SELECT RAISE(FAIL,'partial readiness rollback'); END"); err != nil {
			t.Fatal(err)
		}
		if _, err := worker.task(ctx, review.ID); err == nil {
			t.Fatal("partial readiness fault was not reached")
		}
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_publication_scope','product_publication_scope_history','product_publication_readiness','product_publication_readiness_history')").Scan(&count); err != nil || count != 0 {
			t.Fatal("scope/readiness partially committed", count, err)
		}
		current, err := k.ReadProductTarget(ctx, review.Target(), ready.Value.WorkItemID)
		if err != nil || !reflect.DeepEqual(current, before) {
			t.Fatal("rollback changed product", current, err)
		}
		if _, err := k.DB.Exec("DROP TRIGGER fail_partial_ready"); err != nil {
			t.Fatal(err)
		}
	}
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
	if mode == "publication_residual" {
		if saved.Value.Scope == nil || !strings.Contains(publication.Body, "Criteria outside this PR: extra") || !strings.Contains(publication.Body, "Tracking WorkItem `remaining`") || strings.Contains(publication.Body, "Evaluated criterion: `extra`") {
			t.Fatal("partial body lost or invented coverage", publication, saved.Value.Scope)
		}
	}
	if changed, err := worker.task(ctx, review.ID); err != nil || changed {
		t.Fatal("worker repeated quality admission", changed, err)
	}
	again, err := executor.PublicationGate(ctx, saved.Value.Product)
	if err != nil || !reflect.DeepEqual(publication, again) {
		t.Fatal("publication body was not stable", again, err)
	}
	if !strings.HasPrefix(mode, "publication") {
		checkBranchPublisher(t, executor, saved.Value.Product, mode)
	}
	if mode == "publication_residual" {
		outside := saved.Value.Scope.Value.Outside[0]
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration' AND id=?", outside.Value.Definition.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := executor.PublicationGate(ctx, saved.Value.Product); err == nil {
			t.Fatal("lost residual tracking retained publication authority")
		}
		if _, err := k.PrepareProductPublication(ctx, review.Target(), ready.Value.WorkItemID); err == nil {
			t.Fatal("lost residual regenerated readiness")
		}
		if err := k.Put(ctx, "product_work_item_registration", outside.Value.Definition.ID, review.WaveID, outside); err != nil {
			t.Fatal(err)
		}
		if restored, err := executor.PublicationGate(ctx, saved.Value.Product); err != nil || !reflect.DeepEqual(restored, publication) {
			t.Fatal("original residual proof was not retained", restored, err)
		}
		wave, err := k.Wave(ctx, review.WaveID)
		if err != nil || wave.State == domain.WaveCompleted {
			t.Fatal("partial publication accepted parent", wave, err)
		}
		checkBranchPublisher(t, executor, saved.Value.Product, "publication_http_partial_continuation")
		return
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
