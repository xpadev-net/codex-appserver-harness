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

func TestProductReviewPreparerCreatesIndependentReadOnlyFixedCheckout(t *testing.T) {
	ctx := context.Background()
	s, implementation, task, run := candidateSealerFixture(t)
	sealed, err := s.Seal(ctx, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.AdoptProductTaskResult(ctx, task.SessionID, run.ID, "trunk"); err != nil {
		t.Fatal(err)
	}
	tasks, err := s.Store.Tasks(ctx, task.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	var review domain.Task
	for _, candidate := range tasks {
		if candidate.Spec.Kind == "review" {
			review = candidate
		}
	}
	if review.ID == "" {
		t.Fatal("independent review Task missing")
	}
	subject, err := s.Store.BindProductCandidateSubject(ctx, review.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.ProductWorkspaceRequest{TaskID: review.ID, RepositoryID: implementation.Value.Request.RepositoryID, Repository: implementation.Value.Request.Repository, BaseSHA: sealed.Candidate.SHA}
	p := WorkspacePreparer{Store: s.Store}
	ready, err := p.Prepare(ctx, request)
	if err != nil || ready.State != "ready" || ready.Value.SubjectID != subject.ID || ready.Value.SubjectDigest != subject.Digest || ready.Value.CWD == implementation.Value.CWD {
		t.Fatal(ready, err)
	}
	head, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD")
	if err != nil || head != sealed.Candidate.SHA {
		t.Fatal(head, err)
	}
	data, err := os.ReadFile(filepath.Join(ready.Value.CWD, "implementation.txt"))
	if err != nil || string(data) != "implemented behavior\n" {
		t.Fatal(string(data), err)
	}
	b, err := s.Store.SessionBinding(ctx, review.SessionID)
	if err != nil || b.Sandbox != "read-only" || b.ThreadID != "" {
		t.Fatal("review gained write/model permission", b, err)
	}
	current, err := s.Store.Task(ctx, review.ID)
	if err != nil || current.Attempts != 0 || current.State != contract.TaskReady {
		t.Fatal(current, err)
	}
	again, err := p.Prepare(ctx, request)
	if err != nil || !reflect.DeepEqual(again, ready) {
		t.Fatal("review replay created another checkout", again, err)
	}
	pointer, err := workspace.Git(ctx, ready.Value.CWD, "rev-parse", "--path-format=absolute", "--absolute-git-dir")
	if err != nil || strings.TrimSpace(pointer) == implementation.Proof.GitDir {
		t.Fatal("review reused implementation git-dir", pointer, err)
	}
	var applied bool
	if err := s.Store.DB.QueryRow("SELECT applied FROM kernel_run_bindings WHERE run_id=?", run.ID).Scan(&applied); err != nil || !applied {
		t.Fatal("review preparation disturbed producer run", applied, err)
	}
	report, err := s.Store.ImportArtifact(ctx, storage.ArtifactImport{SessionID: current.SessionID, Target: current.Target(), Key: "independent-review-report", Kind: "review_report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "task", Reference: current.ID}}, []byte("reviewed fixed implementation checkout"))
	if err != nil {
		t.Fatal(err)
	}
	result := contract.TaskResult{SchemaVersion: 1, Target: current.Target(), TaskID: current.ID, PhaseID: current.PhaseID, Outcome: "result", Artifacts: []string{report.ID}, CriterionResults: []contract.CriterionResult{{CriterionID: "AC", Status: "unverified", Basis: "independent review execution report", EvidenceIDs: []string{report.ID}}}, WorkItemRefs: []contract.CandidateReference{}, Remaining: []string{}, Blockers: []string{}, SubscriptionIDs: []string{}, Review: &contract.ReviewResult{TargetDigest: subject.Value.CandidateDigest, Verdict: "clean", Findings: []contract.ReviewFinding{}}}
	reviewRun := seedVerificationCompletedRun(t, s.Store, current, result)
	receipt, err := s.Store.ApplyTaskResult(ctx, current.SessionID, reviewRun.ID)
	if err != nil || receipt.State != contract.TaskCompleted {
		t.Fatal(receipt, err)
	}
	evidence, err := s.Store.ReadProductTaskReview(ctx, receipt.ResultID)
	if err != nil || evidence.Value.Candidate != *sealed.Candidate || evidence.Value.Review.Verdict != "clean" {
		t.Fatal(evidence, err)
	}
	target, err := s.Store.ReadProductTarget(ctx, current.Target(), ready.Value.WorkItemID)
	if err != nil || target.Value.Candidate != *sealed.Candidate || target.Value.WorkItem.Phase != domain.Verifying {
		t.Fatal("review report invented validation/approval", target, err)
	}
}
