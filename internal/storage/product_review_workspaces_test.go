package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func reviewWorkspaceFixture(t *testing.T) (*KernelStore, domain.Wave, domain.Task, ProductWorkspaceRequest, ProductCandidateSubjectSnapshot) {
	t.Helper()
	ctx := context.Background()
	k, w, task, source := productSubjectFixture(t, "review")
	subject, err := k.BindProductCandidateSubject(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := k.ProductSeal(ctx, source.Value.TaskID, source.Value.RunID)
	if err != nil {
		t.Fatal(err)
	}
	request := producer.Value.Workspace.Value.Request
	request.TaskID, request.BaseSHA = task.ID, source.Value.Candidate.SHA
	task, err = k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, task, request, subject
}

func TestProductReviewWorkspacePinsSubjectAndKeepsReadOnlyBinding(t *testing.T) {
	ctx := context.Background()
	k, w, task, request, subject := reviewWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil || saved.Value.SubjectID != subject.ID || saved.Value.SubjectDigest != subject.Digest {
		t.Fatal(saved, err)
	}
	if claim, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	ready, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved))
	if err != nil || ready.State != "ready" {
		t.Fatal(ready, err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "read-only" || b.ThreadID != "" {
		t.Fatal("review acquired write/execute", b, err)
	}
	current, err := k.Task(ctx, task.ID)
	if err != nil || current.InputDigest == task.InputDigest || current.Attempts != 0 {
		t.Fatal(current, err)
	}
	digest, err := productTaskDigest(t, k, w, task.Spec)
	if err != nil || digest != current.InputDigest {
		t.Fatal(digest, current.InputDigest, err)
	}
	manifest := registrationManifest(t, k, task.SessionID, w.Target())
	if manifest.ProductSubject == nil || manifest.ProductSubject.Value.Candidate != subject.Value.Candidate || manifest.ProductWorkspace != nil {
		t.Fatal("review received product-change output identity", manifest.ProductWorkspace)
	}
	if _, err := k.ReserveProductSeal(ctx, task.ID, "review-run"); !errors.Is(err, ErrProductSealHeld) {
		t.Fatal("review workspace admitted candidate sealing", err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_workspace_history' AND id=?", saved.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := productTaskDigest(t, k, w, task.Spec); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("review fell back after workspace proof loss", err)
	}
}

func TestProductReviewWorkspaceRejectsWrongBaseRepositoryAndUnboundRole(t *testing.T) {
	for _, mode := range []string{"base", "repository", "subject", "verification"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, task, request, _ := reviewWorkspaceFixture(t)
			switch mode {
			case "base":
				request.BaseSHA = strings.Repeat("d", 40)
			case "repository":
				request.Repository = operationRepo(t)
			case "subject":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_candidate_subject','product_candidate_subject_history')"); err != nil {
					t.Fatal(err)
				}
			case "verification":
				task.Spec.Kind = "verification"
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := saveTask(ctx, tx, task); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ReserveProductWorkspace(ctx, request); err == nil {
				t.Fatal("invalid review checkout admitted", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_workspace' AND id=?", productWorkspaceID(task.ID)).Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
		})
	}
}

func TestProductReviewWorkspaceCandidateChangeDuringPreparationIsHistoryOnly(t *testing.T) {
	ctx := context.Background()
	k, w, task, request, subject := reviewWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	next := p.Value
	next.Candidate.ID, next.Candidate.SHA = "advanced", strings.Repeat("d", 40)
	next.WorkItem.Candidate = next.Candidate.ID
	next.WorkItem.Version++
	if _, err := k.SetProductTarget(ctx, p.Digest, next); err != nil {
		t.Fatal(err)
	}
	stale, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved))
	if err != nil || stale.State != "stale" {
		t.Fatal(stale, err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "read-only" {
		t.Fatal(b, err)
	}
	original, err := k.ReadProductCandidateSubject(ctx, task.ID)
	if err != nil || original.Value.Candidate != subject.Value.Candidate {
		t.Fatal("stale checkout retargeted original review", original, err)
	}
	ready, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
	if err != nil || len(ready.Tasks) != 0 {
		t.Fatal(ready, err)
	}
}
