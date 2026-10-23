package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func verificationWorkspaceFixture(t *testing.T, configure ...func(*ProductVerificationPolicy)) (*KernelStore, domain.Wave, domain.Task, ProductWorkspaceRequest, ProductCandidateSubjectSnapshot, ProductVerificationPolicySnapshot) {
	t.Helper()
	ctx := context.Background()
	k, w, task, source := productSubjectFixture(t, "verification")
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
	value := verificationPolicyFixture(w.ID, request.Repository, w.Policy.Revision)
	for _, apply := range configure {
		apply(&value)
	}
	p, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), value)
	if err != nil {
		t.Fatal(err)
	}
	task, err = k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, task, request, subject, p
}

func TestProductVerificationWorkspacePinsSubjectPolicyAndComparisonBase(t *testing.T) {
	ctx := context.Background()
	k, w, task, request, subject, p := verificationWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil || saved.Value.VerificationPolicyID != p.ID || saved.Value.VerificationPolicyDigest != p.Digest || saved.Value.SubjectID != subject.ID || saved.Value.ComparisonBaseSHA != subject.Value.Candidate.BaseSHA || saved.Value.Request.BaseSHA != subject.Value.Candidate.SHA {
		t.Fatal(saved, err)
	}
	if saved.Value.ComparisonBaseSHA == saved.Value.Request.BaseSHA {
		t.Fatal("fixture must distinguish comparison base and input commit")
	}
	if claim, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	ready, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved))
	if err != nil || ready.State != "ready" {
		t.Fatal(ready, err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "workspace-write" || b.ThreadID != "" {
		t.Fatal(b, err)
	}
	current, err := k.Task(ctx, task.ID)
	if err != nil || current.InputDigest == task.InputDigest || current.Attempts != 0 {
		t.Fatal(current, err)
	}
	manifest := registrationManifest(t, k, task.SessionID, w.Target())
	if manifest.ProductWorkspace == nil || manifest.ProductWorkspace.Reference.WorkspaceDigest != saved.Digest || manifest.ProductVerificationPolicy == nil || manifest.ProductVerificationPolicy.Digest != p.Digest {
		t.Fatal(manifest.ProductWorkspace, manifest.ProductVerificationPolicy)
	}
	snapshot, err := k.BuildSessionInput(ctx, task.SessionID, []contract.Target{w.Target()})
	if err != nil || !strings.Contains(inputContext(t, snapshot).Instructions, "schema_version=3") || strings.Contains(inputContext(t, snapshot).Instructions, "schema_version=2") {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSeal(ctx, task.ID, "verification-run"); !errors.Is(err, ErrProductSealHeld) {
		t.Fatal("workspace preparation bypassed verification sealing contract", err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_verification_policy_history'"); err != nil {
		t.Fatal(err)
	}
	if _, err := productTaskDigest(t, k, w, task.Spec); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("lost verification policy authorized input", err)
	}
}

func TestProductVerificationWorkspaceRequiresCurrentPinnedPolicy(t *testing.T) {
	for _, mode := range []string{"missing", "history", "corrupt", "repository"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, request, _, _ := verificationWorkspaceFixture(t)
			var err error
			switch mode {
			case "missing":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind IN ('product_verification_policy','product_verification_policy_history')")
			case "history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_verification_policy_history'")
			case "corrupt":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_verification_policy'")
			case "repository":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_verification_policy','product_verification_policy_history')"); err != nil {
					t.Fatal(err)
				}
				_, err = k.RegisterProductVerificationPolicy(ctx, w.Target(), verificationPolicyFixture(w.ID, operationRepo(t), w.Policy.Revision))
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReserveProductWorkspace(ctx, request); err == nil {
				t.Fatal("unbound policy admitted verification workspace", mode)
			}
			b, err := k.SessionBinding(ctx, task.SessionID)
			if err != nil || b.Sandbox != "read-only" {
				t.Fatal(b, err)
			}
		})
	}
}
