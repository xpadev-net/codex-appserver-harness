package product

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func TestProductVerificationPreparerCreatesPolicyBoundCodeAndTestWorkspace(t *testing.T) {
	ctx := context.Background()
	s, implementation, producer, run := candidateSealerFixture(t, func(plan *contract.PlanProposal) { plan.Phases[1].Tasks[0].Kind = "verification" })
	sealed, err := s.Seal(ctx, producer.ID, run.ID)
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
	var taskID string
	for _, task := range tasks {
		if task.Spec.Kind == "verification" {
			taskID = task.ID
		}
	}
	if taskID == "" {
		t.Fatal("independent verifier missing")
	}
	subject, err := s.Store.BindProductCandidateSubject(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Store.RegisterProductVerificationPolicy(ctx, producer.Target(), storage.ProductVerificationPolicy{WaveID: producer.WaveID, PolicyRevision: producer.PolicyRevision, RepositoryID: implementation.Value.Request.RepositoryID, Repository: implementation.Value.Request.Repository, VerifierPaths: []string{"tests/"}, Verification: policy.Verification{Revision: producer.PolicyRevision, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"trusted-test-runner"}, TimeoutSeconds: 30, SuccessPattern: "PASS"}}}})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := (WorkspacePreparer{Store: s.Store}).Prepare(ctx, storage.ProductWorkspaceRequest{TaskID: taskID, RepositoryID: implementation.Value.Request.RepositoryID, Repository: implementation.Value.Request.Repository, BaseSHA: sealed.Candidate.SHA})
	if err != nil || ready.State != "ready" || ready.Value.SubjectID != subject.ID || ready.Value.VerificationPolicyDigest != p.Digest || ready.Value.ComparisonBaseSHA != sealed.Candidate.BaseSHA {
		t.Fatal(ready, err)
	}
	head, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD")
	if err != nil || head != sealed.Candidate.SHA {
		t.Fatal(head, err)
	}
	if err := os.Mkdir(filepath.Join(ready.Value.CWD, "tests"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ready.Value.CWD, "tests", "behavior_test.go"), []byte("test source fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	b, err := s.Store.SessionBinding(ctx, ready.Value.SessionID)
	if err != nil || b.Sandbox != "workspace-write" || b.ThreadID != "" {
		t.Fatal(b, err)
	}
	if _, err := os.Stat(filepath.Join(implementation.Value.CWD, "tests", "behavior_test.go")); !os.IsNotExist(err) {
		t.Fatal("verification mutated implementation workspace", err)
	}
}
