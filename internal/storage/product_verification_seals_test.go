package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func verificationSealFixture(t *testing.T, modify ...func(*verification.Plan)) (*KernelStore, domain.Wave, domain.Task, contract.SessionRun, domain.Artifact, ProductCandidateSubjectSnapshot) {
	t.Helper()
	return verificationSealFixtureWithPolicy(t, nil, modify...)
}

func verificationSealFixtureWithPolicy(t *testing.T, configure func(*ProductVerificationPolicy), modify ...func(*verification.Plan)) (*KernelStore, domain.Wave, domain.Task, contract.SessionRun, domain.Artifact, ProductCandidateSubjectSnapshot) {
	t.Helper()
	return verificationSealFixtureWithPlan(t, nil, configure, modify...)
}

func verificationSealFixtureWithPlan(t *testing.T, planConfig func(*contract.PlanProposal), configure func(*ProductVerificationPolicy), modify ...func(*verification.Plan)) (*KernelStore, domain.Wave, domain.Task, contract.SessionRun, domain.Artifact, ProductCandidateSubjectSnapshot) {
	t.Helper()
	ctx := context.Background()
	var options []func(*ProductVerificationPolicy)
	if configure != nil {
		options = append(options, configure)
	}
	k, w, task, request, subject, _ := verificationWorkspaceWithPlanFixture(t, planConfig, options...)
	ready, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, ready.Digest); err != nil {
		t.Fatal(err)
	}
	ready, err = k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, ready))
	if err != nil {
		t.Fatal(err)
	}
	task, err = k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	plan := verification.Plan{SchemaVersion: 1, WorkItemID: "work", ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, InputCandidateID: subject.Value.Candidate.ID, Revision: 1, CriteriaTests: []verification.Mapping{{CriterionID: "AC", TestRef: "tests/behavior_test.go::TestBehavior", ExpectationBasis: "original requirement AC", ProfileID: "unit"}}, RequestedProfiles: []string{}, UnverifiedCriteria: []string{}, Blockers: []string{}, OnSuccess: "publish_pr"}
	for _, apply := range modify {
		apply(&plan)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	a := mustImport(t, k, ArtifactImport{SessionID: task.SessionID, Target: task.Target(), Key: "verification-plan", Kind: "verification_plan", ContentType: "application/json", Origin: domain.ArtifactOrigin{Kind: "task", Reference: task.ID}}, string(raw))
	_, _, task, run := seedProductSealRun(t, k, w, task, ready, func(result *contract.TaskResult) {
		result.SchemaVersion = 3
		result.Artifacts = []string{a.ID}
		result.ProductVerification = &contract.ProductVerificationReference{SubjectID: subject.ID, SubjectDigest: subject.Digest, PlanArtifactID: a.ID}
	})
	return k, w, task, run, a, subject
}

func TestProductVerificationSealPreservesOriginalPlanAndRebindsFinalCandidate(t *testing.T) {
	ctx := context.Background()
	k, w, task, run, a, subject := verificationSealFixture(t)
	saved, err := k.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil || saved.Value.Verification == nil || saved.Value.Verification.InputPlan.InputCandidateID != subject.Value.Candidate.ID {
		t.Fatal(saved, err)
	}
	if claim, err := k.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	proof := sealProofFixture(saved)
	proof.Frozen.SHA = strings.Repeat("d", 40)
	if _, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("missing physical verification checks accepted", err)
	}
	checked := saved.Value.Verification.CheckProof()
	proof.Verification = &checked
	sealed, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof)
	if err != nil || sealed.State != "sealed" || sealed.Candidate.BaseSHA != subject.Value.Candidate.BaseSHA || sealed.VerificationPlan == nil || sealed.VerificationPlan.InputCandidateID != sealed.Candidate.ID || sealed.Value.Verification.InputPlan.InputCandidateID != subject.Value.Candidate.ID {
		t.Fatal(sealed, err)
	}
	data, err := k.readArtifactObject(a)
	if err != nil {
		t.Fatal(err)
	}
	var original verification.Plan
	if err := contract.DecodeStrict(data, &original); err != nil || original.InputCandidateID != subject.Value.Candidate.ID {
		t.Fatal("original plan rewritten", original, err)
	}
	var final contract.SessionRun
	if err := k.Record(ctx, "agent_run", run.ID, &final); err != nil || final.Final != run.Final {
		t.Fatal("raw final rewritten", err)
	}
	again, err := k.FinishProductSeal(ctx, task.ID, run.ID, proof)
	if err != nil || !reflect.DeepEqual(again, sealed) {
		t.Fatal(again, err)
	}
	p, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil || p.Value.Candidate != subject.Value.Candidate {
		t.Fatal("sealing adopted verification without host result application", p, err)
	}
}

func TestProductVerificationSealRequiresReadyCompleteTrustedPlan(t *testing.T) {
	for _, mode := range []string{"source", "profile", "coverage", "unverified", "blocker"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, task, run, _, _ := verificationSealFixture(t, func(plan *verification.Plan) {
				switch mode {
				case "source":
					plan.InputCandidateID = "other-candidate"
				case "profile":
					plan.RequestedProfiles = []string{"model-chosen-unregistered"}
				case "coverage":
					plan.CriteriaTests = []verification.Mapping{}
				case "unverified":
					plan.UnverifiedCriteria = []string{"AC"}
				case "blocker":
					plan.Blockers = []string{"no independent expectation"}
				}
			})
			if _, err := k.ReserveProductSeal(ctx, task.ID, run.ID); !errors.Is(err, ErrProductChangeReference) {
				t.Fatal("bad verification plan admitted", mode, err)
			}
		})
	}
}

func TestProductVerificationSealRejectsWrongPlanArtifactKindTypeAndOrigin(t *testing.T) {
	for _, mode := range []string{"kind", "content_type", "origin"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, task, run, a, _ := verificationSealFixture(t)
			switch mode {
			case "kind":
				a.Kind = "note"
			case "content_type":
				a.ContentType = "text/plain"
			case "origin":
				a.Origin = domain.ArtifactOrigin{Kind: "external", Reference: "foreign source"}
			}
			raw, err := json.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("UPDATE kernel_artifacts SET payload=? WHERE id=?", raw, a.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReserveProductSeal(ctx, task.ID, run.ID); !errors.Is(err, ErrProductChangeReference) {
				t.Fatal("untyped plan evidence admitted", mode, err)
			}
		})
	}
}
