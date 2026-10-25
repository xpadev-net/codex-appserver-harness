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
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func splitScopeInputFixture(t *testing.T) (*KernelStore, domain.Wave, ProductSplitRegistrations) {
	t.Helper()
	k, r, _ := splitRegistrationsFixture(t)
	saved, err := k.RegisterProductSplitChildren(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(context.Background(), r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, saved
}

func readSplitVerificationScope(t *testing.T, k *KernelStore, w domain.Wave, registration ProductWorkItemRegistrationSnapshot) (verification.Scope, error) {
	t.Helper()
	tx, err := k.kernelTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	return productVerificationScopeTx(context.Background(), tx, w, registration)
}

func TestProductSplitScopeUsesLocalCriteriaAndRequiresEveryLocalDisposition(t *testing.T) {
	ctx := context.Background()
	k, w, saved := splitScopeInputFixture(t)
	p := policy.Verification{Revision: w.Policy.Revision, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"go", "test", "-v", "./..."}, TimeoutSeconds: 30, SuccessPattern: policy.GoTestSuccessPattern}}}
	for i, registration := range saved.Value.Registrations {
		binding := saved.Value.Children[i]
		scope, err := readSplitVerificationScope(t, k, w, registration)
		if err != nil || !reflect.DeepEqual(scope, binding.Value.Scope) || scope.OriginalRequest != binding.Value.Child.Value.Child.Requirement {
			t.Fatal("not complete local scope", scope, err)
		}
		digest, err := contentDigest(scope)
		if err != nil || digest != binding.Value.ScopeDigest {
			t.Fatal("scope digest changed", digest, err)
		}
		candidate := runner.Candidate{ID: "local-candidate", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision}
		plan := verification.Plan{SchemaVersion: 1, WorkItemID: registration.Value.Definition.ID, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, InputCandidateID: candidate.ID, Revision: 1, CriteriaTests: []verification.Mapping{}, RequestedProfiles: []string{}, UnverifiedCriteria: []string{}, Blockers: []string{}, OnSuccess: "publish_pr"}
		for _, criterion := range scope.Criteria {
			plan.CriteriaTests = append(plan.CriteriaTests, verification.Mapping{CriterionID: criterion.ID, TestRef: "tests/local_test.go::TestLocal", ExpectationBasis: criterion.Requirement, ProfileID: "unit"})
		}
		raw, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := verification.DecodePlan(raw, registration.Value.Definition.ID, candidate, scope, p)
		if err != nil || !decoded.Ready() {
			t.Fatal("complete local plan denied", decoded, err)
		}
		plan.CriteriaTests[0].CriterionID = "AC"
		raw, err = json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := verification.DecodePlan(raw, registration.Value.Definition.ID, candidate, scope, p); err == nil {
			t.Fatal("parent criterion replaced local part")
		}
		if len(scope.Criteria) > 1 {
			plan = decoded
			plan.CriteriaTests = plan.CriteriaTests[:1]
			raw, err = json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := verification.DecodePlan(raw, registration.Value.Definition.ID, candidate, scope, p); err == nil {
				t.Fatal("local criterion dropped")
			}
		}
		actual, err := k.ReadProductSplitChildScope(ctx, w.Target(), registration.Value.Definition.ID)
		if err != nil || !reflect.DeepEqual(actual, binding) {
			t.Fatal("public binding changed", actual, err)
		}
		if _, err := k.CheckProductQuality(ctx, w.Target(), registration.Value.Definition.ID); !errors.Is(err, ErrProductQualityHeld) {
			t.Fatal("scope read granted quality", err)
		}
	}
	parent, err := k.ReadProductWorkItemRegistration(ctx, saved.Value.Request.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	root, err := readSplitVerificationScope(t, k, w, parent)
	if err != nil || !reflect.DeepEqual(root, productRootVerificationScope(w, parent)) {
		t.Fatal("root verification changed", root, err)
	}
}

func TestProductSplitScopeInputsAreRoleFilteredAndPinTaskDigest(t *testing.T) {
	ctx := context.Background()
	k, w, saved := splitScopeInputFixture(t)
	first, second := saved.Value.Registrations[0], saved.Value.Registrations[1]
	work := contract.TaskSpec{ID: "child-first", Kind: "work", Instruction: "implement the first local part", Criteria: append([]string{}, first.Value.Definition.Criteria...), DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{first.Value.Definition.ID}}
	peer := work
	peer.ID = "child-second"
	peer.WorkItemIDs = []string{second.Value.Definition.ID}
	peer.Criteria = append([]string{}, second.Value.Definition.Criteria...)
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "child-work", Tasks: []contract.TaskSpec{work, peer}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{work.ID, peer.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
	adopted, err := k.AdoptPlan(ctx, w.Target(), "local-scope-input", plan)
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, adopted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	manifest := registrationManifest(t, k, task.SessionID, w.Target())
	if len(manifest.ProductSplitScopes) != 1 || manifest.ProductSplitScopes[0].WorkItemID != first.Value.Definition.ID || !reflect.DeepEqual(manifest.ProductSplitScopes[0], productSplitScopeInput(saved.Value.Children[0])) || manifest.Wave.OriginalRequest != w.OriginalRequest {
		t.Fatal("Task leaked peer/lost original/local scope", manifest.ProductSplitScopes)
	}
	parent := registrationManifest(t, k, w.OrchestratorSessionID, w.Target())
	if len(parent.ProductSplitScopes) != 2 || len(parent.UnavailableProductSplitScopes) != 0 {
		t.Fatal("parent lost child mapping", parent.ProductSplitScopes, parent.UnavailableProductSplitScopes)
	}
	raw, err := json.Marshal(parent.ProductSplitScopes)
	if err != nil || strings.Contains(string(raw), k.Root) || strings.Contains(string(raw), "harness-split-child:") || strings.Contains(string(raw), "creation_digest") {
		t.Fatal("private proof in scope input", string(raw), err)
	}
	snapshot, err := k.BuildSessionInput(ctx, task.SessionID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(inputContext(t, snapshot).Instructions, "product_split_scopes") {
		t.Fatal("local verification instructions missing")
	}
	digest, err := productTaskDigest(t, k, w, task.Spec)
	if err != nil || digest != task.InputDigest {
		t.Fatal("Task input did not pin original binding", digest, task.InputDigest, err)
	}
	var owner string
	var receipt []byte
	if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind='product_split_registrations' AND id=?", saved.Value.Request.ProposalID).Scan(&owner, &receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_registrations' AND id=?", saved.Value.Request.ProposalID); err != nil {
		t.Fatal(err)
	}
	if _, err := productTaskDigest(t, k, w, task.Spec); !errors.Is(err, ErrProductSplitHeld) || !readinessHoldError(err) {
		t.Fatal("missing proof retained input/fatal scheduler error", err)
	}
	manifest = registrationManifest(t, k, task.SessionID, w.Target())
	if len(manifest.ProductSplitScopes) != 0 || len(manifest.UnavailableProductSplitScopes) != 1 {
		t.Fatal("missing scope fell back to parent", manifest.ProductSplitScopes, manifest.UnavailableProductSplitScopes)
	}
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_registrations',?,?,?)", saved.Value.Request.ProposalID, owner, receipt); err != nil {
		t.Fatal(err)
	}
	restored, err := productTaskDigest(t, k, w, task.Spec)
	if err != nil || restored != digest {
		t.Fatal("exact restoration rewrote input", restored, err)
	}
}

func TestProductSplitScopeCannotFallBackToParentWhenReferenceOrProofIsLost(t *testing.T) {
	ctx := context.Background()
	k, w, saved := splitScopeInputFixture(t)
	registration := saved.Value.Registrations[0]
	missing := registration
	missing.Value.SplitScope = nil
	missing.Digest, _ = contentDigest(missing.Value)
	if _, err := readSplitVerificationScope(t, k, w, missing); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("missing reference fell back to parent", err)
	}
	for _, kind := range []string{"product_split_registrations", "product_split_child_scopes", "product_split_child_scope_source"} {
		id := saved.Value.Request.ProposalID
		if kind == "product_split_child_scope_source" {
			id = "github:17:issue:99"
		}
		var owner string
		var raw []byte
		if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", kind, id).Scan(&owner, &raw); err != nil {
			t.Fatal(err)
		}
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, id); err != nil {
			t.Fatal(err)
		}
		if _, err := readSplitVerificationScope(t, k, w, registration); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("missing proof gave parent scope", kind, err)
		}
		if _, err := k.ReadProductSplitChildScope(ctx, w.Target(), registration.Value.Definition.ID); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("missing proof publicly accepted", kind, err)
		}
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", kind, id, owner, raw); err != nil {
			t.Fatal(err)
		}
	}
	d, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "old-local-scope", Target: w.Target(), Instruction: "new requirements", NextPolicyRevision: w.Policy.Revision + 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readSplitVerificationScope(t, k, current, registration); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("old local scope carried into direction", err)
	}
	if _, err := k.ReadProductSplitChildScope(ctx, current.Target(), registration.Value.Definition.ID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("old child read as current", err)
	}
}

func TestProductSplitScopeMetadataReopensPausedAndSurvivesReplanning(t *testing.T) {
	ctx := context.Background()
	k, w, saved := splitScopeInputFixture(t)
	id := saved.Value.Registrations[0].Value.Definition.ID
	path := k.Root + "/state.db"
	k.Close()
	var err error
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-local-scope-read", Target: w.Target(), Action: "pause", Reason: "audit"}); err != nil {
		t.Fatal(err)
	}
	read, err := k.ReadProductSplitChildScope(ctx, w.Target(), id)
	if err != nil || !reflect.DeepEqual(read, saved.Value.Children[0]) {
		t.Fatal("paused reopen changed local scope", read, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "resume-local-scope-read", Target: w.Target(), Action: "resume", Reason: "continue"}); err != nil {
		t.Fatal(err)
	}
	original := w.Target()
	for i, key := range []string{"plan-local-scope-1", "plan-local-scope-2"} {
		plan := storagePlanFixture()
		if _, err := k.AdoptPlan(ctx, w.Target(), key, plan); err != nil {
			t.Fatal(i, err)
		}
		w, err = k.Wave(ctx, w.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if w.PlanRevision == original.PlanRevision {
		t.Fatal("plan did not advance", w)
	}
	read, err = k.ReadProductSplitChildScope(ctx, w.Target(), id)
	if err != nil || !reflect.DeepEqual(read, saved.Value.Children[0]) || read.Value.Request.Target != original {
		t.Fatal("replanning reset scope/lineage", read, err)
	}
	if _, err := k.ReadProductSplitChildScope(ctx, original, id); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("stale current-target request accepted", err)
	}
	scope, err := readSplitVerificationScope(t, k, w, saved.Value.Registrations[0])
	if err != nil || !reflect.DeepEqual(scope, read.Value.Scope) {
		t.Fatal("new plan lost original local obligation", scope, err)
	}
}
