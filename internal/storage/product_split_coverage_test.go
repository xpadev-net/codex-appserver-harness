package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
)

func TestProductSplitCoverageRetiresOnlyAuthenticatedWholeRegistration(t *testing.T) {
	ctx := context.Background()
	k, w, group := splitScopeInputFixture(t)
	before, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := k.ProductCoverage(ctx, w.Target())
	if err != nil || len(ledger.Value.Splits) != 1 || len(ledger.Value.Retired) != 1 || len(ledger.Value.Unavailable) != 0 || len(ledger.Value.Residual) != 2 || len(ledger.Value.UnmergedWorkItems) != 2 || len(ledger.Value.Merged) != 0 {
		t.Fatal(ledger, err)
	}
	retirement := ledger.Value.Retired[0]
	if retirement.WorkItemID != group.Value.Request.ParentWorkItemID || retirement.SplitID != group.ID || retirement.SplitDigest != group.Digest || len(retirement.ChildWorkItemIDs) != 2 || retirement.ContinuationID != "" {
		t.Fatal(retirement)
	}
	if slices.Contains(ledger.Value.UnmergedWorkItems, retirement.WorkItemID) {
		t.Fatal("delegated root remained independent obligation")
	}
	for i, child := range ledger.Value.Splits[0].Children {
		binding := group.Value.Children[i]
		if child.Merged || child.BindingDigest != binding.Digest || child.ScopeDigest != binding.Value.ScopeDigest || !reflect.DeepEqual(child.Coverage, binding.Value.Child.Value.Child.Coverage) || !reflect.DeepEqual(child.LocalCriteria, binding.Value.Scope.Included) {
			t.Fatal("part/local scope flattened", child)
		}
		if _, err := k.CheckProductQuality(ctx, w.Target(), child.WorkItemID); !errors.Is(err, ErrProductQualityHeld) {
			t.Fatal("delegation enabled execution/quality", err)
		}
	}
	manifest := registrationManifest(t, k, "main", w.Target())
	if manifest.ProductMergeCoverage == nil || !reflect.DeepEqual(*manifest.ProductMergeCoverage, ledger) {
		t.Fatal("parent input omitted split obligations", manifest.ProductMergeCoverage)
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("coverage changed budget", err)
	}
	wave, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(w, wave) {
		t.Fatal("coverage changed Wave", err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-coverage-audit", Target: w.Target(), Action: "pause", Reason: "audit"}); err != nil {
		t.Fatal(err)
	}
	reopened, err := k.ProductCoverage(ctx, w.Target())
	if err != nil || !reflect.DeepEqual(reopened, ledger) {
		t.Fatal("metadata changed on reopen/pause", reopened, err)
	}
}

func TestProductSplitCoverageLosesRetirementOnAnyMissingWholeSetProof(t *testing.T) {
	ctx := context.Background()
	k, w, group := splitScopeInputFixture(t)
	initial, err := k.ProductCoverage(ctx, w.Target())
	if err != nil {
		t.Fatal(err)
	}
	child := group.Value.Registrations[0]
	binding := group.Value.Children[0]
	losses := []struct{ kind, id string }{
		{"product_split_registrations", group.Value.Request.ProposalID},
		{"product_split_registrations_history", group.ID},
		{"product_split_registrations_admission", group.Value.Request.ProposalID},
		{"product_work_item_registration", child.Value.Definition.ID},
		{"product_work_item_registration_version", structureID("product-work-", child.Value.Definition.ID, child.Digest)},
		{"wave_work_item", child.Value.Definition.ID},
		{"product_split_child_scope", child.Value.Definition.ID},
		{"product_split_child_scope_history", binding.ID},
		{"product_split_child_scopes", group.Value.Request.ProposalID},
		{"product_split_create", binding.Value.CreationID},
	}
	for _, loss := range losses {
		t.Run(loss.kind, func(t *testing.T) {
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			held, err := k.ProductCoverage(ctx, w.Target())
			if err != nil {
				t.Fatal(err)
			}
			if len(held.Value.Retired) != 0 || len(held.Value.Splits) != 0 || len(held.Value.Unavailable) == 0 || len(held.Value.Residual) != len(w.Criteria) || !slices.Contains(held.Value.UnmergedWorkItems, group.Value.Request.ParentWorkItemID) {
				t.Fatal("missing proof preserved retirement", held)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			restored, err := k.ProductCoverage(ctx, w.Target())
			if err != nil || !reflect.DeepEqual(restored, initial) {
				t.Fatal("exact restore did not recover without rewrites", restored, err)
			}
		})
	}
}

func TestProductSplitCoveragePartsRequireExactLocalScopeAndEveryDeclaredPart(t *testing.T) {
	ctx := context.Background()
	k, w, _ := splitScopeInputFixture(t)
	ledger, err := k.ProductCoverage(ctx, w.Target())
	if err != nil {
		t.Fatal(err)
	}
	group := ledger.Value.Splits[0]
	facts := map[string]ProductCoverageFact{}
	factFor := func(child ProductSplitCoverageChild) ProductCoverageFact {
		return ProductCoverageFact{WorkItemID: child.WorkItemID, RegistrationDigest: child.RegistrationDigest, SplitScopeDigest: child.ScopeDigest, LocalCriteria: slices.Clone(child.LocalCriteria)}
	}
	groups := []ProductSplitCoverage{group}
	facts[group.Children[0].WorkItemID] = factFor(group.Children[0])
	covered := aggregateProductSplitCoverage(groups, facts)
	if covered["AC"] || covered["extra"] || !groups[0].Children[0].Merged || groups[0].Children[1].Merged {
		t.Fatal("one part counted whole criterion", covered, groups)
	}
	facts[group.Children[1].WorkItemID] = factFor(group.Children[1])
	covered = aggregateProductSplitCoverage(groups, facts)
	if !covered["AC"] || !covered["extra"] {
		t.Fatal("complete parts did not compose", covered)
	}
	for _, mode := range []string{"scope", "registration", "missing_local", "parent_claim"} {
		t.Run(mode, func(t *testing.T) {
			wrong := factFor(group.Children[1])
			switch mode {
			case "scope":
				wrong.SplitScopeDigest = "wrong"
			case "registration":
				wrong.RegistrationDigest = "wrong"
			case "missing_local":
				wrong.LocalCriteria = wrong.LocalCriteria[:1]
			case "parent_claim":
				wrong.LocalCriteria = []string{"AC", "extra"}
			}
			facts[group.Children[1].WorkItemID] = wrong
			covered := aggregateProductSplitCoverage(groups, facts)
			if covered["AC"] || covered["extra"] || groups[0].Children[1].Merged {
				t.Fatal("incomplete/wrong local proof covered root", covered)
			}
		})
	}
}

func TestProductSplitCoverageOldEpochDoesNotRetireNewRequirements(t *testing.T) {
	k, w, group := splitScopeInputFixture(t)
	ctx := context.Background()
	for _, mode := range []string{"scope", "policy", "source"} {
		t.Run(mode, func(t *testing.T) {
			current := w
			switch mode {
			case "scope":
				current.ScopeRevision++
			case "policy":
				current.Policy.Revision++
			case "source":
				current.SourceID += "-changed"
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			ledger, err := k.productCoverageTx(ctx, tx, current)
			if err != nil {
				t.Fatal(err)
			}
			if len(ledger.Value.Retired) != 0 || len(ledger.Value.Splits) != 0 || len(ledger.Value.Unavailable) == 0 || !slices.Contains(ledger.Value.UnmergedWorkItems, group.Value.Request.ParentWorkItemID) || len(ledger.Value.Residual) != len(w.Criteria) {
				t.Fatal("old delegation crossed epoch", ledger)
			}
		})
	}
	// Old JSON omits optional split fields and remains byte-for-byte compatible.
	raw, err := json.Marshal(ProductCoverageValue{Target: w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["splits"]; ok {
		t.Fatal("empty split field changed old wire")
	}
}

// Audit boundary only: reuse an actual sealed verifier and runner fixture, and
// inject the binding/readiness inputs. This does not enable child execution or
// represent a completed split pipeline.
func TestProductSplitCoverageLocalAuditChecksSealedVerifierScope(t *testing.T) {
	ctx := context.Background()
	k, w, plan, runtime, review, run := productQualityFixture(t)
	if _, err := k.ApplyTaskResult(ctx, review.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
	quality, err := k.CheckProductQuality(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	registration, err := k.ReadProductWorkItemRegistration(ctx, "work")
	if err != nil {
		t.Fatal(err)
	}
	scope := productRootVerificationScope(w, registration)
	binding := ProductSplitChildScopeBinding{}
	binding.Value.Scope = scope
	binding.Value.Child.Value.WorkItemID = "work"
	binding.Value.ScopeDigest, err = contentDigest(scope)
	if err != nil {
		t.Fatal(err)
	}
	binding.Digest, err = contentDigest(binding.Value)
	if err != nil {
		t.Fatal(err)
	}
	binding.ID = structureID("split-child-scope-", "work", binding.Digest)
	registration.Value.SplitScope = &ProductSplitChildScopeReference{BindingID: binding.ID, BindingDigest: binding.Digest}
	registration.Digest, err = contentDigest(registration.Value)
	if err != nil {
		t.Fatal(err)
	}
	quality.Value.RegistrationDigest = registration.Digest
	quality.Digest, err = contentDigest(quality.Value)
	if err != nil {
		t.Fatal(err)
	}
	ready := ProductPublicationReadiness{}
	ready.Value.Quality = quality
	if err := k.Put(ctx, "product_split_child_scope_history", binding.ID, w.ID, binding); err != nil {
		t.Fatal(err)
	}
	if err := k.Put(ctx, "product_publication_readiness_history", "split-local-audit", w.ID, ready); err != nil {
		t.Fatal(err)
	}
	job := ProductMerge{}
	job.Value.ReadinessID = "split-local-audit"
	got, criteria, err := splitMergedLocalScope(ctx, k.DB, registration, job)
	if err != nil || got != binding.Value.ScopeDigest || !reflect.DeepEqual(criteria, scope.Included) {
		t.Fatal("exact sealed local plan rejected", got, criteria, err)
	}
	for _, mode := range []string{"wrong_scope", "missing_plan_history", "no_verifier", "wrong_candidate"} {
		t.Run(mode, func(t *testing.T) {
			changed := ready
			switch mode {
			case "wrong_scope":
				wrong := binding
				wrong.Value.Scope.OriginalRequest += " changed"
				wrong.Value.ScopeDigest, _ = contentDigest(wrong.Value.Scope)
				wrong.Digest, _ = contentDigest(wrong.Value)
				// Rehash the local metadata; the independently sealed scope still differs.
				wrong.ID = binding.ID
				changedRegistration := registration
				changedRegistration.Value.SplitScope = &ProductSplitChildScopeReference{BindingID: binding.ID, BindingDigest: wrong.Digest}
				if err := k.Put(ctx, "product_split_child_scope_history", binding.ID, w.ID, wrong); err != nil {
					t.Fatal(err)
				}
				if _, _, err := splitMergedLocalScope(ctx, k.DB, changedRegistration, job); err == nil {
					t.Fatal("rehashing changed requirements replaced sealed scope")
				}
				if err := k.Put(ctx, "product_split_child_scope_history", binding.ID, w.ID, binding); err != nil {
					t.Fatal(err)
				}
				return
			case "missing_plan_history":
				var owner string
				var raw []byte
				if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind='product_verification_plan_history' AND id=?", plan.ID).Scan(&owner, &raw); err != nil {
					t.Fatal(err)
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_verification_plan_history' AND id=?", plan.ID); err != nil {
					t.Fatal(err)
				}
				if _, _, err := splitMergedLocalScope(ctx, k.DB, registration, job); err == nil {
					t.Fatal("missing sealed plan history accepted")
				}
				if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_verification_plan_history',?,?,?)", plan.ID, owner, raw); err != nil {
					t.Fatal(err)
				}
				return
			case "no_verifier":
				changed.Value.Quality.Value.Verifications = nil
			case "wrong_candidate":
				changed.Value.Quality.Value.Candidate.SHA = "wrong"
			}
			changed.Value.Quality.Digest, _ = contentDigest(changed.Value.Quality.Value)
			if err := k.Put(ctx, "product_publication_readiness_history", "split-local-audit", w.ID, changed); err != nil {
				t.Fatal(err)
			}
			if _, _, err := splitMergedLocalScope(ctx, k.DB, registration, job); err == nil {
				t.Fatal("invalid verifier accepted", mode)
			}
			if err := k.Put(ctx, "product_publication_readiness_history", "split-local-audit", w.ID, ready); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProductSplitCoverageReducerRequiresDistinctDeclaredPartIDs(t *testing.T) {
	ctx := context.Background()
	k, w, _ := splitScopeInputFixture(t)
	ledger, err := k.ProductCoverage(ctx, w.Target())
	if err != nil {
		t.Fatal(err)
	}
	original, _ := json.Marshal(ledger.Value.Splits)
	for _, mode := range []string{"all_parts", "missing_child", "duplicate_part", "missing_declared_part", "count_mismatch", "whole_overlap"} {
		t.Run(mode, func(t *testing.T) {
			var groups []ProductSplitCoverage
			if err := json.Unmarshal(original, &groups); err != nil {
				t.Fatal(err)
			}
			facts := map[string]ProductCoverageFact{}
			for _, child := range groups[0].Children {
				facts[child.WorkItemID] = ProductCoverageFact{WorkItemID: child.WorkItemID, RegistrationDigest: child.RegistrationDigest, SplitScopeDigest: child.ScopeDigest, LocalCriteria: slices.Clone(child.LocalCriteria)}
			}
			second := &groups[0].Children[1].Coverage[0]
			switch mode {
			case "missing_child":
				delete(facts, groups[0].Children[1].WorkItemID)
			case "duplicate_part":
				second.PartID = groups[0].Children[0].Coverage[0].PartID
			case "missing_declared_part":
				for i := range groups[0].Children {
					groups[0].Children[i].Coverage[0].PartCount = 3
				}
			case "count_mismatch":
				second.PartCount = 3
			case "whole_overlap":
				second.PartID = ""
				second.PartCount = 0
			}
			covered := aggregateProductSplitCoverage(groups, facts)
			if covered["AC"] != (mode == "all_parts") {
				t.Fatal("declared distinct part set was not enforced", mode, covered)
			}
		})
	}
}
