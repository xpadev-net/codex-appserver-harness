package storage

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func TestProductSplitPreparationHoldsOldRootReadinessButAllowsSplitSourceGate(t *testing.T) {
	ctx := context.Background()
	k, w, task, config := productModelFixture(t, "product_v3")
	release, err := k.BindProductModelRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if set := productModelReady(t, k, w); len(set.Tasks) != 1 {
		t.Fatal("parent not initially ready", set)
	}
	registration, err := k.ReadProductWorkItemRegistration(ctx, "work")
	if err != nil {
		t.Fatal(err)
	}
	proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: "work", ScopeRevision: w.ScopeRevision, Outcome: "split", Children: []splitting.Child{
		{Key: "first", Title: "first piece", Requirement: "first piece of original requirement", Criteria: []verification.Criterion{{ID: "first-ac", Requirement: "first behavior"}}, Coverage: []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: "first-ac", PartID: "first", PartCount: 2, Basis: "first half"}}, DependsOn: []string{}},
		{Key: "second", Title: "second piece", Requirement: "remaining original requirement", Criteria: []verification.Criterion{{ID: "second-ac", Requirement: "remaining behavior"}}, Coverage: []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: "second-ac", PartID: "second", PartCount: 2, Basis: "remaining half"}}, DependsOn: []string{"first"}},
	}}
	saved, err := k.PrepareProductSplitExecution(ctx, ProductSplitProposalRequest{Target: w.Target(), Key: "adopt-whole-split", ParentWorkItemID: "work", RegistrationDigest: registration.Digest, Proposal: proposal})
	if err != nil {
		t.Fatal(err)
	}
	if set := productModelReady(t, k, w); len(set.Tasks) != 0 {
		t.Fatal("split parent kept model authority", set)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, task.ID); err == nil {
		t.Fatal("old parent task prepared")
	}
	if _, err := k.ReadProductSplitProposal(ctx, w.Target(), "work"); err != nil {
		t.Fatal("execution hold blocked split audit/source", err)
	}
	job, err := k.ReadProductSplitCreate(ctx, saved.Value.Creations[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); err != nil {
		t.Fatal("parent execution hold blocked split send preflight", err)
	}
}

func TestProductSplitPreparationHoldsOnlyTheParentWithEverySurvivingWitness(t *testing.T) {
	for _, keep := range []string{"product_split_preparation", "product_split_preparation_history", "product_split_preparation_admission", "product_split_preparation_creation"} {
		t.Run(keep, func(t *testing.T) {
			ctx := context.Background()
			k, w, r := productSplitProposalFixture(t, "independent-root")
			if _, err := k.PrepareProductSplitExecution(ctx, r); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_preparation%' AND kind<>?", keep); err != nil {
				t.Fatal(err)
			}
			held, err := productWorkItemExecutionHeld(ctx, k.DB, w, r.ParentWorkItemID)
			if err != nil || !held {
				t.Fatal("missing preparation evidence enabled old root", held, err)
			}
			held, err = productWorkItemExecutionHeld(ctx, k.DB, w, "independent-root")
			if err != nil || held {
				t.Fatal("split parent held an unrelated root", held, err)
			}
		})
	}
}

func TestProductSplitPreparationHoldSurvivesAnActualPlanRevision(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t, "independent-root")
	if _, err := k.PrepareProductSplitExecution(ctx, r); err != nil {
		t.Fatal(err)
	}
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "after-split", FailurePolicy: "drain_and_block", Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{"old-parent"}}, Tasks: []contract.TaskSpec{{ID: "old-parent", Kind: "work", Instruction: "must not execute old parent after split", Criteria: []string{"AC", "extra"}, WorkItemIDs: []string{r.ParentWorkItemID}, DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}}}}}}
	if _, err := k.AdoptPlan(ctx, w.Target(), "new-plan-after-split", plan); err != nil {
		t.Fatal(err)
	}
	firstPlan, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	plan.Phases[0].ID = "replanned-after-split"
	plan.Phases[0].Tasks[0].ID = "replanned-old-parent"
	plan.Phases[0].Completion.RequiredTaskIDs = []string{"replanned-old-parent"}
	if _, err := k.AdoptPlan(ctx, firstPlan.Target(), "actual-replan-after-split", plan); err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.PlanRevision <= firstPlan.PlanRevision {
		t.Fatal("plan did not advance", current, err)
	}
	held, err := productWorkItemExecutionHeld(ctx, k.DB, current, r.ParentWorkItemID)
	if err != nil || !held {
		t.Fatal("replan enabled old parent", held, err)
	}
	held, err = productWorkItemExecutionHeld(ctx, k.DB, current, "independent-root")
	if err != nil || held {
		t.Fatal("replan held independent root", held, err)
	}
}

func TestProductSplitPreparationLegacyProvenanceIsNotFilledOnReplay(t *testing.T) {
	ctx := context.Background()
	k, w, r := productSplitProposalFixture(t)
	saved, err := k.PrepareProductSplitExecution(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	legacy := saved
	legacy.Value.Target = nil
	legacy.Value.ParentWorkItemID = ""
	legacy.Digest, err = contentDigest(legacy.Value)
	if err != nil {
		t.Fatal(err)
	}
	head := splitProposalHead(r)
	legacy.ID = structureID("split-preparation-", head, legacy.Digest)
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_preparation_history' AND id=?", saved.ID); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct{ kind, id, owner string }{
		{"product_split_preparation", head, w.ID},
		{"product_split_preparation_history", legacy.ID, head},
		{"product_split_preparation_admission", head, w.ID},
	} {
		if err := k.Put(ctx, entry.kind, entry.id, entry.owner, legacy); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := k.DB.Exec("UPDATE records SET payload=json_remove(payload,'$.target','$.parent_work_item_id') WHERE kind='product_split_preparation_creation'"); err != nil {
		t.Fatal(err)
	}
	replay, err := k.PrepareProductSplitExecution(ctx, r)
	if err != nil || !reflect.DeepEqual(replay, legacy) {
		t.Fatal("legacy provenance was inferred", replay, err)
	}
	held, err := productWorkItemExecutionHeld(ctx, k.DB, w, r.ParentWorkItemID)
	if err != nil || !held {
		t.Fatal("unknown legacy provenance enabled execution", held, err)
	}
	raw, err := json.Marshal(replay)
	if err != nil || strings.Contains(string(raw), "\"target\"") || strings.Contains(string(raw), "\"parent_work_item_id\"") {
		t.Fatal("legacy wire format overlaid", string(raw), err)
	}
}
