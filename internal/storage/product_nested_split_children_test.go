package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func nestedChildPlanFixture(t *testing.T) (*KernelStore, ProductSplitChildPlanRequest, ProductNestedSplitChildPlan) {
	t.Helper()
	ctx := context.Background()
	k, r, _, proposal := nestedProposalFixture(t)
	request := ProductSplitChildPlanRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID, ProposalID: proposal.Value.Proposal.ID, ProposalDigest: proposal.Value.Proposal.Digest}
	if _, err := k.PrepareProductSplitChildPlan(ctx, request); err != nil {
		t.Fatal(err)
	}
	var saved ProductNestedSplitChildPlan
	if err := k.Record(ctx, "product_nested_split_children", request.ProposalID, &saved); err != nil {
		t.Fatal(err)
	}
	return k, request, saved
}

func TestProductNestedSplitChildPlanPersistsStableDepthLocalMappingAndWholeAncestor(t *testing.T) {
	ctx := context.Background()
	k, r, proof, proposal := nestedProposalFixture(t)
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	request := ProductSplitChildPlanRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID, ProposalID: proposal.Value.Proposal.ID, ProposalDigest: proposal.Value.Proposal.Digest}
	plan, err := k.PrepareProductSplitChildPlan(ctx, request)
	if err != nil || len(plan.Children) != 2 {
		t.Fatal(plan, err)
	}
	var saved ProductNestedSplitChildPlan
	if err := k.Record(ctx, "product_nested_split_children", request.ProposalID, &saved); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saved.Value.Plan, plan) || !reflect.DeepEqual(saved.Value.ProductNestedSplitProposalValue, proposal.Value) || saved.Value.EnvelopeID != proposal.ID || saved.Value.EnvelopeDigest != proposal.Digest || !reflect.DeepEqual(saved.Value.ProductSplitParentScopeValue, proof.Value) {
		t.Fatal("whole ancestor group not pinned", saved)
	}
	for i, child := range plan.Children {
		if child.Value.Depth != 2 || !reflect.DeepEqual(child.Value.ParentCriteria, []string{[]string{"AC", "extra"}[i]}) || child.Value.Child.Coverage[0].ParentCriterion != r.Proposal.Children[i].Coverage[0].ParentCriterion {
			t.Fatal("root bookkeeping replaced local obligation", child)
		}
		for _, kind := range []string{"product_nested_split_child_intent", "product_nested_split_child_intent_history", "product_nested_split_child_intent_admission"} {
			var witness ProductNestedSplitChildPlan
			if err := k.Record(ctx, kind, child.ID, &witness); err != nil || !reflect.DeepEqual(witness, saved) {
				t.Fatal("naked/partial intent witness", kind, err)
			}
		}
	}
	if _, err := k.ReserveProductSplitChildren(ctx, request); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("metadata intent acquired seats", err)
	}
	if _, err := k.PrepareProductSplitCreate(ctx, request, plan.Children[0].ID, "nested-owner"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("metadata intent created send journal", err)
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("metadata spent/reset budget", err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-nested-plan", Target: r.Target, Action: "pause", Reason: "audit"}); err != nil {
		t.Fatal(err)
	}
	replay, err := k.PrepareProductSplitChildPlan(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, plan) {
		t.Fatal("reopen/pause changed intent IDs", replay, err)
	}
	direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "nested-plan-new-scope", Target: r.Target, Instruction: "new requirements", NextPolicyRevision: r.Target.PolicyRevision + 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	historical, err := k.ReadProductSplitChildPlan(ctx, request)
	if err != nil || !reflect.DeepEqual(historical, plan) {
		t.Fatal("historical plan changed", historical, err)
	}
	w, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	request.Target = w.Target()
	if _, err := k.PrepareProductSplitChildPlan(ctx, request); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("old intents retargeted new scope", err)
	}
}

func TestProductNestedSplitChildPlanIsAtomicAndRequiresFreshLiveSourceForNewIntent(t *testing.T) {
	for _, mode := range []string{"late_failure", "stale", "hold", "pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, r, _, proposal := nestedProposalFixture(t)
			request := ProductSplitChildPlanRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID, ProposalID: proposal.Value.Proposal.ID, ProposalDigest: proposal.Value.Proposal.Digest}
			switch mode {
			case "late_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_nested_intent BEFORE INSERT ON records WHEN NEW.kind='product_nested_split_child_intent_admission' BEGIN SELECT RAISE(ABORT,'late nested intent failure'); END"); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock().Add(2 * time.Minute)
				k.Now = func() time.Time { return now }
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "no-new-nested-intent", Target: r.Target, Action: "pause", Reason: "operator"}); err != nil {
					t.Fatal(err)
				}
			case "hold":
				prior, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
				if err != nil {
					t.Fatal(err)
				}
				facts := prior.Value.Facts
				facts.Hold = true
				if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, prior.ID, facts); err != nil {
					t.Fatal(err)
				}
			}
			plan, err := k.PrepareProductSplitChildPlan(ctx, request)
			if err == nil || plan.Set.ID != "" {
				t.Fatal("failed/held nested intent returned success", plan, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_nested_split_children','product_nested_split_children_history','product_nested_split_children_admission','product_nested_split_child_intent','product_nested_split_child_intent_history','product_nested_split_child_intent_admission')").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial nested intent transaction", count, err)
			}
		})
	}
}

func TestProductNestedSplitChildPlanMissingAnyIndexRequiresExactRestore(t *testing.T) {
	ctx := context.Background()
	k, r, saved := nestedChildPlanFixture(t)
	first := saved.Value.Plan.Children[0]
	for _, loss := range []struct{ kind, id string }{
		{"product_nested_split_children", r.ProposalID}, {"product_nested_split_children_history", saved.ID}, {"product_nested_split_children_admission", r.ProposalID},
		{"product_nested_split_child_intent", first.ID}, {"product_nested_split_child_intent_history", first.ID}, {"product_nested_split_child_intent_admission", first.ID},
		{"product_nested_split_proposal", saved.Value.ProposalHeadID}, {"product_split_parent_scope", r.ParentWorkItemID},
	} {
		t.Run(loss.kind, func(t *testing.T) {
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			if out, err := k.PrepareProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) || out.Set.ID != "" {
				t.Fatal("lost nested intent repaired", out, err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			restored, err := k.ReadProductSplitChildPlan(ctx, r)
			if err != nil || !reflect.DeepEqual(restored, saved.Value.Plan) {
				t.Fatal("exact restore changed plan", restored, err)
			}
		})
	}
	// The whole plan must remember a lost proposal even when every proposal
	// envelope is missing, rather than deriving a new timestamp/identity.
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request')"); err != nil {
		t.Fatal(err)
	}
	if out, err := k.PrepareProductNestedSplitProposal(ctx, saved.Value.Proposal.Value.Request, saved.Value.ParentProof); !errors.Is(err, ErrProductSplitHeld) || out.ID != "" {
		t.Fatal("intent allowed nested proposal reset", out, err)
	}
}

func TestProductNestedSplitChildPlanEachWitnessAlonePreservesAllAncestorSiblings(t *testing.T) {
	for _, kind := range []string{"product_nested_split_children", "product_nested_split_children_history", "product_nested_split_children_admission", "product_nested_split_child_intent", "product_nested_split_child_intent_history", "product_nested_split_child_intent_admission"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, r, saved := nestedChildPlanFixture(t)
			original, err := k.ReadProductSplitProposal(ctx, saved.Value.Request.Target, saved.Value.Request.ParentWorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			keepID := r.ProposalID
			if kind == "product_nested_split_children_history" {
				keepID = saved.ID
			}
			if kind == "product_nested_split_child_intent" || kind == "product_nested_split_child_intent_history" || kind == "product_nested_split_child_intent_admission" {
				keepID = saved.Value.Plan.Children[0].ID
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE (kind LIKE 'product_split_%' OR kind LIKE 'product_nested_split_%') AND NOT(kind=? AND id=?)", kind, keepID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND (json_type(payload,'$.value.split_scope') IS NOT NULL OR json_type(payload,'$.split_scope') IS NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			for _, ancestor := range saved.Value.Children {
				if _, err := k.CreateProductWave(ctx, splitRootRequest(ancestor.Value.Issue.RepositoryID, ancestor.Value.Issue.Number, "github", "root-after-nested-intent-loss")); !errors.Is(err, ErrProductSplitChildSourceHeld) {
					t.Fatal("ancestor sibling admitted as root", err)
				}
				if _, err := k.ReadProductSplitCreate(ctx, ancestor.Value.CreationID); !readinessHoldError(err) {
					t.Fatal("ancestor journal reconstructed", err)
				}
			}
			if _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); !readinessHoldError(err) {
				t.Fatal("ancestor seats/retry refunded", err)
			}
			original.Value.Request.Key = "new-root-after-intent-only"
			if _, err := k.PrepareProductSplitProposal(ctx, original.Value.Request); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("ancestor parent resumed", err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, target := range page.Targets {
				found = found || target.ID == saved.Value.Children[0].Value.CreationID
			}
			if !found {
				t.Fatal("ancestor recovery witness hidden", page)
			}
		})
	}
}
