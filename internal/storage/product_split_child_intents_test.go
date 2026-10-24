package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func splitChildPlanFixture(t *testing.T) (*KernelStore, ProductSplitProposalRequest, ProductSplitProposal, ProductSplitChildPlanRequest) {
	t.Helper()
	k, _, r := productSplitProposalFixture(t)
	r.Proposal.Children[0].Coverage[0] = splitting.Coverage{ParentCriterion: "AC", ChildCriterion: "first-ac", PartID: "one", PartCount: 2, Basis: "first part"}
	r.Proposal.Children[1].Coverage[0] = splitting.Coverage{ParentCriterion: "AC", ChildCriterion: "second-ac", PartID: "two", PartCount: 2, Basis: "remaining part"}
	r.Proposal.Children[1].Criteria = append(r.Proposal.Children[1].Criteria, verification.Criterion{ID: "extra-ac", Requirement: "complete extra requirement"})
	r.Proposal.Children[1].Coverage = append(r.Proposal.Children[1].Coverage, splitting.Coverage{ParentCriterion: "extra", ChildCriterion: "extra-ac"})
	p, err := k.PrepareProductSplitProposal(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return k, r, p, ProductSplitChildPlanRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID, ProposalID: p.ID, ProposalDigest: p.Digest}
}

func TestProductSplitChildPlanStablePartsDependenciesAndReopen(t *testing.T) {
	ctx := context.Background()
	k, r, _, request := splitChildPlanFixture(t)
	before, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := k.PrepareProductSplitChildPlan(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Children) != 2 {
		t.Fatal(plan)
	}
	for i, child := range plan.Children {
		if !reflect.DeepEqual(child.Value.Child, r.Proposal.Children[i]) || child.Value.Depth != 1 || child.Value.Repository != "owner/repo" || child.Value.RepositoryID != 17 {
			t.Fatal("lost child scope", child)
		}
		if !strings.Contains(child.Value.Body, child.Value.Marker) || !strings.Contains(child.Value.Body, "\n\n"+string([]byte{96, 96, 96})+"harness-split-part\n") || strings.Contains(child.Value.Body, "harness-scope") {
			t.Fatal("invalid child body", child.Value.Body)
		}
		if child.Value.WorkItemID == r.ParentWorkItemID || child.ID != plan.Set.Value.ChildIDs[i] {
			t.Fatal("unstable child identity")
		}
	}
	if !reflect.DeepEqual(plan.Children[0].Value.ParentCriteria, []string{"AC"}) || !reflect.DeepEqual(plan.Children[1].Value.ParentCriteria, []string{"AC", "extra"}) || !reflect.DeepEqual(plan.Children[1].Value.DependsOnWorkItemIDs, []string{plan.Children[0].Value.WorkItemID}) {
		t.Fatal("lost parent mapping or dependency", plan)
	}
	retry, err := k.PrepareProductSplitChildPlan(ctx, request)
	if err != nil || !reflect.DeepEqual(retry, plan) {
		t.Fatal("retry changed facts", retry, err)
	}
	after, err := k.Wave(ctx, before.ID)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("changed parent", err)
	}
	afterBudget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil || !reflect.DeepEqual(afterBudget, budget) {
		t.Fatal("changed lifetime budget", err)
	}
	for _, query := range []string{"SELECT count(*) FROM kernel_tasks", "SELECT count(*) FROM kernel_run_bindings", "SELECT count(*) FROM kernel_tool_operations", "SELECT count(*) FROM records WHERE kind IN ('product_target','product_workspace')", "SELECT count(*)-1 FROM records WHERE kind='product_work_item_registration'"} {
		var n int
		if err := k.DB.QueryRow(query).Scan(&n); err != nil || n != 0 {
			t.Fatal("intent granted execution authority", query, n, err)
		}
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	reopened, err := k.ReadProductSplitChildPlan(ctx, request)
	if err != nil || !reflect.DeepEqual(reopened, plan) {
		t.Fatal("restart changed facts", reopened, err)
	}
}

func TestProductSplitChildPlanFaultRollsBackWholeSet(t *testing.T) {
	ctx := context.Background()
	k, _, _, r := splitChildPlanFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_children BEFORE INSERT ON records WHEN NEW.kind='product_split_children_admission' BEGIN SELECT RAISE(FAIL,'late child failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitChildPlan(ctx, r); err == nil {
		t.Fatal("fault not reached")
	}
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_split_child%'").Scan(&n); err != nil || n != 0 {
		t.Fatal("partial child set committed", n, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_children"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitChildPlan(ctx, r); err != nil {
		t.Fatal("rollback prevented retry", err)
	}
}

func TestProductSplitChildPlanEvidenceLossHoldsAndExactRestore(t *testing.T) {
	ctx := context.Background()
	k, parent, p, r := splitChildPlanFixture(t)
	plan, err := k.PrepareProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	losses := []struct{ kind, id string }{
		{"product_split_children", r.ProposalID}, {"product_split_children_history", plan.Set.ID}, {"product_split_children_admission", r.ProposalID},
		{"product_split_proposal", splitProposalHead(parent)}, {"product_split_proposal_history", p.ID}, {"product_split_proposal_admission", splitProposalHead(parent)}, {"product_split_proposal_request", splitProposalRequestID(parent)},
		{"product_work_item_registration_version", structureID("product-work-", parent.ParentWorkItemID, parent.RegistrationDigest)},
	}
	for _, child := range plan.Children {
		for _, kind := range []string{"product_split_child_intent", "product_split_child_intent_history", "product_split_child_intent_admission"} {
			losses = append(losses, struct{ kind, id string }{kind, child.ID})
		}
	}
	for _, loss := range losses {
		t.Run(loss.kind+"/"+loss.id, func(t *testing.T) {
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.PrepareProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("recreated lost evidence", err)
			}
			if _, err := k.ReadProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("read ignored lost evidence", err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
		})
	}
	restored, err := k.PrepareProductSplitChildPlan(ctx, r)
	if err != nil || !reflect.DeepEqual(restored, plan) {
		t.Fatal("reconstructed original facts", restored, err)
	}
}

func TestProductSplitChildWitnessPreventsProposalAndSetRecreation(t *testing.T) {
	for _, witness := range []string{"product_split_child_intent", "product_split_child_intent_history", "product_split_child_intent_admission", "product_split_children_admission"} {
		t.Run(witness, func(t *testing.T) {
			ctx := context.Background()
			k, parent, _, r := splitChildPlanFixture(t)
			if _, err := k.PrepareProductSplitChildPlan(ctx, r); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", witness); err != nil {
				t.Fatal(err)
			}
			parent.Key = "different-key"
			if _, err := k.PrepareProductSplitProposal(ctx, parent); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost parent recreated despite child witness", err)
			}
			if _, err := k.PrepareProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost set recreated despite child witness", err)
			}
		})
	}
}

func TestProductSplitChildPlanPauseAndCurrentSourceDrift(t *testing.T) {
	for _, prepareFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[prepareFirst], func(t *testing.T) {
			ctx := context.Background()
			k, _, _, r := splitChildPlanFixture(t)
			var saved ProductSplitChildPlan
			if prepareFirst {
				var err error
				saved, err = k.PrepareProductSplitChildPlan(ctx, r)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-child-plan", Target: r.Target, Action: "pause", Reason: "inspect split"}); err != nil {
				t.Fatal(err)
			}
			got, err := k.PrepareProductSplitChildPlan(ctx, r)
			if prepareFirst {
				if err != nil || !reflect.DeepEqual(got, saved) {
					t.Fatal("pause hid immutable intent", err)
				}
			} else if !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("pause created intent", err)
			}
		})
	}
	ctx := context.Background()
	k, _, _, r := splitChildPlanFixture(t)
	limits, err := k.WaveOperationLimits(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	limits.SplitChildren--
	if err := k.Put(ctx, "wave_operation_limits", r.Target.WaveID, r.Target.WaveID, limits); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("changed authority created new intent", err)
	}
}

func TestProductSplitChildPlanDirectionChangesOnlyAuditAccess(t *testing.T) {
	for _, prepareFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "new_intent", true: "historical_intent"}[prepareFirst], func(t *testing.T) {
			ctx := context.Background()
			k, _, _, r := splitChildPlanFixture(t)
			var saved ProductSplitChildPlan
			if prepareFirst {
				var err error
				saved, err = k.PrepareProductSplitChildPlan(ctx, r)
				if err != nil {
					t.Fatal(err)
				}
			}
			direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "new-split-scope", Target: r.Target, Instruction: "new scope needs its own split approval"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
				t.Fatal(err)
			}
			got, err := k.PrepareProductSplitChildPlan(ctx, r)
			if prepareFirst {
				if err != nil || !reflect.DeepEqual(got, saved) {
					t.Fatal("new scope erased immutable audit", err)
				}
			} else if !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("old target created intent", err)
			}
		})
	}
}

func TestProductSplitChildPlanRejectsChangedRequestAndCorruptChild(t *testing.T) {
	ctx := context.Background()
	k, _, _, r := splitChildPlanFixture(t)
	plan, err := k.PrepareProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"target", "parent", "proposal", "digest"} {
		bad := r
		switch field {
		case "target":
			bad.Target.ScopeRevision++
		case "parent":
			bad.ParentWorkItemID = "another-parent"
		case "proposal":
			bad.ProposalID = "another-proposal"
		case "digest":
			bad.ProposalDigest = strings.Repeat("e", 64)
		}
		if _, err := k.PrepareProductSplitChildPlan(ctx, bad); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("changed request reused plan", field, err)
		}
	}
	child := plan.Children[0]
	child.Value.Body += "untrusted edit"
	if err := k.Put(ctx, "product_split_child_intent", child.ID, r.ProposalID, child); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitChildPlan(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("corrupt child accepted", err)
	}
}
