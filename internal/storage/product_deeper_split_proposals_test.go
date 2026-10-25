package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func deeperSplitProposalFixture(t *testing.T, depth int, ownPolicy ...string) (*KernelStore, ProductSplitProposalRequest, ProductSplitParentScope, ProductNestedSplitChildScopes) {
	t.Helper()
	ctx := context.Background()
	k, r, previous := nestedRegistrationScopeFixture(t, depth)
	registered, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	binding := previous.Value.Scopes.Value.Children[0]
	registration := registered.Value.Registrations[0]
	facts := splitSourceFacts(binding)
	if len(ownPolicy) > 0 {
		facts.PolicyDigest = ownPolicy[0]
	}
	if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, registration.Value.Definition.ID, "", facts); err != nil {
		t.Fatal(err)
	}
	proof, err := k.PrepareProductSplitParentScope(ctx, r.Target, registration.Value.Definition.ID)
	if err != nil {
		t.Fatal(err)
	}
	local := binding.Value.Scope.Criteria[0]
	proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: registration.Value.Definition.ID, ScopeRevision: r.Target.ScopeRevision, Outcome: "split", Children: []splitting.Child{}}
	for i, key := range []string{"left", "right"} {
		criterion := []string{"AC", "great-right"}[i] // One local ID deliberately collides with the Wave ID.
		requirement := key + " portion of " + local.Requirement
		proposal.Children = append(proposal.Children, splitting.Child{Key: key, Title: "great " + key, Requirement: requirement, Criteria: []verification.Criterion{{ID: criterion, Requirement: requirement}}, Coverage: []splitting.Coverage{{ParentCriterion: local.ID, ChildCriterion: criterion, PartID: key, PartCount: 2, Basis: requirement}}, DependsOn: []string{}})
	}
	return k, ProductSplitProposalRequest{Target: r.Target, Key: "great-proposal", ParentWorkItemID: registration.Value.Definition.ID, RegistrationDigest: registration.Digest, Proposal: proposal}, proof, previous
}

func TestProductDeeperSplitProposalUsesCanonicalGrandchildAndRetainsLocalParts(t *testing.T) {
	ctx := context.Background()
	base, r, parent, previous := deeperSplitProposalFixture(t, 3)
	ref := ProductSplitParentScopeReference{ID: parent.ID, Digest: parent.Digest}
	before, err := base.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	head := splitProposalHead(r)
	for _, mode := range []string{"pause", "stale", "late_failure", "ancestor_hold", "ancestor_closed", "ancestor_stale"} {
		t.Run(mode, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			switch mode {
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "deeper-pause", Target: r.Target, Action: "pause", Reason: "hold"}); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock()
				k.Now = func() time.Time { return now.Add(2 * time.Minute) }
			case "ancestor_hold", "ancestor_closed", "ancestor_stale":
				ancestorID := previous.Value.Scopes.Value.Request.ParentWorkItemID
				ancestor, err := k.ReadProductSplitChildScope(ctx, r.Target, ancestorID)
				if err != nil {
					t.Fatal(err)
				}
				prior, err := k.ReadProductSplitChildSource(ctx, r.Target, ancestorID)
				if err != nil {
					t.Fatal(err)
				}
				facts := splitSourceFacts(ancestor)
				if mode == "ancestor_hold" {
					facts.Hold = true
				} else if mode == "ancestor_closed" {
					facts.IssueState = "closed"
				}
				if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, ancestorID, prior.ID, facts); err != nil {
					t.Fatal(err)
				}
				own, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "ancestor_stale" {
					now := k.clock()
					k.Now = func() time.Time { return now.Add(2 * time.Minute) }
					if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, own.ID, own.Value.Facts); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := k.RequireProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, own.Value.Facts.PolicyDigest, time.Minute); err != nil {
					t.Fatal("fixture own source was not ready", err)
				}
			case "late_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_deeper_proposal BEFORE INSERT ON records WHEN NEW.kind='product_nested_split_proposal_request' BEGIN SELECT RAISE(ABORT,'late deeper proposal failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.PrepareProductNestedSplitProposal(ctx, r, ref); err == nil || mode != "late_failure" && !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("invalid deeper admission accepted or failed for an unrelated reason", err)
			}
			var n int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request') AND json_extract(payload,'$.value.proposal_head_id')=?", head).Scan(&n); err != nil || n != 0 {
				t.Fatal("partial deeper proposal", n, err)
			}
			if mode == "ancestor_stale" {
				ancestorID := previous.Value.Scopes.Value.Request.ParentWorkItemID
				source, err := k.ReadProductSplitChildSource(ctx, r.Target, ancestorID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, ancestorID, source.ID, source.Value.Facts); err != nil {
					t.Fatal(err)
				}
				if _, err := k.PrepareProductNestedSplitProposal(ctx, r, ref); err != nil {
					t.Fatal("refreshing only the stale ancestor did not admit the same proposal at the same clock", err)
				}
			}

		})
	}
	if t.Failed() {
		return
	}
	saved, err := base.PrepareProductNestedSplitProposal(ctx, r, ref)
	if err != nil {
		t.Fatal("canonical grandchild admission denied", err)
	}
	p := saved.Value.Proposal
	if p.Value.Depth != 2 || p.Value.Limits.SplitDepth != 3 || p.Value.ParentSplitScope == nil || p.Value.ParentScopeProof == nil || p.Value.ParentScopeProof.ID != parent.ID || !reflect.DeepEqual(saved.Value.AncestorWitnesses, parent.Value.AncestorWitnesses) || len(saved.Value.AncestorWitnesses) == 0 {
		t.Fatal("parent depth, authority or older witnesses lost")
	}
	local := previous.Value.Scopes.Value.Children[0].Value.Scope.Criteria[0].ID
	if !reflect.DeepEqual(p.Value.ParentCriterionRoots, map[string]string{local: "AC"}) {
		t.Fatal("root mapping replaced local obligation", p.Value.ParentCriterionRoots)
	}
	var legacy int
	if err := base.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_child_scope' AND id=?", r.ParentWorkItemID).Scan(&legacy); err != nil || legacy != 0 {
		t.Fatal("deeper admission required an invented legacy binding", legacy, err)
	}
	planRequest := ProductSplitChildPlanRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID, ProposalID: p.ID, ProposalDigest: p.Digest}
	plan, err := base.PrepareProductSplitChildPlan(ctx, planRequest)
	if err != nil || len(plan.Children) != 2 {
		t.Fatal("deeper local-part plan denied", err)
	}
	for _, child := range plan.Children {
		coverage := child.Value.Child.Coverage[0]
		if child.Value.Depth != 3 || !reflect.DeepEqual(child.Value.ParentCriteria, []string{"AC"}) || coverage.ParentCriterion != local || coverage.PartCount != 2 || coverage.PartID == "" {
			t.Fatal("local parts flattened", child.Value.Depth, child.Value.ParentCriteria, coverage)
		}
	}
	read, err := base.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID)
	if err != nil || !reflect.DeepEqual(read, p) {
		t.Fatal("deeper proposal could not be authenticated", err)
	}
	after, err := base.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("deeper metadata reset/spent operation budgets", err)
	}
	var tasks int
	if err := base.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal("metadata dispatched Tasks", tasks, err)
	}
	t.Run("direction_preserves_immutable_history", func(t *testing.T) {
		k := cloneNestedScopeStore(t, base)
		direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "deeper-history-direction", Target: r.Target, Instruction: "new requirements", NextPolicyRevision: r.Target.PolicyRevision + 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID); !readinessHoldError(err) {
			t.Fatal("current proposal port accepted an old target", err)
		}
		tx, err := k.kernelTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		history, historyErr := loadProductSplitProposal(ctx, tx, r)
		tx.Rollback()
		if historyErr != nil || !reflect.DeepEqual(history, p) {
			t.Fatal("direction lost canonical deeper proposal history", historyErr)
		}
		historyPlan, err := k.ReadProductSplitChildPlan(ctx, planRequest)
		if err != nil || !reflect.DeepEqual(historyPlan, plan) {
			t.Fatal("direction lost canonical deeper plan history", err)
		}
		wave, err := k.Wave(ctx, r.Target.WaveID)
		if err != nil {
			t.Fatal(err)
		}
		changed := cloneSplitRequest(t, r)
		changed.Target, changed.Key, changed.Proposal.ScopeRevision = wave.Target(), "deeper-new-scope", wave.ScopeRevision
		if _, err := k.PrepareProductNestedSplitProposal(ctx, changed, ref); err == nil {
			t.Fatal("historical parent proof admitted a new scope")
		}
	})
	records := []struct{ kind, id string }{
		{"product_nested_split_child_scope", r.ParentWorkItemID},
		{"product_nested_split_registrations", previous.Value.Scopes.Value.Request.ProposalID},
		{"product_split_parent_scope", r.ParentWorkItemID},
		{"product_split_registrations", previous.Value.ProductSplitChildScopesValue.Request.ProposalID},
		{"product_nested_split_proposal_admission", head},
	}
	for _, record := range records {
		t.Run("missing_"+record.kind, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", record.kind, record.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID); err == nil {
				t.Fatal("missing canonical ancestor accepted")
			}
			if _, err := k.PrepareProductNestedSplitProposal(ctx, r, ref); err == nil {
				t.Fatal("missing authority reconstructed")
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", record.kind, record.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			if record.kind == "product_split_parent_scope" {
				restored, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID)
				if err != nil || !reflect.DeepEqual(restored, p) {
					t.Fatal("exact authority restoration denied", err)
				}
			}
		})
	}
}

func TestProductDeeperSplitProposalHonorsDefaultDepthTwo(t *testing.T) {
	k, r, parent, _ := deeperSplitProposalFixture(t, 2)
	ref := ProductSplitParentScopeReference{ID: parent.ID, Digest: parent.Digest}
	if _, err := k.PrepareProductNestedSplitProposal(context.Background(), r, ref); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("default depth admitted great-grandchildren", err)
	}
}

func TestProductDeeperSplitProposalRejectsDifferentAncestorPolicy(t *testing.T) {
	ctx := context.Background()
	k, request, parent, previous := deeperSplitProposalFixture(t, 3, strings.Repeat("b", 64))
	own, err := k.ReadProductSplitChildSource(ctx, request.Target, request.ParentWorkItemID)
	if err != nil || own.Value.Status != "ready" || own.Value.Facts.PolicyDigest != parent.Value.SourcePolicyDigest {
		t.Fatal("own policy fixture invalid", err)
	}
	ancestor, err := k.ReadProductSplitChildSource(ctx, request.Target, previous.Value.Scopes.Value.Request.ParentWorkItemID)
	if err != nil || ancestor.Value.Status != "ready" || ancestor.Value.Facts.PolicyDigest == own.Value.Facts.PolicyDigest {
		t.Fatal("ancestor policy fixture invalid", err)
	}
	if _, err := k.PrepareProductNestedSplitProposal(ctx, request, ProductSplitParentScopeReference{ID: parent.ID, Digest: parent.Digest}); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("different pinned ancestor policy admitted", err)
	}
}
