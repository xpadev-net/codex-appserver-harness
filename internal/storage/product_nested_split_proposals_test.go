package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func nestedProposalFixture(t *testing.T) (*KernelStore, ProductSplitProposalRequest, ProductSplitParentScope, ProductNestedSplitProposal) {
	t.Helper()
	ctx := context.Background()
	k, r, _, _ := nestedSplitContextFixture(t)
	proof, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductNestedSplitProposal(ctx, r, ProductSplitParentScopeReference{ID: proof.ID, Digest: proof.Digest})
	if err != nil {
		t.Fatal(err)
	}
	return k, r, proof, saved
}

func TestProductNestedSplitProposalPinsWholeParentProofAndDoesNotCreateExecution(t *testing.T) {
	ctx := context.Background()
	k, r, _, _ := nestedSplitContextFixture(t)
	proof, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	ref := ProductSplitParentScopeReference{ID: proof.ID, Digest: proof.Digest}
	saved, err := k.PrepareProductNestedSplitProposal(ctx, r, ref)
	if err != nil || !reflect.DeepEqual(saved.Value.ProductSplitParentScopeValue, proof.Value) || saved.Value.ParentProof != ref || saved.Value.Proposal.Value.ParentScopeProof == nil || *saved.Value.Proposal.Value.ParentScopeProof != ref || saved.Value.Proposal.Value.Depth != 1 {
		t.Fatal(saved, err)
	}
	read, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID)
	if err != nil || !reflect.DeepEqual(read, saved.Value.Proposal) {
		t.Fatal("generic proposal reader lost typed envelope", read, err)
	}
	planRequest := ProductSplitChildPlanRequest{Target: r.Target, ParentWorkItemID: r.ParentWorkItemID, ProposalID: read.ID, ProposalDigest: read.Digest}
	plan, err := k.PrepareProductSplitChildPlan(ctx, planRequest)
	if err != nil || len(plan.Children) != 2 {
		t.Fatal("ancestor-pinned metadata plan denied", plan, err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, planRequest); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("metadata plan reserved execution seats", err)
	}
	if _, err := k.PrepareProductSplitExecution(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("ordinary root port adopted nested implicitly", err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_child_intent' AND json_extract(payload,'$.value.parent_work_item_id')=?", r.ParentWorkItemID).Scan(&count); err != nil || count != 0 {
		t.Fatal("nested metadata wrote unprotected legacy intents", count, err)
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("nested metadata charged/reset budget", err)
	}
	changed := cloneSplitRequest(t, r)
	changed.Proposal.Children[0].Title += " changed"
	if out, err := k.PrepareProductNestedSplitProposal(ctx, changed, ref); !errors.Is(err, ErrProductSplitHeld) || out.ID != "" {
		t.Fatal("same head adopted changed request", out, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "nested-meta-pause", Target: r.Target, Action: "pause", Reason: "audit"}); err != nil {
		t.Fatal(err)
	}
	replay, err := k.PrepareProductNestedSplitProposal(ctx, r, ref)
	if err != nil || !reflect.DeepEqual(replay, saved) {
		t.Fatal("reopen/pause changed metadata", replay, err)
	}
	direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "nested-meta-direction", Target: r.Target, Instruction: "new requirement", NextPolicyRevision: r.Target.PolicyRevision + 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductSplitProposal(ctx, w.Target(), r.ParentWorkItemID); err == nil {
		t.Fatal("old nested metadata became new scope", err)
	}
	var historical ProductNestedSplitProposal
	if err := k.Record(ctx, "product_nested_split_proposal", saved.Value.ProposalHeadID, &historical); err != nil || !reflect.DeepEqual(historical, saved) {
		t.Fatal("direction rewrote history", historical, err)
	}
}

func TestProductNestedSplitProposalRequiresExplicitProofAndAtomicCompleteEnvelope(t *testing.T) {
	for _, mode := range []string{"wrong_id", "wrong_digest", "root_key_reuse", "missing_proof", "source_hold", "late_failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, r, binding, facts := nestedSplitContextFixture(t)
			proof, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			ref := ProductSplitParentScopeReference{ID: proof.ID, Digest: proof.Digest}
			switch mode {
			case "wrong_id":
				ref.ID += "-wrong"
			case "wrong_digest":
				ref.Digest = "wrong"
			case "root_key_reuse":
				root, err := k.ReadProductSplitProposal(ctx, binding.Value.Request.Target, binding.Value.Request.ParentWorkItemID)
				if err != nil {
					t.Fatal(err)
				}
				r.Key = root.Value.Request.Key
			case "missing_proof":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_parent_scope' AND id=?", r.ParentWorkItemID); err != nil {
					t.Fatal(err)
				}
			case "source_hold":
				prior, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
				if err != nil {
					t.Fatal(err)
				}
				facts.Hold = true
				if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, prior.ID, facts); err != nil {
					t.Fatal(err)
				}
			case "late_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_nested_envelope BEFORE INSERT ON records WHEN NEW.kind='product_nested_split_proposal_request' BEGIN SELECT RAISE(ABORT,'late nested envelope failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			out, err := k.PrepareProductNestedSplitProposal(ctx, r, ref)
			if err == nil || out.ID != "" {
				t.Fatal("invalid/failed nested admission acknowledged", out, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_nested_split_proposal','product_nested_split_proposal_history','product_nested_split_proposal_admission','product_nested_split_proposal_request')").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial nested envelope", count, err)
			}
		})
	}
}

func TestProductNestedSplitProposalMissingProofRequiresExactRestore(t *testing.T) {
	ctx := context.Background()
	k, r, proof, saved := nestedProposalFixture(t)
	ref := ProductSplitParentScopeReference{ID: proof.ID, Digest: proof.Digest}
	for _, loss := range []struct{ kind, id string }{
		{"product_nested_split_proposal", saved.Value.ProposalHeadID}, {"product_nested_split_proposal_history", saved.ID}, {"product_nested_split_proposal_admission", saved.Value.ProposalHeadID}, {"product_nested_split_proposal_request", saved.Value.RequestKeyID},
		{"product_split_parent_scope", r.ParentWorkItemID}, {"product_split_registrations", proof.Value.Request.ProposalID},
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
			if out, err := k.PrepareProductNestedSplitProposal(ctx, r, ref); !errors.Is(err, ErrProductSplitHeld) || out.ID != "" {
				t.Fatal("lost nested proof reconstructed", out, err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			restored, err := k.ReadProductSplitProposal(ctx, r.Target, r.ParentWorkItemID)
			if err != nil || !reflect.DeepEqual(restored, saved.Value.Proposal) {
				t.Fatal("exact restore changed context", restored, err)
			}
		})
	}
	// Losing every parent-proof sidecar is still known from the full envelope.
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission')"); err != nil {
		t.Fatal(err)
	}
	if out, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID); !errors.Is(err, ErrProductSplitHeld) || out.ID != "" {
		t.Fatal("envelope allowed parent proof reset", out, err)
	}
}

func TestProductNestedSplitProposalEachEnvelopeAloneKeepsWholeAncestorProhibitions(t *testing.T) {
	for _, kind := range []string{"product_nested_split_proposal", "product_nested_split_proposal_history", "product_nested_split_proposal_admission", "product_nested_split_proposal_request"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, r, proof, saved := nestedProposalFixture(t)
			original, err := k.ReadProductSplitProposal(ctx, proof.Value.Request.Target, proof.Value.Request.ParentWorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' OR (kind LIKE 'product_nested_split_%' AND kind<>?)", kind); err != nil {
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
			for _, child := range saved.Value.Children {
				if _, err := k.CreateProductWave(ctx, splitRootRequest(child.Value.Issue.RepositoryID, child.Value.Issue.Number, "github", "root-after-nested-proof-loss")); !errors.Is(err, ErrProductSplitChildSourceHeld) {
					t.Fatal("ancestor sibling became root", err)
				}
				known, err := k.IsKnownProductSplitChildWorkItem(ctx, child.Value.Child.Value.WorkItemID)
				if err != nil || !known {
					t.Fatal("ancestor sibling identity released", known, err)
				}
				if _, err := k.ReadProductSplitCreate(ctx, child.Value.CreationID); !readinessHoldError(err) {
					t.Fatal("ancestor creation reconstructed", err)
				}
				if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, child.Value.CreationID, "reset-owner"); !readinessHoldError(err) {
					t.Fatal("ancestor retry reset", err)
				}
			}
			for _, check := range []func() error{
				func() error { _, err := k.RegisterProductSplitChildren(ctx, proof.Value.Request); return err },
				func() error { _, err := k.BindProductSplitChildScopes(ctx, proof.Value.Request); return err },
				func() error { _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); return err },
				func() error {
					_, err := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "new-after-nested-loss", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC", "extra"}})
					return err
				},
			} {
				if err := check(); !readinessHoldError(err) {
					t.Fatal("lone envelope refunded/recreated authority", err)
				}
			}
			original.Value.Request.Key = "replace-root-after-nested-loss"
			if _, err := k.PrepareProductSplitProposal(ctx, original.Value.Request); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("original root resumed", err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, target := range page.Targets {
				found = found || target.ID == proof.Value.Children[0].Value.CreationID
			}
			if !found {
				t.Fatal("nested envelope lost from ancestor recovery", page)
			}
		})
	}
}
