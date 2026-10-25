package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestProductSplitParentScopeAtomicallyPinsWholeOriginalGroupAndReplays(t *testing.T) {
	ctx := context.Background()
	k, r, binding, facts := nestedSplitContextFixture(t)
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	group, err := k.ReadProductSplitRegistrations(ctx, binding.Value.Request)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	if err != nil || saved.Value.ParentWorkItemID != r.ParentWorkItemID || saved.Value.BindingID != binding.ID || saved.Value.BindingDigest != binding.Digest || saved.Value.GroupID != group.ID || saved.Value.GroupDigest != group.Digest || saved.Value.SourcePolicyDigest != facts.PolicyDigest || !reflect.DeepEqual(saved.Value.ProductSplitRegistrationsValue, group.Value) {
		t.Fatal(saved, err)
	}
	if len(saved.Value.Children) != 2 || len(saved.Value.Children[0].Value.Creations) != 2 {
		t.Fatal("sibling or consumed retry lineage dropped", saved)
	}
	if _, err := k.PrepareProductSplitParentScope(ctx, r.Target, binding.Value.Request.ParentWorkItemID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("root treated as registered split child", err)
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("parent proof charged/reset budget", err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "parent-proof-pause", Target: r.Target, Action: "pause", Reason: "audit"}); err != nil {
		t.Fatal(err)
	}
	for _, read := range []func() (ProductSplitParentScope, error){func() (ProductSplitParentScope, error) {
		return k.ReadProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	}, func() (ProductSplitParentScope, error) {
		return k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	}} {
		replay, err := read()
		if err != nil || !reflect.DeepEqual(replay, saved) {
			t.Fatal("reopen/pause replay changed proof", replay, err)
		}
	}
	var tasks int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal("parent proof dispatched Tasks", tasks, err)
	}
	direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "parent-proof-old-scope", Target: r.Target, Instruction: "new obligations", NextPolicyRevision: r.Target.PolicyRevision + 1})
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
	if _, err := k.ReadProductSplitParentScope(ctx, w.Target(), r.ParentWorkItemID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("historical parent proof became current scope", err)
	}
	var historical ProductSplitParentScope
	if err := k.Record(ctx, "product_split_parent_scope", r.ParentWorkItemID, &historical); err != nil || !reflect.DeepEqual(historical, saved) {
		t.Fatal("direction rewrote historical proof", historical, err)
	}
}

func TestProductSplitParentScopeRequiresFreshSourceAndRollsBackAllWitnesses(t *testing.T) {
	for _, mode := range []string{"hold", "stale", "pause", "late_failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, r, _, facts := nestedSplitContextFixture(t)
			before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "hold":
				prior, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
				if err != nil {
					t.Fatal(err)
				}
				facts.Hold = true
				if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, prior.ID, facts); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock().Add(2 * time.Minute)
				k.Now = func() time.Time { return now }
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "no-new-parent-proof", Target: r.Target, Action: "pause", Reason: "operator"}); err != nil {
					t.Fatal(err)
				}
			case "late_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_parent_scope BEFORE INSERT ON records WHEN NEW.kind='product_split_parent_scope_admission' BEGIN SELECT RAISE(ABORT,'late parent proof failure'); END"); err != nil {
					t.Fatal(err)
				}
			}
			saved, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
			if err == nil || saved.ID != "" {
				t.Fatal("held/failed proof returned success data", saved, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_split_parent_scope','product_split_parent_scope_history','product_split_parent_scope_admission')").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial proof transaction", count, err)
			}
			after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("failed proof spent/refunded budget", err)
			}
		})
	}
}

func TestProductSplitParentScopeMissingProofNeverRepairsOrRepinsSource(t *testing.T) {
	ctx := context.Background()
	k, r, binding, facts := nestedSplitContextFixture(t)
	saved, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	losses := []struct{ kind, id string }{
		{"product_split_parent_scope", r.ParentWorkItemID}, {"product_split_parent_scope_history", saved.ID}, {"product_split_parent_scope_admission", r.ParentWorkItemID},
		{"product_split_registrations", binding.Value.Request.ProposalID}, {"product_split_child_scopes", binding.Value.Request.ProposalID},
		{"product_split_child_source_policy", r.ParentWorkItemID},
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
			if out, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID); !errors.Is(err, ErrProductSplitHeld) || out.ID != "" {
				t.Fatal("lost proof recreated", out, err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			restored, err := k.ReadProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
			if err != nil || !reflect.DeepEqual(restored, saved) {
				t.Fatal("exact proof restore did not recover", restored, err)
			}
		})
	}
	// Once a parent proof pins its own policy, losing every old source record is
	// known missing evidence, not a first observation under a new host policy.
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_split_child_source','product_split_child_source_history','product_split_child_source_policy','product_split_child_source_policy_history')"); err != nil {
		t.Fatal(err)
	}
	facts.PolicyDigest = strings.Repeat("b", 64)
	if out, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, "", facts); !errors.Is(err, ErrProductSplitHeld) || out.Snapshot.ID != "" {
		t.Fatal("parent proof allowed source policy reset", out, err)
	}
}

func TestProductSplitParentScopeEachWitnessAloneRetainsEverySiblingAndBudget(t *testing.T) {
	for _, kind := range []string{"product_split_parent_scope", "product_split_parent_scope_history", "product_split_parent_scope_admission"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, r, binding, _ := nestedSplitContextFixture(t)
			original, err := k.ReadProductSplitProposal(ctx, binding.Value.Request.Target, binding.Value.Request.ParentWorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			saved, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", kind); err != nil {
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
				known, err := k.IsKnownProductSplitChildIssue(ctx, child.Value.Issue.RepositoryID, child.Value.Issue.Number)
				if err != nil || !known {
					t.Fatal("sibling numeric source released", known, err)
				}
				if _, err := k.CreateProductWave(ctx, splitRootRequest(child.Value.Issue.RepositoryID, child.Value.Issue.Number, "github", "new-root-after-parent-proof-loss")); !errors.Is(err, ErrProductSplitChildSourceHeld) {
					t.Fatal("sibling admitted as new root", err)
				}
				held, err := k.IsKnownProductSplitChildWorkItem(ctx, child.Value.Child.Value.WorkItemID)
				if err != nil || !held {
					t.Fatal("sibling identity forgotten", held, err)
				}
				if _, err := k.ReadProductSplitCreate(ctx, child.Value.CreationID); !readinessHoldError(err) {
					t.Fatal("creation journal recovered from prohibition", err)
				}
				if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, child.Value.CreationID, "new-owner"); !readinessHoldError(err) {
					t.Fatal("retry reset from lone proof", err)
				}
			}
			for i, check := range []func() error{
				func() error { _, err := k.BindProductSplitChildScopes(ctx, binding.Value.Request); return err },
				func() error { _, err := k.RegisterProductSplitChildren(ctx, binding.Value.Request); return err },
				func() error { _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); return err },
				func() error {
					_, err := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "new-after-parent-loss", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC", "extra"}})
					return err
				},
			} {
				if err := check(); !readinessHoldError(err) {
					t.Fatal("lone witness recreated/refunded authority", i, err)
				}
			}
			original.Value.Request.Key = "new-after-parent-proof-loss"
			if _, err := k.PrepareProductSplitProposal(ctx, original.Value.Request); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("root split replaced", err)
			}
			w, err := k.Wave(ctx, r.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			held, err := productWorkItemExecutionHeld(ctx, k.DB, w, binding.Value.Request.ParentWorkItemID)
			if err != nil || !held {
				t.Fatal("original root resumed", held, err)
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
				t.Fatal("lone parent proof invisible to recovery", page)
			}
		})
	}
}

func TestProductSplitParentScopeMalformedForeignRowsDoNotPoisonPeerAndOwnCorruptionHolds(t *testing.T) {
	ctx := context.Background()
	k, r, _, _ := nestedSplitContextFixture(t)
	saved, err := k.PrepareProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"{", "{}"} {
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_parent_scope_history',?,'foreign-wave',?)", strings.Repeat(raw, 600), raw); err != nil {
			t.Fatal(err)
		}
	}
	healthy, err := k.ReadProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID)
	if err != nil || !reflect.DeepEqual(healthy, saved) {
		t.Fatal("foreign corruption poisoned healthy proof", healthy, err)
	}
	page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	bad := 0
	for _, target := range page.Targets {
		if target.Unavailable {
			bad++
		}
	}
	if bad != 2 {
		t.Fatal("malformed parent proofs hidden from bounded recovery", bad, page)
	}
	if _, err := k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_split_parent_scope' AND id=?", r.ParentWorkItemID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductSplitParentScope(ctx, r.Target, r.ParentWorkItemID); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("own malformed proof granted metadata authority", err)
	}
	if _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); !readinessHoldError(err) {
		t.Fatal("own unknown proof refunded allocation", err)
	}
}
