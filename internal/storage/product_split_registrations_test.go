package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

func splitRegistrationsFixture(t *testing.T) (*KernelStore, ProductSplitChildPlanRequest, ProductSplitChildScopes) {
	t.Helper()
	k, _, r, _ := splitChildScopesFixture(t, true, true)
	scopes, err := k.BindProductSplitChildScopes(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return k, r, scopes
}

func assertNoRegisteredSplitChildren(t *testing.T, k *KernelStore) {
	t.Helper()
	var n int
	if err := k.DB.QueryRow(`SELECT count(*) FROM records WHERE kind LIKE 'product_split_registrations%'
 OR (kind IN ('product_work_item_registration','product_work_item_registration_version') AND json_extract(payload,'$.value.split_scope') IS NOT NULL)
 OR (kind='wave_work_item' AND json_extract(payload,'$.split_scope') IS NOT NULL)`).Scan(&n); err != nil || n != 0 {
		t.Fatal("partial child registration", n, err)
	}
}

func TestProductSplitRegistrationsAtomicallyPreserveScopeAndLifetimeSeats(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	parent, err := k.ReadProductWorkItemRegistration(ctx, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil || len(saved.Value.Registrations) != 2 || !reflect.DeepEqual(saved.Value.ProductSplitChildScopesValue, scopes.Value) {
		t.Fatal(saved, err)
	}
	for i, registered := range saved.Value.Registrations {
		binding := scopes.Value.Children[i]
		ref := registered.Value.SplitScope
		if ref == nil || ref.BindingID != binding.ID || ref.BindingDigest != binding.Digest || ref.SourceID != binding.Value.SourceID || ref.SetID != scopes.ID || ref.SetDigest != scopes.Digest || !reflect.DeepEqual(ref.Creations, binding.Value.Creations) || ref.ParentHeadID != binding.Value.ParentHeadID || registered.Value.Definition.ID != binding.Value.Child.Value.WorkItemID || !reflect.DeepEqual(registered.Value.Definition.Criteria, binding.Value.Child.Value.ParentCriteria) || registered.Value.SourceID != parent.Value.SourceID {
			t.Fatal("scope/source lineage lost", registered)
		}
		head, err := k.ReadProductWorkItemRegistration(ctx, registered.Value.Definition.ID)
		if err != nil || !reflect.DeepEqual(head, registered) {
			t.Fatal(head, err)
		}
		if _, err := k.RegisterProductWorkItem(ctx, r.Target, registered.Digest, registered.Value.Definition); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("ordinary registration erased split scope", err)
		}
		if _, err := k.CheckProductQuality(ctx, r.Target, registered.Value.Definition.ID); !errors.Is(err, ErrProductQualityHeld) {
			t.Fatal("parent criteria granted child quality", err)
		}
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("registration recharged/reset budget", after, err)
	}
	parentAfter, err := k.ReadProductWorkItemRegistration(ctx, r.ParentWorkItemID)
	if err != nil || !reflect.DeepEqual(parent, parentAfter) {
		t.Fatal("parent registration replaced", parentAfter, err)
	}
	coverage, err := k.ProductCoverage(ctx, r.Target)
	if err != nil || len(coverage.Value.Residual) != 2 || len(coverage.Value.UnmergedWorkItems) != 3 || len(coverage.Value.Retired) != 0 || len(coverage.Value.Merged) != 0 {
		t.Fatal("registration completed or retired parent", coverage, err)
	}
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&n); err != nil || n != 0 {
		t.Fatal("registration created Tasks", n, err)
	}
	w, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{r.ParentWorkItemID, saved.Value.Registrations[0].Value.Definition.ID} {
		held, err := productWorkItemExecutionHeld(ctx, k.DB, w, id)
		if err != nil || !held {
			t.Fatal("split became executable before local adapters", id, held, err)
		}
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-registered-split", Target: r.Target, Action: "pause", Reason: "audit"}); err != nil {
		t.Fatal(err)
	}
	replay, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil || !reflect.DeepEqual(saved, replay) {
		t.Fatal("replay/reopen/pause changed adoption", replay, err)
	}
}

func TestProductSplitRegistrationsRollbackEveryChildAndLateReceipt(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"product_work_item_registration", "wave_work_item", "product_split_registrations_admission"} {
		t.Run(kind, func(t *testing.T) {
			k, r, scopes := splitRegistrationsFixture(t)
			before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			condition := "NEW.kind='" + kind + "'"
			if kind != "product_split_registrations_admission" {
				condition += " AND NEW.id='" + scopes.Value.Children[1].Value.Child.Value.WorkItemID + "'"
			}
			if _, err := k.DB.Exec("CREATE TRIGGER fail_split_registration BEFORE INSERT ON records WHEN " + condition + " BEGIN SELECT RAISE(ABORT,'late registration failure'); END"); err != nil {
				t.Fatal(err)
			}
			if saved, err := k.RegisterProductSplitChildren(ctx, r); err == nil || saved.ID != "" {
				t.Fatal("partial registration success", saved, err)
			}
			assertNoRegisteredSplitChildren(t, k)
			after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("failed transaction changed budget", after, err)
			}
			if _, err := k.DB.Exec("DROP TRIGGER fail_split_registration"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
				t.Fatal("rollback not retryable", err)
			}
		})
	}
}

func TestProductSplitRegistrationsMissingProofsHoldUntilExactRestore(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	saved, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	child := saved.Value.Registrations[0]
	for _, loss := range []struct{ kind, id string }{
		{"product_split_registrations", r.ProposalID}, {"product_split_registrations_history", saved.ID}, {"product_split_registrations_admission", r.ProposalID},
		{"product_work_item_registration", child.Value.Definition.ID}, {"product_work_item_registration_version", structureID("product-work-", child.Value.Definition.ID, child.Digest)},
		{"wave_work_item", child.Value.Definition.ID}, {"product_split_child_scopes", r.ProposalID}, {"product_split_child_scope_source", "github:17:issue:99"}, {"product_split_create", scopes.Value.Children[0].Value.CreationID},
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
			if _, err := k.RegisterProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost proof recreated", loss, err)
			}
			if _, err := k.ReadProductSplitRegistrations(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost proof accepted", loss, err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			replay, err := k.ReadProductSplitRegistrations(ctx, r)
			if err != nil || !reflect.DeepEqual(saved, replay) {
				t.Fatal("exact restore changed adoption", replay, err)
			}
		})
	}
}

func TestProductSplitRegistrationsIndependentWitnessesNeverRefundOrRecreate(t *testing.T) {
	ctx := context.Background()
	for _, keep := range []string{"product_split_registrations", "product_split_registrations_history", "product_split_registrations_admission", "product_work_item_registration", "product_work_item_registration_version", "wave_work_item"} {
		t.Run(keep, func(t *testing.T) {
			k, r, scopes := splitRegistrationsFixture(t)
			if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE (kind LIKE 'product_split_%' OR kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item')) AND kind<>?", keep); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			for _, check := range []func() error{
				func() error { _, e := k.RegisterProductSplitChildren(ctx, r); return e },
				func() error { _, e := k.BindProductSplitChildScopes(ctx, r); return e },
				func() error {
					_, e := k.PrepareProductSplitCreate(ctx, r, scopes.Value.Children[0].Value.Child.ID, "new-owner-after-loss")
					return e
				},
				func() error {
					_, e := k.ReadProductSplitCreate(ctx, scopes.Value.Children[0].Value.CreationID)
					return e
				},
				func() error { _, e := k.WaveOperationBudget(ctx, r.Target.WaveID); return e },
			} {
				if err := check(); !errors.Is(err, ErrProductSplitHeld) {
					t.Fatal("independent witness lost prohibition", keep, err)
				}
			}
			known, err := k.IsKnownProductSplitChildIssue(ctx, 17, 99)
			if err != nil || !known {
				t.Fatal("numeric source released", known, err)
			}
			w, err := k.Wave(ctx, r.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			held, err := splitChildScopeParentExecutionHeld(ctx, k.DB, w, r.ParentWorkItemID, "")
			if err != nil || !held {
				t.Fatal("parent released", held, err)
			}
			held, err = productSplitChildExecutionHeld(ctx, k.DB, scopes.Value.Children[0].Value.Child.Value.WorkItemID)
			if err != nil || !held {
				t.Fatal("child released", held, err)
			}
		})
	}
}

func TestProductSplitRegistrationsFreshAdoptionRequiresLiveOriginalEpoch(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"pause", "clock", "direction"} {
		t.Run(mode, func(t *testing.T) {
			k, r, scopes := splitRegistrationsFixture(t)
			switch mode {
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-new-children", Target: r.Target, Action: "pause", Reason: "operator"}); err != nil {
					t.Fatal(err)
				}
			case "clock":
				k.Now = func() time.Time { return scopes.Value.CreatedAt.Add(-time.Second) }
			case "direction":
				d, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "before-child-registration", Target: r.Target, Instruction: "new epoch", NextPolicyRevision: r.Target.PolicyRevision + 1})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
					t.Fatal(err)
				}
			}
			if saved, err := k.RegisterProductSplitChildren(ctx, r); saved.ID != "" || !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("held adoption mutated registration", saved, err)
			}
			assertNoRegisteredSplitChildren(t, k)
		})
	}
}

func TestProductSplitRegistrationsConcurrentStoresConvergeWithoutPartialSets(t *testing.T) {
	ctx := context.Background()
	k, r, _ := splitRegistrationsFixture(t)
	other, err := OpenExistingKernel(k.Root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	type result struct {
		value ProductSplitRegistrations
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, store := range []*KernelStore{k, other} {
		go func(store *KernelStore) {
			<-start
			v, e := store.RegisterProductSplitChildren(ctx, r)
			results <- result{v, e}
		}(store)
	}
	close(start)
	a, b := <-results, <-results
	if a.err != nil && b.err != nil {
		t.Fatal("no complete winner", a.err, b.err)
	}
	for _, out := range []result{a, b} {
		if out.err != nil {
			var busy sqlite3.Error
			if !errors.As(out.err, &busy) || (busy.Code != sqlite3.ErrBusy && busy.Code != sqlite3.ErrLocked) || out.value.ID != "" {
				t.Fatal("partial success", out)
			}
		}
	}
	a.value, a.err = k.RegisterProductSplitChildren(ctx, r)
	b.value, b.err = other.RegisterProductSplitChildren(ctx, r)
	if a.err != nil || b.err != nil || !reflect.DeepEqual(a.value, b.value) {
		t.Fatal("retry diverged", a.err, b.err)
	}
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&n); err != nil || n != 3 {
		t.Fatal("duplicate/subset registrations", n, err)
	}
}

func TestProductRegistrationOldWireDigestDoesNotAcquireSplitScope(t *testing.T) {
	var saved ProductWorkItemRegistration
	raw := []byte(`{"wave_id":"w","scope_revision":1,"policy_revision":1,"source":"direct","source_id":"r","definition":{"id":"work","repository":"owner/repo","repository_id":17,"criteria":["AC"]}}`)
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(saved)
	if err != nil || string(raw) != string(encoded) || strings.Contains(string(encoded), "split_scope") {
		t.Fatal("legacy registration rehashed", string(encoded), err)
	}
}

func TestProductSplitRegistrationsLateOwnershipConflictRollsBackEarlierChildren(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	id := scopes.Value.Children[1].Value.Child.Value.WorkItemID
	raw := []byte(`{"target":{"wave_id":"earlier-owner","scope_revision":1,"plan_revision":1,"policy_revision":1},"candidate_id":""}`)
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('wave_work_item',?,'earlier-owner',?)", id, raw); err != nil {
		t.Fatal(err)
	}
	if out, err := k.RegisterProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) || out.ID != "" {
		t.Fatal("ownership conflict returned success", out, err)
	}
	assertNoRegisteredSplitChildren(t, k)
	var retained []byte
	if err := k.DB.QueryRow("SELECT payload FROM records WHERE kind='wave_work_item' AND id=?", id).Scan(&retained); err != nil || string(retained) != string(raw) {
		t.Fatal("earlier owner overwritten", string(retained), err)
	}
}

func TestProductSplitRegistrationsStayHistoricalAfterDirection(t *testing.T) {
	ctx := context.Background()
	k, r, _ := splitRegistrationsFixture(t)
	saved, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	d, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "after-child-registration", Target: r.Target, Instruction: "changed scope and policy", NextPolicyRevision: r.Target.PolicyRevision + 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil || w.Target() == r.Target {
		t.Fatal("no actual direction", w, err)
	}
	replay, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil || !reflect.DeepEqual(saved, replay) {
		t.Fatal("history rewritten", replay, err)
	}
	for _, child := range saved.Value.Registrations {
		if productRegistrationScope(child, w) {
			t.Fatal("old child gained current scope", child)
		}
		if _, err := k.RegisterProductWorkItem(ctx, w.Target(), child.Digest, child.Value.Definition); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("generic refresh reset child lineage", err)
		}
	}
}
