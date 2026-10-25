package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

// One physical fixture is copied only after every native group is stopped.
// Adversarial metadata tests never repeat or infer an external creation.
func nestedObservedScopesFixture(t *testing.T) (*KernelStore, ProductSplitChildPlanRequest) {
	t.Helper()
	ctx := context.Background()
	k, r, first := nestedCreationFixture(t)
	plan := first.Value.Ancestry.Value.Plan
	for i, child := range plan.Children {
		job, err := k.PrepareProductSplitCreate(ctx, r, child.ID, []string{first.Value.OwnerID, "second-scope-child"}[i])
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if _, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "scope-unsent"); err != nil {
				t.Fatal(err)
			}
			job, err = k.ReserveProductSplitCreateUnsentRetry(ctx, job.ID, "scope-retry-owner")
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Remove(filepath.Join(k.Root, "split-http-started.json")); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		identity := startSplitCreationOwner(t, k, job, 20*time.Second)
		stop, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := process.StopGroup(stop, identity); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "scope-physical-stop"); err != nil {
			t.Fatal(err)
		}
		if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: int64(501 + i), Number: 51 + i, Title: child.Value.Child.Title, Body: child.Value.Body, State: "open", ObservedAt: k.clock()}); err != nil {
			t.Fatal(err)
		}
	}
	return k, r
}

func cloneNestedScopeStore(t *testing.T, base *KernelStore) *KernelStore {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "state.db")
	if _, err := base.DB.Exec("VACUUM INTO ?", path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	k, err := OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	now := base.clock()
	k.Now = func() time.Time { return now }
	t.Cleanup(func() { k.Close() })
	return k
}

func TestProductNestedSplitScopesKeepWholeAncestorAndOwnGroupAcrossProofLoss(t *testing.T) {
	ctx := context.Background()
	base, r := nestedObservedScopesFixture(t)
	before, err := base.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	scopes, err := base.BindProductSplitChildScopes(ctx, r)
	if err != nil || len(scopes.Value.Children) != 2 {
		t.Fatal(scopes, err)
	}
	var saved ProductNestedSplitChildScopes
	if err := base.Record(ctx, "product_nested_split_child_scopes", r.ProposalID, &saved); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saved.Value.Scopes, scopes) || len(saved.Value.Children) != 2 || len(scopes.Value.Children[0].Value.Creations) != 2 {
		t.Fatal("partial source or retry lineage")
	}
	records := nestedSplitScopeRecords(saved)
	if len(records) != 11 {
		t.Fatal("incomplete atomic index set", len(records))
	}
	for _, record := range records {
		var witness ProductNestedSplitChildScopes
		if err := base.Record(ctx, record.kind, record.id, &witness); err != nil || !reflect.DeepEqual(witness, saved) {
			t.Fatal("naked scope/source witness", record.kind, err)
		}
	}
	for _, binding := range scopes.Value.Children {
		if binding.Value.Child.Value.Depth != 2 || len(binding.Value.Scope.Criteria) != 1 || binding.Value.Scope.Included[0] != binding.Value.Child.Value.Child.Criteria[0].ID {
			t.Fatal("local scope flattened", binding)
		}
	}
	if _, err := base.RegisterProductSplitChildren(ctx, r); !readinessHoldError(err) {
		t.Fatal("binding enabled nested registration", err)
	}
	after, err := base.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("binding changed budget", err)
	}
	proofs := []struct{ kind, id string }{}
	kinds := map[string]bool{}
	for _, record := range records {
		if !kinds[record.kind] {
			proofs = append(proofs, record)
			kinds[record.kind] = true
		}
	}
	proofs = append(proofs, struct{ kind, id string }{"product_nested_split_allocation", r.ProposalID})
	for _, record := range proofs {
		t.Run("missing_"+record.kind+"_"+record.id, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			var raw []byte
			var owner string
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", record.kind, record.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductSplitChildScopes(ctx, r); !readinessHoldError(err) {
				t.Fatal("missing scope proof accepted", err)
			}
			if _, err := k.BindProductSplitChildScopes(ctx, r); !readinessHoldError(err) {
				t.Fatal("missing scope repaired", err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", record.kind, record.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			var restoredRaw []byte
			if err := k.DB.QueryRow("SELECT payload FROM records WHERE kind=? AND id=?", record.kind, record.id).Scan(&restoredRaw); err != nil || !bytes.Equal(restoredRaw, raw) {
				t.Fatal("exact witness bytes not restored", err)
			}
			// Two distinct key shapes exercise the full restored read. Every other
			// copy is restored byte-for-byte against the same validated fixture.
			if record.kind == "product_nested_split_child_scopes" || record.kind == "product_nested_split_child_scope_source" {
				restored, err := k.ReadProductSplitChildScopes(ctx, r)
				if err != nil || !reflect.DeepEqual(restored, scopes) {
					t.Fatal("exact restoration denied", err)
				}
			}
		})
	}
	t.Run("pause_direction_history", func(t *testing.T) {
		k := cloneNestedScopeStore(t, base)
		if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-nested-scopes", Target: r.Target, Action: "pause", Reason: "audit"}); err != nil {
			t.Fatal(err)
		}
		replay, err := k.BindProductSplitChildScopes(ctx, r)
		if err != nil || !reflect.DeepEqual(replay, scopes) {
			t.Fatal("pause changed saved scopes", err)
		}
		direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "nested-scope-direction", Target: r.Target, Instruction: "new scope", NextPolicyRevision: r.Target.PolicyRevision + 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
			t.Fatal(err)
		}
		historical, err := k.ReadProductSplitChildScopes(ctx, r)
		if err != nil || !reflect.DeepEqual(historical, scopes) {
			t.Fatal("direction lost immutable scopes", err)
		}
		wave, err := k.Wave(ctx, r.Target.WaveID)
		if err != nil {
			t.Fatal(err)
		}
		current := r
		current.Target = wave.Target()
		if _, err := k.BindProductSplitChildScopes(ctx, current); !readinessHoldError(err) {
			t.Fatal("old scope retargeted", err)
		}
	})
	for _, kind := range []string{"product_nested_split_child_scopes", "product_nested_split_child_scopes_history", "product_nested_split_child_scopes_admission", "product_nested_split_child_scope", "product_nested_split_child_scope_history", "product_nested_split_child_scope_admission", "product_nested_split_child_scope_source"} {
		t.Run("sole_"+kind, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			keepID := ""
			for _, record := range records {
				if record.kind == kind {
					keepID = record.id
					break
				}
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
			for _, bindings := range [][]ProductSplitChildScopeBinding{saved.Value.Children, scopes.Value.Children} {
				for _, binding := range bindings {
					if _, err := k.CreateProductWave(ctx, splitRootRequest(binding.Value.Issue.RepositoryID, binding.Value.Issue.Number, "github", fmt.Sprintf("root-after-%s", kind))); !errors.Is(err, ErrProductSplitChildSourceHeld) {
						t.Fatal("ancestor/own sibling root claim lost", err)
					}
					if _, err := k.ReadProductSplitCreate(ctx, binding.Value.CreationID); !readinessHoldError(err) {
						t.Fatal("creation reconstructed", err)
					}
				}
			}
			if _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); !readinessHoldError(err) {
				t.Fatal("known retry refunded", err)
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !readinessHoldError(err) {
				t.Fatal("own seats refunded", err)
			}
			if _, err := k.BindProductSplitChildScopes(ctx, r); !readinessHoldError(err) {
				t.Fatal("whole group regenerated", err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			foundAncestor, foundOwn := false, false
			for _, target := range page.Targets {
				foundAncestor = foundAncestor || target.ID == saved.Value.Children[0].Value.CreationID
				foundOwn = foundOwn || target.ID == scopes.Value.Children[0].Value.CreationID
			}
			if !foundAncestor || !foundOwn {
				t.Fatal("ancestor/own recovery hidden", page)
			}
		})
	}
}

func TestProductNestedSplitScopesNewBindingIsLiveAtomicAndResistsSourceCollision(t *testing.T) {
	ctx := context.Background()
	base, r := nestedObservedScopesFixture(t)
	for _, mode := range []string{"pause", "stale", "late_failure", "source_collision"} {
		t.Run(mode, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			switch mode {
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-before-scope", Target: r.Target, Action: "pause", Reason: "hold"}); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock()
				k.Now = func() time.Time { return now.Add(2 * time.Minute) }
			case "late_failure":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_nested_scope BEFORE INSERT ON records WHEN NEW.kind='product_nested_split_child_scope_source' BEGIN SELECT RAISE(ABORT,'late scope failure'); END"); err != nil {
					t.Fatal(err)
				}
			case "source_collision":
				if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_scope_source','github:17:issue:51','foreign-wave',?)", []byte(`{"request":{"target":{"wave_id":"foreign-wave"}},"parent_head_id":"foreign"}`)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.BindProductSplitChildScopes(ctx, r); err == nil {
				t.Fatal("invalid scope admission succeeded")
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_nested_split_child_scopes','product_nested_split_child_scopes_history','product_nested_split_child_scopes_admission','product_nested_split_child_scope','product_nested_split_child_scope_history','product_nested_split_child_scope_admission','product_nested_split_child_scope_source')").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial scope saved", count, err)
			}
		})
	}
}
