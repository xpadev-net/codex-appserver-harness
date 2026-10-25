package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func TestProductDeeperSplitCreationScopesRegistrationsAndSourcesRetainEveryLayer(t *testing.T) {
	ctx := context.Background()
	k, request, parent, previous := deeperSplitProposalFixture(t, 3)
	admitted, err := k.PrepareProductNestedSplitProposal(ctx, request, ProductSplitParentScopeReference{ID: parent.ID, Digest: parent.Digest})
	if err != nil {
		t.Fatal(err)
	}
	p := admitted.Value.Proposal
	r := ProductSplitChildPlanRequest{Target: request.Target, ParentWorkItemID: request.ParentWorkItemID, ProposalID: p.ID, ProposalDigest: p.Digest}
	plan, err := k.PrepareProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	t.Run("earlier_retry_budget_is_not_refilled", func(t *testing.T) {
		isolated := cloneNestedScopeStore(t, k)
		before, err := isolated.WaveOperationBudget(ctx, r.Target.WaveID)
		if err != nil {
			t.Fatal(err)
		}
		job, err := isolated.PrepareProductSplitCreate(ctx, r, plan.Children[0].ID, "great-budget-owner")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := isolated.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "great-unsent-budget"); err != nil {
			t.Fatal(err)
		}
		if _, err := isolated.ReserveProductSplitCreateUnsentRetry(ctx, job.ID, "great-budget-retry"); !errors.Is(err, ErrOperationBudget) {
			t.Fatal("deeper split refilled consumed retry budget", err)
		}
		after, err := isolated.WaveOperationBudget(ctx, r.Target.WaveID)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("failed retry changed budget", err)
		}
	})
	for i, child := range plan.Children {
		job, err := k.PrepareProductSplitCreate(ctx, r, child.ID, []string{"great-left-owner", "great-right-owner"}[i])
		if err != nil {
			t.Fatal(err)
		}
		if job.Value.Ancestry == nil || job.Value.Child.Value.Depth != 3 || !reflect.DeepEqual(job.Value.Ancestry.Value.AncestorWitnesses, parent.Value.AncestorWitnesses) {
			t.Fatal("great creation lost older ancestry")
		}
		if err := os.Remove(filepath.Join(k.Root, "split-http-started.json")); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		identity := startSplitCreationOwner(t, k, job, time.Minute)
		stop, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := process.StopGroup(stop, identity); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "great-stop"); err != nil {
			t.Fatal(err)
		}
		if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: int64(701 + i), Number: 71 + i, Title: child.Value.Child.Title, Body: child.Value.Body, State: "open", ObservedAt: k.clock()}); err != nil {
			t.Fatal(err)
		}
	}
	scopes, err := k.BindProductSplitChildScopes(ctx, r)
	if err != nil || len(scopes.Value.Children) != 2 {
		t.Fatal("great scopes denied", err)
	}
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	registered, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil || len(registered.Value.Registrations) != 2 {
		t.Fatal("great registration denied", err)
	}
	for i, binding := range scopes.Value.Children {
		child := registered.Value.Registrations[i]
		if binding.Value.Child.Value.Depth != 3 || child.Value.SplitScope == nil || child.Value.SplitScope.AncestorSet == nil || !reflect.DeepEqual(child.Value.Definition.Criteria, []string{"AC"}) || binding.Value.Child.Value.Child.Coverage[0].ParentCriterion != previous.Value.Scopes.Value.Children[0].Value.Scope.Criteria[0].ID {
			t.Fatal("great local part or original mapping lost")
		}
		if i == 0 && binding.Value.Scope.Criteria[0].ID != "AC" {
			t.Fatal("local/Wave criterion collision was flattened")
		}
		update, err := k.ObserveProductSplitChildSource(ctx, r.Target, child.Value.Definition.ID, "", splitSourceFacts(binding))
		if err != nil || update.Snapshot.Value.Status != "ready" || !reflect.DeepEqual(update.Snapshot.Value.AncestorSet, child.Value.SplitScope.AncestorSet) {
			t.Fatal("great source lost ancestry", err)
		}
		current, err := k.ReadProductSplitChildScope(ctx, r.Target, child.Value.Definition.ID)
		if err != nil || !reflect.DeepEqual(current, binding) {
			t.Fatal("great canonical scope unreadable", err)
		}
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("registration/source refilled budget", err)
	}
	wave, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range registered.Value.Registrations {
		held, err := productWorkItemExecutionHeld(ctx, k.DB, wave, child.Value.Definition.ID)
		if err != nil || !held {
			t.Fatal("metadata granted great-child execution", held, err)
		}
	}
	var tasks int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal("metadata created Tasks", tasks, err)
	}
	all := [][]ProductSplitChildScopeBinding{previous.Value.Children, previous.Value.Scopes.Value.Children, scopes.Value.Children}
	first := registered.Value.Registrations[0]
	for _, record := range []struct{ kind, id string }{
		{"product_work_item_registration", first.Value.Definition.ID},
		{"product_split_child_source_policy_history", first.Value.Definition.ID},
	} {
		t.Run("sole_"+record.kind, func(t *testing.T) {
			isolated := cloneNestedScopeStore(t, k)
			if _, err := isolated.DB.Exec("DELETE FROM records WHERE (kind LIKE 'product_split_%' OR kind LIKE 'product_nested_split_%') AND NOT(kind=? AND id=?)", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			if _, err := isolated.DB.Exec("DELETE FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND (json_type(payload,'$.value.split_scope') IS NOT NULL OR json_type(payload,'$.split_scope') IS NOT NULL) AND NOT(kind=? AND id=?)", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			if _, err := isolated.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
				t.Fatal(err)
			}
			if _, err := isolated.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			for _, layer := range all {
				for _, binding := range layer {
					if _, err := isolated.CreateProductWave(ctx, splitRootRequest(binding.Value.Issue.RepositoryID, binding.Value.Issue.Number, "github", "great-sole-"+record.kind)); !errors.Is(err, ErrProductSplitChildSourceHeld) {
						t.Fatal("ancestor or sibling became a new root", err)
					}
				}
			}
			if _, err := isolated.WaveOperationBudget(ctx, r.Target.WaveID); !readinessHoldError(err) {
				t.Fatal("consumed retries refunded", err)
			}
			if _, err := isolated.ReserveProductSplitChildren(ctx, r); !readinessHoldError(err) {
				t.Fatal("lifetime seats refunded", err)
			}
			if _, err := isolated.RegisterProductSplitChildren(ctx, r); !readinessHoldError(err) {
				t.Fatal("lost canonical group reconstructed", err)
			}
			page, err := isolated.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, target := range page.Targets {
				seen[target.ID] = true
			}
			for _, layer := range all {
				recovered := false
				for _, binding := range layer {
					recovered = recovered || seen[binding.Value.CreationID]
				}
				if !recovered {
					t.Fatal("one ancestry layer disappeared from recovery", record.kind)
				}
			}
		})
	}
}
