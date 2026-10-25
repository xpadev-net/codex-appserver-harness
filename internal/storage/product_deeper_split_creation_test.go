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
		if i == 0 {
			t.Run("ancestor_source_gate", func(t *testing.T) {
				checkDeeperSplitAncestorSourceGate(t, cloneNestedScopeStore(t, k), job, previous.Value.Children[1])
			})
			if t.Failed() {
				return
			}
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

// This uses the same actual depth-three allocation as the physical creation
// regression. Only source GET observations are changed; ancestry stays intact.
func checkDeeperSplitAncestorSourceGate(t *testing.T, k *KernelStore, job ProductSplitCreate, ancestor ProductSplitChildScopeBinding) {
	t.Helper()
	ctx := context.Background()
	target := job.Value.Request.Target
	ancestorID := ancestor.Value.Child.Value.WorkItemID
	source, err := k.ReadProductSplitChildSource(ctx, target, ancestorID)
	if err != nil {
		t.Fatal(err)
	}
	observeGate := func() (ProductSplitCreateGate, error) {
		return k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", "")
	}
	readGate := func(send bool) error {
		tx, err := k.kernelTx(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if send {
			_, err = k.splitCreateSendGateTx(ctx, tx, job)
		} else {
			_, err = loadSplitCreateGate(ctx, tx, job)
		}
		return err
	}
	gate, err := observeGate()
	if err != nil || len(gate.AncestorSources) != 1 || gate.AncestorSources[0].WorkItemID != ancestorID || gate.AncestorSources[0].SourceID != source.ID {
		t.Fatal("ancestor source not captured", err)
	}
	if err := readGate(true); err != nil {
		t.Fatal("ready lineage denied", err)
	}
	for _, mode := range []string{"hold", "closed", "unavailable", "policy_changed"} {
		t.Run(mode, func(t *testing.T) {
			facts := source.Value.Facts
			switch mode {
			case "hold":
				facts.Hold = true
			case "closed":
				facts.IssueState = "closed"
			case "unavailable":
				facts.Available = false
				facts.IssueState = ""
				facts.RequestDigest = ""
				facts.UpdatedAt = time.Time{}
				facts.Failure = "read_failed"
			case "policy_changed":
				facts.PolicyDigest = strings.Repeat("b", 64)
			}
			held, err := k.ObserveProductSplitChildSource(ctx, target, ancestorID, source.ID, facts)
			if err != nil {
				t.Fatal(err)
			}
			if err := readGate(false); err != nil {
				t.Fatal("live ancestor change invalidated immutable gate history", err)
			}
			if err := readGate(true); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("unready ancestor passed send", err)
			}
			if _, err := observeGate(); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("unready ancestor renewed gate", err)
			}
			ready, err := k.ObserveProductSplitChildSource(ctx, target, ancestorID, held.Snapshot.ID, source.Value.Facts)
			if err != nil || ready.Snapshot.ID == source.ID || ready.Snapshot.Digest != source.Digest {
				t.Fatal("ancestor ABA fixture failed", err)
			}
			if err := readGate(true); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("ancestor ABA reused old gate", err)
			}
			refreshed, err := observeGate()
			if err != nil || len(refreshed.AncestorSources) != 1 || refreshed.AncestorSources[0].SourceID != ready.Snapshot.ID {
				t.Fatal("fresh ancestor gate denied", err)
			}
			if err := readGate(true); err != nil {
				t.Fatal(err)
			}
			source = ready.Snapshot
		})
		if t.Failed() {
			return
		}
	}
	now := k.clock()
	k.Now = func() time.Time { return now.Add(6 * time.Second) }
	own, err := k.ReadProductSplitChildSource(ctx, target, job.Value.Request.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSplitChildSource(ctx, target, job.Value.Request.ParentWorkItemID, own.ID, own.Value.Facts); err != nil {
		t.Fatal(err)
	}
	if _, err := observeGate(); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("fresh own source renewed stale ancestor", err)
	}
	fresh, err := k.ObserveProductSplitChildSource(ctx, target, ancestorID, source.ID, source.Value.Facts)
	if err != nil || fresh.Snapshot.ID != source.ID {
		t.Fatal("benign ancestor refresh changed semantic identity", err)
	}
	if _, err := observeGate(); err != nil {
		t.Fatal("refreshed lineage denied", err)
	}
	t.Run("missing_ancestor_gate_reference", func(t *testing.T) {
		tx, err := k.kernelTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		saved, err := loadSplitCreateGate(ctx, tx, job)
		if err != nil {
			t.Fatal(err)
		}
		saved.Value.AncestorSources = nil
		saved.Digest, err = contentDigest(saved.Value)
		if err != nil {
			t.Fatal(err)
		}
		if err := kernelRecordTx(ctx, tx, "product_split_create_gate", job.ID, target.WaveID, saved); err != nil {
			t.Fatal(err)
		}
		if err := kernelRecordTx(ctx, tx, "product_split_create_gate_history", saved.Digest, job.ID, saved); err != nil {
			t.Fatal(err)
		}
		if _, err := loadSplitCreateGate(ctx, tx, job); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("missing ancestor reference was silently inferred", err)
		}
	})
	for _, kind := range []string{"product_split_child_source", "product_split_child_source_history", "product_split_child_source_policy_history"} {
		t.Run("lost_"+kind, func(t *testing.T) {
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			id := ancestorID
			if kind == "product_split_child_source_history" {
				id = source.ID
			}
			if _, err := tx.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, id); err != nil {
				t.Fatal(err)
			}
			if _, _, err := k.nestedSplitCreationSourceTx(ctx, tx, job); !readinessHoldError(err) {
				t.Fatal("missing ancestor evidence passed live check", err)
			}
			if kind != "product_split_child_source" {
				if _, err := loadSplitCreateGate(ctx, tx, job); !readinessHoldError(err) {
					t.Fatal("missing ancestor history passed historical check", err)
				}
			}
		})
	}
}
