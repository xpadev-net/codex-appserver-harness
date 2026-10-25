package storage

import (
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
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func nestedCreationFixture(t *testing.T) (*KernelStore, ProductSplitChildPlanRequest, ProductSplitCreate) {
	t.Helper()
	ctx := context.Background()
	k, _, rootRequest, _ := splitChildScopesFixture(t, true, false)
	if _, err := k.BindProductSplitChildScopes(ctx, rootRequest); err != nil {
		t.Fatal(err)
	}
	group, err := k.RegisterProductSplitChildren(ctx, rootRequest)
	if err != nil {
		t.Fatal(err)
	}
	binding := group.Value.Children[1]
	registration := group.Value.Registrations[1]
	if _, err := k.ObserveProductSplitChildSource(ctx, rootRequest.Target, registration.Value.Definition.ID, "", splitSourceFacts(binding)); err != nil {
		t.Fatal(err)
	}
	proof, err := k.PrepareProductSplitParentScope(ctx, rootRequest.Target, registration.Value.Definition.ID)
	if err != nil {
		t.Fatal(err)
	}
	proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: registration.Value.Definition.ID, ScopeRevision: rootRequest.Target.ScopeRevision, Outcome: "split", Children: []splitting.Child{}}
	for i, c := range binding.Value.Scope.Criteria {
		key := []string{"one", "two"}[i]
		proposal.Children = append(proposal.Children, splitting.Child{Key: key, Title: "nested " + key, Requirement: c.Requirement, Criteria: []verification.Criterion{{ID: "nested-" + key, Requirement: c.Requirement}}, Coverage: []splitting.Coverage{{ParentCriterion: c.ID, ChildCriterion: "nested-" + key}}, DependsOn: []string{}})
	}
	admitted, err := k.PrepareProductNestedSplitProposal(ctx, ProductSplitProposalRequest{Target: rootRequest.Target, Key: "nested-create", ParentWorkItemID: registration.Value.Definition.ID, RegistrationDigest: registration.Digest, Proposal: proposal}, ProductSplitParentScopeReference{ID: proof.ID, Digest: proof.Digest})
	if err != nil {
		t.Fatal(err)
	}
	r := ProductSplitChildPlanRequest{Target: rootRequest.Target, ParentWorkItemID: registration.Value.Definition.ID, ProposalID: admitted.Value.Proposal.ID, ProposalDigest: admitted.Value.Proposal.Digest}
	plan, err := k.PrepareProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	now := k.clock()
	k.Now = func() time.Time { return now }
	job, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[0].ID, "nested-controller")
	if err != nil {
		t.Fatal(err)
	}
	source, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, source.ID, source.Value.Facts); err != nil {
		t.Fatal(err)
	}
	return k, r, job
}

func TestProductNestedSplitCreationPinsWholeAncestryAndPhysicalOwnerBeforeGETBinding(t *testing.T) {
	ctx := context.Background()
	k, r, job := nestedCreationFixture(t)
	if job.Value.Ancestry == nil || job.Value.Child.Value.Depth != 2 || len(job.Value.Ancestry.Value.Children) != 2 {
		t.Fatal("missing whole ancestry")
	}
	if err := os.Remove(filepath.Join(k.Root, "split-http-started.json")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	readStart := time.Now()
	if _, err := k.ReadProductSplitCreate(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	readElapsed := time.Since(readStart)
	var witnessCount, witnessBytes int64
	if err := k.DB.QueryRow("SELECT count(*),coalesce(sum(length(payload)),0) FROM records WHERE kind LIKE 'product_split_%' OR kind LIKE 'product_nested_split_%'").Scan(&witnessCount, &witnessBytes); err != nil {
		t.Fatal(err)
	}
	t.Logf("nested creation audit: witnesses=%d payload_bytes=%d authenticated_read=%s", witnessCount, witnessBytes, readElapsed)
	identity := startSplitCreationOwner(t, k, job, 20*time.Second)
	active, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || active.State != "unknown" {
		t.Fatal(err)
	}
	gate, err := func() (splitCreateGateRecord, error) {
		tx, e := k.kernelTx(ctx)
		if e != nil {
			return splitCreateGateRecord{}, e
		}
		defer tx.Rollback()
		return loadSplitCreateGate(ctx, tx, active)
	}()
	if err != nil || gate.Value.ParentSourceID == "" || gate.Value.ParentSourceDigest == "" || !reflect.DeepEqual(gate.Value.Ancestry, job.Value.Ancestry) {
		t.Fatal("own Issue source not pinned", err)
	}
	if _, err := k.ReserveProductSplitCreateUnsentRetry(ctx, job.ID, "must-not-resend"); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("unknown journal retried", err)
	}
	stop, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := process.StopGroup(stop, identity); err != nil {
		t.Fatal(err)
	}
	stopped, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "nested-physical-stop")
	if err != nil || stopped.State != "stopped" {
		t.Fatal(err)
	}
	observed, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: 501, Number: 51, Title: job.Value.Child.Value.Child.Title, Body: job.Value.Child.Value.Body, State: "open", ObservedAt: k.clock()})
	if err != nil || observed.State != "observed" {
		t.Fatal(err)
	}
	var resource splitCreatedResource
	if err := k.Record(ctx, "product_split_created_resource", "github:17:issue:51", &resource); err != nil || !reflect.DeepEqual(resource.Ancestry, job.Value.Ancestry) {
		t.Fatal("resource dropped ancestry", err)
	}
	if _, err := k.CreateProductWave(ctx, splitRootRequest(17, 51, "github", "nested-created-root")); !errors.Is(err, ErrProductSplitChildSourceHeld) {
		t.Fatal("grandchild Issue became root", err)
	}
	replay, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID)
	if err != nil || replay.State != "observed" || !reflect.DeepEqual(replay.Value.Ancestry, job.Value.Ancestry) {
		t.Fatal("replay changed creation", err)
	}
	second := job.Value.Ancestry.Value.Plan.Children[1]
	secondJob, err := k.PrepareProductSplitCreate(ctx, r, second.ID, "second-nested-controller")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(k.Root, "split-http-started.json")); err != nil {
		t.Fatal(err)
	}
	secondOwner := startSplitCreationOwner(t, k, secondJob, 20*time.Second)
	secondStop, secondCancel := context.WithTimeout(ctx, 3*time.Second)
	defer secondCancel()
	if err := process.StopGroup(secondStop, secondOwner); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductSplitCreateHTTP(ctx, secondJob.ID, secondJob.Value.OwnerID, secondJob.Digest, strings.Repeat("a", 64), "second-nested-stop"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RecordProductSplitCreatedIssue(ctx, secondJob.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: 502, Number: 52, Title: second.Value.Child.Title, Body: second.Value.Body, State: "open", ObservedAt: k.clock()}); err != nil {
		t.Fatal(err)
	}
	scopes, err := k.BindProductSplitChildScopes(ctx, r)
	if err != nil || len(scopes.Value.Children) != 2 {
		t.Fatal("complete nested scope binding denied", err)
	}
	registered, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil || len(registered.Value.Registrations) != 2 {
		t.Fatal("explicit nested registration denied", err)
	}
}

func TestProductNestedSplitCreationParentSourceGateRejectsStaleHoldAndABA(t *testing.T) {
	ctx := context.Background()
	k, r, job := nestedCreationFixture(t)
	source, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	now := k.clock()
	gate, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", "")
	if err != nil || gate.ParentSourceID != source.ID {
		t.Fatal(err)
	}
	read := func() error {
		tx, err := k.kernelTx(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		_, err = k.splitCreateSendGateTx(ctx, tx, job)
		return err
	}
	if err := read(); err != nil {
		t.Fatal(err)
	}
	k.Now = func() time.Time { return now.Add(6 * time.Second) }
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("stale own source renewed gate", err)
	}
	k.Now = func() time.Time { return now.Add(time.Millisecond) }
	facts := source.Value.Facts
	facts.Hold = true
	held, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, source.ID, facts)
	if err != nil {
		t.Fatal(err)
	}
	if err := read(); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("own hold passed gate", err)
	}
	ready, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, held.Snapshot.ID, source.Value.Facts)
	if err != nil || ready.Snapshot.ID == source.ID || ready.Snapshot.Digest != source.Digest {
		t.Fatal("fixture did not create semantic ABA", err)
	}
	if err := read(); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("old gate accepted ABA", err)
	}
	renewed, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", "")
	if err != nil || renewed.ParentSourceID != ready.Snapshot.ID {
		t.Fatal("fresh own source failed renewal", err)
	}
	if err := read(); err != nil {
		t.Fatal(err)
	}
}

func TestProductNestedSplitCreationWitnessesRetainAncestorsAfterAllPriorEvidenceLoss(t *testing.T) {
	for _, kind := range []string{"product_split_create", "product_split_create_event", "product_split_create_history", "product_split_create_admission", "product_split_create_gate", "product_split_create_gate_history", "product_split_created_resource", "product_split_create_retry_budget", "retry_ledger"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, r, job := nestedCreationFixture(t)
			if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); err != nil {
				t.Fatal(err)
			}
			keepID := job.ID
			if kind == "product_split_create_event" {
				keepID = splitCreateEventID(job.ID, 0)
			}
			if kind == "product_split_create_gate_history" {
				var gate splitCreateGateRecord
				if err := k.Record(ctx, "product_split_create_gate", job.ID, &gate); err != nil {
					t.Fatal(err)
				}
				keepID = gate.Digest
			}
			if kind == "product_split_created_resource" {
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
				if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "nested-resource-stop"); err != nil {
					t.Fatal(err)
				}
				if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: 501, Number: 51, Title: job.Value.Child.Value.Child.Title, Body: job.Value.Child.Value.Body, State: "open", ObservedAt: k.clock()}); err != nil {
					t.Fatal(err)
				}
				keepID = "github:17:issue:51"
			}
			if kind == "product_split_create_retry_budget" || kind == "retry_ledger" {
				if _, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "nested-unsent"); err != nil {
					t.Fatal(err)
				}
				next, err := k.ReserveProductSplitCreateUnsentRetry(ctx, job.ID, "nested-retry")
				if err != nil {
					t.Fatal(err)
				}
				keepID = next.ID
				job = next
			}
			ancestor := job.Value.Ancestry.Value.Children
			priorBudget, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE (kind LIKE 'product_split_%' OR kind LIKE 'product_nested_split_%') AND NOT(kind=? AND id=?)", kind, keepID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND (json_type(payload,'$.value.split_scope') IS NOT NULL OR json_type(payload,'$.split_scope') IS NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			if kind == "retry_ledger" {
				if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry') AND NOT(kind='split_create_retry' AND operation_key=?)", job.ID); err != nil {
					t.Fatal(err)
				}
			} else if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			for _, binding := range ancestor {
				if _, err := k.CreateProductWave(ctx, splitRootRequest(binding.Value.Issue.RepositoryID, binding.Value.Issue.Number, "github", fmt.Sprintf("ancestor-after-%s", kind))); !errors.Is(err, ErrProductSplitChildSourceHeld) {
					t.Fatal("ancestor sibling root claim lost", err)
				}
				if _, err := k.ReadProductSplitCreate(ctx, binding.Value.CreationID); !readinessHoldError(err) {
					t.Fatal("ancestor journal rebuilt", err)
				}
			}
			budget, budgetErr := k.WaveOperationBudget(ctx, r.Target.WaveID)
			if budgetErr != nil && !readinessHoldError(budgetErr) || budgetErr == nil && !reflect.DeepEqual(budget, priorBudget) {
				t.Fatal("ancestor retry budget refunded", budget, budgetErr)
			}
			if _, err := k.ReserveProductSplitChildren(ctx, r); !readinessHoldError(err) {
				t.Fatal("own seats refunded", err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, target := range page.Targets {
				found = found || target.ID == ancestor[0].Value.CreationID
			}
			if !found {
				t.Fatal("ancestor recovery witness hidden", page)
			}
		})
	}
}

func TestProductNestedSplitCreationMissingProofCannotBeRepairedAndLateFailureIsAtomic(t *testing.T) {
	ctx := context.Background()
	k, r, job := nestedCreationFixture(t)
	records := []struct{ kind, id string }{{"product_split_create", job.ID}, {"product_split_create_history", job.ID}, {"product_split_create_admission", job.ID}, {"product_split_create_event", splitCreateEventID(job.ID, 0)}, {"product_nested_split_allocation", r.ProposalID}}
	for _, record := range records {
		t.Run(record.kind, func(t *testing.T) {
			var raw []byte
			var owner string
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", record.kind, record.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", record.kind, record.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID); !readinessHoldError(err) {
				t.Fatal("missing proof repaired", err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", record.kind, record.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductSplitCreate(ctx, job.ID); err != nil {
				t.Fatal("exact restoration rejected", err)
			}
		})
	}
	// The second child has no journal yet; its failure must not leak a lease,
	// partial history or admission, and must leave the first child's proof intact.
	child := job.Value.Ancestry.Value.Plan.Children[1]
	if _, err := k.DB.Exec("CREATE TRIGGER fail_nested_creation BEFORE INSERT ON records WHEN NEW.kind='product_split_create_event' AND NEW.work_item_id='" + splitCreateID(child.ID) + "' BEGIN SELECT RAISE(ABORT,'late nested creation failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PrepareProductSplitCreate(ctx, r, child.ID, "second-nested-owner"); err == nil {
		t.Fatal("late failure ignored")
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE id=? OR (kind='product_split_create_event' AND work_item_id=?)", splitCreateID(child.ID), splitCreateID(child.ID)).Scan(&count); err != nil || count != 0 {
		t.Fatal("partial journal", count, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE run_id=?", splitCreateID(child.ID)).Scan(&count); err != nil || count != 0 {
		t.Fatal("partial lease", count, err)
	}
	if _, err := k.ReadProductSplitCreate(ctx, job.ID); err != nil {
		t.Fatal("first child damaged", err)
	}
}

func TestProductNestedSplitCreationForeignMalformedAncestryDoesNotPoisonHealthyWave(t *testing.T) {
	ctx := context.Background()
	k, r, job := nestedCreationFixture(t)
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_create_history','foreign-creation','foreign-wave',?)", []byte(`{"ancestry":"broken"}`)); err != nil {
		t.Fatal(err)
	}
	replay, err := k.PrepareProductSplitCreate(ctx, r, job.Value.Child.ID, job.Value.OwnerID)
	if err != nil || !reflect.DeepEqual(replay, job) {
		t.Fatal("foreign witness poisoned own read", err)
	}
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); err != nil {
		t.Fatal("foreign witness poisoned own gate", err)
	}
	page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
	if err != nil {
		t.Fatal("corrupt witness broke bounded recovery", err)
	}
	unavailable := false
	found := false
	for _, target := range page.Targets {
		unavailable = unavailable || target.Unavailable
		found = found || target.ID == job.ID
	}
	if !unavailable || !found {
		t.Fatal("recovery lost corrupt/healthy targets", page)
	}
}

func TestProductNestedSplitCreationGateRequiresOriginalSourceHistory(t *testing.T) {
	ctx := context.Background()
	k, _, job := nestedCreationFixture(t)
	gate, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", "")
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	var owner string
	if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind='product_split_child_source_history' AND id=?", gate.ParentSourceID).Scan(&owner, &raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_child_source_history' AND id=?", gate.ParentSourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); !readinessHoldError(err) {
		t.Fatal("source history loss renewed gate", err)
	}
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_source_history',?,?,?)", gate.ParentSourceID, owner, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSplitCreateGate(ctx, job.ID, job.Value.OwnerID, job.Digest, "", ""); err != nil {
		t.Fatal("exact source history restoration rejected", err)
	}
}
