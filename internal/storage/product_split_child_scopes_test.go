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

	"github.com/mattn/go-sqlite3"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func splitChildScopesFixture(t *testing.T, all, retry bool, before ...func(*KernelStore, ProductSplitChildPlanRequest)) (*KernelStore, ProductSplitProposalRequest, ProductSplitChildPlanRequest, []ProductSplitCreate) {
	t.Helper()
	return splitChildScopesFixtureWithDepth(t, all, retry, nil, before...)
}

func splitChildScopesFixtureWithDepth(t *testing.T, all, retry bool, depth []int, before ...func(*KernelStore, ProductSplitChildPlanRequest)) (*KernelStore, ProductSplitProposalRequest, ProductSplitChildPlanRequest, []ProductSplitCreate) {
	t.Helper()
	ctx := context.Background()
	k, parent, _, r := splitChildPlanFixture(t, depth...)
	prepared, err := k.PrepareProductSplitExecution(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range before {
		f(k, r)
	}
	jobs := []ProductSplitCreate{}
	for i, ref := range prepared.Value.Creations {
		job, err := k.ReadProductSplitCreate(ctx, ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && retry {
			if _, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "scope-fixture-unsent"); err != nil {
				t.Fatal(err)
			}
			job, err = k.ReserveProductSplitCreateUnsentRetry(ctx, job.ID, "scope-fixture-retry")
			if err != nil {
				t.Fatal(err)
			}
		}
		if all || i == 0 {
			if err := os.Remove(filepath.Join(k.Root, "split-http-started.json")); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			identity := startSplitCreationOwner(t, k, job)
			stop, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := process.StopGroup(stop, identity); err != nil {
				cancel()
				t.Fatal(err)
			}
			cancel()
			if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "scope-fixture-stop"); err != nil {
				t.Fatal(err)
			}
			job, err = k.RecordProductSplitCreatedIssue(ctx, job.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: int64(900 + i), Number: 99 + i, Title: job.Value.Child.Value.Child.Title, Body: job.Value.Child.Value.Body, State: "open", ObservedAt: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
		}
		jobs = append(jobs, job)
	}
	return k, parent, r, jobs
}
func assertNoSplitChildScopes(t *testing.T, k *KernelStore) {
	t.Helper()
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_split_child_scope%'").Scan(&n); err != nil || n != 0 {
		t.Fatal("partial child scope binding", n, err)
	}
}
func TestProductSplitChildScopesPreserveEveryPartSourceDepthAndRetryLineage(t *testing.T) {
	ctx := context.Background()
	k, parent, r, jobs := splitChildScopesFixture(t, true, true)
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.BindProductSplitChildScopes(ctx, r)
	if err != nil || len(saved.Value.Children) != 2 {
		t.Fatal(saved, err)
	}
	for i, binding := range saved.Value.Children {
		v := binding.Value
		if !reflect.DeepEqual(v.Child.Value.Child, parent.Proposal.Children[i]) || v.Child.Value.Depth != 1 || v.CreationID != jobs[i].ID || v.InitialCreationID != splitCreateID(v.Child.ID) || v.Scope.OriginalRequest != v.Child.Value.Child.Requirement || !reflect.DeepEqual(v.Scope.Criteria, parent.Proposal.Children[i].Criteria) || len(v.Scope.Included) != len(v.Scope.Criteria) || v.Issue.ID != int64(900+i) {
			t.Fatal("lost local scope or identity", binding)
		}
		if v.ParentScopeDigest == "" || v.SourceDigest == "" || v.ScopeDigest == "" || v.CreationDigest == "" || v.Child.Value.Target != r.Target {
			t.Fatal("unbound scope", v)
		}
	}
	first := saved.Value.Children[0].Value
	if first.SourceID != "r17-i99" || len(first.Creations) != 2 || first.Creations[0].Attempt != 0 || first.Creations[1].Attempt != 1 || first.Creations[1].PreviousID != first.Creations[0].ID {
		t.Fatal("retry became fresh lineage", first)
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) || after.SplitCreateRetries != 1 {
		t.Fatal("binding changed budgets", after, err)
	}
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&n); err != nil || n != 1 {
		t.Fatal("binding granted child execution registration", n, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&n); err != nil || n != 0 {
		t.Fatal("binding created tasks", n, err)
	}
	again, err := k.BindProductSplitChildScopes(ctx, r)
	if err != nil || !reflect.DeepEqual(saved, again) {
		t.Fatal("binding retry changed metadata", again, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-scope-audit", Target: r.Target, Action: "pause", Reason: "metadata audit"}); err != nil {
		t.Fatal(err)
	}
	again, err = k.ReadProductSplitChildScopes(ctx, r)
	if err != nil || !reflect.DeepEqual(saved, again) {
		t.Fatal("reopen/pause erased scopes", again, err)
	}
}
func TestProductSplitChildScopesWaitForEveryGETAndRollbackLateFailures(t *testing.T) {
	ctx := context.Background()
	k, _, r, _ := splitChildScopesFixture(t, false, false)
	if _, err := k.BindProductSplitChildScopes(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("partially created set bound", err)
	}
	assertNoSplitChildScopes(t, k)
	for _, kind := range []string{"product_split_child_scope_admission", "product_split_child_scopes_admission"} {
		t.Run(kind, func(t *testing.T) {
			k, _, r, _ := splitChildScopesFixture(t, true, false)
			before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			condition := "NEW.kind='" + kind + "'"
			if kind == "product_split_child_scope_admission" {
				condition += " AND (SELECT count(*) FROM records WHERE kind='" + kind + "')=1"
			}
			if _, err := k.DB.Exec("CREATE TRIGGER fail_scope BEFORE INSERT ON records WHEN " + condition + " BEGIN SELECT RAISE(ABORT,'late scope failure'); END"); err != nil {
				t.Fatal(err)
			}
			if saved, err := k.BindProductSplitChildScopes(ctx, r); err == nil || saved.ID != "" {
				t.Fatal("partial scope success returned", saved, err)
			}
			assertNoSplitChildScopes(t, k)
			after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("rollback changed budget", after, err)
			}
			if _, err := k.DB.Exec("DROP TRIGGER fail_scope"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.BindProductSplitChildScopes(ctx, r); err != nil {
				t.Fatal("rollback could not retry", err)
			}
		})
	}
}
func TestProductSplitChildScopesEvidenceLossHoldsAndExactRestore(t *testing.T) {
	ctx := context.Background()
	k, _, r, jobs := splitChildScopesFixture(t, true, false)
	saved, err := k.BindProductSplitChildScopes(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	child := saved.Value.Children[0]
	for _, loss := range []struct{ kind, id string }{
		{"product_split_child_scopes", r.ProposalID}, {"product_split_child_scopes_history", saved.ID}, {"product_split_child_scopes_admission", r.ProposalID},
		{"product_split_child_scope", child.Value.Child.Value.WorkItemID}, {"product_split_child_scope_history", child.ID}, {"product_split_child_scope_admission", child.Value.Child.Value.WorkItemID}, {"product_split_child_scope_source", "github:17:issue:99"},
		{"product_split_create", jobs[0].ID},
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
			if _, err := k.BindProductSplitChildScopes(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost scope proof recreated", loss.kind, err)
			}
			if _, err := k.ReadProductSplitChildScopes(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost proof accepted", loss.kind, err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			replay, err := k.ReadProductSplitChildScopes(ctx, r)
			if err != nil || !reflect.DeepEqual(saved, replay) {
				t.Fatal("restore changed scope", replay, err)
			}
		})
	}
}
func TestProductSplitChildScopesRetainProhibitionsWhenOnlyOneWitnessKindRemains(t *testing.T) {
	for _, keep := range []string{"product_split_child_scopes", "product_split_child_scopes_history", "product_split_child_scopes_admission", "product_split_child_scope", "product_split_child_scope_history", "product_split_child_scope_admission", "product_split_child_scope_source"} {
		t.Run(keep, func(t *testing.T) {
			ctx := context.Background()
			k, parent, r, jobs := splitChildScopesFixture(t, true, true)
			if _, err := k.BindProductSplitChildScopes(ctx, r); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", keep); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.BindProductSplitChildScopes(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("scope set regenerated", keep, err)
			}
			parent.Key = "new-key-after-scope-loss"
			if _, err := k.PrepareProductSplitProposal(ctx, parent); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("new proposal replaced scope lineage", keep, err)
			}
			for _, id := range []string{splitCreateID(jobs[0].Value.Child.ID), jobs[0].ID} {
				if _, err := k.ReadProductSplitCreate(ctx, id); !errors.Is(err, ErrProductSplitHeld) {
					t.Fatal("known lineage journal appeared absent", keep, id, err)
				}
			}
			if _, err := k.PrepareProductSplitCreate(ctx, r, jobs[0].Value.Child.ID, jobs[0].Value.OwnerID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("known first POST could be recreated", keep, err)
			}
			if _, err := k.WaveOperationBudget(ctx, r.Target.WaveID); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("lost retry proofs refunded budget", keep, err)
			}
			known, err := k.IsKnownProductSplitChildIssue(ctx, 17, 99)
			if err != nil || !known {
				t.Fatal("numeric child source released", keep, known, err)
			}
			w, err := k.Wave(ctx, r.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			held, err := productWorkItemExecutionHeld(ctx, k.DB, w, r.ParentWorkItemID)
			if err != nil || !held {
				t.Fatal("old parent execution released", keep, held, err)
			}
			if _, err := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "new-after-scope-loss", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("child seats refunded", keep, err)
			}
		})
	}
}

func TestProductSplitChildScopesRemainHistoricalAfterDirectionAndDoNotCarryScope(t *testing.T) {
	ctx := context.Background()
	k, _, r, _ := splitChildScopesFixture(t, true, false)
	saved, err := k.BindProductSplitChildScopes(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "after-child-bindings", Target: r.Target, Instruction: "new contract epoch", NextPolicyRevision: r.Target.PolicyRevision + 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	current, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil || current.ScopeRevision == r.Target.ScopeRevision {
		t.Fatal("scope did not advance", current, err)
	}
	replay, err := k.ReadProductSplitChildScopes(ctx, r)
	if err != nil || !reflect.DeepEqual(saved, replay) || replay.Value.Request.Target == current.Target() {
		t.Fatal("old child scope gained current contract", replay, err)
	}
	replay, err = k.BindProductSplitChildScopes(ctx, r)
	if err != nil || !reflect.DeepEqual(saved, replay) {
		t.Fatal("historical replay changed facts", replay, err)
	}
}

func TestProductSplitChildScopesClockRollbackAndRootSourceCollisionLeaveNoSubset(t *testing.T) {
	t.Run("clock", func(t *testing.T) {
		ctx := context.Background()
		k, _, r, jobs := splitChildScopesFixture(t, true, false)
		k.Now = func() time.Time { return jobs[1].Issue.ObservedAt.Add(-time.Second) }
		if saved, err := k.BindProductSplitChildScopes(ctx, r); saved.ID != "" || !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("future observation bound", saved, err)
		}
		assertNoSplitChildScopes(t, k)
		k.Now = time.Now
		if _, err := k.BindProductSplitChildScopes(ctx, r); err != nil {
			t.Fatal(err)
		}
	})
	for _, mode := range []string{"wave", "work_item"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, r, _ := splitChildScopesFixture(t, true, false, func(k *KernelStore, r ProductSplitChildPlanRequest) {
				if mode == "wave" {
					if _, err := k.CreateProductWave(ctx, splitRootRequest(17, 100, "github", "r17-i100")); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := k.RegisterProductWorkItem(ctx, r.Target, "", ProductWorkItemDefinition{ID: "r17-i100", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"extra"}}); err != nil {
						t.Fatal(err)
					}
				}
			})
			if saved, err := k.BindProductSplitChildScopes(ctx, r); saved.ID != "" || !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("binding stole an earlier root claim", mode, saved, err)
			}
			assertNoSplitChildScopes(t, k)
			if _, err := k.ReadProductWorkItemRegistration(ctx, "r17-i100"); err != nil {
				t.Fatal("earlier root metadata changed", err)
			}
		})
	}
}

func TestProductSplitChildScopesConcurrentConnectionsConvergeOnOneSet(t *testing.T) {
	ctx := context.Background()
	k, _, r, _ := splitChildScopesFixture(t, true, false)
	other, err := OpenExistingKernel(k.Root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	type result struct {
		saved ProductSplitChildScopes
		err   error
	}
	start := make(chan struct{})
	done := make(chan result, 2)
	for _, store := range []*KernelStore{k, other} {
		go func(store *KernelStore) {
			<-start
			saved, err := store.BindProductSplitChildScopes(ctx, r)
			done <- result{saved, err}
		}(store)
	}
	close(start)
	first, second := <-done, <-done
	if first.err != nil && second.err != nil {
		t.Fatal("no scope set committed", first.err, second.err)
	}
	for _, out := range []result{first, second} {
		if out.err != nil {
			var busy sqlite3.Error
			if out.saved.ID != "" || !errors.As(out.err, &busy) || busy.Code != sqlite3.ErrBusy && busy.Code != sqlite3.ErrLocked {
				t.Fatal("unexpected concurrent failure", out.err)
			}
		}
	}
	winner := first.saved
	if first.err != nil {
		winner = second.saved
	}
	for _, store := range []*KernelStore{k, other} {
		got, err := store.BindProductSplitChildScopes(ctx, r)
		if err != nil || !reflect.DeepEqual(got, winner) {
			t.Fatal("retry changed winning scope set", got, err)
		}
	}
	var sets, sources int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_child_scopes'").Scan(&sets); err != nil || sets != 1 {
		t.Fatal(sets, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_child_scope_source'").Scan(&sources); err != nil || sources != 2 {
		t.Fatal(sources, err)
	}
}
