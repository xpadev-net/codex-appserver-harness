package storage

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func splitSourceFacts(binding ProductSplitChildScopeBinding) ProductSplitChildSourceFacts {
	v := binding.Value
	return ProductSplitChildSourceFacts{RepositoryID: v.Issue.RepositoryID, IssueID: v.Issue.ID, IssueNumber: v.Issue.Number, PolicyDigest: strings.Repeat("a", 64), Available: true, IssueState: "open", RequestDigest: ProductSourceRequestDigest(v.Issue.Title + "\n\n" + v.Issue.Body), UpdatedAt: v.Issue.ObservedAt}
}

func TestProductSplitChildSourcePinsIdentityPolicyAndBoundedFreshnessWithoutWaveHold(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	before, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	clock := scopes.Value.CreatedAt.Add(time.Minute)
	k.Now = func() time.Time { return clock }
	first := scopes.Value.Children[0]
	id := first.Value.Child.Value.WorkItemID
	facts := splitSourceFacts(first)
	update, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", facts)
	if err != nil || !update.Changed || update.Snapshot.Value.Status != "ready" || update.Snapshot.Version != 1 {
		t.Fatal(update, err)
	}
	read, err := k.RequireProductSplitChildSource(ctx, r.Target, id, facts.PolicyDigest, time.Minute)
	if err != nil || !reflect.DeepEqual(read, update.Snapshot) {
		t.Fatal("fresh source unavailable", read, err)
	}
	clock = clock.Add(2 * time.Minute)
	if _, err := k.RequireProductSplitChildSource(ctx, r.Target, id, facts.PolicyDigest, time.Minute); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("stale source accepted", err)
	}
	renewed, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, update.Snapshot.ID, facts)
	if err != nil || renewed.Changed || renewed.Snapshot.ID != update.Snapshot.ID || renewed.Snapshot.Version != 1 || !renewed.Snapshot.CheckedAt.Equal(clock) || !renewed.Snapshot.ObservedAt.Equal(update.Snapshot.ObservedAt) {
		t.Fatal("unchanged GET grew semantic history", renewed, err)
	}
	var histories int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_child_source_history'").Scan(&histories); err != nil || histories != 1 {
		t.Fatal("renewal duplicated history", histories, err)
	}
	read, err = k.RequireProductSplitChildSource(ctx, r.Target, id, facts.PolicyDigest, time.Minute)
	if err != nil || read.ID != update.Snapshot.ID {
		t.Fatal("fresh read did not renew", read, err)
	}
	after, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("child observation changed global Wave", after, err)
	}
	for _, age := range []time.Duration{0, -time.Second, 6 * time.Minute} {
		if _, err := k.RequireProductSplitChildSource(ctx, r.Target, id, facts.PolicyDigest, age); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("unbounded freshness accepted", age, err)
		}
	}
	clock = update.Snapshot.ObservedAt.Add(-time.Second)
	if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, read.ID, facts); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("clock rollback refreshed source", err)
	}
}

func TestProductSplitChildSourceProhibitionsAreLocalAndPolicyDoesNotRepin(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"unavailable", "identity_changed", "closed", "hold", "request_changed", "policy_changed"} {
		t.Run(mode, func(t *testing.T) {
			k, r, scopes := splitRegistrationsFixture(t)
			if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
				t.Fatal(err)
			}
			first, second := scopes.Value.Children[0], scopes.Value.Children[1]
			id, other := first.Value.Child.Value.WorkItemID, second.Value.Child.Value.WorkItemID
			facts := splitSourceFacts(first)
			initial, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", facts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, other, "", splitSourceFacts(second)); err != nil {
				t.Fatal(err)
			}
			changed := facts
			switch mode {
			case "unavailable":
				changed.Available = false
				changed.IssueState = ""
				changed.RequestDigest = ""
				changed.UpdatedAt = time.Time{}
				changed.Failure = "read_failed"
			case "identity_changed":
				changed.IssueID++
			case "closed":
				changed.IssueState = "closed"
			case "hold":
				changed.Hold = true
			case "request_changed":
				changed.RequestDigest = strings.Repeat("b", 64)
			case "policy_changed":
				changed.PolicyDigest = strings.Repeat("c", 64)
			}
			update, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, initial.Snapshot.ID, changed)
			if err != nil || !update.Changed || update.Snapshot.Value.Status != mode || update.Snapshot.Version != 2 {
				t.Fatal(mode, update, err)
			}
			if _, err := k.RequireProductSplitChildSource(ctx, r.Target, id, facts.PolicyDigest, time.Minute); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("prohibition authorized source", mode, err)
			}
			if _, err := k.RequireProductSplitChildSource(ctx, r.Target, other, facts.PolicyDigest, time.Minute); err != nil {
				t.Fatal("peer source was starved", mode, err)
			}
			pin, err := k.ReadProductSplitChildSourcePolicy(ctx, r.Target, id)
			if err != nil || pin != facts.PolicyDigest {
				t.Fatal("source observation repinned host policy", pin, err)
			}
			w, err := k.Wave(ctx, r.Target.WaveID)
			if err != nil || w.SourceHold != nil {
				t.Fatal("child prohibition became global hold", w.SourceHold, err)
			}
			restored, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, update.Snapshot.ID, facts)
			if err != nil || restored.Snapshot.Value.Status != "ready" || restored.Snapshot.Version != 3 {
				t.Fatal("restoration lost history", restored, err)
			}
			if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, initial.Snapshot.ID, changed); !errors.Is(err, ErrProductSplitHeld) {
				t.Fatal("stale CAS overwrote current facts", err)
			}
		})
	}
}

func TestProductSplitChildSourceMissingProofsHoldUntilExactRestore(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	binding := scopes.Value.Children[0]
	id := binding.Value.Child.Value.WorkItemID
	facts := splitSourceFacts(binding)
	initial, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", facts)
	if err != nil {
		t.Fatal(err)
	}
	for _, loss := range []struct{ kind, id string }{{"product_split_child_source", id}, {"product_split_child_source_history", initial.Snapshot.ID}, {"product_split_child_source_policy", id}, {"product_split_child_source_policy_history", id}} {
		t.Run(loss.kind, func(t *testing.T) {
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductSplitChildSource(ctx, r.Target, id); !readinessHoldError(err) {
				t.Fatal("lost source proof accepted", loss, err)
			}
			if out, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, initial.Snapshot.ID, facts); !readinessHoldError(err) || out.Snapshot.ID != "" {
				t.Fatal("lost source proof repaired", out, err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			actual, err := k.ReadProductSplitChildSource(ctx, r.Target, id)
			if err != nil || !reflect.DeepEqual(actual, initial.Snapshot) {
				t.Fatal("exact restore rewrote observation", actual, err)
			}
		})
	}
}

func TestProductSplitChildSourceLateFailureRollsBackPolicyAndHistory(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"product_split_child_source_policy_history", "product_split_child_source_history", "product_split_child_source"} {
		t.Run(kind, func(t *testing.T) {
			k, r, scopes := splitRegistrationsFixture(t)
			if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("CREATE TRIGGER fail_child_source BEFORE INSERT ON records WHEN NEW.kind='" + kind + "' BEGIN SELECT RAISE(ABORT,'source rollback'); END"); err != nil {
				t.Fatal(err)
			}
			binding := scopes.Value.Children[0]
			id := binding.Value.Child.Value.WorkItemID
			if out, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", splitSourceFacts(binding)); err == nil || out.Snapshot.ID != "" {
				t.Fatal("partial source success", out, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_split_child_source%'").Scan(&count); err != nil || count != 0 {
				t.Fatal("source transaction left partial metadata", count, err)
			}
			if _, err := k.DB.Exec("DROP TRIGGER fail_child_source"); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", splitSourceFacts(binding)); err != nil {
				t.Fatal("source retry could not proceed", err)
			}
		})
	}
}

func TestProductSplitChildSourceIndependentRecordsDoNotRefundCreationOrSeats(t *testing.T) {
	ctx := context.Background()
	for _, keep := range []string{"product_split_child_source", "product_split_child_source_history", "product_split_child_source_policy", "product_split_child_source_policy_history"} {
		t.Run(keep, func(t *testing.T) {
			k, r, scopes := splitRegistrationsFixture(t)
			if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
				t.Fatal(err)
			}
			binding := scopes.Value.Children[0]
			id := binding.Value.Child.Value.WorkItemID
			if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", splitSourceFacts(binding)); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>?", keep); err != nil {
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
			for _, check := range []func() error{
				func() error { _, e := k.BindProductSplitChildScopes(ctx, r); return e },
				func() error { _, e := k.RegisterProductSplitChildren(ctx, r); return e },
				func() error { _, e := k.ReadProductSplitCreate(ctx, binding.Value.CreationID); return e },
				func() error { _, e := k.WaveOperationBudget(ctx, r.Target.WaveID); return e },
			} {
				if err := check(); !readinessHoldError(err) {
					t.Fatal("source witness refunded/recreated provenance", keep, err)
				}
			}
			known, err := k.IsKnownProductSplitChildIssue(ctx, 17, binding.Value.Issue.Number)
			if err != nil || !known {
				t.Fatal("source witness lost canonical source", known, err)
			}
			page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, target := range page.Targets {
				found = found || target.ID == binding.Value.CreationID
			}
			if !found {
				t.Fatal("source witness disappeared from recovery", page)
			}
		})
	}
}

func TestProductSplitChildSourceReopensPausedAndDoesNotCarryDirection(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	binding := scopes.Value.Children[0]
	id := binding.Value.Child.Value.WorkItemID
	initial, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", splitSourceFacts(binding))
	if err != nil {
		t.Fatal(err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-child-source", Target: r.Target, Action: "pause", Reason: "audit"}); err != nil {
		t.Fatal(err)
	}
	read, err := k.ReadProductSplitChildSource(ctx, r.Target, id)
	if err != nil || !reflect.DeepEqual(read, initial.Snapshot) {
		t.Fatal("reopen/pause changed source facts", read, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "resume-child-source", Target: r.Target, Action: "resume", Reason: "continue"}); err != nil {
		t.Fatal(err)
	}
	d, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "new-child-source-epoch", Target: r.Target, Instruction: "new contract", NextPolicyRevision: r.Target.PolicyRevision + 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []contract.Target{r.Target, w.Target()} {
		if _, err := k.ReadProductSplitChildSource(ctx, target, id); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("old source carried into current scope", target, err)
		}
		if _, err := k.ObserveProductSplitChildSource(ctx, target, id, initial.Snapshot.ID, splitSourceFacts(binding)); !errors.Is(err, ErrProductSplitHeld) {
			t.Fatal("old source authorized new observation", target, err)
		}
	}
}

func TestProductSplitChildSourceVersionLimitRetainsHistoryAndAllowsFreshnessOnly(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	binding := scopes.Value.Children[0]
	id := binding.Value.Child.Value.WorkItemID
	facts := splitSourceFacts(binding)
	initial, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", facts)
	if err != nil {
		t.Fatal(err)
	}
	last := initial.Snapshot
	last.Version = maxSplitSourceVersions
	last.ID = structureID("split-child-source-", id, strconv.Itoa(last.Version)+":"+last.Digest)
	for _, record := range []struct{ kind, id string }{{"product_split_child_source", id}, {"product_split_child_source_history", last.ID}} {
		if err := k.Put(ctx, record.kind, record.id, r.Target.WaveID, last); err != nil {
			t.Fatal(err)
		}
	}
	renewed, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, last.ID, facts)
	if err != nil || renewed.Changed || renewed.Snapshot.ID != last.ID {
		t.Fatal("limit blocked freshness-only read", renewed, err)
	}
	facts.Hold = true
	if out, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, last.ID, facts); !errors.Is(err, ErrProductSplitHeld) || out.Snapshot.ID != "" {
		t.Fatal("source history wrapped/reset at limit", out, err)
	}
	read, err := k.ReadProductSplitChildSource(ctx, r.Target, id)
	if err != nil || read.Version != maxSplitSourceVersions || read.ID != last.ID {
		t.Fatal("limit changed saved source", read, err)
	}
}

func TestProductSplitChildSourceConcurrentCASHasOneSemanticWinner(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	binding := scopes.Value.Children[0]
	id := binding.Value.Child.Value.WorkItemID
	facts := splitSourceFacts(binding)
	initial, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, "", facts)
	if err != nil {
		t.Fatal(err)
	}
	other, err := OpenExistingKernel(k.Root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	type result struct {
		update ProductSplitChildSourceUpdate
		err    error
	}
	start := make(chan struct{})
	done := make(chan result, 2)
	for i, store := range []*KernelStore{k, other} {
		go func(i int, store *KernelStore) {
			<-start
			change := facts
			if i == 0 {
				change.Hold = true
			} else {
				change.IssueState = "closed"
			}
			u, e := store.ObserveProductSplitChildSource(ctx, r.Target, id, initial.Snapshot.ID, change)
			done <- result{u, e}
		}(i, store)
	}
	close(start)
	a, b := <-done, <-done
	winners := 0
	for _, out := range []result{a, b} {
		if out.err == nil {
			winners++
			if !out.update.Changed || out.update.Snapshot.Version != 2 {
				t.Fatal("invalid CAS winner", out.update)
			}
			continue
		}
		var busy sqlite3.Error
		if !errors.Is(out.err, ErrProductSplitHeld) && (!errors.As(out.err, &busy) || busy.Code != sqlite3.ErrBusy && busy.Code != sqlite3.ErrLocked) {
			t.Fatal("unexpected CAS failure", out.err)
		}
		if out.update.Snapshot.ID != "" {
			t.Fatal("losing transaction returned success data", out.update)
		}
	}
	if winners != 1 {
		t.Fatal("CAS admitted conflicting source facts", a, b)
	}
	read, err := k.ReadProductSplitChildSource(ctx, r.Target, id)
	if err != nil || read.Version != 2 || read.Value.Status != "hold" && read.Value.Status != "closed" {
		t.Fatal("CAS did not converge", read, err)
	}
}
