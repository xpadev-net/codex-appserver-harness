package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductSplitDependenciesIgnoreOmittedTaskDAGAndClosedIssue(t *testing.T) {
	ctx := context.Background()
	k, w, group := splitScopeInputFixture(t)
	first, second := group.Value.Registrations[0].Value.Definition.ID, group.Value.Registrations[1].Value.Definition.ID
	before, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The first Issue is closed, but no current local-quality merge exists.
	facts := splitSourceFacts(group.Value.Children[0])
	facts.IssueState = "closed"
	if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), first, "", facts); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"work", "fix", "verification", "review"} {
		t.Run(kind, func(t *testing.T) {
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			task := domain.Task{Spec: contract.TaskSpec{Kind: kind, WorkItemIDs: []string{second}, DependsOn: []contract.TaskDependency{}}}
			if err := k.productMergeDependenciesTx(ctx, tx, w, task); !errors.Is(err, ErrProductMergeDependencyHeld) {
				t.Fatal("omitted Task DAG or closed Issue bypassed immutable sibling", kind, err)
			}
			task.Spec.WorkItemIDs = []string{first}
			if err := k.productMergeDependenciesTx(ctx, tx, w, task); err != nil {
				t.Fatal("independent first child inherited peer dependency", kind, err)
			}
		})
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("dependency inspection changed budget", err)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(current, w) {
		t.Fatal("dependency inspection changed Wave", err)
	}
	var runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("dependency read dispatched model", runs, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ids, err := productSplitDependencyIDsTx(ctx, tx, w, []string{second, second})
	if err != nil || !reflect.DeepEqual(ids, []string{first}) {
		t.Fatal("reopen lost/doubled immutable dependency", ids, err)
	}
}

func TestProductSplitDependenciesRequireFullGroupAndCurrentScope(t *testing.T) {
	ctx := context.Background()
	k, w, group := splitScopeInputFixture(t)
	id := group.Value.Registrations[1].Value.Definition.ID
	check := func(current domain.Wave) ([]string, error) {
		tx, err := k.kernelTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		return productSplitDependencyIDsTx(ctx, tx, current, []string{id})
	}
	want, err := check(w)
	if err != nil || len(want) != 1 {
		t.Fatal(want, err)
	}
	for _, kind := range []string{"product_split_registrations", "product_split_child_scopes"} {
		t.Run(kind, func(t *testing.T) {
			var owner string
			var raw []byte
			key := group.Value.Request.ProposalID
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", kind, key).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, key); err != nil {
				t.Fatal(err)
			}
			if ids, err := check(w); !errors.Is(err, ErrProductSplitHeld) || len(ids) != 0 {
				t.Fatal("lost proof became dependency-free root", ids, err)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", kind, key, owner, raw); err != nil {
				t.Fatal(err)
			}
			if ids, err := check(w); err != nil || !reflect.DeepEqual(ids, want) {
				t.Fatal("exact restore changed dependencies", ids, err)
			}
		})
	}
	for _, mode := range []string{"scope", "policy", "source"} {
		t.Run(mode, func(t *testing.T) {
			current := w
			switch mode {
			case "scope":
				current.ScopeRevision++
			case "policy":
				current.Policy.Revision++
			case "source":
				current.SourceID += "-other"
			}
			if ids, err := check(current); !errors.Is(err, ErrProductSplitHeld) || len(ids) != 0 {
				t.Fatal("old child dependency crossed requirement epoch", ids, err)
			}
		})
	}
	// Omitting a known child's reference must not turn it into a root.
	registration, err := k.ReadProductWorkItemRegistration(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	registration.Value.SplitScope = nil
	registration.Digest, _ = contentDigest(registration.Value)
	if err := k.Put(ctx, "product_work_item_registration", id, w.ID, registration); err != nil {
		t.Fatal(err)
	}
	if ids, err := check(w); err == nil || len(ids) != 0 {
		t.Fatal("erased reference bypassed immutable dependency", ids, err)
	}
}
