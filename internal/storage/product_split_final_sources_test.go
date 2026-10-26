package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestProductSplitFinalSourcesReadEveryChildAndPinSemanticVersions(t *testing.T) {
	ctx := context.Background()
	k, w, group := splitScopeInputFixture(t)
	now := k.clock()
	k.Now = func() time.Time { return now }
	before, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductSplitFinalSources(ctx, w.Target()); err == nil {
		t.Fatal("unobserved tree accepted")
	}
	for _, binding := range group.Value.Children {
		if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), binding.Value.Child.Value.WorkItemID, "", splitSourceFacts(binding)); err != nil {
			t.Fatal(err)
		}
	}
	proof, err := k.ReadProductSplitFinalSources(ctx, w.Target())
	if err != nil || proof == nil || len(proof.Value.Sources) != 2 || !validSourceDigest(proof.Digest) {
		t.Fatal("complete open source tree unavailable", proof, err)
	}
	checkPin := func(expected string) error {
		tx, err := k.kernelTx(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		coverage, err := k.productCoverageTx(ctx, tx, w)
		if err != nil {
			return err
		}
		return k.requireProductSplitFinalSourcesTx(ctx, tx, w, coverage, expected)
	}
	if err := checkPin(proof.Digest); err != nil {
		t.Fatal("current semantic pin unavailable", err)
	}
	if err := checkPin(""); !errors.Is(err, ErrProductFinalPlanHeld) {
		t.Fatal("legacy split plan without a source pin accepted", err)
	}
	now = now.Add(time.Second)
	for _, binding := range group.Value.Children {
		id := binding.Value.Child.Value.WorkItemID
		prior, err := k.ReadProductSplitChildSource(ctx, w.Target(), id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), id, prior.ID, splitSourceFacts(binding)); err != nil {
			t.Fatal(err)
		}
	}
	refreshed, err := k.ReadProductSplitFinalSources(ctx, w.Target())
	if err != nil || !reflect.DeepEqual(refreshed, proof) {
		t.Fatal("same GET invalidated semantic pin", refreshed, err)
	}
	now = now.Add(ProductSplitFinalSourceMaxAge + time.Second)
	if _, err := k.ReadProductSplitFinalSources(ctx, w.Target()); !errors.Is(err, ErrProductFinalPlanHeld) {
		t.Fatal("stale tree accepted", err)
	}
	now = now.Add(-ProductSplitFinalSourceMaxAge) // No source write occurred during the stale read.
	binding := group.Value.Children[0]
	id := binding.Value.Child.Value.WorkItemID
	prior, err := k.ReadProductSplitChildSource(ctx, w.Target(), id)
	if err != nil {
		t.Fatal(err)
	}
	changed := splitSourceFacts(binding)
	changed.RequestDigest = ProductSourceRequestDigest("edited local requirement")
	update, err := k.ObserveProductSplitChildSource(ctx, w.Target(), id, prior.ID, changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductSplitFinalSources(ctx, w.Target()); !errors.Is(err, ErrProductFinalPlanHeld) {
		t.Fatal("edited tree accepted", err)
	}
	if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), id, update.Snapshot.ID, splitSourceFacts(binding)); err != nil {
		t.Fatal(err)
	}
	restored, err := k.ReadProductSplitFinalSources(ctx, w.Target())
	if err != nil || restored.Digest == proof.Digest {
		t.Fatal("semantic ABA reused old pin", err)
	}
	if err := checkPin(proof.Digest); !errors.Is(err, ErrProductFinalPlanHeld) {
		t.Fatal("old pin survived semantic ABA", err)
	}
	if err := checkPin(restored.Digest); err != nil {
		t.Fatal("new semantic pin unavailable", err)
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("source read changed budget", err)
	}
	var tasks int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal("source read granted Task execution", tasks, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_child_source_history' AND id=?", restored.Value.Sources[0].SnapshotID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductSplitFinalSources(ctx, w.Target()); err == nil {
		t.Fatal("lost source history accepted")
	}
}

// Status reduction consumes already authenticated canonical facts. This boundary
// test supplies no real merge acceptance and does not enable child execution.
func TestProductSplitFinalClosedSourceRequiresOwnMergeAndNeverWaivesHold(t *testing.T) {
	child := ProductSplitCoverageChild{WorkItemID: "child", RegistrationDigest: "registration", ScopeDigest: "scope", LocalCriteria: []string{"local"}}
	fact := ProductCoverageFact{WorkItemID: "child", RegistrationDigest: "registration", SplitScopeDigest: "scope", LocalCriteria: []string{"local"}, MergeID: "merge", MergeSHA: strings.Repeat("a", 40), ReceiptDigest: strings.Repeat("b", 64)}
	source := ProductSplitChildSource{Value: ProductSplitChildSourceValue{Status: "closed", Facts: ProductSplitChildSourceFacts{Available: true, IssueState: "closed"}}}
	for _, mode := range []string{"own_merge", "no_merge", "hold", "delegated", "registration", "scope", "local", "unavailable", "policy"} {
		t.Run(mode, func(t *testing.T) {
			current, receipt := source, fact
			delegated := false
			switch mode {
			case "no_merge":
				receipt = ProductCoverageFact{}
			case "hold":
				current.Value.Facts.Hold = true
			case "delegated":
				delegated = true
			case "registration":
				receipt.RegistrationDigest = "wrong"
			case "scope":
				receipt.SplitScopeDigest = "wrong"
			case "local":
				receipt.LocalCriteria = []string{"Wave criterion"}
			case "unavailable":
				current.Value.Facts.Available = false
			case "policy":
				current.Value.Status = "policy_changed"
			}
			if got := splitFinalSourceStatusAllowed(current, child, receipt, delegated); got != (mode == "own_merge") {
				t.Fatal("closed Issue bypassed final proof", mode, got)
			}
		})
	}
}

func TestProductNestedSplitFinalSourcesIncludeDelegatedIntermediateIssues(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := nestedRegistrationScopeFixture(t)
	nested, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	fixed := k.clock()
	k.Now = func() time.Time { return fixed }
	bindings := append(append([]ProductSplitChildScopeBinding{}, scopes.Value.Children...), nested.Value.Children...)
	for _, binding := range bindings {
		id := binding.Value.Child.Value.WorkItemID
		prior, err := k.ReadProductSplitChildSource(ctx, r.Target, id)
		expected := ""
		if err == nil {
			expected = prior.ID
		} else if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, id, expected, splitSourceFacts(binding)); err != nil {
			t.Fatal(err)
		}
	}
	proof, err := k.ReadProductSplitFinalSources(ctx, r.Target)
	if err != nil || proof == nil || len(proof.Value.Sources) != 4 {
		t.Fatal("nested tree omitted a layer", proof, err)
	}
	seen := map[string]bool{}
	for _, source := range proof.Value.Sources {
		seen[source.WorkItemID] = true
	}
	for _, binding := range bindings {
		if !seen[binding.Value.Child.Value.WorkItemID] {
			t.Fatal("registered source omitted", binding.ID)
		}
	}
	// The parent of the nested group has no own merge. A fresh held/closed
	// intermediate Issue cannot disappear just because all leaves are observed.
	parentID := r.ParentWorkItemID
	var parent ProductSplitChildScopeBinding
	for _, binding := range bindings {
		if binding.Value.Child.Value.WorkItemID == parentID {
			parent = binding
		}
	}
	source, err := k.ReadProductSplitChildSource(ctx, r.Target, parentID)
	if err != nil {
		t.Fatal(err)
	}
	facts := splitSourceFacts(parent)
	facts.Hold = true
	facts.IssueState = "closed"
	if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, parentID, source.ID, facts); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductSplitFinalSources(ctx, r.Target); !errors.Is(err, ErrProductFinalPlanHeld) {
		t.Fatal("held intermediate Issue disappeared from final gate", err)
	}
}
