package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func nestedSplitContextFixture(t *testing.T) (*KernelStore, ProductSplitProposalRequest, ProductSplitChildScopeBinding, ProductSplitChildSourceFacts) {
	t.Helper()
	k, w, group := splitScopeInputFixture(t)
	binding := group.Value.Children[1]
	registration := group.Value.Registrations[1]
	facts := splitSourceFacts(binding)
	if _, err := k.ObserveProductSplitChildSource(context.Background(), w.Target(), registration.Value.Definition.ID, "", facts); err != nil {
		t.Fatal(err)
	}
	proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: registration.Value.Definition.ID, ScopeRevision: w.ScopeRevision, Outcome: "split", Children: []splitting.Child{}}
	for i, criterion := range binding.Value.Scope.Criteria {
		key := []string{"one", "two"}[i]
		proposal.Children = append(proposal.Children, splitting.Child{Key: key, Title: "nested " + key, Requirement: criterion.Requirement, Criteria: []verification.Criterion{{ID: "nested-" + key, Requirement: criterion.Requirement}}, Coverage: []splitting.Coverage{{ParentCriterion: criterion.ID, ChildCriterion: "nested-" + key}}, DependsOn: []string{}})
	}
	return k, ProductSplitProposalRequest{Target: w.Target(), Key: "nested-context", ParentWorkItemID: registration.Value.Definition.ID, RegistrationDigest: registration.Digest, Proposal: proposal}, binding, facts
}

func TestProductNestedSplitContextPreservesLocalScopeDepthAndRootMappingWithoutAdoption(t *testing.T) {
	ctx := context.Background()
	k, r, binding, _ := nestedSplitContextFixture(t)
	before, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := k.DB.QueryRow("SELECT count(*) FROM records").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	view, err := k.InspectProductSplitProposalContext(ctx, r)
	if err != nil || view.Value.Depth != 1 || view.Value.InitialRegistrationDigest != r.RegistrationDigest || view.Value.ParentSplitScope == nil || view.Value.ParentSplitScope.BindingDigest != binding.Digest || view.Value.ParentScopeDigest != binding.Value.ScopeDigest || !reflect.DeepEqual(view.Value.ParentCriterionRoots, map[string]string{"second-ac": "AC", "extra-ac": "extra"}) || !view.Value.CreatedAt.IsZero() {
		t.Fatal(view, err)
	}
	registration, err := k.ReadProductWorkItemRegistration(ctx, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	// Pure derivation has no journal or authority. No nested proposal can yet be
	// persisted, and these intents are not submitted to the creation worker.
	children, err := deriveProductSplitChildIntents(ProductSplitProposal{ID: "read-only-context", Value: view.Value, Digest: view.Digest}, registration)
	if err != nil || len(children) != 2 {
		t.Fatal(children, err)
	}
	for i, child := range children {
		if child.Value.Depth != 2 || !reflect.DeepEqual(child.Value.ParentCriteria, []string{[]string{"AC", "extra"}[i]}) || child.Value.Child.Coverage[0].ParentCriterion != r.Proposal.Children[i].Coverage[0].ParentCriterion {
			t.Fatal("local obligations flattened or root bookkeeping lost", child)
		}
	}
	if _, err := k.PrepareProductSplitProposal(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("read-only context became durable nested admission", err)
	}
	if _, err := k.PrepareProductSplitExecution(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("nested source/send adapters bypassed", err)
	}
	var afterRows int
	if err := k.DB.QueryRow("SELECT count(*) FROM records").Scan(&afterRows); err != nil || afterRows != rows {
		t.Fatal("context/admission failure leaked records", rows, afterRows, err)
	}
	after, err := k.WaveOperationBudget(ctx, r.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("preview spent/reset budget", err)
	}
	wrong := cloneSplitRequest(t, r)
	wrong.Proposal.Children[0].Coverage[0].ParentCriterion = "AC"
	if _, err := k.InspectProductSplitProposalContext(ctx, wrong); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("root criterion stood in for local part", err)
	}
	wrong = cloneSplitRequest(t, r)
	wrong.Proposal.Children = wrong.Proposal.Children[:1]
	if _, err := k.InspectProductSplitProposalContext(ctx, wrong); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("local requirement dropped", err)
	}
}

func TestProductNestedSplitContextRequiresFreshOwnSourceAndStablePolicy(t *testing.T) {
	ctx := context.Background()
	k, r, _, facts := nestedSplitContextFixture(t)
	first, err := k.InspectProductSplitProposalContext(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	originalClock := k.clock()
	clock := originalClock
	k.Now = func() time.Time { return clock }
	for _, mode := range []string{"hold", "closed", "edited", "policy", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			prior, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			changed := facts
			switch mode {
			case "hold":
				changed.Hold = true
			case "closed":
				changed.IssueState = "closed"
			case "edited":
				changed.RequestDigest = strings.Repeat("b", 64)
			case "policy":
				changed.PolicyDigest = strings.Repeat("c", 64)
			case "unavailable":
				changed.Available = false
				changed.IssueState = ""
				changed.RequestDigest = ""
				changed.UpdatedAt = time.Time{}
				changed.Failure = "read_failed"
			}
			update, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, prior.ID, changed)
			if err != nil {
				t.Fatal(err)
			}
			if view, err := k.InspectProductSplitProposalContext(ctx, r); !errors.Is(err, ErrProductSplitHeld) || view.Digest != "" {
				t.Fatal("forbidden own source accepted", mode, view, err)
			}
			clock = clock.Add(time.Second)
			if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, update.Snapshot.ID, facts); err != nil {
				t.Fatal(err)
			}
			restored, err := k.InspectProductSplitProposalContext(ctx, r)
			if err != nil || !reflect.DeepEqual(restored, first) {
				t.Fatal("source recovery rewrote derived scope/identity", restored, err)
			}
		})
	}
	clock = clock.Add(2 * time.Minute)
	if _, err := k.InspectProductSplitProposalContext(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("stale own source accepted", err)
	}
	prior, err := k.ReadProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ObserveProductSplitChildSource(ctx, r.Target, r.ParentWorkItemID, prior.ID, facts); err != nil {
		t.Fatal(err)
	}
	refreshed, err := k.InspectProductSplitProposalContext(ctx, r)
	if err != nil || !reflect.DeepEqual(refreshed, first) {
		t.Fatal("freshness-only renewal changed context", refreshed, err)
	}
	clock = originalClock.Add(-time.Minute)
	if _, err := k.InspectProductSplitProposalContext(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("clock rollback accepted", err)
	}
}

func TestProductNestedSplitContextMissingParentProofCannotFallBackAndRootWireStaysStable(t *testing.T) {
	ctx := context.Background()
	k, r, binding, _ := nestedSplitContextFixture(t)
	root, err := k.ReadProductSplitProposal(ctx, binding.Value.Request.Target, binding.Value.Request.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	rootContext, err := k.InspectProductSplitProposalContext(ctx, root.Value.Request)
	if err != nil {
		t.Fatal(err)
	}
	root.Value.CreatedAt = time.Time{}
	if !reflect.DeepEqual(rootContext.Value, root.Value) || rootContext.Value.ParentSplitScope != nil || len(rootContext.Value.ParentCriterionRoots) != 0 {
		t.Fatal("root context changed", rootContext, root)
	}
	raw, err := json.Marshal(root.Value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "parent_split_scope") || strings.Contains(string(raw), "parent_criterion_roots") {
		t.Fatal("optional nested fields changed root wire")
	}
	for _, kind := range []string{"product_split_registrations", "product_split_child_scopes"} {
		key := binding.Value.Request.ProposalID
		var owner string
		var payload []byte
		if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", kind, key).Scan(&owner, &payload); err != nil {
			t.Fatal(err)
		}
		if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, key); err != nil {
			t.Fatal(err)
		}
		if view, err := k.InspectProductSplitProposalContext(ctx, r); !errors.Is(err, ErrProductSplitHeld) || view.Digest != "" {
			t.Fatal("lost ancestor treated as root", kind, view, err)
		}
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", kind, key, owner, payload); err != nil {
			t.Fatal(err)
		}
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-nested-inspect", Target: r.Target, Action: "pause", Reason: "metadata audit"}); err != nil {
		t.Fatal(err)
	}
	if _, err := k.InspectProductSplitProposalContext(ctx, r); err != nil {
		t.Fatal("pause hid read-only context", err)
	}
	direction, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "nested-old-scope", Target: r.Target, Instruction: "new requirement", NextPolicyRevision: r.Target.PolicyRevision + 1})
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
	r.Target = w.Target()
	r.Proposal.ScopeRevision = w.ScopeRevision
	if _, err := k.InspectProductSplitProposalContext(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("old local obligation crossed direction", err)
	}
}

func TestProductNestedSplitContextHonorsSavedDepthCapAndBoundedInput(t *testing.T) {
	ctx := context.Background()
	k, r, _, _ := nestedSplitContextFixture(t)
	limits, err := k.WaveOperationLimits(ctx, r.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the persisted-limit read boundary. A smaller saved cap must not
	// become the default depth or be inferred as depth zero from the root Wave.
	limits.SplitDepth = 1
	if err := k.Put(ctx, "wave_operation_limits", r.Target.WaveID, r.Target.WaveID, limits); err != nil {
		t.Fatal(err)
	}
	if _, err := k.InspectProductSplitProposalContext(ctx, r); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("saved parent depth was reset or cap ignored", err)
	}
	stored, err := k.WaveOperationLimits(ctx, r.Target.WaveID)
	if err != nil || stored != limits {
		t.Fatal("inspection rewrote saved lifetime limits", stored, err)
	}
	for _, mode := range []string{"key", "size", "target", "digest"} {
		t.Run(mode, func(t *testing.T) {
			wrong := cloneSplitRequest(t, r)
			switch mode {
			case "key":
				wrong.Key = strings.Repeat("k", 257)
			case "size":
				wrong.Proposal.Children[0].Requirement = strings.Repeat("x", 1<<20)
			case "target":
				wrong.Target.ScopeRevision = 0
			case "digest":
				wrong.RegistrationDigest = "bad"
			}
			if view, err := k.InspectProductSplitProposalContext(ctx, wrong); !errors.Is(err, ErrProductSplitHeld) || view.Digest != "" {
				t.Fatal("unbounded/invalid read-only request accepted", mode, view, err)
			}
		})
	}
}
