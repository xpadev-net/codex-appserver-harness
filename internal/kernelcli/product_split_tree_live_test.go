package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

// This opt-in exercises real production Issue subprocesses at the adopted
// fan-out limit. Only the remote API is controlled; no model or credential is
// needed. A single branch is recursively split, rather than claiming an
// unbounded 8^3 tree fits the lifetime budget.
func TestLiveProductBoundedSplitTree(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_SPLIT_TREE") != "1" {
		t.Skip("explicit controlled split tree exercise only")
	}
	cli, root, output := os.Getenv("HARNESS_LIVE_HARNESS_BINARY"), os.Getenv("HARNESS_LIVE_PRODUCT_ROOT"), os.Getenv("HARNESS_LIVE_PRODUCT_EVIDENCE")
	for _, path := range []string{cli, root, output} {
		if !filepath.IsAbs(path) {
			t.Fatal("absolute production binary, NEW root and evidence required")
		}
	}
	for _, path := range []string{root, output} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatal("controlled tree must not overwrite a previous observation")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if err := Run(ctx, []string{"init", "--root", root, "--model", "controlled-no-model"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	k, main, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := k.Close(); err != nil {
			t.Error(err)
		}
	})
	started := time.Now()
	evidence := map[string]any{"stage": "controlled production split tree", "local_execution": "not_accepted", "full_p_acceptance": "not_accepted", "model_runs": 0, "real_github_operations": 0, "cloud_native_approval": "unknown_not_observed", "plan_origin": "host_authored_controlled_fixture", "credential_values_recorded": false, "private_paths_recorded": false}
	t.Cleanup(func() {
		var live, runs, maxRecord, unstoppedCreations, creationStops int
		clean := context.Background()
		for _, entry := range []struct {
			query string
			out   *int
		}{
			{"SELECT count(*) FROM records WHERE kind='managed_process' AND json_extract(payload,'$.state')<>'stopped'", &live},
			{"SELECT count(*) FROM records WHERE kind='agent_run'", &runs},
			{"SELECT coalesce(max(length(payload)),0) FROM records", &maxRecord},
			{"SELECT count(*) FROM records WHERE kind='product_split_create' AND json_type(payload,'$.value.http') IS NOT NULL AND json_type(payload,'$.value.stop') IS NULL", &unstoppedCreations},
			{"SELECT count(*) FROM records WHERE kind='product_split_create' AND json_type(payload,'$.value.stop') IS NOT NULL", &creationStops},
		} {
			if err := k.DB.QueryRowContext(clean, entry.query).Scan(entry.out); err != nil {
				*entry.out = -1
				t.Error(err)
			}
		}
		if live != 0 || runs != 0 || unstoppedCreations != 0 {
			t.Error("controlled tree left live processes or started a model")
		}
		evidence["all_managed_processes_stopped"], evidence["model_runs"], evidence["max_record_bytes"] = live == 0 && unstoppedCreations == 0, runs, maxRecord
		evidence["stopped_creation_proofs"] = creationStops
		if !t.Failed() && creationStops != 24 {
			t.Error("complete tree lacks all 24 creation stopping proofs")
		}
		evidence["elapsed_ms"], evidence["observed_at"] = time.Since(started).Milliseconds(), time.Now().UTC()
		if t.Failed() {
			evidence["local_execution"] = "not_accepted"
		}
		raw, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil {
			t.Error(err)
			return
		}
		f, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Error(err)
			return
		}
		_, writeErr := f.Write(append(raw, '\n'))
		if err := errors.Join(writeErr, f.Close()); err != nil {
			t.Error(err)
		}
	})
	limits := storage.DefaultWaveOperationLimits()
	if limits.SplitChildren != 8 || limits.SplitDepth != 2 {
		t.Fatal("default split contract changed")
	}
	limits.SplitDepth = 3 // Explicit trusted configuration before Wave admission.
	w, err := k.CreateProductWave(ctx, storage.ProductWaveRequest{Wave: storage.WaveRequest{OrchestratorSessionID: main.SessionID, Source: "direct", SourceID: "bounded-split-tree", OriginalRequest: "Deliver distinct outputs 000 through 007, 01 through 07, and 1 through 7. Prefix 0 contains 00 and 01 through 07; prefix 00 contains 000 through 007. Every required output must be preserved.", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "Deliver every required output in the original request, without duplication or omission."}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: main.Limits}, OperationLimits: &limits}, WorkItems: []storage.ProductWorkItemDefinition{{ID: "work", Repository: "fixture/local-product", RepositoryID: 17, Criteria: []string{"AC"}}}})
	if err != nil {
		t.Fatal(err)
	}
	directory := prepareLiveProductGitHub(t, k, strings.Repeat("a", 40), 24)
	policy := product.IssueSourcePolicy{Repository: "fixture/local-product", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}
	gate := product.KernelSourceGate{Store: k, Policies: map[int64]product.IssueSourcePolicy{17: policy}, API: product.GitHubResourceAPI}
	lineage := []storage.ProductSplitChildScopeBinding{}
	refresh := func() {
		t.Helper()
		ids := make([]string, 0, len(lineage))
		for _, binding := range lineage {
			ids = append(ids, binding.Value.Child.Value.WorkItemID)
		}
		if len(ids) == 0 {
			return
		}
		observer := product.SplitChildSourceObserver{Store: k, API: product.GitHubResourceAPI(storage.ProductTarget{Repository: "fixture/local-product", WorkItem: domain.WorkItem{RepositoryID: 17}}), Policy: policy, Attempts: 1}
		updates, acknowledged, err := observer.ObserveMany(ctx, w.Target(), ids)
		if err != nil || !acknowledged || len(updates) != len(ids) {
			t.Fatal("actual ancestor source read", err)
		}
		for _, update := range updates {
			if update.Snapshot.Value.Status != "ready" {
				t.Fatal("ancestor source not ready")
			}
		}
	}
	parentID, criterion, prefix := "work", "AC", ""
	layers := []map[string]any{}
	for depth := 1; depth <= 3; depth++ {
		layerStart := time.Now()
		refresh()
		registration, err := k.ReadProductWorkItemRegistration(ctx, parentID)
		if err != nil {
			t.Fatal(err)
		}
		request := storage.ProductSplitProposalRequest{Target: w.Target(), Key: fmt.Sprintf("tree-depth-%d", depth), ParentWorkItemID: parentID, RegistrationDigest: registration.Digest, Proposal: boundedTreeProposal(parentID, criterion, prefix, w.ScopeRevision)}
		var prepared storage.ProductSplitPreparation
		if depth == 1 {
			tooMany := request
			tooMany.Key = "over-default-child-limit"
			tooMany.Proposal.Children = append(append([]splitting.Child{}, request.Proposal.Children...), request.Proposal.Children[0])
			tooMany.Proposal.Children[8].Key = "ninth"
			tooMany.Proposal.Children[8].Criteria = []verification.Criterion{{ID: "local-ninth", Requirement: "additional distinct local partition"}}
			tooMany.Proposal.Children[8].Coverage = []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: "local-ninth", PartID: "ninth", PartCount: 9, Basis: "additional distinct local partition"}}
			for i := range tooMany.Proposal.Children {
				tooMany.Proposal.Children[i].Coverage = append([]splitting.Coverage{}, tooMany.Proposal.Children[i].Coverage...)
				tooMany.Proposal.Children[i].Coverage[0].PartCount = 9
			}
			rootScope := verification.Scope{Revision: w.ScopeRevision, OriginalRequest: w.OriginalRequest, Criteria: []verification.Criterion{{ID: "AC", Requirement: w.Criteria[0].Requirement}}, Included: []string{"AC"}, Remaining: []string{}, PreviouslyMerged: []string{}}
			if err := tooMany.Proposal.Validate(parentID, rootScope, 0, 3, 9); err != nil {
				t.Fatal("oversize fixture was invalid independently of the child limit", err)
			}
			if _, err := k.PrepareProductSplitExecution(ctx, tooMany); !errors.Is(err, storage.ErrProductSplitHeld) {
				t.Fatal("oversized proposal was not held", err)
			}
			prepared, err = k.PrepareProductSplitExecution(ctx, request)
			if err != nil {
				t.Fatal("root preparation", err)
			}
		} else {
			proof, err := k.PrepareProductSplitParentScope(ctx, w.Target(), parentID)
			if err != nil {
				t.Fatal("parent scope", err)
			}
			// Parent-scope preparation is a separate adoption and may consume
			// a substantial part of a source observation's bounded lifetime.
			refresh()
			prepared, err = k.PrepareProductNestedSplitExecution(ctx, request, storage.ProductSplitParentScopeReference{ID: proof.ID, Digest: proof.Digest})
			if err != nil {
				t.Fatal("nested proposal", err)
			}
		}
		planRequest := storage.ProductSplitChildPlanRequest{Target: w.Target(), ParentWorkItemID: parentID, ProposalID: prepared.Value.ProposalID, ProposalDigest: prepared.Value.ProposalDigest}
		if len(prepared.Value.Creations) != 8 {
			t.Fatal("full default fan-out was lost")
		}
		t.Logf("depth %d: complete preparation in %dms", depth, time.Since(layerStart).Milliseconds())
		for _, creation := range prepared.Value.Creations {
			if depth > 1 {
				// The production transport enforces a 15-second activation
				// window. Refresh the actual ancestor reads immediately before
				// each sequential send so a long fan-out cannot consume it.
				refresh()
			}
			transport := product.SplitHTTPTransport{Store: k, Executable: cli}
			if depth > 1 {
				transport.SourcePolicy = &policy
			}
			stopped, err := transport.Start(ctx, creation.ID)
			if err != nil || stopped.State != "stopped" || stopped.Stop == nil {
				t.Fatal("production creation stopping proof", depth, stopped.State, err)
			}
			observed, _, err := (product.SplitCreationReconciler{Store: k, API: product.GitHubSplitResourceAPI, MaxPages: 32}).Reconcile(ctx, creation.ID)
			if err != nil || observed.State != "observed" {
				t.Fatal("production creation GET reconciliation", depth, err)
			}
			if _, err := transport.Start(ctx, creation.ID); err == nil {
				t.Fatal("stopped creation was sent again")
			}
		}
		// Deeper bindings consume the registered ancestor source lineage after
		// all eight sequential Issue creations and GET reconciliations.
		refresh()
		if _, err := k.BindProductSplitChildScopes(ctx, planRequest); err != nil {
			t.Fatal(err)
		}
		// As in the production registration worker, a new adoption gets its
		// own actual source read after the previous SQL stage has completed.
		refresh()
		registered, err := k.RegisterProductSplitChildren(ctx, planRequest)
		if err != nil || len(registered.Value.Children) != 8 {
			t.Fatal("complete registration", depth, err)
		}
		var selected storage.ProductSplitChildScopeBinding
		for _, child := range registered.Value.Children {
			if child.Value.Child.Value.Depth != depth || len(child.Value.Scope.Included) != 1 {
				t.Fatal("local scope lost depth or obligation")
			}
			if child.Value.Child.Value.Child.Key == "p0" {
				selected = child
			}
		}
		if selected.ID == "" {
			t.Fatal("next parent missing")
		}
		lineage = append(lineage, selected)
		parentID, criterion, prefix = selected.Value.Child.Value.WorkItemID, selected.Value.Scope.Included[0], prefix+"0"
		posts := liveProductSplitPosts(t, directory)
		if posts != depth*8 {
			t.Fatal("duplicate or missing Issue creation", posts)
		}
		layers = append(layers, map[string]any{"depth": depth, "children": 8, "elapsed_ms": time.Since(layerStart).Milliseconds(), "controlled_issue_posts": posts})
		evidence["layers"] = layers
		t.Logf("depth %d: eight registered children, %d total POSTs", depth, posts)
	}
	refresh()
	deepParent, err := k.PrepareProductSplitParentScope(ctx, w.Target(), parentID)
	if err != nil {
		t.Fatal(err)
	}
	deepRegistration, err := k.ReadProductWorkItemRegistration(ctx, parentID)
	if err != nil {
		t.Fatal(err)
	}
	tooDeep := storage.ProductSplitProposalRequest{Target: w.Target(), Key: "over-configured-depth", ParentWorkItemID: parentID, RegistrationDigest: deepRegistration.Digest, Proposal: boundedTreeProposal(parentID, criterion, prefix, w.ScopeRevision)}
	if _, err := k.PrepareProductNestedSplitProposal(ctx, tooDeep, storage.ProductSplitParentScopeReference{ID: deepParent.ID, Digest: deepParent.Digest}); !errors.Is(err, storage.ErrProductSplitHeld) {
		t.Fatal("fourth layer bypassed configured depth", err)
	}
	if liveProductSplitPosts(t, directory) != 24 {
		t.Fatal("limit probes created another Issue")
	}
	sourceStart := time.Now()
	scan, err := (product.SplitChildSourceObserver{Store: k, API: product.GitHubResourceAPI(storage.ProductTarget{Repository: "fixture/local-product", WorkItem: domain.WorkItem{RepositoryID: 17}}), Policy: policy, Attempts: 32}).Scan(ctx)
	if err != nil || scan.Attempted != 24 || len(scan.Failed) != 0 {
		t.Fatal("complete production child source scan", scan.Attempted, len(scan.Failed), err)
	}
	evidence["source_scan_ms"] = time.Since(sourceStart).Milliseconds()
	sourceStart = time.Now()
	if err := gate.CheckFinalSplitSources(ctx, w.Target()); err != nil {
		t.Fatal("joined final source gate at the original freshness limit", err)
	}
	evidence["final_source_gate_ms"] = time.Since(sourceStart).Milliseconds()
	coverage, err := k.ProductCoverage(ctx, w.Target())
	if err != nil || len(coverage.Value.Splits) != 3 || len(coverage.Value.Merged) != 0 || len(coverage.Value.UnmergedWorkItems) != 22 || len(coverage.Value.Unavailable) != 0 {
		t.Fatal("tree lost leaf obligations or manufactured completion", err)
	}
	if _, err := k.CheckProductQuality(ctx, w.Target(), "work"); err == nil {
		t.Fatal("unmerged tree completed Root quality")
	}
	var taskCount int
	if err := k.DB.QueryRowContext(ctx, "SELECT count(*) FROM kernel_tasks").Scan(&taskCount); err != nil || taskCount != 0 {
		t.Fatal("structural workload started a Task", taskCount, err)
	}
	evidence["configured_depth"], evidence["children_per_split"], evidence["registered_children"] = 3, 8, 24
	evidence["unmerged_leaves"], evidence["controlled_issue_posts"], evidence["coverage_digest"] = 22, liveProductSplitPosts(t, directory), coverage.Digest
	evidence["local_execution"] = "completed_with_registered_unmerged_tree"
}

func boundedTreeProposal(parent, criterion, prefix string, revision int) splitting.Proposal {
	p := splitting.Proposal{SchemaVersion: 1, WorkItemID: parent, ScopeRevision: revision, Outcome: "split", Children: []splitting.Child{}}
	for i := 0; i < 8; i++ {
		label := fmt.Sprintf("%s%d", prefix, i)
		id := "local-" + label
		requirement := "Deliver every originally required output whose name has prefix " + label + "."
		p.Children = append(p.Children, splitting.Child{Key: fmt.Sprintf("p%d", i), Title: "required output partition " + label, Requirement: requirement, Criteria: []verification.Criterion{{ID: id, Requirement: requirement}}, Coverage: []splitting.Coverage{{ParentCriterion: criterion, ChildCriterion: id, PartID: label, PartCount: 8, Basis: "disjoint prefix partition " + label + " of required output names"}}, DependsOn: []string{}})
	}
	return p
}
