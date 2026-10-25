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

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func scopedExecutionFixture(t *testing.T) (*KernelStore, domain.Wave, ProductSplitRegistrations, []domain.Task) {
	t.Helper()
	ctx := context.Background()
	k, w, split := splitScopeInputFixture(t)
	var tasks []contract.TaskSpec
	var ids []string
	for i, registration := range split.Value.Registrations {
		id := []string{"first", "second"}[i]
		tasks = append(tasks, contract.TaskSpec{ID: id, Kind: "work", Instruction: "implement the complete local obligation", Criteria: append([]string{}, registration.Value.Definition.Criteria...), DependsOn: []contract.TaskDependency{}, InputArtifactIDs: []string{}, WorkItemIDs: []string{registration.Value.Definition.ID}})
		ids = append(ids, id)
	}
	plan := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "children", Tasks: tasks, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: ids}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "scoped-execution", plan)
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	var out []domain.Task
	for _, id := range admitted.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, task)
	}
	now := k.clock().Add(time.Minute)
	k.Now = func() time.Time { return now }
	for _, binding := range split.Value.Children {
		if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), binding.Value.Child.Value.WorkItemID, "", splitSourceFacts(binding)); err != nil {
			t.Fatal(err)
		}
	}
	return k, w, split, out
}

func scopedGuard(t *testing.T, k *KernelStore, w domain.Wave, workID string) (bool, error) {
	t.Helper()
	tx, err := k.kernelTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	return k.productWorkItemExecutionHeld(context.Background(), tx, w, workID)
}

func TestProductScopedExecutionPreparesOnlyFreshLeafLocalScopeWithoutQualityOrDependencyBypass(t *testing.T) {
	ctx := context.Background()
	k, w, split, tasks := scopedExecutionFixture(t)
	id := split.Value.Children[0].Value.Child.Value.WorkItemID
	if held, err := scopedGuard(t, k, w, id); err != nil || held {
		t.Fatal("fresh canonical leaf still held", held, err)
	}
	if held, err := productWorkItemExecutionHeld(ctx, k.DB, w, id); err != nil || !held {
		t.Fatal("metadata predicate granted execution", held, err)
	}
	prepared, err := k.ReadProductTaskPreparation(ctx, tasks[0].ID)
	if err != nil || prepared.SplitScope == nil || !reflect.DeepEqual(prepared.SplitScope.Scope, split.Value.Children[0].Value.Scope) {
		t.Fatal("production preparation lost local scope", prepared, err)
	}
	if _, err := k.ReadProductTaskPreparation(ctx, tasks[1].ID); !errors.Is(err, ErrProductMergeDependencyHeld) {
		t.Fatal("omitted Task DAG bypassed first leaf merge dependency", err)
	}
	if _, err := k.CheckProductQuality(ctx, w.Target(), id); err == nil {
		t.Fatal("source readiness became quality", err)
	}
	if held, err := scopedGuard(t, k, w, split.Value.Request.ParentWorkItemID); err != nil || !held {
		t.Fatal("delegated parent became executable", held, err)
	}
	var runs, operations int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations").Scan(&operations); err != nil || runs != 0 || operations != 0 {
		t.Fatal("preparation fabricated external work", runs, operations, err)
	}
}

func TestProductScopedExecutionLiveSourceAndControlsNeverComeFromProofMemo(t *testing.T) {
	ctx := context.Background()
	base, w, split, _ := scopedExecutionFixture(t)
	for _, mode := range []string{"stale", "future", "own_hold", "own_closed", "own_unavailable", "peer_hold", "pause", "cancel", "missing_registration", "missing_source", "missing_scope"} {
		t.Run(mode, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			binding := split.Value.Children[0]
			id := binding.Value.Child.Value.WorkItemID
			if held, err := scopedGuard(t, k, w, id); err != nil || held {
				t.Fatal("positive control failed", held, err)
			}
			switch mode {
			case "stale", "future":
				now := k.clock()
				if mode == "stale" {
					now = now.Add(productExecutionSourceMaxAge + time.Nanosecond)
				} else {
					now = now.Add(-time.Nanosecond)
				}
				k.Now = func() time.Time { return now }
			case "own_hold", "own_closed", "own_unavailable", "peer_hold":
				observedID := id
				if mode == "peer_hold" {
					binding = split.Value.Children[1]
					observedID = binding.Value.Child.Value.WorkItemID
				}
				prior, err := k.ReadProductSplitChildSource(ctx, w.Target(), observedID)
				if err != nil {
					t.Fatal(err)
				}
				facts := splitSourceFacts(binding)
				if mode == "own_hold" || mode == "peer_hold" {
					facts.Hold = true
				}
				if mode == "own_closed" {
					facts.IssueState = "closed"
				}
				if mode == "own_unavailable" {
					facts.Available = false
					facts.IssueState = ""
					facts.RequestDigest = ""
					facts.UpdatedAt = time.Time{}
					facts.Failure = "read_failed"
				}
				if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), observedID, prior.ID, facts); err != nil {
					t.Fatal(err)
				}
			case "pause", "cancel":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "guard-control", Target: w.Target(), Action: mode, Reason: "operator stop"}); err != nil {
					t.Fatal(err)
				}
			case "missing_registration":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration' AND id=?", id); err != nil {
					t.Fatal(err)
				}
			case "missing_source":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_child_source' AND work_item_id=?", w.ID); err != nil {
					t.Fatal(err)
				}
			case "missing_scope":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_child_scopes' AND id=?", split.Value.Request.ProposalID); err != nil {
					t.Fatal(err)
				}
			}
			if held, err := scopedGuard(t, k, w, id); err != nil || held != (mode != "peer_hold") {
				t.Fatal("live prohibition or missing proof was ignored", held, err)
			}
		})
	}
}

func TestProductScopedExecutionCandidateRefreshPreservesScopeAndParentControls(t *testing.T) {
	ctx := context.Background()
	base, w, split, _ := scopedExecutionFixture(t)
	for _, mode := range []string{"own_ready", "own_pause", "own_cancel", "parent_split", "parent_merged", "parent_pause", "parent_cancel", "parent_merge_hold"} {
		t.Run(mode, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			workID := split.Value.Children[0].Value.Child.Value.WorkItemID
			projectedID, phase := workID, domain.Verifying
			if strings.HasPrefix(mode, "parent_") {
				projectedID, phase = split.Value.Request.ParentWorkItemID, domain.Split
			}
			if mode == "parent_merged" {
				phase = domain.Merged
			}
			if mode == "own_cancel" || mode == "parent_cancel" {
				phase = domain.Canceled
			}
			p := ProductTarget{Target: w.Target(), Repository: "owner/repo", BaseBranch: "master", WorkItem: domain.WorkItem{ID: projectedID, RepositoryID: 17, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, Phase: phase, Version: 1, Candidate: "projection", Branch: "harness/projection", Paused: mode == "own_pause" || mode == "parent_pause", MergeHold: mode == "parent_merge_hold"}, Candidate: runner.Candidate{ID: "projection", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision}}
			if _, err := k.SetProductTarget(ctx, "", p); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductSplitChildScope(ctx, w.Target(), workID); err != nil {
				t.Fatal("candidate/control refresh erased local scope", err)
			}
			expectHeld := mode == "own_pause" || mode == "own_cancel" || mode == "parent_pause" || mode == "parent_cancel"
			if held, err := scopedGuard(t, k, w, workID); err != nil || held != expectHeld {
				t.Fatal("wrong descendant control or terminal ancestry decision", held, expectHeld, err)
			}
		})
	}
}

func TestProductScopedExecutionRequiresEachCanonicalAncestorSource(t *testing.T) {
	ctx := context.Background()
	base, request, proof := nestedRegistrationScopeFixture(t)
	if _, err := base.RegisterProductSplitChildren(ctx, request); err != nil {
		t.Fatal(err)
	}
	w, err := base.Wave(ctx, request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	binding := proof.Value.Scopes.Value.Children[0]
	id := binding.Value.Child.Value.WorkItemID
	now := base.clock().Add(time.Minute)
	base.Now = func() time.Time { return now }
	lineage, err := base.ReadProductSplitSourceLineage(ctx, w.Target(), id)
	if err != nil || len(lineage) != 2 {
		t.Fatal("expected a nested leaf", lineage, err)
	}
	for _, workID := range lineage {
		scope, err := base.ReadProductSplitChildScope(ctx, w.Target(), workID)
		if err != nil {
			t.Fatal(err)
		}
		prior, readErr := base.ReadProductSplitChildSource(ctx, w.Target(), workID)
		expected := ""
		if readErr == nil {
			expected = prior.ID
		}
		if _, err := base.ObserveProductSplitChildSource(ctx, w.Target(), workID, expected, splitSourceFacts(scope)); err != nil {
			t.Fatal(err)
		}
	}
	if held, err := scopedGuard(t, base, w, id); err != nil || held {
		t.Fatal("fresh nested leaf denied", held, err)
	}
	for _, mode := range []string{"hold", "closed", "stale", "missing"} {
		t.Run(mode, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			ancestorID := lineage[1]
			if mode == "missing" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_split_child_source' AND id=?", ancestorID); err != nil {
					t.Fatal(err)
				}
			} else if mode == "stale" {
				later := k.clock().Add(productExecutionSourceMaxAge + time.Nanosecond)
				k.Now = func() time.Time { return later }
				prior, err := k.ReadProductSplitChildSource(ctx, w.Target(), id)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), id, prior.ID, splitSourceFacts(binding)); err != nil {
					t.Fatal(err)
				}
			} else {
				scope, err := k.ReadProductSplitChildScope(ctx, w.Target(), ancestorID)
				if err != nil {
					t.Fatal(err)
				}
				prior, err := k.ReadProductSplitChildSource(ctx, w.Target(), ancestorID)
				if err != nil {
					t.Fatal(err)
				}
				facts := splitSourceFacts(scope)
				facts.Hold = mode == "hold"
				if mode == "closed" {
					facts.IssueState = "closed"
				}
				if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), ancestorID, prior.ID, facts); err != nil {
					t.Fatal(err)
				}
			}
			if held, err := scopedGuard(t, k, w, id); err != nil || !held {
				t.Fatal("own fresh source hid ancestor prohibition", held, err)
			}
		})
	}
}

func TestProductScopedExecutionPermitExpiresAtConsumerEndAndCannotCrossTransactions(t *testing.T) {
	k, w, split, tasks := scopedExecutionFixture(t)
	ctx := context.Background()
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var permit productExecutionPermit
	held, err := k.productWorkItemExecutionPermitTx(ctx, tx, w, split.Value.Children[0].Value.Child.Value.WorkItemID, &permit)
	if err != nil || held || !permit.current(k, tx) {
		tx.Rollback()
		t.Fatal("positive permit failed", held, err)
	}
	now := k.clock()
	k.Now = func() time.Time { return now.Add(productExecutionSourceMaxAge + time.Nanosecond) }
	if permit.current(k, tx) {
		tx.Rollback()
		t.Fatal("remaining proofs could extend source age")
	}
	tx.Rollback()
	k.Now = func() time.Time { return now }
	other, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if permit.current(k, other) {
		other.Rollback()
		t.Fatal("permit escaped its transaction")
	}
	other.Rollback()
	calls := 0
	k.Now = func() time.Time { calls++; return now.Add(time.Duration(calls-1) * 4 * time.Second) }
	if _, err := k.ReadProductTaskPreparation(ctx, tasks[0].ID); !errors.Is(err, ErrProductWorkspaceHeld) {
		t.Fatal("production preparation used an expired source", calls, err)
	}
}

func githubScopedExecutionFixture(t *testing.T, configure ...func(*splitting.Proposal)) (*KernelStore, domain.Wave, ProductSplitRegistrations, ProductSourceFacts) {
	t.Helper()
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	now := k.clock()
	k.Now = func() time.Time { return now }
	request.Policy.Stage, request.Policy.Product = "P", true
	request.Source, request.SourceID = "github", "r17-i42"
	root := ProductWorkItemDefinition{ID: request.SourceID, Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}
	w, err := k.CreateProductWave(ctx, ProductWaveRequest{Wave: request, SourcePolicyDigest: strings.Repeat("a", 64), WorkItems: []ProductWorkItemDefinition{root}})
	if err != nil {
		t.Fatal(err)
	}
	facts := ProductSourceFacts{RepositoryID: 17, IssueNumber: 42, PolicyDigest: strings.Repeat("a", 64), Available: true, IssueState: "open", Ready: true, RequestDigest: ProductSourceRequestDigest(w.OriginalRequest), UpdatedAt: now}
	if _, err := k.ObserveProductSourceAt(ctx, w.Target(), "", facts, now); err != nil {
		t.Fatal(err)
	}
	registration, err := k.ReadProductWorkItemRegistration(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: root.ID, ScopeRevision: w.ScopeRevision, Outcome: "split", Children: []splitting.Child{}}
	for _, key := range []string{"left", "right"} {
		proposal.Children = append(proposal.Children, splitting.Child{Key: key, Title: key + " part", Requirement: "implement the " + key + " part", Criteria: []verification.Criterion{{ID: key, Requirement: key + " local behavior"}}, Coverage: []splitting.Coverage{{ParentCriterion: "AC", ChildCriterion: key, PartID: key, PartCount: 2, Basis: key + " portion of the original request"}}, DependsOn: []string{}})
	}
	for _, apply := range configure {
		apply(&proposal)
	}
	r := ProductSplitProposalRequest{Target: w.Target(), Key: "github-scoped", ParentWorkItemID: root.ID, RegistrationDigest: registration.Digest, Proposal: proposal}
	prepared, err := k.PrepareProductSplitExecution(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	var childRequest ProductSplitChildPlanRequest
	for i, ref := range prepared.Value.Creations {
		job, err := k.ReadProductSplitCreate(ctx, ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		childRequest = job.Value.Request
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
		if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "github-scoped-stop"); err != nil {
			t.Fatal(err)
		}
		if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: int64(900 + i), Number: 99 + i, Title: job.Value.Child.Value.Child.Title, Body: job.Value.Child.Value.Body, State: "open", ObservedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := k.BindProductSplitChildScopes(ctx, childRequest); err != nil {
		t.Fatal(err)
	}
	split, err := k.RegisterProductSplitChildren(ctx, childRequest)
	if err != nil {
		t.Fatal(err)
	}
	for _, binding := range split.Value.Children {
		if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), binding.Value.Child.Value.WorkItemID, "", splitSourceFacts(binding)); err != nil {
			t.Fatal(err)
		}
	}
	return k, w, split, facts
}

func TestProductScopedExecutionGithubRootRequiresActualCurrentGET(t *testing.T) {
	ctx := context.Background()
	base, w, split, facts := githubScopedExecutionFixture(t)
	id := split.Value.Children[0].Value.Child.Value.WorkItemID
	if held, err := scopedGuard(t, base, w, id); err != nil || held {
		t.Fatal("fresh Github path failed", held, err)
	}
	for _, mode := range []string{"missing_read", "stale_read", "legacy_replay", "root_hold", "root_closed", "root_unavailable"} {
		t.Run(mode, func(t *testing.T) {
			k := cloneNestedScopeStore(t, base)
			snapshot, err := k.ReadProductSourceObservation(ctx, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing_read":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_source_read'"); err != nil {
					t.Fatal(err)
				}
			case "stale_read", "legacy_replay":
				now := k.clock().Add(productExecutionSourceMaxAge + time.Nanosecond)
				k.Now = func() time.Time { return now }
				for _, binding := range split.Value.Children {
					prior, err := k.ReadProductSplitChildSource(ctx, w.Target(), binding.Value.Child.Value.WorkItemID)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := k.ObserveProductSplitChildSource(ctx, w.Target(), binding.Value.Child.Value.WorkItemID, prior.ID, splitSourceFacts(binding)); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "legacy_replay" {
					if _, err := k.ObserveProductSource(ctx, w.Target(), "", facts); err != nil {
						t.Fatal(err)
					}
				}
			default:
				changed := facts
				changed.Hold = mode == "root_hold"
				if mode == "root_closed" {
					changed.IssueState = "closed"
				}
				readAt := k.clock()
				if mode == "root_unavailable" {
					changed.Available = false
					changed.IssueState = ""
					changed.Ready = false
					changed.RequestDigest = ""
					changed.UpdatedAt = time.Time{}
					changed.Failure = "read_failed"
					readAt = time.Time{}
				}
				if _, err := k.ObserveProductSourceAt(ctx, w.Target(), snapshot.ID, changed, readAt); err != nil {
					t.Fatal(err)
				}
			}
			if held, err := scopedGuard(t, k, w, id); err != nil || !held {
				t.Fatal("fresh child hid Root prohibition or stale GET", held, err)
			}
		})
	}
}
