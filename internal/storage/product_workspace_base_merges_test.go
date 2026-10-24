package storage

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// The DB fixture supplies trusted physical facts. Real Git merge behavior is
// separately exercised by workspace.MergePinnedBase integration tests.
func existingBaseWorkspaceFixture(t *testing.T) (*KernelStore, ProductBasePlan, ProductWorkspace, workspace.RepositoryPin, workspace.FrozenCheckout) {
	t.Helper()
	ctx := context.Background()
	k, input, observation := productBasePlanFixture(t)
	task := input.Value.Preparation.Task
	candidate := runner.Candidate{ID: "old-candidate", SHA: strings.Repeat("d", 40), BaseSHA: observation.Head.InitialSHA, ScopeRevision: task.ScopeRevision, PolicyRevision: task.PolicyRevision}
	product := ProductTarget{Target: task.Target(), Repository: "owner/repo", BaseBranch: "master", WorkItem: domain.WorkItem{ID: "work", RepositoryID: 17, ScopeRevision: task.ScopeRevision, PolicyRevision: task.PolicyRevision, Version: 1, Phase: domain.Reviewing, Candidate: candidate.ID, Branch: "harness/old"}, Candidate: candidate}
	original, err := k.SetProductTarget(ctx, "", product)
	if err != nil {
		t.Fatal(err)
	}
	proposal := storagePlanFixture()
	spec := &proposal.Phases[0].Tasks[0]
	spec.ID, spec.Kind, spec.WorkItemIDs = "new-base-fix", "fix", []string{"work"}
	proposal.Phases[0].ID, proposal.Phases[1].ID = "base-repair", "base-review"
	proposal.Phases[0].Completion.RequiredTaskIDs = []string{spec.ID}
	proposal.Phases[1].Tasks[0].ID = "new-base-review"
	proposal.Phases[1].Completion.RequiredTaskIDs = []string{"new-base-review"}
	proposal.Phases[1].Tasks[0].DependsOn[0].TaskID = spec.ID
	admitted, err := k.AdoptPlan(ctx, task.Target(), "fresh-fix-for-existing-base", proposal)
	if err != nil {
		t.Fatal(err)
	}
	task, err = k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	product.Target = task.Target()
	if _, err := k.SetProductTarget(ctx, original.Digest, product); err != nil {
		t.Fatal(err)
	}
	input, err = k.ReadProductBaseInput(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	observation.ObservedAt = k.clock()
	plan, err := k.PrepareProductBasePlan(ctx, task.ID, input.Digest, observation)
	if err != nil {
		t.Fatal(err)
	}
	request := ProductWorkspaceRequest{TaskID: task.ID, RepositoryID: 17, Repository: input.Value.Policy.Value.Repository, BaseSHA: candidate.SHA, BasePlanID: plan.ID, BasePlanDigest: plan.Digest}
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil || saved.Value.ComparisonBaseSHA != observation.Head.HeadSHA {
		t.Fatal(saved, err)
	}
	pin, err := workspace.PinRepository(ctx, request.Repository)
	if err != nil {
		t.Fatal(err)
	}
	source := workspace.FrozenCheckout{PinnedCheckout: workspace.PinnedCheckout{Repository: request.Repository, CWD: saved.Value.CWD, Branch: saved.Value.Branch, GitDir: filepath.Join(pin.CommonDir, "worktrees", saved.Value.SessionID), CommonDir: pin.CommonDir, BaseSHA: candidate.BaseSHA}, SHA: candidate.SHA}
	return k, plan, saved, pin, source
}

func existingBaseProof(t *testing.T, k *KernelStore, plan ProductBasePlan, saved ProductWorkspace, pin workspace.RepositoryPin, source workspace.FrozenCheckout, outcome string) ProductWorkspaceProof {
	t.Helper()
	proof := workspaceProofFixture(t, k, saved)
	proof.Fetch = &workspace.CommitFetch{Source: pin, Remote: plan.Value.Head.Remote, SHA: plan.Value.Head.HeadSHA, Available: true}
	merge := &workspace.BaseMerge{Source: source, BaseSHA: plan.Value.Head.HeadSHA, Outcome: outcome, HeadSHA: source.SHA}
	if outcome == "merged" {
		merge.Attempted, merge.HeadSHA = true, strings.Repeat("c", 40)
	}
	if outcome == "conflict" {
		merge.Attempted, merge.Conflicts = true, []string{"shared.go"}
		proof = ProductWorkspaceProof{ProofID: "conflicting-merge-stopped", Outcome: "failed", Reason: "ordinary merge left a verified unresolved index", Fetch: proof.Fetch}
	}
	proof.BaseMerge = merge
	if outcome != "conflict" {
		proof.HeadSHA = merge.HeadSHA
	}
	return proof
}

func TestExistingCandidateBaseWorkspaceBindsMergeAndNewComparisonBase(t *testing.T) {
	for _, outcome := range []string{"merged", "already_current", "conflict"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			k, plan, saved, pin, source := existingBaseWorkspaceFixture(t)
			if claim, err := k.ClaimProductWorkspace(ctx, saved.Value.Request.TaskID, saved.Digest); err != nil || !claim.Start {
				t.Fatal(claim, err)
			}
			if outcome != "already_current" {
				if err := k.MarkProductWorkspaceBaseMergeMaySend(ctx, saved.Value.Request.TaskID, saved.Digest, source); err != nil {
					t.Fatal(err)
				}
				if err := k.MarkProductWorkspaceBaseMergeMaySend(ctx, saved.Value.Request.TaskID, saved.Digest, source); err == nil {
					t.Fatal("ordinary merge sent twice")
				}
			}
			proof := existingBaseProof(t, k, plan, saved, pin, source, outcome)
			finished, err := k.FinishProductWorkspace(ctx, saved.Value.Request.TaskID, proof)
			if err != nil || finished.Proof == nil || !reflect.DeepEqual(finished.Proof.BaseMerge, proof.BaseMerge) {
				t.Fatal(finished, err)
			}
			if (outcome == "conflict") != (finished.State == "failed") || (outcome != "conflict") != (finished.State == "ready") {
				t.Fatal(finished.State)
			}
			reservation, err := k.OperationReservation(ctx, saved.ID)
			if err != nil || reservation.State != "released" {
				t.Fatal(reservation, err)
			}
			if outcome != "conflict" {
				if current, err := k.CurrentProductBasePlan(ctx, saved.Value.Request.TaskID); err != nil || current.ID != plan.ID {
					t.Fatal("materialization changed original base input", current, err)
				}
			}
			root := k.Root
			k.Close()
			k, err = OpenExistingKernel(filepath.Join(root, "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			replayed, err := k.ProductWorkspace(ctx, saved.Value.Request.TaskID)
			if err != nil || !reflect.DeepEqual(replayed, finished) {
				t.Fatal("restart changed merge observation", replayed, err)
			}
			if claim, err := k.ClaimProductWorkspace(ctx, saved.Value.Request.TaskID, saved.Digest); err != nil || claim.Start {
				t.Fatal("restart repeated Git", claim, err)
			}
		})
	}
}

func TestExistingCandidateBaseMergeFenceRejectsStaleOwners(t *testing.T) {
	for _, mode := range []string{"unclaimed", "source", "lease", "stale", "pause", "head", "candidate"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, plan, saved, _, source := existingBaseWorkspaceFixture(t)
			if mode != "unclaimed" {
				if _, err := k.ClaimProductWorkspace(ctx, saved.Value.Request.TaskID, saved.Digest); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "source":
				source.BaseSHA = source.SHA // A divergent candidate is not its comparison base.
			case "lease":
				if _, err := k.DB.Exec("UPDATE kernel_resource_leases SET generation=generation+1 WHERE run_id=?", saved.ID); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock().Add(31 * time.Second)
				k.Now = func() time.Time { return now }
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-existing-base", Target: saved.Value.Target, Action: "pause", Reason: "fixture"}); err != nil {
					t.Fatal(err)
				}
			case "head":
				observation := plan.Admission
				observation.Head.HeadSHA, observation.ObservedAt = strings.Repeat("e", 40), k.clock()
				if _, err := k.PrepareProductBasePlan(ctx, saved.Value.Request.TaskID, plan.Value.Input.Digest, observation); err != nil {
					t.Fatal(err)
				}
			case "candidate":
				old := *plan.Value.Input.Value.Preparation.Product
				next := old.Value
				next.Candidate.ID, next.Candidate.SHA, next.WorkItem.Candidate = "other-candidate", strings.Repeat("b", 40), "other-candidate"
				next.WorkItem.Version++
				if _, err := k.SetProductTarget(ctx, old.Digest, next); err != nil {
					t.Fatal(err)
				}
			}
			if err := k.MarkProductWorkspaceBaseMergeMaySend(ctx, saved.Value.Request.TaskID, saved.Digest, source); err == nil {
				t.Fatal("held owner consumed merge fence", mode)
			}
			var sends int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_workspace_base_merge_send' AND id=?", saved.ID).Scan(&sends); err != nil || sends != 0 {
				t.Fatal(sends, err)
			}
		})
	}
}

func TestExistingCandidateBaseMergeLostProofNeverReconstructs(t *testing.T) {
	for _, kind := range []string{"product_workspace", "product_workspace_base_merge_send", "product_base_workspace_claim", "product_base_workspace_terminal"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, plan, saved, pin, source := existingBaseWorkspaceFixture(t)
			if _, err := k.ClaimProductWorkspace(ctx, saved.Value.Request.TaskID, saved.Digest); err != nil {
				t.Fatal(err)
			}
			if err := k.MarkProductWorkspaceBaseMergeMaySend(ctx, saved.Value.Request.TaskID, saved.Digest, source); err != nil {
				t.Fatal(err)
			}
			if _, err := k.FinishProductWorkspace(ctx, saved.Value.Request.TaskID, existingBaseProof(t, k, plan, saved, pin, source, "merged")); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, saved.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ProductWorkspace(ctx, saved.Value.Request.TaskID); err == nil {
				t.Fatal("missing merge history authenticated", kind)
			}
			if _, err := k.ReserveProductWorkspace(ctx, saved.Value.Request); err == nil {
				t.Fatal("missing history reconstructed", kind)
			}
			if err := k.MarkProductWorkspaceBaseMergeMaySend(ctx, saved.Value.Request.TaskID, saved.Digest, source); err == nil {
				t.Fatal("missing history resent merge", kind)
			}
		})
	}
}

func TestExistingCandidateBaseMergeProofRejectsMismatchedFacts(t *testing.T) {
	for _, mode := range []string{"no_fence", "old_source", "new_base", "head", "conflict_as_ready", "unsafe_paths", "not_started"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, plan, saved, pin, source := existingBaseWorkspaceFixture(t)
			if _, err := k.ClaimProductWorkspace(ctx, saved.Value.Request.TaskID, saved.Digest); err != nil {
				t.Fatal(err)
			}
			if mode != "no_fence" {
				if err := k.MarkProductWorkspaceBaseMergeMaySend(ctx, saved.Value.Request.TaskID, saved.Digest, source); err != nil {
					t.Fatal(err)
				}
			}
			proof := existingBaseProof(t, k, plan, saved, pin, source, "merged")
			switch mode {
			case "old_source":
				proof.BaseMerge.Source.SHA = strings.Repeat("b", 40)
			case "new_base":
				proof.BaseMerge.BaseSHA = source.BaseSHA
			case "head":
				proof.HeadSHA = source.SHA
			case "conflict_as_ready":
				proof.BaseMerge.Outcome, proof.BaseMerge.HeadSHA, proof.HeadSHA = "conflict", source.SHA, source.SHA
				proof.BaseMerge.Conflicts = []string{"shared.go"}
			case "unsafe_paths":
				proof = existingBaseProof(t, k, plan, saved, pin, source, "conflict")
				proof.BaseMerge.Conflicts = []string{"../private"}
			case "not_started":
				proof = ProductWorkspaceProof{ProofID: "erased-merge", Outcome: "not_started", Reason: "cannot erase consumed merge", BaseMerge: proof.BaseMerge}
			}
			if _, err := k.FinishProductWorkspace(ctx, saved.Value.Request.TaskID, proof); err == nil {
				t.Fatal("mismatched merge proof accepted", mode)
			}
			reservation, err := k.OperationReservation(ctx, saved.ID)
			if err != nil || reservation.State != "held" {
				t.Fatal("invalid proof released Git", reservation, err)
			}
		})
	}
}

func TestExistingCandidateBaseMergePartialFailureRollsBackAndRetainsObservation(t *testing.T) {
	ctx := context.Background()
	k, plan, saved, pin, source := existingBaseWorkspaceFixture(t)
	if _, err := k.ClaimProductWorkspace(ctx, saved.Value.Request.TaskID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkProductWorkspaceBaseMergeMaySend(ctx, saved.Value.Request.TaskID, saved.Digest, source); err != nil {
		t.Fatal(err)
	}
	// A launching/unknown owner has no terminal proof and retains capacity.
	if claim, err := k.ClaimProductWorkspace(ctx, saved.Value.Request.TaskID, saved.Digest); err != nil || claim.Start || claim.Workspace.BaseMergeAt == nil {
		t.Fatal(claim, err)
	}
	proof := existingBaseProof(t, k, plan, saved, pin, source, "conflict")
	proof.BaseMerge.Outcome, proof.BaseMerge.HeadSHA, proof.BaseMerge.Conflicts = "", "", nil
	proof.BaseMerge.Failure = &workspace.BaseMergeFailure{HeadSHA: source.SHA, MergeHeadSHA: plan.Value.Head.HeadSHA, Unresolved: []string{}}
	if _, err := k.DB.Exec("CREATE TRIGGER reject_base_merge_terminal BEFORE INSERT ON records WHEN NEW.kind='product_base_workspace_terminal' BEGIN SELECT RAISE(FAIL,'merge terminal rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductWorkspace(ctx, saved.Value.Request.TaskID, proof); err == nil || !strings.Contains(err.Error(), "merge terminal rollback") {
		t.Fatal("terminal rollback not reached", err)
	}
	reservation, err := k.OperationReservation(ctx, saved.ID)
	if err != nil || reservation.State != "held" {
		t.Fatal("rollback released Git", reservation, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER reject_base_merge_terminal"); err != nil {
		t.Fatal(err)
	}
	finished, err := k.FinishProductWorkspace(ctx, saved.Value.Request.TaskID, proof)
	if err != nil || finished.State != "failed" || !reflect.DeepEqual(finished.Proof.BaseMerge.Failure, proof.BaseMerge.Failure) {
		t.Fatal("partial change lost", finished, err)
	}
}

func TestExistingCandidateMergedBaseModelAdmissionKeepsOriginalAndNewBase(t *testing.T) {
	for _, moved := range []bool{false, true} {
		t.Run(map[bool]string{false: "original_candidate", true: "candidate_moved"}[moved], func(t *testing.T) {
			ctx := context.Background()
			k, plan, saved, pin, source := existingBaseWorkspaceFixture(t)
			if _, err := k.ClaimProductWorkspace(ctx, saved.Value.Request.TaskID, saved.Digest); err != nil {
				t.Fatal(err)
			}
			if err := k.MarkProductWorkspaceBaseMergeMaySend(ctx, saved.Value.Request.TaskID, saved.Digest, source); err != nil {
				t.Fatal(err)
			}
			ready, err := k.FinishProductWorkspace(ctx, saved.Value.Request.TaskID, existingBaseProof(t, k, plan, saved, pin, source, "merged"))
			if err != nil {
				t.Fatal(err)
			}
			b, err := k.SessionBinding(ctx, ready.Value.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			config := ProductModelRuntime{Model: b.Model, ToolProfile: "product_v2", RuntimeDigest: strings.Repeat("a", 64), Repositories: []ProductModelRepository{{Name: "owner/repo", BaseBranch: "master", InitialSHA: plan.Value.Head.InitialSHA, Policy: plan.Value.Input.Value.Policy.Value, Runner: ProductRunnerRuntime{Root: filepath.Join(k.Root, "merge-model-runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}}}}
			if err := k.RegisterIsolatedRuntime(config.RuntimeDigest, config.Model, config.ToolProfile); err != nil {
				t.Fatal(err)
			}
			revoke, err := k.BindProductModelRuntime(config)
			if err != nil {
				t.Fatal(err)
			}
			defer revoke()
			w, err := k.Wave(ctx, saved.Value.Target.WaveID)
			if err != nil {
				t.Fatal(err)
			}
			if set := productModelReady(t, k, w); len(set.Tasks) != 1 {
				t.Fatal("merged checkout did not become ready", set)
			}
			if moved {
				old := *plan.Value.Input.Value.Preparation.Product
				next := old.Value
				next.Candidate.ID, next.Candidate.SHA, next.WorkItem.Candidate = "newer-candidate", strings.Repeat("b", 40), "newer-candidate"
				next.WorkItem.Version++
				if _, err := k.SetProductTarget(ctx, old.Digest, next); err != nil {
					t.Fatal(err)
				}
				if set := productModelReady(t, k, w); len(set.Tasks) != 0 {
					t.Fatal("old merge admitted replacement candidate", set)
				}
				return
			}
			b = prepareWakeSession(t, k, b.ID)
			snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
			if err != nil {
				t.Fatal(err)
			}
			run := contract.SessionRun{ID: "merged-base-first-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
			if err := k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
				t.Fatal(err)
			}
			var audit ProductModelAdmission
			if err := k.Record(ctx, "product_model_admission", run.ID, &audit); err != nil || audit.BaseSelection == nil || audit.BaseSelection.Plan.ID != plan.ID || audit.WorkspaceDigest != ready.Digest {
				t.Fatal(audit, err)
			}
			if ready.Value.Request.BaseSHA != source.SHA || productWorkspaceComparisonBase(ready.Value) != plan.Value.Head.HeadSHA || ready.Proof.HeadSHA == source.SHA {
				t.Fatal("original candidate, merge result and comparison base conflated", ready)
			}
		})
	}
}
