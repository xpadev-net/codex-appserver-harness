package product

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
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func conflictFixFixture(t *testing.T) (Executor, domain.Task, storage.ProductWorkspace, storage.ProductWorkspace, string) {
	t.Helper()
	ctx := context.Background()
	e, sourceTask, _, _, base := existingBaseExecutorFixture(t, "conflict")
	worker := NewExecutionWorker(e, time.Second)
	if _, err := worker.task(ctx, sourceTask.ID); !errors.Is(err, storage.ErrProductWorkspaceHeld) {
		t.Fatal("original conflict was not held", err)
	}
	source, err := e.Store.ProductWorkspace(ctx, sourceTask.ID)
	if err != nil || source.State != "failed" {
		t.Fatal(source, err)
	}
	if _, err := e.Store.ReconcileNotifications(ctx, 128); err != nil {
		t.Fatal(err)
	}
	notes, err := e.Store.PendingWakeNotifications(ctx, sourceTask.ParentSessionID, 128)
	if err != nil {
		t.Fatal(err)
	}
	var ref *contract.ProductBaseConflictReference
	for _, note := range notes {
		if note.Reason == "product_base_merge_conflict" && note.Preparation != nil {
			ref = note.Preparation.Conflict
		}
	}
	if ref == nil {
		t.Fatal("parent has no stopped conflict reference", notes)
	}
	w, err := e.Store.Wave(ctx, sourceTask.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	spec := sourceTask.Spec
	spec.ID, spec.Kind, spec.Instruction, spec.ProductBaseConflict = "resolve-stopped-conflict", "fix", "resolve both intended changes against AC; edit files only", ref
	review := contract.TaskSpec{ID: "review-resolved-conflict", Kind: "review", Instruction: "independently review the newly verified candidate", Criteria: []string{"AC"}, DependsOn: []contract.TaskDependency{{TaskID: spec.ID}}, InputArtifactIDs: []string{}, WorkItemIDs: spec.WorkItemIDs}
	proposal := contract.PlanProposal{SchemaVersion: 1, Phases: []contract.PhaseSpec{{ID: "resolve-stopped-conflict", Tasks: []contract.TaskSpec{spec}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{spec.ID}}, FailurePolicy: "drain_and_block"}, {ID: "evaluate-resolved-conflict", Tasks: []contract.TaskSpec{review}, Completion: contract.CompletionCondition{Kind: "all_success", RequiredTaskIDs: []string{review.ID}}, FailurePolicy: "drain_and_block"}}, Templates: []contract.FixReviewTemplate{}}
	admitted, err := e.Store.AdoptPlan(ctx, w.Target(), "repair-observed-base-conflict", proposal)
	if err != nil {
		t.Fatal(err)
	}
	task, err := e.Store.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil || task.ID == sourceTask.ID || task.SessionID == sourceTask.SessionID {
		t.Fatal("conflict repair reused old Task/session", task, err)
	}
	if changed, err := worker.task(ctx, task.ID); err != nil || !changed {
		t.Fatal("new fix did not prepare its own conflict checkout", changed, err)
	}
	ready, err := e.Store.ProductWorkspace(ctx, task.ID)
	if err != nil || ready.State != "ready" || !ready.PreparedBaseConflict() || ready.Value.CWD == source.Value.CWD || ready.Value.ComparisonBaseSHA != base {
		t.Fatal("new conflict checkout not authenticated", ready, err)
	}
	unchanged, err := e.Store.ProductWorkspace(ctx, sourceTask.ID)
	if err != nil || !reflect.DeepEqual(unchanged, source) {
		t.Fatal("repair rewrote historical failed owner", unchanged, err)
	}
	old, err := e.Store.Task(ctx, sourceTask.ID)
	if err != nil || old.State != contract.TaskCanceled || old.Attempts != 0 {
		t.Fatal("old Task was resumed", old, err)
	}
	return e, task, ready, source, base
}

func TestConflictFixUsesFreshCheckoutThenHostSealsOrRetainsFailure(t *testing.T) {
	for _, mode := range []string{"resolved", "unchanged_markers", "new_file_markers", "aborted"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			e, task, ready, source, base := conflictFixFixture(t)
			if mode != "unchanged_markers" && mode != "aborted" {
				if err := os.WriteFile(filepath.Join(ready.Value.CWD, "base.txt"), []byte("original candidate change and concurrent merged predecessor\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "new_file_markers" {
				if err := os.WriteFile(filepath.Join(ready.Value.CWD, "new.txt"), []byte("<<<<<<< HEAD\nours\n=======\ntheirs\n>>>>>>> incoming\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "aborted" {
				// Simulate metadata corruption by an external host actor; actual
				// workers cannot run this write through their read-only Git mount.
				if _, err := workspace.Git(ctx, ready.Value.CWD, "merge", "--abort"); err != nil {
					t.Fatal(err)
				}
			}
			// Normal model completion/stopping is a synthetic fixture; staging,
			// merge commit, sealing and candidate adoption are real Git/SQLite.
			task, run := stoppedProductWriterRunFixture(t, e.Store, ready, task)
			applied, applyErr := e.Apply(ctx, task.ID, run.ID)
			sealed, err := e.Store.ProductSeal(ctx, task.ID, run.ID)
			if err != nil || sealed.Proof == nil || sealed.Proof.Resolution == nil {
				t.Fatal("resolution receipt lost", sealed, err, applyErr)
			}
			p, err := e.Store.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "resolved" {
				if applyErr != nil || applied.State != contract.TaskCompleted || sealed.State != "sealed" || sealed.Proof.Resolution.Frozen == nil || p.Value.Candidate.BaseSHA != base || p.Value.WorkItem.Phase != domain.Verifying {
					t.Fatal("resolved files were not adopted for new verification", applied, sealed, p, applyErr)
				}
				parents, err := workspace.Git(ctx, ready.Value.CWD, "rev-list", "--parents", "-n", "1", p.Value.Candidate.SHA)
				if err != nil || strings.Join(strings.Fields(parents), " ") != p.Value.Candidate.SHA+" "+ready.Value.Request.BaseSHA+" "+base {
					t.Fatal("host resolution lost a required parent", parents, err)
				}
				if replay, err := e.Apply(ctx, task.ID, run.ID); err != nil || !reflect.DeepEqual(replay, applied) {
					t.Fatal("resolution application replay changed facts", replay, err)
				}
			} else if applyErr == nil || sealed.State != "failed" || sealed.Candidate != nil || p.Value.Candidate.SHA != ready.Value.Request.BaseSHA {
				t.Fatal("unresolved or changed state was adopted", sealed, p, applyErr)
			}
			if mode == "new_file_markers" && (!sealed.Proof.Resolution.Attempted || sealed.Proof.Resolution.Failure == nil) {
				t.Fatal("staged failure lost its observation", sealed)
			}
			original, err := e.Store.ProductWorkspace(ctx, source.Value.Request.TaskID)
			if err != nil || !reflect.DeepEqual(original, source) {
				t.Fatal("failed original checkout receipt was rewritten", original, err)
			}
			raw, err := os.ReadFile(filepath.Join(source.Value.CWD, "base.txt"))
			if err != nil || !strings.Contains(string(raw), "<<<<<<< HEAD") {
				t.Fatal("original conflict contents were discarded", string(raw), err)
			}
		})
	}
}

func TestConflictFixReadyAndSameHeadRefreshPreserveMergeHold(t *testing.T) {
	ctx := context.Background()
	e, task, ready, source, _ := conflictFixFixture(t)
	k := e.Store
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	repo := e.Repositories[17]
	policy := repo.Policy
	policy.WaveID, policy.PolicyRevision = task.WaveID, task.PolicyRevision
	config := storage.ProductModelRuntime{Model: b.Model, ToolProfile: "product_v2", RuntimeDigest: strings.Repeat("a", 64), Repositories: []storage.ProductModelRepository{{Name: repo.Name, BaseBranch: repo.BaseBranch, InitialSHA: repo.InitialSHA, Policy: policy, Runner: storage.ProductRunnerRuntime{Root: filepath.Join(k.Root, "conflict-model-runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}}}}
	if err := k.RegisterIsolatedRuntime(config.RuntimeDigest, config.Model, config.ToolProfile); err != nil {
		t.Fatal(err)
	}
	revoke, err := k.BindProductModelRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	defer revoke()
	p, err := k.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	controlled := p.Value
	controlled.WorkItem.MergeHold = true
	if _, err := k.SetProductTarget(ctx, p.Digest, controlled); err != nil {
		t.Fatal(err)
	}
	set, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: task.Target()})
	if err != nil || len(set.Tasks) != 1 || set.Tasks[0].Task.ID != task.ID {
		t.Fatal("merge-only hold blocked authenticated conflict fix", set, err)
	}
	now := ready.FinishedAt.Add(31 * time.Second)
	k.Now = func() time.Time { return now }
	plan, err := e.BasePlanner.Prepare(ctx, task.ID)
	if err != nil || plan.ID != ready.Value.Request.BasePlanID || plan.Digest != ready.Value.Request.BasePlanDigest {
		t.Fatal("merge hold changed immutable same-head plan", plan, err)
	}
	if set, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: task.Target()}); err != nil || len(set.Tasks) != 1 {
		t.Fatal("refreshed held-merge fix not ready", set, err)
	}
	snapshot, err := k.BuildSessionInput(ctx, task.SessionID, []contract.Target{task.Target()})
	if err != nil || !strings.Contains(string(snapshot.Input), "Git metadata is read-only") {
		t.Fatal("fix instructions did not preserve the host staging boundary", err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_base_workspace_terminal' AND id=?", source.ID); err != nil {
		t.Fatal(err)
	}
	if set, err := k.ReadyTasks(ctx, storage.ReadyRequest{Target: task.Target()}); err != nil || len(set.Tasks) != 0 {
		t.Fatal("lost conflict source still admitted a model", set, err)
	}
}

func TestConflictResolutionPermissionRejectsChangedControlLeaseAndOrigin(t *testing.T) {
	for _, mode := range []string{"valid", "unclaimed", "pause", "lease", "candidate", "source_lost"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			e, task, ready, source, _ := conflictFixFixture(t)
			task, run := stoppedProductWriterRunFixture(t, e.Store, ready, task)
			saved, err := e.Store.ReserveProductSeal(ctx, task.ID, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode != "unclaimed" {
				if claim, err := e.Store.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
					t.Fatal(claim, err)
				}
			}
			switch mode {
			case "pause":
				if _, err := e.Store.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause-resolution", Target: task.Target(), Action: "pause", Reason: "fixture"}); err != nil {
					t.Fatal(err)
				}
			case "lease":
				if _, err := e.Store.DB.Exec("UPDATE kernel_resource_leases SET generation=generation+1 WHERE run_id=?", saved.ID); err != nil {
					t.Fatal(err)
				}
			case "candidate":
				moveConflictCandidate(t, e, task, ready)
			case "source_lost":
				if _, err := e.Store.DB.Exec("DELETE FROM records WHERE kind='product_base_workspace_terminal' AND id=?", source.ID); err != nil {
					t.Fatal(err)
				}
			}
			err = e.Store.CheckProductSealBeforeFreeze(ctx, task.ID, run.ID, saved.Digest)
			if (mode == "valid") != (err == nil) {
				t.Fatal(mode, err)
			}
			if head, err := workspace.Resolve(ctx, ready.Value.CWD, "HEAD"); err != nil || head != ready.Proof.HeadSHA {
				t.Fatal("permission read mutated checkout", head, err)
			}
			reservation, err := e.Store.OperationReservation(ctx, saved.ID)
			if err != nil || reservation.State != "held" {
				t.Fatal("permission read released an owner", reservation, err)
			}
		})
	}
}

func moveConflictCandidate(t *testing.T, e Executor, task domain.Task, ready storage.ProductWorkspace) storage.ProductTargetSnapshot {
	t.Helper()
	p, err := e.Store.ReadProductTarget(context.Background(), task.Target(), ready.Value.WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	next := p.Value
	next.Candidate.ID, next.Candidate.SHA, next.WorkItem.Candidate = "concurrent-candidate", strings.Repeat("e", 40), "concurrent-candidate"
	next.WorkItem.Version++
	moved, err := e.Store.SetProductTarget(context.Background(), p.Digest, next)
	if err != nil {
		t.Fatal(err)
	}
	return moved
}

func TestConflictResolutionAdoptionDoesNotOverwriteChangedCandidate(t *testing.T) {
	ctx := context.Background()
	e, task, ready, _, _ := conflictFixFixture(t)
	if err := os.WriteFile(filepath.Join(ready.Value.CWD, "base.txt"), []byte("resolved both changes\n"), 0600); err != nil {
		t.Fatal(err)
	}
	task, run := stoppedProductWriterRunFixture(t, e.Store, ready, task)
	sealed, err := (CandidateSealer{Store: e.Store}).Seal(ctx, task.ID, run.ID)
	if err != nil || sealed.State != "sealed" {
		t.Fatal(sealed, err)
	}
	moved := moveConflictCandidate(t, e, task, ready)
	if _, err := e.Store.AdoptProductTaskResult(ctx, task.SessionID, run.ID, e.Repositories[17].BaseBranch); err == nil {
		t.Fatal("old resolution overwrote a concurrent candidate")
	}
	current, err := e.Store.ReadProductTarget(ctx, task.Target(), ready.Value.WorkItemID)
	if err != nil || !reflect.DeepEqual(current, moved) {
		t.Fatal(current, err)
	}
}

func TestConflictResolutionStoppingProofBindsAllFixedIdentities(t *testing.T) {
	ctx := context.Background()
	e, task, ready, _, base := conflictFixFixture(t)
	task, run := stoppedProductWriterRunFixture(t, e.Store, ready, task)
	saved, err := e.Store.ReserveProductSeal(ctx, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := e.Store.ClaimProductSeal(ctx, task.ID, run.ID, saved.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	for _, mode := range []string{"missing", "source", "base", "initial_head", "merge_head", "frozen", "not_attempted"} {
		t.Run(mode, func(t *testing.T) {
			pin := workspace.PinnedCheckout{Repository: ready.Value.Request.Repository, CWD: ready.Value.CWD, Branch: ready.Value.Branch, GitDir: ready.Proof.GitDir, CommonDir: ready.Proof.CommonDir, BaseSHA: ready.SealBaseSHA()}
			frozen := workspace.FrozenCheckout{PinnedCheckout: pin, SHA: strings.Repeat("c", 40)}
			r := &workspace.ResolvedBase{Source: pin, BaseSHA: base, InitialHeadSHA: ready.Proof.HeadSHA, MergeHeadSHA: base, Attempted: true, Frozen: &frozen}
			proof := storage.ProductSealProof{ProofID: "mismatched-resolution-stopping", Outcome: "sealed", Reason: "synthetic mismatch must be rejected", Frozen: &frozen, Resolution: r}
			switch mode {
			case "missing":
				proof.Resolution = nil
			case "source":
				r.Source.Branch = "harness/wrong"
			case "base":
				r.BaseSHA = ready.Value.Request.BaseSHA
			case "initial_head":
				r.InitialHeadSHA = base
			case "merge_head":
				r.MergeHeadSHA = ""
			case "frozen":
				other := frozen
				other.SHA = strings.Repeat("d", 40)
				r.Frozen = &other
			case "not_attempted":
				r.Attempted = false
			}
			if _, err := e.Store.FinishProductSeal(ctx, task.ID, run.ID, proof); err == nil {
				t.Fatal("mismatched resolution facts accepted", mode)
			}
			reservation, err := e.Store.OperationReservation(ctx, saved.ID)
			if err != nil || reservation.State != "held" {
				t.Fatal("mismatched stopping proof released Git", reservation, err)
			}
		})
	}
}
