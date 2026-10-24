package storage

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productModelFixture(t *testing.T) (*KernelStore, domain.Wave, domain.Task, ProductModelRuntime) {
	t.Helper()
	ctx := context.Background()
	k, w, task, request := productWorkspaceFixture(t)
	policy := verificationPolicyFixture(w.ID, request.Repository, w.Policy.Revision)
	if _, err := k.RegisterProductVerificationPolicy(ctx, w.Target(), policy); err != nil {
		t.Fatal(err)
	}
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	if _, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved)); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	task, err = k.Task(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	config := ProductModelRuntime{Model: b.Model, ToolProfile: "product_v2", RuntimeDigest: strings.Repeat("a", 64), Repositories: []ProductModelRepository{{Name: "owner/repo", BaseBranch: "master", InitialSHA: request.BaseSHA, Policy: policy, Runner: ProductRunnerRuntime{Root: filepath.Join(k.Root, "runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}}}}
	if err := k.RegisterIsolatedRuntime(config.RuntimeDigest, config.Model, config.ToolProfile); err != nil {
		t.Fatal(err)
	}
	return k, w, task, config
}
func productModelReady(t *testing.T, k *KernelStore, w domain.Wave) ReadySet {
	t.Helper()
	set, err := k.ReadyTasks(context.Background(), ReadyRequest{Target: w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	return set
}
func TestProductModelReadinessRequiresLiveBoundRuntimeAndPreservesRevocation(t *testing.T) {
	k, w, task, config := productModelFixture(t)
	if set := productModelReady(t, k, w); len(set.Tasks) != 0 {
		t.Fatal("metadata admitted product model", set)
	}
	release, err := k.BindProductModelRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	if set := productModelReady(t, k, w); len(set.Tasks) != 1 || set.Tasks[0].Task.ID != task.ID {
		t.Fatal("bound prepared task not ready", set)
	}
	// Mutating the caller's config cannot change the frozen host grant.
	config.Repositories[0].Policy.Verification.Profiles["unit"] = config.Repositories[0].Policy.Verification.Profiles["missing"]
	if set := productModelReady(t, k, w); len(set.Tasks) != 1 {
		t.Fatal("caller mutated live runtime", set)
	}
	if _, err := k.BindProductModelRuntime(config); err == nil {
		t.Fatal("second owner replaced runtime")
	}
	release()
	if set := productModelReady(t, k, w); len(set.Tasks) != 0 {
		t.Fatal("revoked runtime still ready", set)
	}
	fresh := config
	// Use the original repo binding rather than the unrelated fresh fixture.
	fresh.Repositories[0] = ProductModelRepository{Name: "owner/repo", BaseBranch: "master", InitialSHA: config.Repositories[0].InitialSHA, Policy: verificationPolicyFixture(w.ID, config.Repositories[0].Policy.Repository, w.Policy.Revision), Runner: config.Repositories[0].Runner}
	fresh.Model = config.Model
	release2, err := k.BindProductModelRuntime(fresh)
	if err != nil {
		t.Fatal(err)
	}
	defer release2()
	release() // An old repeated revoke must not revoke the newer owner.
	if set := productModelReady(t, k, w); len(set.Tasks) != 1 {
		t.Fatal("old owner revoked new runtime", set)
	}
	root := k.Root
	k.Close()
	reopened, err := OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.BindProductModelRuntime(fresh); err == nil {
		t.Fatal("reopened DB accepted an unregistered runtime")
	}
	if set := productModelReady(t, reopened, w); len(set.Tasks) != 0 {
		t.Fatal("restart restored ambient readiness", set)
	}
}
func TestProductModelReadinessRejectsChangedHostAndMissingPreparationProof(t *testing.T) {
	for _, mode := range []string{"model", "other_runtime", "repository", "path", "base", "policy", "workspace_head", "workspace_history", "policy_history", "release", "pause", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, config := productModelFixture(t)
			switch mode {
			case "model":
				config.Model = "other-model"
			case "other_runtime":
				config.RuntimeDigest = strings.Repeat("b", 64)
			case "repository":
				config.Repositories[0].Name = "other/repository"
			case "path":
				config.Repositories[0].Policy.Repository = t.TempDir()
			case "base":
				config.Repositories[0].InitialSHA = strings.Repeat("b", 40)
			case "policy":
				p := config.Repositories[0].Policy.Verification.Profiles["unit"]
				p.Argv = []string{"other-program"}
				config.Repositories[0].Policy.Verification.Profiles["unit"] = p
			}
			release, err := k.BindProductModelRuntime(config)
			if mode == "model" || mode == "other_runtime" {
				if err == nil {
					release()
					t.Fatal("another runtime model was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			switch mode {
			case "workspace_head", "workspace_history", "policy_history":
				kind, id := map[string]string{"workspace_head": "product_workspace", "workspace_history": "product_workspace_history", "policy_history": "product_verification_policy_history"}[mode], productWorkspaceID(task.ID)
				if mode == "policy_history" {
					id = productVerificationPolicyID(w.ID, w.Policy.Revision, 17)
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, id); err != nil {
					t.Fatal(err)
				}
			case "release":
				if _, err := k.DB.Exec("UPDATE kernel_dispatch_intents SET state='held' WHERE run_id=?", productWorkspaceID(task.ID)); err != nil {
					t.Fatal(err)
				}
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-product-readiness", Target: w.Target(), Action: "pause", Reason: "hold"}); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				release()
			}
			set, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
			if err == nil && len(set.Tasks) > 0 {
				t.Fatal("invalid readiness admitted model", mode, set)
			}
			var admissions int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_model_admission'").Scan(&admissions); err != nil || admissions != 0 {
				t.Fatal("readiness observation started a model", admissions, err)
			}
		})
	}
}
func TestProductModelReadinessConfigurationRefusesUnknownProfilesAndDuplicateOwners(t *testing.T) {
	k, _, _, config := productModelFixture(t)
	for _, mode := range []string{"profile", "digest", "repositories", "duplicate"} {
		bad := config
		switch mode {
		case "profile":
			bad.ToolProfile = "product_v1"
		case "digest":
			bad.RuntimeDigest = "ready"
		case "repositories":
			bad.Repositories = nil
		case "duplicate":
			bad.Repositories = append(append([]ProductModelRepository{}, config.Repositories...), config.Repositories[0])
		}
		if _, err := k.BindProductModelRuntime(bad); err == nil {
			t.Fatal("bad runtime admitted", mode)
		}
	}
	release, err := k.BindProductModelRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := k.BindProductModelRuntime(config); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("runtime owner overwritten", err)
	}
}

func TestProductModelReadinessRechecksAtTurnAndRecordsAtomicAdmission(t *testing.T) {
	for _, mode := range []string{"start", "revoked", "changed_model", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, config := productModelFixture(t)
			release, err := k.BindProductModelRuntime(config)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if set := productModelReady(t, k, w); len(set.Tasks) != 1 {
				t.Fatal(set)
			}
			b := prepareWakeSession(t, k, task.SessionID)
			snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
			if err != nil {
				t.Fatal(err)
			}
			run := contract.SessionRun{ID: "bound-product-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
			switch mode {
			case "revoked":
				release()
			case "changed_model":
				release()
				config.Model = "different-runtime-model"
				next, err := k.BindProductModelRuntime(config)
				if err == nil {
					next()
					t.Fatal("runtime replacement did not require reconstruction")
				}
			case "rollback":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_product_model_admission BEFORE INSERT ON records WHEN NEW.kind='product_model_admission' BEGIN SELECT RAISE(FAIL,'product admission rollback'); END"); err != nil {
					t.Fatal(err)
				}
			}
			err = k.ReserveSessionRun(ctx, b, snapshot, run)
			if (err == nil) != (mode == "start") {
				t.Fatal("turn admission did not recheck current runtime", mode, err)
			}
			current, readErr := k.Task(ctx, task.ID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_model_admission'").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if mode != "start" {
				if count != 0 || current.Attempts != 0 || current.State != contract.TaskReady {
					t.Fatal("failed admission partially reserved a turn", count, current)
				}
				return
			}
			if count != 1 || current.Attempts != 1 || current.State != contract.TaskRunning {
				t.Fatal("product turn not reserved exactly once", count, current)
			}
			var admission ProductModelAdmission
			if err := k.Record(ctx, "product_model_admission", run.ID, &admission); err != nil {
				t.Fatal(err)
			}
			if admission.RunID != run.ID || admission.Epoch == "" || admission.HostConfigDigest == "" || admission.RuntimeDigest != config.RuntimeDigest || admission.Model != b.Model || admission.SnapshotID != snapshot.ID || admission.SnapshotDigest != snapshot.Digest || admission.Target != w.Target() || admission.WorkspaceDigest == "" {
				t.Fatal("turn lacks exact host/input provenance", admission)
			}
		})
	}
}

func TestProductModelReadinessPinsVerificationAndReviewSubjects(t *testing.T) {
	for _, kind := range []string{"verification", "review", "review_without_verification"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			var k *KernelStore
			var w domain.Wave
			var task domain.Task
			var request ProductWorkspaceRequest
			var policy ProductVerificationPolicySnapshot
			if kind == "verification" {
				k, w, task, request, _, policy = verificationWorkspaceFixture(t)
			} else if kind == "review" {
				var verification ProductVerificationPlanSnapshot
				var runtime ProductRunnerRuntime
				k, w, verification, runtime, task = productQualityPreparationFixture(t)
				completeProductRunnerFixture(t, k, verification, runtime, "unit", "passed")
				subject, err := k.BindProductCandidateSubject(ctx, task.ID)
				if err != nil {
					t.Fatal(err)
				}
				sealed, err := k.ProductSeal(ctx, verification.Value.TaskID, verification.Value.RunID)
				if err != nil {
					t.Fatal(err)
				}
				request = sealed.Value.Workspace.Value.Request
				request.TaskID, request.BaseSHA = task.ID, subject.Value.Candidate.SHA
				policy, err = k.ReadProductVerificationPolicy(ctx, w.Target(), request.RepositoryID)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				k, w, task, request, _ = reviewWorkspaceFixture(t)
				var err error
				policy, err = k.RegisterProductVerificationPolicy(ctx, w.Target(), verificationPolicyFixture(w.ID, request.Repository, w.Policy.Revision))
				if err != nil {
					t.Fatal(err)
				}
			}
			saved, err := k.ReserveProductWorkspace(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if claim, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil || !claim.Start {
				t.Fatal(claim, err)
			}
			if _, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved)); err != nil {
				t.Fatal(err)
			}
			b, err := k.SessionBinding(ctx, task.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			config := ProductModelRuntime{Model: b.Model, ToolProfile: "product_v2", RuntimeDigest: strings.Repeat("a", 64), Repositories: []ProductModelRepository{{Name: "owner/repo", BaseBranch: "master", InitialSHA: request.BaseSHA, Policy: policy.Value, Runner: ProductRunnerRuntime{Root: filepath.Join(k.Root, "runner"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}}}}
			if err := k.RegisterIsolatedRuntime(config.RuntimeDigest, config.Model, config.ToolProfile); err != nil {
				t.Fatal(err)
			}
			release, err := k.BindProductModelRuntime(config)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			ready := productModelReady(t, k, w)
			if kind == "review_without_verification" {
				if len(ready.Tasks) != 0 {
					t.Fatal("independent review bypassed machine verification", ready)
				}
				return
			}
			if len(ready.Tasks) != 1 || ready.Tasks[0].Task.ID != task.ID {
				t.Fatal("fixed independent subject not ready", kind, ready)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_candidate_subject_history' AND id=?", productSubjectID(task.ID)); err != nil {
				t.Fatal(err)
			}
			if ready := productModelReady(t, k, w); len(ready.Tasks) != 0 {
				t.Fatal("lost subject still dispatched", kind, ready)
			}
		})
	}
}
