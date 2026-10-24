package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func productBaseWorkspaceFixture(t *testing.T) (*KernelStore, ProductBasePlan, ProductWorkspaceRequest, workspace.RepositoryPin) {
	t.Helper()
	ctx := context.Background()
	k, input, obs := productBasePlanFixture(t)
	plan, err := k.PrepareProductBasePlan(ctx, input.Value.Preparation.Task.ID, input.Digest, obs)
	if err != nil {
		t.Fatal(err)
	}
	request := ProductWorkspaceRequest{TaskID: input.Value.Preparation.Task.ID, RepositoryID: 17, Repository: input.Value.Policy.Value.Repository, BaseSHA: obs.Head.HeadSHA, BasePlanID: plan.ID, BasePlanDigest: plan.Digest}
	pin, err := workspace.PinRepository(ctx, request.Repository)
	if err != nil {
		t.Fatal(err)
	}
	return k, plan, request, pin
}
func completeBaseWorkspace(t *testing.T, k *KernelStore, plan ProductBasePlan, request ProductWorkspaceRequest, pin workspace.RepositoryPin) ProductWorkspace {
	t.Helper()
	ctx := context.Background()
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.ClaimProductWorkspace(ctx, request.TaskID, saved.Digest)
	if err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	if err := k.MarkProductWorkspaceFetchMaySend(ctx, request.TaskID, saved.Digest, pin); err != nil {
		t.Fatal(err)
	}
	proof := workspaceProofFixture(t, k, saved)
	// Physical facts are trusted fixtures here; real FetchPinnedCommit/checkout
	// execution is exercised by the host adapter integration, not this DB test.
	proof.Fetch = &workspace.CommitFetch{Source: pin, Remote: plan.Value.Head.Remote, SHA: request.BaseSHA, Attempted: true, Available: true}
	ready, err := k.FinishProductWorkspace(ctx, request.TaskID, proof)
	if err != nil {
		t.Fatal(err)
	}
	return ready
}
func TestProductBaseWorkspaceClaimsSendsStopsAndReplaysExactlyOnce(t *testing.T) {
	ctx := context.Background()
	k, plan, request, pin := productBaseWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := k.MarkProductWorkspaceFetchMaySend(ctx, request.TaskID, saved.Digest, pin); err == nil {
		t.Fatal("unclaimed workspace sent")
	}
	claim, err := k.ClaimProductWorkspace(ctx, request.TaskID, saved.Digest)
	if err != nil || !claim.Start || claim.Workspace.BaseClaim == nil {
		t.Fatal(claim, err)
	}
	if next, err := k.ClaimProductWorkspace(ctx, request.TaskID, saved.Digest); err != nil || next.Start {
		t.Fatal("unknown claim restarted", next, err)
	}
	if err := k.MarkProductWorkspaceFetchMaySend(ctx, request.TaskID, saved.Digest, pin); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkProductWorkspaceFetchMaySend(ctx, request.TaskID, saved.Digest, pin); err == nil {
		t.Fatal("fetch fence reused")
	}
	proof := workspaceProofFixture(t, k, saved)
	proof.Fetch = &workspace.CommitFetch{Source: pin, Remote: plan.Value.Head.Remote, SHA: request.BaseSHA, Attempted: true, Available: true}
	ready, err := k.FinishProductWorkspace(ctx, request.TaskID, proof)
	if err != nil || ready.State != "ready" || ready.BaseFetchAt == nil {
		t.Fatal(ready, err)
	}
	replay, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, ready) {
		t.Fatal("historical replay changed proof", replay, err)
	}
	if repeated, err := k.FinishProductWorkspace(ctx, request.TaskID, proof); err != nil || !reflect.DeepEqual(repeated, ready) {
		t.Fatal(repeated, err)
	}
	current, err := k.CurrentProductBasePlan(ctx, request.TaskID)
	if err != nil || current.ID != plan.ID {
		t.Fatal("input materialization invalidated base", current, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	read, err := k.ProductWorkspace(ctx, request.TaskID)
	if err != nil || !reflect.DeepEqual(read, ready) {
		t.Fatal("reopen changed stopped proof", read, err)
	}
	if claim, err := k.ClaimProductWorkspace(ctx, request.TaskID, ready.Digest); err != nil || claim.Start {
		t.Fatal("reopen restarted fetch", claim, err)
	}
}
func TestProductBaseWorkspaceMissingJournalNeverRegeneratesOrResends(t *testing.T) {
	for _, kind := range []string{"product_workspace", "product_base_workspace_admission", "product_base_workspace_claim", "product_workspace_fetch_send", "product_base_workspace_terminal"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, plan, request, pin := productBaseWorkspaceFixture(t)
			ready := completeBaseWorkspace(t, k, plan, request, pin)
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, ready.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.ProductWorkspace(ctx, request.TaskID); err == nil {
				t.Fatal("lost journal still authenticated", kind)
			}
			if _, err := k.ReserveProductWorkspace(ctx, request); err == nil {
				t.Fatal("lost workspace regenerated", kind)
			}
			if claim, err := k.ClaimProductWorkspace(ctx, request.TaskID, ready.Digest); err == nil || claim.Start {
				t.Fatal("lost journal restarted owner", claim, err)
			}
			if err := k.MarkProductWorkspaceFetchMaySend(ctx, request.TaskID, ready.Digest, pin); err == nil {
				t.Fatal("lost journal resent fetch")
			}
		})
	}
}
func TestProductBaseWorkspaceLatePausePreventsSendButRetainsStoppedHistory(t *testing.T) {
	ctx := context.Background()
	k, _, request, pin := productBaseWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := k.ClaimProductWorkspace(ctx, request.TaskID, saved.Digest); err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-before-base-fetch", Target: saved.Value.Target, Action: "pause", Reason: "fixture"}); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkProductWorkspaceFetchMaySend(ctx, request.TaskID, saved.Digest, pin); err == nil {
		t.Fatal("paused owner sent fetch")
	}
	done, err := k.FinishProductWorkspace(ctx, request.TaskID, ProductWorkspaceProof{ProofID: "stopped-base-owner", Outcome: "failed", Reason: "fixture stopped before fetch"})
	if err != nil || done.State != "failed" || done.BaseFetchAt != nil {
		t.Fatal(done, err)
	}
	r, err := k.OperationReservation(ctx, saved.ID)
	if err != nil || r.State != "released" || r.Proof == nil {
		t.Fatal("known stopping did not release lease", r, err)
	}
}
func TestLegacyWorkspaceEncodingHasNoNewZeroValueFields(t *testing.T) {
	k, _, _, request := productWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"base_claim", "base_fetch_at", "base_plan_id", "base_plan_digest", "fetch"} {
		if strings.Contains(string(raw), "\""+field+"\"") {
			t.Fatal("old record checksum would change", field, string(raw))
		}
	}
}
func TestProductBaseWorkspaceAdmitsActualPreparedThreadAndRetainsResumeProvenance(t *testing.T) {
	ctx := context.Background()
	k, plan, request, pin := productBaseWorkspaceFixture(t)
	ready := completeBaseWorkspace(t, k, plan, request, pin)
	task, err := k.Task(ctx, request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, task.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	config := ProductModelRuntime{Model: b.Model, ToolProfile: "product_v2", RuntimeDigest: strings.Repeat("a", 64), Repositories: []ProductModelRepository{{Name: plan.Value.Head.Repository, BaseBranch: plan.Value.Head.BaseBranch, InitialSHA: plan.Value.Head.InitialSHA, Policy: plan.Value.Input.Value.Policy.Value, Runner: ProductRunnerRuntime{Root: filepath.Join(k.Root, "base-model-runners"), LauncherContract: "argv_passthrough_v1", Launcher: []string{"/bin/true"}}}}}
	if err := k.RegisterIsolatedRuntime(config.RuntimeDigest, config.Model, config.ToolProfile); err != nil {
		t.Fatal(err)
	}
	revoke, err := k.BindProductModelRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	defer revoke()
	if set := productModelReady(t, k, w); len(set.Tasks) != 1 {
		t.Fatal("new fixed base required obsolete static initial SHA", set)
	}
	b = prepareWakeSession(t, k, b.ID)
	now := k.clock().Add(31 * time.Second)
	k.Now = func() time.Time { return now }
	snapshot, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "base-bound-first-run", ThreadID: b.ThreadID, State: "starting", Started: k.clock(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err := k.ReserveSessionRun(ctx, b, snapshot, run); err == nil {
		t.Fatal("stale selection started first turn", err)
	}
	unchanged, err := k.Task(ctx, task.ID)
	if err != nil || unchanged.Attempts != 0 {
		t.Fatal("held first run consumed an attempt", unchanged, err)
	}
	input, err := k.ReadProductBaseInput(ctx, task.ID)
	if err != nil {
		t.Fatal("prepared initial thread could not refresh read-only source", err)
	}
	obs := ProductBaseObservation{Head: plan.Value.Head, ObservedAt: now}
	refreshed, err := k.PrepareProductBasePlan(ctx, task.ID, input.Digest, obs)
	if err != nil || refreshed.ID != plan.ID {
		t.Fatal("materialization/thread changed base plan", refreshed, err)
	}
	if err := k.ReserveSessionRun(ctx, b, snapshot, run); err != nil {
		t.Fatal("fresh prepared thread did not start", err)
	}
	var admission ProductModelAdmission
	if err := k.Record(ctx, "product_model_admission", run.ID, &admission); err != nil || admission.BaseSelection == nil || admission.BaseSelection.Plan.ID != plan.ID || admission.WorkspaceDigest != ready.Digest {
		t.Fatal("run lost fixed base provenance", admission, err)
	}
	completeWakeWaiting(t, k, b, run)
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyTaskResult(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(31 * time.Second)
	if _, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: w.OrchestratorSessionID, Target: task.Target(), Key: "base-resume-input", Recipient: task.ID, Body: "continue the same incomplete implementation"}); err != nil {
		t.Fatal(err)
	}
	notes, err := k.PendingWakeNotifications(ctx, b.ID, 16)
	if err != nil || len(notes) != 1 {
		t.Fatal(notes, err)
	}
	wake, err := k.ReserveWake(ctx, WakeRequest{SessionID: b.ID, Key: "base-resume", NotificationIDs: []string{notes[0].ID}})
	if err != nil {
		t.Fatal(err)
	}
	b = prepareWakeSession(t, k, b.ID)
	if err := k.ReserveSessionRun(ctx, b, wake.Snapshot, wakeStartingRun(k, b, wake)); err != nil {
		t.Fatal("normal incomplete Task resume invalidated original base", err)
	}
}

func TestProductBaseWorkspacePreflightChecksFreshSourceAndActualLease(t *testing.T) {
	for _, mode := range []string{"source_pin", "lease", "stale", "pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, request, pin := productBaseWorkspaceFixture(t)
			saved, err := k.ReserveProductWorkspace(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if claim, err := k.ClaimProductWorkspace(ctx, request.TaskID, saved.Digest); err != nil || !claim.Start {
				t.Fatal(claim, err)
			}
			switch mode {
			case "source_pin":
				pin.CommonDir = filepath.Join(k.Root, "other-common")
			case "lease":
				if _, err := k.DB.Exec("UPDATE kernel_resource_leases SET generation=generation+1 WHERE run_id=? AND state='held'", saved.ID); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock().Add(31 * time.Second)
				k.Now = func() time.Time { return now }
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "hold-base-preflight", Target: saved.Value.Target, Action: "pause", Reason: "fixture"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := k.CheckProductWorkspaceBaseFetch(ctx, request.TaskID, saved.Digest, pin); err == nil {
				t.Fatal("held source/lease allowed physical profile", mode)
			}
			if err := k.MarkProductWorkspaceFetchMaySend(ctx, request.TaskID, saved.Digest, pin); err == nil {
				t.Fatal("held source/lease consumed send", mode)
			}
			var sends int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_workspace_fetch_send' AND id=?", saved.ID).Scan(&sends); err != nil || sends != 0 {
				t.Fatal(sends, err)
			}
		})
	}
}
