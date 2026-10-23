package storage

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func productWorkspaceFixture(t *testing.T) (*KernelStore, domain.Wave, domain.Task, ProductWorkspaceRequest) {
	t.Helper()
	ctx := context.Background()
	k, source := waveStoreFixture(t)
	source.Policy.Stage, source.Policy.Product = "P", true
	w, err := k.CreateProductWave(ctx, ProductWaveRequest{Wave: source, WorkItems: []ProductWorkItemDefinition{{ID: "work", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC"}}}})
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	plan.Phases[0].Tasks[0].WorkItemIDs = []string{"work"}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "product-workspace-plan", plan)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	repo := operationRepo(t)
	base, err := workspace.Resolve(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return k, w, task, ProductWorkspaceRequest{TaskID: task.ID, RepositoryID: 17, Repository: repo, BaseSHA: base}
}
func workspaceProofFixture(t *testing.T, k *KernelStore, saved ProductWorkspace) ProductWorkspaceProof {
	t.Helper()
	reservation, err := k.OperationReservation(context.Background(), saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	common := ""
	for _, lease := range reservation.Resources {
		if strings.HasPrefix(lease.Resource, "repository_git:") {
			common = strings.TrimPrefix(lease.Resource, "repository_git:")
		}
	}
	return ProductWorkspaceProof{ProofID: "trusted-stopped-proof", Outcome: "prepared", Reason: "test host verified local Git stopped", CWD: saved.Value.CWD, Branch: saved.Value.Branch, HeadSHA: saved.Value.Request.BaseSHA, GitDir: filepath.Join(common, "worktrees", saved.Value.SessionID), CommonDir: common, Clean: true}
}
func TestProductWorkspaceReservationClaimAndProofAreAtomicAndOneUse(t *testing.T) {
	ctx := context.Background()
	k, w, task, request := productWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil || saved.State != "prepared" {
		t.Fatal(saved, err)
	}
	replay, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil || !reflect.DeepEqual(saved, replay) {
		t.Fatal(replay, err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "read-only" {
		t.Fatal("reservation changed worker permission", b, err)
	}
	if err := k.ReserveSessionThread(ctx, b); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("app process started during workspace preparation", err)
	}
	claims := make([]ProductWorkspaceClaim, 2)
	errs := make([]error, 2)
	var group sync.WaitGroup
	for i := range claims {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			claims[i], errs[i] = k.ClaimProductWorkspace(ctx, task.ID, saved.Digest)
		}(i)
	}
	group.Wait()
	starts := 0
	for i, claim := range claims {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if claim.Start {
			starts++
		}
	}
	if starts != 1 {
		t.Fatal("workspace preparation started twice", claims)
	}
	if _, err := k.FinishProductWorkspace(ctx, task.ID, ProductWorkspaceProof{ProofID: "not-started", Outcome: "not_started", Reason: "cannot erase a launching intent"}); err == nil {
		t.Fatal("launching was relabeled not-started")
	}
	proof := workspaceProofFixture(t, k, saved)
	bad := proof
	bad.HeadSHA = strings.Repeat("f", 40)
	if _, err := k.FinishProductWorkspace(ctx, task.ID, bad); err == nil {
		t.Fatal("wrong fixed base accepted")
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_workspace_history BEFORE INSERT ON records WHEN NEW.kind='product_workspace_history' BEGIN SELECT RAISE(FAIL,'workspace proof rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductWorkspace(ctx, task.ID, proof); err == nil || !strings.Contains(err.Error(), "workspace proof rollback") {
		t.Fatal("rollback injection not reached", err)
	}
	reservation, err := k.OperationReservation(ctx, saved.ID)
	b, bindingErr := k.SessionBinding(ctx, task.SessionID)
	current, readErr := k.ProductWorkspace(ctx, task.ID)
	if err != nil || bindingErr != nil || readErr != nil || reservation.State != "held" || b.Sandbox != "read-only" || current.State != "launching" || current.Proof != nil {
		t.Fatal("partial workspace proof committed", reservation, b, current, err, bindingErr, readErr)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_workspace_history"); err != nil {
		t.Fatal(err)
	}
	ready, err := k.FinishProductWorkspace(ctx, task.ID, proof)
	if err != nil || ready.State != "ready" {
		t.Fatal(ready, err)
	}
	b, err = k.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "workspace-write" {
		t.Fatal(b, err)
	}
	reservation, err = k.OperationReservation(ctx, saved.ID)
	if err != nil || reservation.State != "released" {
		t.Fatal(reservation, err)
	}
	again, err := k.FinishProductWorkspace(ctx, task.ID, proof)
	if err != nil || !reflect.DeepEqual(ready, again) {
		t.Fatal(again, err)
	}
	changed := request
	changed.BaseSHA = strings.Repeat("f", 40)
	if _, err := k.ReserveProductWorkspace(ctx, changed); err == nil {
		t.Fatal("workspace request was rewritten")
	}
	tasks, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
	if err != nil || len(tasks.Tasks) != 0 || len(tasks.Held) == 0 || tasks.Held[0].Reason != "product_readiness_unavailable" {
		t.Fatal("workspace proof bypassed product candidate gate", tasks, err)
	}
	var runs, candidates int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal(runs, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_target','product_candidate')").Scan(&candidates); err != nil || candidates != 0 {
		t.Fatal("workspace proof manufactured candidate", candidates, err)
	}
}
func TestProductWorkspaceLaunchingReopenDoesNotGrantAnotherStart(t *testing.T) {
	ctx := context.Background()
	k, _, task, request := productWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	first, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest)
	if err != nil || !first.Start {
		t.Fatal(first, err)
	}
	file := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenExistingKernel(file)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest)
	if err != nil || replay.Start || replay.Workspace.State != "launching" {
		t.Fatal("reopen granted duplicate Git start", replay, err)
	}
	reservation, err := k.OperationReservation(ctx, saved.ID)
	if err != nil || reservation.State != "held" {
		t.Fatal("unknown owner released resources", reservation, err)
	}
}
func TestProductWorkspaceCanceledTaskKeepsHistoryWithoutWritableBinding(t *testing.T) {
	ctx := context.Background()
	k, w, task, request := productWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-during-git", Target: w.Target(), Action: "cancel", Reason: "cancel source during local preparation"}); err != nil {
		t.Fatal(err)
	}
	final, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved))
	if err != nil || final.State != "stale" {
		t.Fatal(final, err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "read-only" {
		t.Fatal("canceled source gained writable binding", b, err)
	}
	reservation, err := k.OperationReservation(ctx, saved.ID)
	if err != nil || reservation.State != "released" {
		t.Fatal("known stopped stale preparation retained resources", reservation, err)
	}
}
func TestProductWorkspaceNotStartedCanBeReleasedWithoutInventingAClaim(t *testing.T) {
	ctx := context.Background()
	k, _, task, request := productWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	final, err := k.FinishProductWorkspace(ctx, task.ID, ProductWorkspaceProof{ProofID: "verified-never-started", Outcome: "not_started", Reason: "controller canceled before consuming start permission"})
	if err != nil || final.State != "failed" || !final.ClaimedAt.IsZero() {
		t.Fatal(final, err)
	}
	claim, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest)
	if err != nil || claim.Start {
		t.Fatal("not-started receipt granted later start", claim, err)
	}
}
func TestProductWorkspaceRejectsWrongOwnerMetadataAndPausedTask(t *testing.T) {
	for _, mode := range []string{"repository", "base", "registration", "pause_task", "pause_wave", "review", "scope"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, task, request := productWorkspaceFixture(t)
			switch mode {
			case "repository":
				request.RepositoryID++
			case "base":
				request.BaseSHA = "moving-branch"
			case "registration":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration'"); err != nil {
					t.Fatal(err)
				}
			case "pause_task":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-task", Target: w.Target(), TaskID: task.ID, Action: "pause", Reason: "operator pause"}); err != nil {
					t.Fatal(err)
				}
			case "pause_wave":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-wave", Target: w.Target(), Action: "pause", Reason: "operator pause"}); err != nil {
					t.Fatal(err)
				}
			case "review":
				tasks, err := k.Tasks(ctx, w.ID)
				if err != nil {
					t.Fatal(err)
				}
				for _, candidate := range tasks {
					if candidate.Spec.Kind == "review" {
						request.TaskID = candidate.ID
					}
				}
			case "scope":
				direction, err := k.RequestWaveDirection(ctx, directionRequest(w))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ReserveProductWorkspace(ctx, request); err == nil {
				t.Fatal("invalid product workspace admitted", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_workspace'").Scan(&count); err != nil || count != 0 {
				t.Fatal("invalid workspace left intent", count, err)
			}
		})
	}
}

func TestProductWorkspaceDoesNotBindCheckoutAfterCanonicalInputChanged(t *testing.T) {
	ctx := context.Background()
	k, w, task, request := productWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	if err := k.Put(ctx, "wave_work_item", "work", w.ID, struct {
		Target      contract.Target `json:"target"`
		CandidateID string          `json:"candidate_id"`
	}{w.Target(), "newer-canonical-candidate"}); err != nil {
		t.Fatal(err)
	}
	final, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved))
	if err != nil || final.State != "stale" {
		t.Fatal("stale canonical input gained checkout permission", final, err)
	}
	b, err := k.SessionBinding(ctx, task.SessionID)
	if err != nil || b.Sandbox != "read-only" {
		t.Fatal(b, err)
	}
}

func TestProductWorkspaceReservationFailureDoesNotPublishPartialOwnership(t *testing.T) {
	ctx := context.Background()
	k, _, _, request := productWorkspaceFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_workspace_intent BEFORE INSERT ON records WHEN NEW.kind='product_workspace' BEGIN SELECT RAISE(FAIL,'workspace intent rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductWorkspace(ctx, request); err == nil || !strings.Contains(err.Error(), "workspace intent rollback") {
		t.Fatal(err)
	}
	for _, table := range []string{"kernel_dispatch_intents", "kernel_resource_leases"} {
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("partial workspace ownership committed", table, count, err)
		}
	}
}
func TestProductWorkspaceCorruptMutableProofIsNotAReadyReceipt(t *testing.T) {
	ctx := context.Background()
	k, _, task, request := productWorkspaceFixture(t)
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductWorkspace(ctx, task.ID, workspaceProofFixture(t, k, saved)); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("UPDATE records SET payload=json_set(payload,'$.workspace.proof.head_sha',?) WHERE kind='product_workspace' AND id=?", strings.Repeat("f", 40), saved.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ProductWorkspace(ctx, task.ID); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("corrupt mutable proof was accepted", err)
	}
}
