package storage

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func operationFixture(t *testing.T) (*KernelStore, domain.Wave, WaveRequest) {
	t.Helper()
	k, request := waveStoreFixture(t)
	request.Policy.Stage = "P"
	request.Policy.Product = true
	w, err := k.CreateWave(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return k, w, request
}
func operationRequest(t *testing.T, w domain.Wave, kind, id string) OperationCapacityRequest {
	t.Helper()
	r := OperationCapacityRequest{ID: id, Kind: kind, SessionID: w.OrchestratorSessionID, Target: w.Target()}
	if kind == "runner" {
		r.Worktree = t.TempDir()
	} else {
		r.ResourceKey = "repository/work-item"
	}
	return r
}
func releaseOperation(t *testing.T, k *KernelStore, id, disposition string) OperationReleaseProof {
	t.Helper()
	proof := OperationReleaseProof{OperationID: id, ProofID: "verified:" + id, Disposition: disposition, Reason: "trusted process/external verifier observed terminal outcome"}
	if err := k.ReleaseOperationCapacity(context.Background(), proof); err != nil {
		t.Fatal(err)
	}
	return proof
}
func TestOperationPoolsAreIdempotentAndRequireVerifiedTerminalProof(t *testing.T) {
	for _, kind := range []string{"runner", "pr", "cloud_review"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			k, w, _ := operationFixture(t)
			request := operationRequest(t, w, kind, "first")
			var group sync.WaitGroup
			out := make([]OperationReservation, 2)
			errs := make([]error, 2)
			for i := range out {
				group.Add(1)
				go func(i int) { defer group.Done(); out[i], errs[i] = k.ReserveOperationCapacity(ctx, request) }(i)
			}
			group.Wait()
			if errs[0] != nil || errs[1] != nil || !reflect.DeepEqual(out[0], out[1]) {
				t.Fatal(out, errs)
			}
			second := operationRequest(t, w, kind, "second")
			second.ResourceKey = request.ResourceKey
			if _, err := k.ReserveOperationCapacity(ctx, second); !errors.Is(err, ErrExecutionCapacity) {
				t.Fatal("pool overflow", err)
			}
			if err := k.ReleaseOperationCapacity(ctx, OperationReleaseProof{OperationID: request.ID, ProofID: "timeout", Disposition: "unknown", Reason: "response missing"}); err == nil {
				t.Fatal("unknown released a slot")
			}
			changed := request
			changed.Target.ScopeRevision++
			if _, err := k.ReserveOperationCapacity(ctx, changed); err == nil {
				t.Fatal("operation key changed scope")
			}
			disposition := "completed"
			if kind == "runner" {
				disposition = "process_stopped"
			} else if kind == "pr" {
				disposition = "closed"
			}
			proof := releaseOperation(t, k, request.ID, disposition)
			if _, err := k.ReserveOperationCapacity(ctx, second); err != nil {
				t.Fatal(err)
			}
			if err := k.ReleaseOperationCapacity(ctx, proof); err != nil {
				t.Fatal(err)
			} // cannot release second's generation.
			replay, err := k.ReserveOperationCapacity(ctx, request)
			if err != nil || replay.State != "released" {
				t.Fatal(replay, err)
			}
			current, err := k.OperationReservation(ctx, second.ID)
			if err != nil || current.State != "held" {
				t.Fatal(current, err)
			}
			for _, lease := range current.Resources {
				var owner, state string
				if err = k.DB.QueryRow("SELECT run_id,state FROM kernel_resource_leases WHERE resource=?", lease.Resource).Scan(&owner, &state); err != nil || owner != second.ID || state != "held" {
					t.Fatal(owner, state, err)
				}
			}
			proof.Reason = "different proof"
			if err = k.ReleaseOperationCapacity(ctx, proof); err == nil {
				t.Fatal("release proof identity changed")
			}
		})
	}
}
func TestRunnerCapacitySharesWorkspaceExclusionWithAppServers(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	cwd := t.TempDir()
	b := capacityMain(t, k, "worker", cwd)
	b = reserveSessionFixture(t, k, b)
	request := operationRequest(t, w, "runner", "job")
	request.Worktree = cwd
	if _, err := k.ReserveOperationCapacity(ctx, request); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("runner overlapped app-server workspace", err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveOperationCapacity(ctx, request); err != nil {
		t.Fatal(err)
	}
	other := capacityMain(t, k, "next-worker", cwd)
	if err := k.ReserveSessionThread(ctx, other); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("app-server overlapped runner workspace", err)
	}
	releaseOperation(t, k, request.ID, "process_stopped")
	if err := k.ReserveSessionThread(ctx, other); err != nil {
		t.Fatal(err)
	}
}
func operationRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	for _, args := range [][]string{{"init", "--initial-branch=trunk"}, {"config", "user.name", "test"}, {"config", "user.email", "test@example.invalid"}, {"-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "base"}} {
		if _, err := workspace.Git(context.Background(), repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	return repo
}
func TestGitCapacityUsesCommonRepositoryAcrossWavesAndLinkedWorktrees(t *testing.T) {
	ctx := context.Background()
	k, first, source := operationFixture(t)
	source.SourceID = "second"
	second, err := k.CreateWave(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	repo := operationRepo(t)
	linked := filepath.Join(t.TempDir(), "linked")
	if _, err = workspace.Git(ctx, repo, "worktree", "add", "-b", "linked", "--", linked, "HEAD"); err != nil {
		t.Fatal(err)
	}
	a := OperationCapacityRequest{ID: "git-a", Kind: "git", SessionID: "main", Target: first.Target(), Repository: repo}
	b := OperationCapacityRequest{ID: "git-b", Kind: "git", SessionID: "main", Target: second.Target(), Repository: linked}
	if _, err = k.ReserveOperationCapacity(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReserveOperationCapacity(ctx, b); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("linked worktrees bypassed shared Git exclusion", err)
	}
	c := b
	c.ID = "git-other-repo"
	c.Repository = operationRepo(t)
	if _, err = k.ReserveOperationCapacity(ctx, c); err != nil {
		t.Fatal("independent repository was serialized", err)
	}
	releaseOperation(t, k, a.ID, "process_stopped")
	if _, err = k.ReserveOperationCapacity(ctx, b); err != nil {
		t.Fatal(err)
	}
	model := capacityMain(t, k, "linked-model", linked)
	if err = k.ReserveSessionThread(ctx, model); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("Git operation did not retain checkout ownership", err)
	}
}
func TestOperationAdmissionAndReleaseRollBackWithEveryLease(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	r := OperationCapacityRequest{ID: "git", Kind: "git", SessionID: "main", Target: w.Target(), Repository: operationRepo(t)}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_operation_capacity BEFORE INSERT ON kernel_dispatch_intents BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveOperationCapacity(ctx, r); err == nil {
		t.Fatal("failed operation admission committed")
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE state='held'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_operation_capacity"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveOperationCapacity(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_operation_release BEFORE UPDATE ON kernel_dispatch_intents WHEN NEW.state='released' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	proof := OperationReleaseProof{OperationID: r.ID, ProofID: "verified", Disposition: "process_stopped", Reason: "trusted process owner confirmed stop"}
	if err := k.ReleaseOperationCapacity(ctx, proof); err == nil {
		t.Fatal("failed operation release committed")
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE state='held'").Scan(&count); err != nil || count != 2 {
		t.Fatal("partial lease release", count, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_operation_release"); err != nil {
		t.Fatal(err)
	}
	if err := k.ReleaseOperationCapacity(ctx, proof); err != nil {
		t.Fatal(err)
	}
}
func TestOperationUnknownOwnershipSurvivesRestartAndWaveCancellation(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	request := operationRequest(t, w, "pr", "publication")
	if _, err := k.ReserveOperationCapacity(ctx, request); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	reopened, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	k = reopened
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.State, w.CompletedAt = domain.WaveCanceled, k.clock()
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	replay, err := k.ReserveOperationCapacity(ctx, request)
	if err != nil || replay.State != "held" || replay.Request.Target != request.Target {
		t.Fatal(replay, err)
	}
	request.ID = "new-publication"
	if _, err = k.ReserveOperationCapacity(ctx, request); err == nil {
		t.Fatal("new operation started on terminal Wave")
	}
	if err = k.ConfigureExecutionLimits(ctx, domain.LegacyExecutionLimits()); err == nil {
		t.Fatal("unknown external ownership was dropped during configuration change")
	}
	releaseOperation(t, k, "publication", "closed") // late fact retains its original target.
}
func TestOperationCapacityRejectsForeignScopeAndProductClaimsInK(t *testing.T) {
	ctx := context.Background()
	k, w, _ := mailboxFixture(t)
	pr := operationRequest(t, w, "pr", "pr")
	if _, err := k.ReserveOperationCapacity(ctx, pr); err == nil {
		t.Fatal("K claimed product capacity")
	}
	runner := operationRequest(t, w, "runner", "runner")
	other := capacityMain(t, k, "other-parent", t.TempDir())
	runner.SessionID = other.ID
	if _, err := k.ReserveOperationCapacity(ctx, runner); err == nil {
		t.Fatal("foreign parent claimed Wave resources")
	}
	runner.SessionID = "main"
	runner.Target.ScopeRevision++
	if _, err := k.ReserveOperationCapacity(ctx, runner); err == nil {
		t.Fatal("stale target reserved resources")
	}
	runner.Target = w.Target()
	runner.Kind = "app_server"
	if _, err := k.ReserveOperationCapacity(ctx, runner); err == nil {
		t.Fatal("generic admission bypassed process preparation")
	}
}

func TestPendingOperationPagesDoNotLoseOperationsBehindProcessReservations(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	main, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	reserveSessionFixture(t, k, main)
	for i, kind := range []string{"pr", "cloud_review", "runner"} {
		request := operationRequest(t, w, kind, string(rune('a'+i)))
		if _, err = k.ReserveOperationCapacity(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	after := ""
	ids := []string{}
	for {
		page, err := k.PendingOperationReservations(ctx, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Reservations) > 1 {
			t.Fatal(page)
		}
		for _, r := range page.Reservations {
			ids = append(ids, r.ID)
		}
		if page.NextAfter == "" {
			break
		}
		if page.NextAfter <= after {
			t.Fatal("nonadvancing cursor", page)
		}
		after = page.NextAfter
	}
	if !reflect.DeepEqual(ids, []string{"a", "b", "c"}) {
		t.Fatal(ids)
	}
	releaseOperation(t, k, "b", "completed")
	page, err := k.PendingOperationReservations(ctx, "", 10)
	if err != nil || len(page.Reservations) != 2 || page.NextAfter != "" {
		t.Fatal(page, err)
	}
}
func TestRunnerDoesNotStealWorkspaceFromUntrackedPreparation(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	request := operationRequest(t, w, "runner", "job")
	old := capacityMain(t, k, "old-process", request.Worktree)
	old.PreparationID = "pre-feature-prepare"
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec("UPDATE kernel_sessions SET creating=1,process_stopped=0,payload=? WHERE id=?", raw, old.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReserveOperationCapacity(ctx, request); !errors.Is(err, ErrResourceBusy) {
		t.Fatal(err)
	}
	if err = k.MarkSessionStopped(ctx, old.ID, old.PreparationID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReserveOperationCapacity(ctx, request); err != nil {
		t.Fatal(err)
	}
}
