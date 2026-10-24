package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func finalFetchFixture(t *testing.T) (*KernelStore, ProductFinalPlan, ProductFinalFetchRequest) {
	t.Helper()
	ctx := context.Background()
	k, job, coverage, heads := finalPlanFixture(t)
	plan, err := k.PrepareProductFinalPlan(ctx, job.Value.Product.Value.Target, coverage.Digest, heads)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := k.ReadProductVerificationPolicy(ctx, plan.Value.Target, heads[0].RepositoryID)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := workspace.PinRepository(ctx, policy.Value.Repository)
	if err != nil {
		t.Fatal(err)
	}
	return k, plan, ProductFinalFetchRequest{PlanID: plan.ID, RepositoryID: heads[0].RepositoryID, Source: pin, Remote: "https://example.invalid/owner/repo.git"}
}
func finalFetchProof(f ProductFinalFetch, available, attempted bool) ProductFinalFetchProof {
	outcome := "failed"
	if available {
		outcome = "available"
	}
	return ProductFinalFetchProof{ProofID: f.ID + ":stopped", Outcome: outcome, Reason: "fixture Git owner physically stopped", Observation: workspace.CommitFetch{Source: f.Value.Request.Source, Remote: f.Value.Request.Remote, SHA: f.Value.SHA, Available: available, Attempted: attempted}}
}
func TestProductFinalFetchOneUseOwnershipAndHistoricalStopping(t *testing.T) {
	ctx := context.Background()
	k, plan, request := finalFetchFixture(t)
	before, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	f, err := k.ReserveProductFinalFetch(ctx, request)
	if err != nil || f.State != "prepared" {
		t.Fatal(f, err)
	}
	replay, err := k.ReserveProductFinalFetch(ctx, request)
	if err != nil || !reflect.DeepEqual(replay, f) {
		t.Fatal("reservation replay changed identity", replay, err)
	}
	changed := request
	changed.Remote = "https://other.invalid/repo.git"
	if _, err := k.ReserveProductFinalFetch(ctx, changed); err == nil {
		t.Fatal("host remote changed under existing claim")
	}
	reservation, err := k.OperationReservation(ctx, f.ID)
	if err != nil || reservation.State != "held" || reservation.Kind != "git" {
		t.Fatal(reservation, err)
	}
	if _, err := k.ReserveOperationCapacity(ctx, OperationCapacityRequest{ID: "overlap", Kind: "git", SessionID: f.Value.ParentSessionID, Target: plan.Value.Target, Repository: request.Source.Repository}); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("shared repository lease not excluded", err)
	}
	f, start, err := k.ClaimProductFinalFetch(ctx, f.ID, f.Digest)
	if err != nil || !start || f.State != "launching" {
		t.Fatal(f, start, err)
	}
	if _, start, err := k.ClaimProductFinalFetch(ctx, f.ID, f.Digest); err == nil || start {
		t.Fatal("unknown launch reclaimed", start, err)
	}
	if err := k.AuthorizeProductFinalFetch(ctx, f.ID, f.Digest); err != nil {
		t.Fatal(err)
	}
	if err := k.AuthorizeProductFinalFetch(ctx, f.ID, f.Digest); err == nil {
		t.Fatal("send fence reused")
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-final-fetch", Target: plan.Value.Target, Action: "cancel", Reason: "fixture cancel after physical start"}); err != nil {
		t.Fatal(err)
	}
	proof := finalFetchProof(f, true, true)
	finished, err := k.FinishProductFinalFetch(ctx, f.ID, f.Digest, proof)
	if err != nil || finished.State != "available" {
		t.Fatal("cancellation lost stopping facts", finished, err)
	}
	replay, err = k.FinishProductFinalFetch(ctx, f.ID, f.Digest, proof)
	if err != nil || !reflect.DeepEqual(replay, finished) {
		t.Fatal("terminal replay", replay, err)
	}
	if _, start, err := k.ClaimProductFinalFetch(ctx, f.ID, f.Digest); err == nil || start {
		t.Fatal("historical success authorized new process")
	}
	r, err := k.OperationReservation(ctx, f.ID)
	if err != nil || r.State != "released" || r.Proof == nil || r.Proof.Disposition != "process_stopped" {
		t.Fatal(r, err)
	}
	after, err := k.WaveOperationBudget(ctx, plan.Value.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("fetch invented execution or review budget", after, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_runner_job','product_wave_completion') AND id LIKE 'product-final-%'").Scan(&count); err != nil || count != 0 {
		t.Fatal("fetch became runner/acceptance", count, err)
	}
	root := k.Root
	k.Close()
	reopened, err := OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	read, err := reopened.ReadProductFinalFetch(ctx, f.ID)
	if err != nil || !reflect.DeepEqual(read, finished) {
		t.Fatal("historical fetch changed after reopen", read, err)
	}
}
func TestProductFinalFetchRechecksCurrentPlanControlFreshnessAndLeases(t *testing.T) {
	for _, mode := range []string{"pause", "stale", "head", "lease", "history"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, plan, request := finalFetchFixture(t)
			f, err := k.ReserveProductFinalFetch(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-fetch", Target: plan.Value.Target, Action: "pause", Reason: "fixture"}); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock().Add(31 * time.Second)
				k.Now = func() time.Time { return now }
			case "head":
				heads := append([]ProductFinalHeadObservation{}, plan.Admission...)
				heads[0].HeadSHA = strings.Repeat("b", 40)
				if _, err := k.PrepareProductFinalPlan(ctx, plan.Value.Target, plan.Value.CoverageDigest, heads); err != nil {
					t.Fatal(err)
				}
			case "lease":
				if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE run_id=?", f.ID); err != nil {
					t.Fatal(err)
				}
			case "history":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_final_fetch_history' AND id=?", f.ID); err != nil {
					t.Fatal(err)
				}
			}
			if _, start, err := k.ClaimProductFinalFetch(ctx, f.ID, f.Digest); err == nil || start {
				t.Fatal("stale/unavailable source started", mode, start, err)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_final_fetch_claim'").Scan(&count); err != nil || count != 0 {
				t.Fatal("held claim left grant", count, err)
			}
		})
	}
}
func TestProductFinalFetchMissingHistoryAndSendCannotRestart(t *testing.T) {
	for _, mode := range []string{"head", "claim", "send", "hidden_send", "terminal"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, _, request := finalFetchFixture(t)
			f, err := k.ReserveProductFinalFetch(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			f, _, err = k.ClaimProductFinalFetch(ctx, f.ID, f.Digest)
			if err != nil {
				t.Fatal(err)
			}
			if err := k.AuthorizeProductFinalFetch(ctx, f.ID, f.Digest); err != nil {
				t.Fatal(err)
			}
			if mode == "terminal" {
				if _, err := k.FinishProductFinalFetch(ctx, f.ID, f.Digest, finalFetchProof(f, true, true)); err != nil {
					t.Fatal(err)
				}
			}
			kind := "product_final_fetch_" + mode
			if mode == "head" {
				kind = "product_final_fetch"
			}
			if mode == "hidden_send" {
				current, err := k.ReadProductFinalFetch(ctx, f.ID)
				if err != nil {
					t.Fatal(err)
				}
				current.MayHaveSentAt = time.Time{}
				if err := k.Put(ctx, "product_final_fetch", f.ID, current.Value.Target.WaveID, current); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, f.ID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ReadProductFinalFetch(ctx, f.ID); err == nil {
				t.Fatal("missing physical journal accepted", mode)
			}
			if _, err := k.ReserveProductFinalFetch(ctx, request); err == nil {
				t.Fatal("lost evidence regenerated", mode)
			}
		})
	}
}
func TestProductFinalFetchClaimRollbackLeavesNoGrant(t *testing.T) {
	ctx := context.Background()
	k, _, request := finalFetchFixture(t)
	f, err := k.ReserveProductFinalFetch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_final_fetch_claim BEFORE INSERT ON records WHEN NEW.kind='product_final_fetch_claim' BEGIN SELECT RAISE(FAIL,'claim rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, start, err := k.ClaimProductFinalFetch(ctx, f.ID, f.Digest); err == nil || start {
		t.Fatal("rolled-back claim authorized process", start, err)
	}
	read, err := k.ReadProductFinalFetch(ctx, f.ID)
	if err != nil || read.State != "prepared" || read.Claim != nil {
		t.Fatal("claim rollback changed head", read, err)
	}
}

func TestProductFinalFetchSendFenceRechecksAfterClaim(t *testing.T) {
	for _, mode := range []string{"pause", "stale", "head", "lease"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, plan, request := finalFetchFixture(t)
			f, err := k.ReserveProductFinalFetch(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			f, _, err = k.ClaimProductFinalFetch(ctx, f.ID, f.Digest)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "pause":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-before-fetch-send", Target: plan.Value.Target, Action: "pause", Reason: "fixture"}); err != nil {
					t.Fatal(err)
				}
			case "stale":
				now := k.clock().Add(31 * time.Second)
				k.Now = func() time.Time { return now }
			case "head":
				heads := append([]ProductFinalHeadObservation{}, plan.Admission...)
				heads[0].HeadSHA = strings.Repeat("b", 40)
				if _, err := k.PrepareProductFinalPlan(ctx, plan.Value.Target, plan.Value.CoverageDigest, heads); err != nil {
					t.Fatal(err)
				}
			case "lease":
				if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE run_id=?", f.ID); err != nil {
					t.Fatal(err)
				}
			}
			if err := k.AuthorizeProductFinalFetch(ctx, f.ID, f.Digest); err == nil {
				t.Fatal("held source allowed send", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_final_fetch_send'").Scan(&count); err != nil || count != 0 {
				t.Fatal("held fence left send grant", count, err)
			}
			if mode != "lease" {
				if _, err := k.FinishProductFinalFetch(ctx, f.ID, f.Digest, finalFetchProof(f, false, false)); err != nil {
					t.Fatal("source change blocked actual stopped owner cleanup", err)
				}
			}
		})
	}
}
func TestProductFinalFetchFreshObservationCanClaimUnstartedHistoricalPlan(t *testing.T) {
	ctx := context.Background()
	k, plan, request := finalFetchFixture(t)
	f, err := k.ReserveProductFinalFetch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	now := k.clock().Add(31 * time.Second)
	k.Now = func() time.Time { return now }
	heads := append([]ProductFinalHeadObservation{}, plan.Admission...)
	for i := range heads {
		heads[i].ObservedAt = now
	}
	same, err := k.PrepareProductFinalPlan(ctx, plan.Value.Target, plan.Value.CoverageDigest, heads)
	if err != nil || same.ID != plan.ID {
		t.Fatal("same HEAD duplicated plan", same, err)
	}
	claimed, start, err := k.ClaimProductFinalFetch(ctx, f.ID, f.Digest)
	if err != nil || !start || !claimed.Claim.Selection.ObservedAt.Equal(now) {
		t.Fatal("fresh source did not authorize original unclaimed preparation", claimed, start, err)
	}
}
