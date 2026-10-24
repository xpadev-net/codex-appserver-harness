package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func unfencedMergeHTTPFixture(t *testing.T) (*KernelStore, ProductMerge) {
	t.Helper()
	k, requestID := mergeJournalFixture(t)
	job, err := k.PrepareProductMergeHTTP(context.Background(), requestID, "unfenced-http-owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.ClaimProductMerge(context.Background(), job.ID, job.Value.OwnerID, job.Digest)
	if err != nil || !claim.Start {
		t.Fatal(claim, err)
	}
	return k, claim.Merge
}

func TestProductMergeHTTPAbortUnfencedClaimAfterReopenSealsLateParentAndClient(t *testing.T) {
	ctx := context.Background()
	k, job := unfencedMergeHTTPFixture(t)
	gate := mergeGateObservationFixture(k, job)
	other, err := OpenExistingKernel(k.Root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	aborted, err := other.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "seal-unfenced", "seal delayed parent and HTTP admission")
	if err != nil || aborted.State != "aborted_http" || aborted.HTTPAbort == nil || aborted.HTTP != nil || aborted.Stop != nil || !aborted.MayHaveSentAt.IsZero() || aborted.GateDigest != "" {
		t.Fatal("unfenced claim could not be sealed after restart", aborted, err)
	}
	// The original parent/epoch is STILL alive. Its own valid fresh packet must
	// fail because of the atomic seal, not because a new controller inferred death.
	if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, gate); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("live old parent fenced after seal", err)
	}
	if late := activateHTTPChild(t, k, job); late.Start || late.Error == "" {
		t.Fatal("late client activated after unfenced seal", late)
	}
	claim, err := k.ClaimProductMerge(ctx, job.ID, job.Value.OwnerID, job.Digest)
	if err != nil || claim.Start || !reflect.DeepEqual(claim.Merge, aborted) {
		t.Fatal("claim replay reopened admission", claim, err)
	}
	seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
	if err != nil || seat.State != "held" {
		t.Fatal("seal released PR lifetime", seat, err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next, err := claimResourceTx(ctx, tx, job.MutationLease.Resource, "next-unfenced-owner", job.Value.Product.Value.Target.WaveID)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil || next.Generation <= job.MutationLease.Generation {
		t.Fatal(next, err)
	}
	replay, err := other.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "seal-unfenced", "seal delayed parent and HTTP admission")
	if err != nil || !reflect.DeepEqual(replay, aborted) {
		t.Fatal("replay changed seal", replay, err)
	}
	var owner, state string
	if err := k.DB.QueryRow("SELECT run_id,state FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&owner, &state); err != nil || owner != "next-unfenced-owner" || state != "held" {
		t.Fatal("replay released successor", owner, state, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_http_abort' AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := other.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "seal-unfenced", "seal delayed parent and HTTP admission"); !errors.Is(err, ErrTaskEvidence) {
		t.Fatal("lost independent seal evidence regenerated", err)
	}
}

func TestProductMergeHTTPUnfencedSealRollbackAndParentFenceRace(t *testing.T) {
	t.Run("rollback", func(t *testing.T) {
		ctx := context.Background()
		k, job := unfencedMergeHTTPFixture(t)
		if _, err := k.DB.Exec("CREATE TRIGGER fail_unfenced_abort BEFORE INSERT ON records WHEN NEW.kind='product_merge_http_abort' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
			t.Fatal(err)
		}
		if _, err := k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "rollback-seal", "injected failure"); err == nil {
			t.Fatal("failed seal succeeded")
		}
		unchanged, err := k.ReadProductMerge(ctx, job.ID)
		if err != nil || !reflect.DeepEqual(unchanged, job) {
			t.Fatal("failed seal changed journal", unchanged, err)
		}
		if _, err := k.DB.Exec("DROP TRIGGER fail_unfenced_abort"); err != nil {
			t.Fatal(err)
		}
		if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, mergeGateObservationFixture(k, job)); err != nil {
			t.Fatal("rollback consumed parent fence", err)
		}
	})
	for i := 0; i < 4; i++ {
		t.Run("race", func(t *testing.T) {
			ctx := context.Background()
			k, job := unfencedMergeHTTPFixture(t)
			gate := mergeGateObservationFixture(k, job)
			var fenceErr, sealErr error
			var wg sync.WaitGroup
			start := make(chan struct{})
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_, fenceErr = k.MarkProductMergeMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, gate)
			}()
			go func() {
				defer wg.Done()
				<-start
				_, sealErr = k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, "racing-seal", "seal concurrent parent admission")
			}()
			close(start)
			wg.Wait()
			if sealErr != nil || fenceErr != nil && !errors.Is(fenceErr, ErrProductMergeHeld) {
				t.Fatal("seal/fence race failed", sealErr, fenceErr)
			}
			sealed, err := k.ReadProductMerge(ctx, job.ID)
			if err != nil || sealed.HTTPAbort == nil || sealed.State != "aborted_http" {
				t.Fatal(sealed, err)
			}
			if late := activateHTTPChild(t, k, job); late.Start || late.Error == "" {
				t.Fatal("race allowed native client after seal", late)
			}
		})
	}
}

func TestProductMergeHTTPUnfencedSealRejectsPreparedLegacyAndWrongIdentity(t *testing.T) {
	for _, mode := range []string{"prepared", "legacy", "wrong_owner", "wrong_digest", "lost_claim"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, requestID := mergeJournalFixture(t)
			var job ProductMerge
			var err error
			if mode == "legacy" {
				job, err = k.PrepareProductMerge(ctx, requestID, "owner", "merge")
			} else {
				job, err = k.PrepareProductMergeHTTP(ctx, requestID, "owner", "merge")
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode != "prepared" {
				claim, err := k.ClaimProductMerge(ctx, job.ID, job.Value.OwnerID, job.Digest)
				if err != nil || !claim.Start {
					t.Fatal(claim, err)
				}
				job = claim.Merge
			}
			owner, digest := job.Value.OwnerID, job.Digest
			expectedErr := ErrProductMergeHeld
			switch mode {
			case "wrong_owner":
				owner = "different-owner"
			case "wrong_digest":
				digest = strings.Repeat("0", 64)
			case "lost_claim":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_claim' AND id=?", job.ID); err != nil {
					t.Fatal(err)
				}
				expectedErr = ErrTaskEvidence
			}
			if _, err := k.AbortUnactivatedProductMergeHTTP(ctx, job.ID, owner, digest, "invalid-seal", "must not broaden admission"); !errors.Is(err, expectedErr) {
				t.Fatal("invalid seal accepted", err)
			}
			var state string
			if err := k.DB.QueryRow("SELECT state FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&state); err != nil || state != "held" {
				t.Fatal("failed seal released ownership", state, err)
			}
		})
	}
}
