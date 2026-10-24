package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func mergeResultFixture(t *testing.T, sent bool) (*KernelStore, ProductMerge, ProductMergeStop) {
	t.Helper()
	ctx := context.Background()
	k, id := mergeJournalFixture(t)
	job, err := k.PrepareProductMerge(ctx, id, "result-owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.ClaimProductMerge(ctx, job.ID, job.Value.OwnerID, job.Digest)
	if err != nil {
		t.Fatal(err)
	}
	job = claim.Merge
	if sent {
		job, err = k.MarkProductMergeMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, mergeGateObservationFixture(k, job))
		if err != nil {
			t.Fatal(err)
		}
	}
	proof := ProductMergeStop{MergeID: job.ID, ProofID: "physical-stop", Reason: "fixture synchronous transport owner joined", StartedAt: job.ClaimedAt, FinishedAt: k.clock(), Disposition: "not_started"}
	if sent {
		proof.Disposition = "response_received"
		p := job.Value.Product.Value
		proof.Response = &ProductMergeResponse{RepositoryID: p.WorkItem.RepositoryID, PRNumber: p.PRNumber, ExpectedHeadSHA: p.Candidate.SHA, Method: job.Value.Method, Merged: true, MergeSHA: strings.Repeat("e", 40)}
	}
	return k, job, proof
}

func TestProductMergeResultsPersistOnlyOwnHistoricalFactsAndCapacity(t *testing.T) {
	for _, mode := range []string{"merged", "pause", "cancel", "new_candidate", "rejected", "unsent", "unknown", "canceled_unknown"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, proof := mergeResultFixture(t, mode != "unsent")
			expected := "merged"
			switch mode {
			case "pause", "cancel", "canceled_unknown":
				action := mode
				if action == "canceled_unknown" {
					action = "cancel"
				}
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "after-merge-send", Target: job.Value.Product.Value.Target, Action: action, Reason: "old result must still be recorded"}); err != nil {
					t.Fatal(err)
				}
			case "new_candidate":
				p := job.Value.Product.Value
				p.Candidate.ID = "next"
				p.WorkItem.Candidate = "next"
				p.WorkItem.Version++
				if _, err := k.SetProductTarget(ctx, job.Value.Product.Digest, p); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "rejected" {
				proof.Response.Merged = false
				proof.Response.MergeSHA = ""
				expected = "rejected"
			}
			if mode == "unsent" {
				expected = "stopped_unsent"
			}
			if mode == "unknown" || mode == "canceled_unknown" {
				proof.Disposition = "outcome_unknown"
				proof.Response = nil
				expected = "stopped_unknown"
			}
			p := job.Value.Product.Value
			before, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
			if err != nil {
				t.Fatal(err)
			}
			stopped, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, proof)
			if err != nil || stopped.State != expected || stopped.Stop == nil {
				t.Fatal(stopped, err)
			}
			var leaseState string
			if err := k.DB.QueryRow("SELECT state FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&leaseState); err != nil {
				t.Fatal(err)
			}
			if (leaseState == "held") != (expected == "stopped_unknown") {
				t.Fatal("incorrect unknown/known lease disposition", leaseState, expected)
			}
			seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
			if err != nil || (seat.State == "released") != (expected == "merged") {
				t.Fatal("incorrect PR capacity release", seat, err)
			}
			after, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("old result overwrote current Product", after, err)
			}
			var approvals int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&approvals); err != nil || approvals != 0 {
				t.Fatal("transport fact created approval/acceptance", approvals, err)
			}
			if expected != "stopped_unknown" {
				tx, err := k.kernelTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := claimResourceTx(ctx, tx, job.MutationLease.Resource, "next-owner", p.Target.WaveID); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			again, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, proof)
			if err != nil || !reflect.DeepEqual(stopped, again) {
				t.Fatal("stop replay altered facts/next owner", again, err)
			}
			if mode == "merged" || mode == "unknown" {
				root := k.Root
				k.Close()
				k, err = OpenExistingKernel(root + "/state.db")
				if err != nil {
					t.Fatal(err)
				}
				defer k.Close()
				read, err := k.ReadProductMerge(ctx, job.ID)
				if err != nil || !reflect.DeepEqual(stopped, read) {
					t.Fatal("receipt not retained after reopen", read, err)
				}
				if claim, err := k.ClaimProductMerge(ctx, job.ID, job.Value.OwnerID, job.Digest); err != nil || claim.Start {
					t.Fatal("stopped operation relaunched", claim, err)
				}
			}
		})
	}
}

func TestProductMergeResultsRejectUnboundStopsAndCannotRewriteUnknown(t *testing.T) {
	ctx := context.Background()
	k, job, base := mergeResultFixture(t, true)
	for _, mode := range []string{"foreign_owner", "wrong_merge", "wrong_repository", "wrong_pr", "wrong_head", "wrong_method", "missing_sha", "before_claim", "future", "not_started", "missing_response", "unknown_with_response"} {
		t.Run(mode, func(t *testing.T) {
			proof := base
			response := *base.Response
			proof.Response = &response
			owner := job.Value.OwnerID
			switch mode {
			case "foreign_owner":
				owner = "foreign"
			case "wrong_merge":
				proof.MergeID = "foreign"
			case "wrong_repository":
				proof.Response.RepositoryID++
			case "wrong_pr":
				proof.Response.PRNumber++
			case "wrong_head":
				proof.Response.ExpectedHeadSHA = strings.Repeat("f", 40)
			case "wrong_method":
				proof.Response.Method = "squash"
			case "missing_sha":
				proof.Response.MergeSHA = ""
			case "before_claim":
				proof.StartedAt = job.ClaimedAt.Add(-time.Second)
			case "future":
				proof.FinishedAt = k.clock().Add(time.Hour)
			case "not_started":
				proof.Disposition = "not_started"
				proof.Response = nil
			case "missing_response":
				proof.Response = nil
			case "unknown_with_response":
				proof.Disposition = "outcome_unknown"
			}
			if _, err := k.RecordProductMergeStopped(ctx, job.ID, owner, job.Digest, proof); !errors.Is(err, ErrProductMergeHeld) {
				t.Fatal("invalid stop accepted", mode, err)
			}
			read, err := k.ReadProductMerge(ctx, job.ID)
			if err != nil || read.State != "unknown" || read.Stop != nil {
				t.Fatal("invalid stop changed journal", read, err)
			}
		})
	}
	unknown := base
	unknown.Disposition = "outcome_unknown"
	unknown.Response = nil
	if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, unknown); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, base); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("immutable unknown stop rewritten as merged", err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_stop' AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductMerge(ctx, job.ID); err == nil {
		t.Fatal("lost stop evidence accepted")
	}
	if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, unknown); err == nil {
		t.Fatal("lost stop regenerated")
	}
}

func TestProductMergeResultsRollbackAndReopenedOwnerCannotCertifyStop(t *testing.T) {
	for _, mode := range []string{"rollback", "reopen"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, proof := mergeResultFixture(t, true)
			if mode == "rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_merge_stop BEFORE INSERT ON records WHEN NEW.kind='product_merge_stop' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
					t.Fatal(err)
				}
			} else {
				root := k.Root
				k.Close()
				var err error
				k, err = OpenExistingKernel(root + "/state.db")
				if err != nil {
					t.Fatal(err)
				}
				defer k.Close()
			}
			if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, proof); err == nil {
				t.Fatal("unsafe/failed stop succeeded")
			}
			read, err := k.ReadProductMerge(ctx, job.ID)
			if err != nil || read.State != "unknown" || read.Stop != nil {
				t.Fatal("failed stop changed journal", read, err)
			}
			seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
			if err != nil || seat.State != "held" {
				t.Fatal("failed stop released capacity", seat, err)
			}
			var state string
			if err := k.DB.QueryRow("SELECT state FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&state); err != nil || state != "held" {
				t.Fatal("failed stop released lease", state, err)
			}
		})
	}
}
