package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func registeredHTTPStopFixture(t *testing.T, mode string) (*KernelStore, ProductMerge, ProductMergeStop) {
	t.Helper()
	k, job := registeredMergeHTTPFixture(t)
	if child := activateHTTPChild(t, k, job); !child.Start || child.Error != "" {
		t.Fatal(child)
	}
	job, err := k.ReadProductMerge(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	proof := ProductMergeStop{MergeID: job.ID, ProofID: "registered-http-stop", Reason: "fixture registered transport group joined", StartedAt: job.HTTP.ActivatedAt, FinishedAt: k.clock(), Disposition: mode}
	if mode == "merged" || mode == "rejected" {
		p := job.Value.Product.Value
		proof.Disposition = "response_received"
		proof.Response = &ProductMergeResponse{RepositoryID: p.WorkItem.RepositoryID, PRNumber: p.PRNumber, ExpectedHeadSHA: p.Candidate.SHA, Method: job.Value.Method, Merged: mode == "merged"}
		if mode == "merged" {
			proof.Response.MergeSHA = strings.Repeat("e", 40)
		}
	}
	return k, job, proof
}

func TestProductMergeHTTPFinishVerifiesRegisteredGroupAfterReopenAndPreservesFacts(t *testing.T) {
	for _, mode := range []string{"merged", "rejected", "outcome_unknown", "client_not_started"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, proof := registeredHTTPStopFixture(t, mode)
			root := k.Root
			k.Close()
			var err error
			k, err = OpenExistingKernel(root + "/state.db")
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-after-native-activation", Target: job.Value.Product.Value.Target, Action: "cancel", Reason: "historical native result remains recordable"}); err != nil {
				t.Fatal(err)
			}
			p := job.Value.Product.Value
			before, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("b", 64), proof); !errors.Is(err, ErrProductMergeHeld) {
				t.Fatal("wrong spec accepted", err)
			}
			stopped, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof)
			expected := mode
			if mode == "outcome_unknown" {
				expected = "stopped_unknown"
			}
			if mode == "client_not_started" {
				expected = "stopped_before_http"
			}
			if err != nil || stopped.State != expected || stopped.HTTPStop == nil || stopped.HTTPStop.Process != job.HTTP.Process {
				t.Fatal(stopped, err)
			}
			after, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("old HTTP result overwrote Product", after, err)
			}
			seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
			if err != nil || (seat.State == "released") != (mode == "merged") {
				t.Fatal(seat, err)
			}
			if mode == "outcome_unknown" {
				// The normal positive-read port now has a real saved registered-group stop.
				positive := mergeReconciliationFixture(k, job)
				positive.StopProofID = proof.ProofID
				stopped, err = k.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, positive)
				if err != nil {
					t.Fatal(err)
				}
			}
			// Once terminal, exact receipt replay cannot release a newer owner again.
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := claimResourceTx(ctx, tx, job.MutationLease.Resource, "next-http-owner", p.Target.WaveID); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			again, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof)
			if err != nil || !reflect.DeepEqual(stopped, again) {
				t.Fatal("HTTP stop replay changed facts/next owner", again, err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_http_stop' AND id=?", job.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof); err == nil {
				t.Fatal("lost physical witness regenerated")
			}
		})
	}
}

func TestProductMergeHTTPFinishRejectsLiveRegisteredGroup(t *testing.T) {
	ctx := context.Background()
	k, job := registeredMergeHTTPFixture(t)
	t.Setenv("MERGE_HTTP_TEST_HOLD", "1")
	cmd := mergeHTTPActivationCommand(t, k, job)
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	var reply mergeHTTPHelperReply
	if err := json.NewDecoder(output).Decode(&reply); err != nil {
		t.Fatal(err)
	}
	if !reply.Start || reply.Error != "" {
		t.Fatal(reply)
	}
	job, err = k.ReadProductMerge(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	proof := ProductMergeStop{MergeID: job.ID, ProofID: "too-early", Reason: "live registered group must hold", StartedAt: job.HTTP.ActivatedAt, FinishedAt: k.clock(), Disposition: "outcome_unknown"}
	check, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := k.FinishProductMergeHTTP(check, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof); err == nil {
		t.Fatal("live HTTP group certified stopped")
	}
	read, err := k.ReadProductMerge(ctx, job.ID)
	if err != nil || read.Stop != nil || read.HTTPStop != nil || read.State != "unknown" {
		t.Fatal("failed physical check changed journal", read, err)
	}
}

func TestProductMergeHTTPFinishRollbackRetainsOwnership(t *testing.T) {
	ctx := context.Background()
	k, job, proof := registeredHTTPStopFixture(t, "merged")
	if _, err := k.DB.Exec("CREATE TRIGGER fail_http_stop BEFORE INSERT ON records WHEN NEW.kind='product_merge_http_stop' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof); err == nil {
		t.Fatal("failed HTTP witness committed")
	}
	read, err := k.ReadProductMerge(ctx, job.ID)
	if err != nil || read.Stop != nil || read.HTTPStop != nil || read.State != "unknown" {
		t.Fatal(read, err)
	}
	seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
	if err != nil || seat.State != "held" {
		t.Fatal("rollback released PR", seat, err)
	}
}
