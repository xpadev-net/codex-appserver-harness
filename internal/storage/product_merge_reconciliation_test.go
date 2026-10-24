package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func mergeReconciliationFixture(k *KernelStore, job ProductMerge) ProductMergeReconciliation {
	p := job.Value.Product.Value
	return ProductMergeReconciliation{MergeID: job.ID, StopProofID: "physical-stop", MergeSHA: strings.Repeat("e", 40), Observation: PRResourceObservation{ProofID: "positive-native-merged-read", RepositoryID: p.WorkItem.RepositoryID, HeadRepositoryID: p.WorkItem.RepositoryID, BaseRepositoryID: p.WorkItem.RepositoryID, Number: p.PRNumber, MarkerWorkItemID: p.WorkItem.ID, HeadSHA: p.Candidate.SHA, HeadBranch: p.WorkItem.Branch, BaseBranch: p.BaseBranch, State: "closed", Merged: true, ObservedAt: k.clock()}}
}

func TestProductMergeReconciliationRequiresPositiveOriginalMergedReadAfterPhysicalStop(t *testing.T) {
	ctx := context.Background()
	k, job, stop := mergeResultFixture(t, true)
	proof := mergeReconciliationFixture(k, job)
	if _, err := k.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, proof); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("positive read certified missing physical stop", err)
	}
	stop.Disposition, stop.Response = "outcome_unknown", nil
	stopped, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, stop)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"open", "closed_unmerged", "wrong_head", "wrong_pr", "wrong_repository", "wrong_base_repo", "wrong_marker", "wrong_branch", "wrong_base", "missing_sha", "wrong_stop", "old_read", "future_read"} {
		t.Run(mode, func(t *testing.T) {
			value := mergeReconciliationFixture(k, job)
			switch mode {
			case "open":
				value.Observation.State = "open"
				value.Observation.Merged = false
			case "closed_unmerged":
				value.Observation.Merged = false
			case "wrong_head":
				value.Observation.HeadSHA = strings.Repeat("f", 40)
			case "wrong_pr":
				value.Observation.Number++
			case "wrong_repository":
				value.Observation.RepositoryID++
			case "wrong_base_repo":
				value.Observation.BaseRepositoryID++
			case "wrong_marker":
				value.Observation.MarkerWorkItemID = "other"
			case "wrong_branch":
				value.Observation.HeadBranch = "other"
			case "wrong_base":
				value.Observation.BaseBranch = "other"
			case "missing_sha":
				value.MergeSHA = ""
			case "wrong_stop":
				value.StopProofID = "other"
			case "old_read":
				value.Observation.ObservedAt = stop.FinishedAt.Add(-time.Second)
			case "future_read":
				value.Observation.ObservedAt = k.clock().Add(time.Hour)
			}
			if _, err := k.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, value); !errors.Is(err, ErrProductMergeHeld) {
				t.Fatal("unproven outcome accepted", mode, err)
			}
			read, err := k.ReadProductMerge(ctx, job.ID)
			if err != nil || !reflect.DeepEqual(read, stopped) {
				t.Fatal("failed read changed stop", read, err)
			}
		})
	}
	// A restarted reconciler can use a saved real stop; it cannot manufacture one.
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-before-merged-read", Target: job.Value.Product.Value.Target, Action: "cancel", Reason: "old merge fact still matters"}); err != nil {
		t.Fatal(err)
	}
	before, err := k.ReadProductTarget(ctx, job.Value.Product.Value.Target, job.Value.Product.Value.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	proof = mergeReconciliationFixture(k, job)
	merged, err := k.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, proof)
	if err != nil || merged.State != "merged" || !reflect.DeepEqual(merged.Stop, stopped.Stop) || merged.Reconciliation == nil {
		t.Fatal(merged, err)
	}
	after, err := k.ReadProductTarget(ctx, job.Value.Product.Value.Target, job.Value.Product.Value.WorkItem.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("reconciliation overwrote Product", after, err)
	}
	seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
	if err != nil || seat.State != "released" {
		t.Fatal(seat, err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claimResourceTx(ctx, tx, job.MutationLease.Resource, "next-owner", job.Value.Product.Value.Target.WaveID); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	again, err := k.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, proof)
	if err != nil || !reflect.DeepEqual(again, merged) {
		t.Fatal("replay changed fact/next lease", again, err)
	}
	if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, stop); err != nil {
		t.Fatal("original stop replay lost", err)
	}
	var leaseOwner, state string
	if err := k.DB.QueryRow("SELECT run_id,state FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&leaseOwner, &state); err != nil || leaseOwner != "next-owner" || state != "held" {
		t.Fatal("reconciliation replay released next owner", leaseOwner, state, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_reconciliation' AND id=?", job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, proof); err == nil {
		t.Fatal("missing positive receipt regenerated")
	}
}

func TestProductMergeReconciliationRollbackRetainsBothReservations(t *testing.T) {
	ctx := context.Background()
	k, job, stop := mergeResultFixture(t, true)
	stop.Disposition, stop.Response = "outcome_unknown", nil
	if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, stop); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_merge_read BEFORE INSERT ON records WHEN NEW.kind='product_merge_reconciliation' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, mergeReconciliationFixture(k, job)); err == nil {
		t.Fatal("failed reconciliation succeeded")
	}
	seat, err := k.OperationReservation(ctx, job.Value.PRReservationID)
	if err != nil || seat.State != "held" {
		t.Fatal("failed read released PR", seat, err)
	}
	read, err := k.ReadProductMerge(ctx, job.ID)
	if err != nil || read.State != "stopped_unknown" || read.Reconciliation != nil {
		t.Fatal(read, err)
	}
}
