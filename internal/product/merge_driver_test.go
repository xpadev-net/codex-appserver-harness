package product

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func checkMergeDriver(t *testing.T, g KernelMergeGate, original storage.ProductMerge, bin, httpMode, mode string) {
	t.Helper()
	k := g.Cloud.Store
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("HTTP_WRAPPER_DATA", bin)
	t.Setenv("HTTP_WRAPPER_MODE", httpMode)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gateCalls := 0
	hold := errors.New("explicit initial gate hold")
	d := MergeDriver{Store: k, Transport: MergeHTTPTransport{Store: k, Executable: exe}}
	d.Gate = func(ctx context.Context, saved storage.ProductTargetSnapshot, id string) (storage.ProductMergeGateObservation, error) {
		gateCalls++
		if mode == "gate_hold" && gateCalls == 1 {
			return storage.ProductMergeGateObservation{}, hold
		}
		if mode == "post_claim_hold" && gateCalls == 2 {
			if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "hold-after-parent-claim", Target: saved.Value.Target, Action: "pause", Reason: "fresh parent fence must observe control"}); err != nil {
				return storage.ProductMergeGateObservation{}, err
			}
		}
		fresh, err := g.Check(ctx, saved, id)
		if err == nil && gateCalls == 2 {
			if mode == "cancel" {
				cancel()
			}
			if mode == "spec_failure" {
				err = os.WriteFile(filepath.Join(k.Root, "merge-http"), []byte("not a directory"), 0600)
			}
		}
		return fresh, err
	}
	if mode == "preclaimed" {
		if _, err := k.ClaimProductMerge(ctx, original.ID, original.Value.OwnerID, original.Digest); err != nil {
			t.Fatal(err)
		}
		fresh, err := g.Check(ctx, original.Value.Product, original.Value.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := k.MarkProductMergeMayHaveSent(ctx, original.ID, original.Value.OwnerID, original.Digest, fresh); err != nil {
			t.Fatal(err)
		}
		job, err := d.Merge(ctx, original.Value.RequestID, original.Value.OwnerID, "merge")
		if !errors.Is(err, storage.ErrProductMergeHeld) || job.HTTP != nil || job.HTTPAbort != nil || gateCalls != 0 {
			t.Fatal("driver recovered/relaunched a previously claimed intent", job, err, gateCalls)
		}
		if _, err := os.Stat(filepath.Join(k.Root, "merge-http")); !os.IsNotExist(err) {
			t.Fatal("claimed intent produced a new spec", err)
		}
		return
	}
	job, err := d.Merge(ctx, original.Value.RequestID, original.Value.OwnerID, "merge")
	if mode == "gate_hold" {
		if !errors.Is(err, hold) || job.State != "prepared" || !job.ClaimedAt.IsZero() {
			t.Fatal("initial hold consumed physical claim", job, err)
		}
		if _, err := os.Stat(filepath.Join(k.Root, "merge-http")); !os.IsNotExist(err) {
			t.Fatal("held gate created a wrapper spec", err)
		}
		job, err = d.Merge(ctx, original.Value.RequestID, original.Value.OwnerID, "merge")
	}
	expected := "merged"
	switch mode {
	case "post_claim_hold", "cancel":
		expected = "stopped_unsent"
		if err == nil || job.HTTP != nil || !job.MayHaveSentAt.IsZero() || job.Stop == nil || job.Stop.Disposition != "not_started" {
			t.Fatal("failed parent gate did not preserve own unsent proof", job, err)
		}
		if mode == "cancel" && !errors.Is(err, context.Canceled) {
			t.Fatal("parent fence lost cancellation", err)
		}
	case "spec_failure":
		expected = "aborted_http"
		if err == nil || job.HTTPAbort == nil || job.HTTP != nil {
			t.Fatal("failed spec did not seal late native activation", job, err)
		}
	default:
		if err != nil || job.HTTPStop == nil || job.HTTPWrite == nil {
			t.Fatal("driver did not finish through native registered transport", job, err)
		}
	}
	if job.State != expected {
		t.Fatal("wrong parent outcome", job, expected)
	}
	var leaseState string
	if err := k.DB.QueryRow("SELECT state FROM kernel_resource_leases WHERE resource=?", original.MutationLease.Resource).Scan(&leaseState); err != nil || leaseState != "released" {
		t.Fatal("own resolved lease held", leaseState, err)
	}
	seat, err := k.OperationReservation(context.Background(), original.Value.PRReservationID)
	if err != nil || (seat.State == "released") != (expected == "merged") {
		t.Fatal("wrong PR lifetime", seat, err)
	}
	before, _ := os.ReadFile(filepath.Join(bin, "calls"))
	if (expected == "merged" && strings.Count(string(before), "--method PUT") != 1) || (expected != "merged" && len(before) != 0) {
		t.Fatal("unexpected native request", string(before))
	}
	priorCalls := gateCalls
	replay, err := d.Merge(context.Background(), original.Value.RequestID, original.Value.OwnerID, "merge")
	after, _ := os.ReadFile(filepath.Join(bin, "calls"))
	if err != nil || replay.State != job.State || string(after) != string(before) || gateCalls != priorCalls {
		t.Fatal("terminal driver replay performed work", replay, err, string(after), gateCalls)
	}
}
