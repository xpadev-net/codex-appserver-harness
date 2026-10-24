package product

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func checkMergeDispatchWorker(t *testing.T, g KernelMergeGate, saved storage.ProductTargetSnapshot, requestID, bin, httpMode, mode string) {
	t.Helper()
	ctx := context.Background()
	k := g.Cloud.Store
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("HTTP_WRAPPER_DATA", bin)
	t.Setenv("HTTP_WRAPPER_MODE", httpMode)
	var before int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&before); err != nil {
		t.Fatal(err)
	}
	d := MergeDriver{Store: k, Gate: g.Check, Transport: MergeHTTPTransport{Store: k, Executable: exe}}
	w := MergeDispatchWorker{Driver: d, Methods: map[int64]string{saved.Value.WorkItem.RepositoryID: "merge"}, Workers: 2, Every: time.Hour}
	var target storage.ProductMerge
	if mode == "recover" || mode == "stale" {
		target, err = k.PrepareProductMergeHTTP(ctx, requestID, "saved-worker-owner", "merge")
		if err != nil {
			t.Fatal(err)
		}
		if mode == "recover" {
			if _, err := k.ClaimProductMerge(ctx, target.ID, target.Value.OwnerID, target.Digest); err != nil {
				t.Fatal(err)
			}
		} else {
			p := saved.Value
			p.Candidate.ID, p.WorkItem.Candidate = "next-worker-candidate", "next-worker-candidate"
			p.WorkItem.Version++
			if _, err := k.SetProductTarget(ctx, saved.Digest, p); err != nil {
				t.Fatal(err)
			}
		}
	}
	if mode == "hold" {
		var calls atomic.Int64
		w.Driver.Gate = func(context.Context, storage.ProductTargetSnapshot, string) (storage.ProductMergeGateObservation, error) {
			calls.Add(1)
			return storage.ProductMergeGateObservation{}, storage.ErrProductMergeHeld
		}
		var hints, failures int
		w.OnMutation = func() { hints++ }
		w.Report = func(error) { failures++ }
		wait, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		err := w.Run(wait)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 || hints != 0 || failures != 0 {
			t.Fatal("formal hold spun, emitted a hint or acquired work", err, calls.Load(), hints, failures)
		}
		var journals int
		if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_merge'").Scan(&journals); err != nil || journals != 0 {
			t.Fatal("unapproved request reserved a merge mutation", journals, err)
		}
		// A canceled host gate must finish cleanup before Run returns.
		started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		w.Driver.Gate = func(ctx context.Context, _ storage.ProductTargetSnapshot, _ string) (storage.ProductMergeGateObservation, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release
			return storage.ProductMergeGateObservation{}, ctx.Err()
		}
		run, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan error, 1)
		go func() { done <- w.Run(run) }()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("gate not dispatched")
		}
		stop()
		select {
		case <-canceled:
		case <-time.After(5 * time.Second):
			t.Fatal("gate not canceled")
		}
		select {
		case err := <-done:
			t.Fatal("worker returned before joining gate", err)
		case <-time.After(30 * time.Millisecond):
		}
		close(release)
		if err := <-done; !errors.Is(err, context.Canceled) || failures != 0 {
			t.Fatal("cancellation reported as a failed dispatch", err, failures)
		}
	} else {
		if mode == "request" {
			// A corrupt first journal cannot starve the later healthy request.
			if err := k.Put(ctx, "product_merge", "000-damaged", saved.Value.Target.WaveID, map[string]any{"merge": map[string]string{"state": "bad"}}); err != nil {
				t.Fatal(err)
			}
			w.Workers = 1
		}
		if mode == "alias" {
			w.Every = 10 * time.Millisecond
		}
		expectedID := target.ID
		if expectedID == "" {
			page, err := k.MergeDispatchTargets(ctx, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range page.Targets {
				if entry.Kind == "request" && entry.ID == requestID {
					expectedID = entry.MergeID
				}
			}
			if expectedID == "" {
				t.Fatal("original request absent from discovery")
			}
		}
		run, stop := context.WithTimeout(ctx, 15*time.Second)
		defer stop()
		hints := make(chan struct{}, 8)
		reported := make(chan error, 16)
		w.Report = func(err error) { reported <- err }
		w.OnMutation = func() {
			committed, err := k.ReadProductMerge(ctx, expectedID)
			if err != nil || (committed.Stop == nil && committed.HTTPAbort == nil && committed.Cancellation == nil) {
				reported <- errors.New("terminal hint preceded journal commit")
			}
			if committed.State == "merged" {
				if _, err := k.ReadProductMergedWorkItem(ctx, expectedID); err != nil {
					reported <- errors.New("merge hint preceded authenticated WorkItem receipt")
				}
			}
			hints <- struct{}{}
		}
		done := make(chan error, 1)
		go func() { done <- w.Run(run) }()
		if mode == "alias" {
			deadline := time.Now().Add(8 * time.Second)
			for {
				page, err := k.MergeDispatchTargets(ctx, "", 128)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range page.Targets {
					if entry.Kind == "merge" {
						job, err := k.ReadProductMerge(ctx, entry.ID)
						if err == nil && job.Value.RequestID == requestID && job.HTTP != nil {
							target = job
						}
					}
				}
				if target.HTTP != nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("queued wrapper never registered")
				}
				time.Sleep(5 * time.Millisecond)
			}
			// Several scans can now see a merge: row instead of request: row.
			// The common flight key must keep those scans from recovering it.
			time.Sleep(60 * time.Millisecond)
			live, err := k.ReadProductMerge(ctx, target.ID)
			if err != nil || live.State != "unknown" || live.HTTPAbort != nil || live.Stop != nil {
				t.Fatal("worker recovered its own in-flight transport", live, err)
			}
			if err := os.WriteFile(filepath.Join(bin, "release"), []byte("continue"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-hints:
		case <-run.Done():
			t.Fatal("queued operation did not finish", run.Err())
		}
		stop()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		var failureList []error
	drain:
		for {
			select {
			case err := <-reported:
				failureList = append(failureList, err)
			default:
				break drain
			}
		}
		if mode == "request" {
			if len(failureList) != 1 || !errors.Is(failureList[0], storage.ErrTaskEvidence) {
				t.Fatal("corrupt row not independently reported", failureList)
			}
		} else if len(failureList) != 0 {
			t.Fatal(failureList)
		}
		if target.ID == "" {
			target, err = k.PrepareProductMergeHTTP(ctx, requestID, dispatchOwner("merge:"+requestID), "merge")
		} else {
			target, err = k.ReadProductMerge(ctx, target.ID)
		}
		expected := "merged"
		if mode == "recover" {
			expected = "aborted_http"
		}
		if mode == "stale" {
			expected = "canceled"
		}
		if err != nil || target.State != expected {
			t.Fatal("wrong queued result", target, err, expected)
		}
		if mode == "request" || mode == "alias" {
			calls, err := os.ReadFile(filepath.Join(bin, "calls"))
			if err != nil || strings.Count(string(calls), "--method PUT") != 1 || target.HTTPStop == nil {
				t.Fatal("missing/duplicate native PUT or missing actual stop", string(calls), err, target)
			}
			receipt, err := k.ReadProductMergedWorkItem(ctx, target.ID)
			current, currentErr := k.ReadProductTarget(ctx, saved.Value.Target, saved.Value.WorkItem.ID)
			if err != nil || currentErr != nil || receipt.Value.Projection == nil || current.Value.WorkItem.Phase != domain.Merged || current.Value.Candidate != saved.Value.Candidate {
				t.Fatal("native result not reflected into original WorkItem", receipt, current, err, currentErr)
			}
			if mode == "alias" {
				checkFinalPlanner(t, k, saved.Value.Target)
			}
		} else if _, err := os.Stat(filepath.Join(bin, "calls")); !os.IsNotExist(err) {
			t.Fatal("recovery/cleanup launched a native client", err)
		}
	}
	var after int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&after); err != nil || before != after {
		t.Fatal("merge worker launched a model", before, after, err)
	}
}

func TestMergeWorkerSoleCancellationPreservesJoinedFailures(t *testing.T) {
	if !mergeSoleCause(errors.Join(context.Canceled), context.Canceled) || mergeSoleCause(errors.Join(context.Canceled, errors.New("stop unconfirmed")), context.Canceled) || mergeSoleCause(errors.New("context canceled"), context.Canceled) {
		t.Fatal("joined unrelated failure suppressed")
	}
}
