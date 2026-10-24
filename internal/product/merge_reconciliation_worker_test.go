package product

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func checkMergeReconciliationWorker(t *testing.T, reader MergeReconciler, job storage.ProductMerge, api *cloudReadAPI) (storage.ProductMerge, error) {
	t.Helper()
	k := reader.Store
	var before int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&before); err != nil {
		t.Fatal(err)
	}
	// Ordinary open/unmerged waiting reads once, then sleeps until its interval.
	baseline := api.pr
	api.pr.State, api.pr.Merged = "open", false
	var hints, failures int
	w := MergeReconciliationWorker{Reconciler: reader, Workers: 1, Every: time.Hour, OnMutation: func() { hints++ }, Report: func(error) { failures++ }}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := w.Run(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || hints != 0 || failures != 0 || api.metadata != 1 {
		t.Fatal("held reconciliation spun or emitted hints", err, hints, failures, api.metadata)
	}
	api.pr = baseline
	// Even a transport which needs extra cleanup after cancellation must join
	// before the worker returns store/controller ownership to the caller.
	blocked := &joiningCompletionAPI{ResourceAPI: api, started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	w.Reconciler.API = func(storage.ProductTarget) ResourceAPI { return blocked }
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-blocked.started:
	case <-time.After(3 * time.Second):
		t.Fatal("read never started")
	}
	stop()
	select {
	case <-blocked.canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("read not canceled")
	}
	select {
	case err := <-done:
		t.Fatal("returned before read joined", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(blocked.release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// A corrupt first head cannot starve a healthy request with one worker.
	if err := k.Put(context.Background(), "product_merge", "000-damaged", job.Value.Product.Value.Target.WaveID, map[string]any{"merge": map[string]string{"state": "stopped_unknown"}}); err != nil {
		t.Fatal(err)
	}
	w.Reconciler = reader
	api.pulls, api.metadata = 0, 0
	changed := make(chan struct{}, 1)
	reported := make(chan error, 8)
	w.OnMutation = func() {
		saved, err := k.ReadProductMerge(context.Background(), job.ID)
		if err != nil || saved.State != "merged" {
			reported <- errors.New("hint preceded merge fact commit")
		}
		changed <- struct{}{}
	}
	w.Report = func(err error) { reported <- err }
	ctx, stop = context.WithCancel(context.Background())
	defer stop()
	done = make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-changed:
	case <-time.After(3 * time.Second):
		t.Fatal("corrupt first row starved healthy reconciliation")
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	corruptReported := false
drain:
	for {
		select {
		case err := <-reported:
			if !errors.Is(err, storage.ErrTaskEvidence) {
				t.Fatal("unexpected reconciliation failure", err)
			}
			corruptReported = true
		default:
			break drain
		}
	}
	if !corruptReported {
		t.Fatal("corruption not reported")
	}

	var after int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&after); err != nil || before != after {
		t.Fatal("reconciliation launched model", before, after, err)
	}
	return k.ReadProductMerge(context.Background(), job.ID)
}
