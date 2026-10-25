package product

import (
	"context"
	"errors"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestSplitCreationReconciliationWorkerSkipsCorruptionAndHintsAfterCommit(t *testing.T) {
	k, job, api := stoppedSplitGETFixture(t)
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_create','000-corrupt',?,'{')", job.Value.Request.Target.WaveID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_created_resource','000-corrupt-resource',?,'{')", job.Value.Request.Target.WaveID); err != nil {
		t.Fatal(err)
	}
	var reports atomic.Int64
	committed := make(chan struct{})
	var once sync.Once
	worker := SplitCreationReconciliationWorker{Reconciler: SplitCreationReconciler{Store: k, MaxPages: 2, API: func(storage.ProductSplitCreate) ResourceAPI { return api }}, Workers: 2, Every: time.Second,
		Report: func(error) { reports.Add(1) },
		OnMutation: func() {
			saved, err := k.ReadProductSplitCreate(context.Background(), job.ID)
			if err != nil || saved.State != "observed" {
				t.Error("hint preceded committed binding", saved.State, err)
			}
			once.Do(func() { close(committed) })
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-committed:
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("bad entry starved healthy reconciliation")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not join")
	}
	if reports.Load() < 1 {
		t.Fatal("invalid evidence was not reported")
	}
	if len(api.reads()) != 4 {
		t.Fatal("resource index caused duplicate GET", api.reads())
	}
	var runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("read worker invoked a model", runs, err)
	}
}

func TestSplitCreationReconciliationWorkerJoinsCanceledGET(t *testing.T) {
	k, job, api := stoppedSplitGETFixture(t)
	api.mode = "blocked"
	api.entered = make(chan struct{})
	var hints, reports atomic.Int64
	worker := SplitCreationReconciliationWorker{Reconciler: SplitCreationReconciler{Store: k, MaxPages: 2, API: func(storage.ProductSplitCreate) ResourceAPI { return api }}, Workers: 2, Every: time.Second, OnMutation: func() { hints.Add(1) }, Report: func(error) { reports.Add(1) }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-api.entered:
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("GET did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("GET outlived worker")
	}
	if hints.Load() != 0 || reports.Load() != 0 {
		t.Fatal("ordinary cancellation emitted mutation or operational fault", hints.Load(), reports.Load())
	}
	saved, err := k.ReadProductSplitCreate(context.Background(), job.ID)
	if err != nil || saved.State != "stopped" || saved.Issue != nil {
		t.Fatal("canceled GET bound an Issue", saved, err)
	}
}

func TestSplitCreationGETWorkerNeverInterruptsActiveSender(t *testing.T) {
	ctx := context.Background()
	k, job, policy, bin := splitHTTPFixture(t, "direct")
	t.Setenv("HARNESS_SPLIT_HTTP_MODE", "hang")
	transport := splitTestTransport(t, k, policy)
	spec, err := WriteSplitHTTPSpec(ctx, k, job.ID, policy)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(transport.Executable, "_split-http", spec.Path())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	identity := process.Identify(cmd.Process.Pid)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = process.StopGroup(cleanup, identity)
		_ = cmd.Wait()
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		current, err := k.ReadProductSplitCreate(ctx, job.ID)
		if err == nil && current.State == "unknown" && splitHTTPCalls(t, bin) != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	api := &splitGETFixtureAPI{job: job}
	worker := SplitCreationReconciliationWorker{Reconciler: SplitCreationReconciler{Store: k, MaxPages: 2, API: func(storage.ProductSplitCreate) ResourceAPI { return api }}, Workers: 1, Every: 10 * time.Millisecond}
	running, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(running) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("GET worker failed to join")
	}
	alive, err := process.GroupAlive(identity)
	current, readErr := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || !alive || readErr != nil || current.State != "unknown" || current.Stop != nil || len(api.reads()) != 0 {
		t.Fatal("GET worker interfered with active POST", alive, current.State, api.reads(), err, readErr)
	}
	if stopped, err := transport.Recover(ctx, job.ID); err != nil || stopped.State != "stopped" {
		t.Fatal("sending supervisor could not recover owner", stopped, err)
	}
}
