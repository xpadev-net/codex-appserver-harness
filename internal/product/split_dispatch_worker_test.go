package product

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func splitDispatchFixture(t *testing.T, k *storage.KernelStore) SplitCreationDispatchWorker {
	t.Helper()
	transport := splitTestTransport(t, k, nil)
	return SplitCreationDispatchWorker{Store: k, Executable: transport.Executable, Repositories: map[int64]string{17: "owner/repo"}, Workers: 2, Every: 10 * time.Millisecond}
}
func TestSplitCreationDispatchPipelineSealsOldSpecRetriesAndGETBinds(t *testing.T) {
	ctx := context.Background()
	k, initial, _, bin := splitHTTPFixture(t, "direct")
	if _, err := WriteSplitHTTPSpec(ctx, k, initial.ID, nil); err != nil {
		t.Fatal(err)
	}
	d := splitDispatchFixture(t, k)
	committed := make(chan struct{})
	var once sync.Once
	api := &splitGETFixtureAPI{job: initial}
	get := SplitCreationReconciliationWorker{Reconciler: SplitCreationReconciler{Store: k, MaxPages: 2, API: func(storage.ProductSplitCreate) ResourceAPI { return api }}, Workers: 2, Every: 10 * time.Millisecond, OnMutation: func() { once.Do(func() { close(committed) }) }}
	running, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 2)
	go func() { done <- d.Run(running) }()
	go func() { done <- get.Run(running) }()
	select {
	case <-committed:
	case <-time.After(8 * time.Second):
		cancel()
		<-done
		<-done
		t.Fatal("durable split pipeline did not bind child")
	}
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("pipeline did not join")
		}
	}
	latest, err := k.ReadLatestProductSplitCreate(ctx, initial.Value.Child.ID)
	if err != nil || latest.State != "observed" || latest.Value.Attempt != 1 || latest.Value.Child.Value.Marker != initial.Value.Child.Value.Marker {
		t.Fatal("wrong resumed intent", latest, err)
	}
	old, err := k.ReadProductSplitCreate(ctx, initial.ID)
	if err != nil || old.State != "aborted" || old.HTTP != nil {
		t.Fatal("old spec gained send authority", old, err)
	}
	if strings.Count(splitHTTPCalls(t, bin), "POST\n") != 1 {
		t.Fatal("old/new attempts caused duplicate POST", splitHTTPCalls(t, bin))
	}
	budget, err := k.WaveOperationBudget(ctx, initial.Value.Request.Target.WaveID)
	if err != nil || budget.SplitCreateRetries != 1 {
		t.Fatal("retry not counted", budget, err)
	}
	var registrations, runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&registrations); err != nil || registrations != 1 {
		t.Fatal("dispatch granted child execution", registrations, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("dispatch invoked model", runs, err)
	}
}

func TestSplitCreationDispatchPauseAndRepositoryAllowlistHoldWithoutLaunch(t *testing.T) {
	ctx := context.Background()
	k, job, _, bin := splitHTTPFixture(t, "direct")
	d := splitDispatchFixture(t, k)
	d.Repositories = map[int64]string{18: "another/repo"}
	if changed, err := d.process(ctx, job.ID); changed || !errors.Is(err, storage.ErrProductSplitHeld) || splitHTTPCalls(t, bin) != "" {
		t.Fatal("nonallowlisted repo launched", changed, err)
	}
	d.Repositories = map[int64]string{17: "owner/repo"}
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause-dispatch", Target: job.Value.Request.Target, Action: "pause", Reason: "hold new Issue"}); err != nil {
		t.Fatal(err)
	}
	if changed, err := d.process(ctx, job.ID); changed || !errors.Is(err, storage.ErrProductSplitHeld) || splitHTTPCalls(t, bin) != "" {
		t.Fatal("pause launched", changed, err)
	}
	current, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || current.State != "prepared" || current.HTTP != nil {
		t.Fatal("hold consumed intent", current, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "resume-dispatch", Target: job.Value.Request.Target, Action: "resume", Reason: "allow new Issue"}); err != nil {
		t.Fatal(err)
	}
	if changed, err := d.process(ctx, job.ID); !changed || err != nil {
		t.Fatal("resume did not dispatch", changed, err)
	}
	if changed, err := d.process(ctx, job.ID); changed || err != nil {
		t.Fatal("stopped outcome dispatched again", changed, err)
	}
	if strings.Count(splitHTTPCalls(t, bin), "POST\n") != 1 {
		t.Fatal("resume duplicated POST")
	}
}

func TestSplitCreationDispatchRecoversUnknownOwnerEvenAfterAllowlistRemoval(t *testing.T) {
	ctx := context.Background()
	k, job, _, bin := splitHTTPFixture(t, "direct")
	t.Setenv("HARNESS_SPLIT_HTTP_MODE", "hang")
	spec, err := WriteSplitHTTPSpec(ctx, k, job.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	d := splitDispatchFixture(t, k)
	cmd := exec.Command(d.Executable, "_split-http", spec.Path())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	identity := process.Identify(cmd.Process.Pid)
	defer func() {
		cleanup, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_ = process.StopGroup(cleanup, identity)
		_ = cmd.Wait()
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		current, err := k.ReadProductSplitCreate(ctx, job.ID)
		if err == nil && current.State == "unknown" && strings.Contains(splitHTTPCalls(t, bin), "POST\n") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	d.Repositories = map[int64]string{18: "replacement/repo"}
	changed, err := d.process(ctx, job.ID)
	if err != nil || !changed {
		t.Fatal("old owner not recovered", changed, err)
	}
	current, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || current.State != "stopped" || current.Issue != nil {
		t.Fatal("unknown result inferred known", current, err)
	}
	alive, err := process.GroupAlive(identity)
	if err != nil || alive {
		t.Fatal("original sender still live", alive, err)
	}
	if strings.Count(splitHTTPCalls(t, bin), "POST\n") != 1 {
		t.Fatal("unknown owner was resent")
	}
}

func TestSplitCreationDispatchHintsRequireCommittedEvidence(t *testing.T) {
	k, job, _, _ := splitHTTPFixture(t, "direct")
	d := splitDispatchFixture(t, k)
	// A failed storage Commit may return this in-memory terminal value while
	// the durable record remains prepared. It must never generate a hint.
	returned := job
	returned.State = "aborted"
	returned.Abort = &storage.ProductSplitCreateStop{ProofID: "not-committed", VerifiedAt: time.Now().UTC()}
	failure := errors.New("fixture commit failure")
	if changed, err := d.finish(returned, failure); changed || !errors.Is(err, failure) {
		t.Fatal("uncommitted output generated hint", changed, err)
	}
}

func TestSplitCreationDispatchCancellationJoinsSenderBeforeReturning(t *testing.T) {
	ctx := context.Background()
	k, job, _, bin := splitHTTPFixture(t, "direct")
	t.Setenv("HARNESS_SPLIT_HTTP_MODE", "hang")
	d := splitDispatchFixture(t, k)
	running, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(running) }()
	deadline := time.Now().Add(5 * time.Second)
	var identity process.Identity
	for time.Now().Before(deadline) {
		current, err := k.ReadProductSplitCreate(ctx, job.ID)
		if err == nil && current.HTTP != nil && strings.Contains(splitHTTPCalls(t, bin), "POST\n") {
			identity = current.HTTP.Process
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !identity.Valid() {
		cancel()
		<-done
		t.Fatal("sender did not activate")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("dispatch returned before cleanup joined")
	}
	alive, err := process.GroupAlive(identity)
	if err != nil || alive {
		t.Fatal("sender outlived controller", alive, err)
	}
	current, err := k.ReadProductSplitCreate(ctx, job.ID)
	if err != nil || current.State != "stopped" {
		t.Fatal("stopping not durable", current, err)
	}
}
