package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type observedReadAPI struct {
	pr      github.PR
	block   bool
	started chan struct{}
	calls   atomic.Int64
	failed  bool
}

func (a *observedReadAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	a.calls.Add(1)
	if path == "" {
		if a.started != nil {
			select {
			case a.started <- struct{}{}:
			default:
			}
		}
		if a.block {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if a.failed {
			return nil, errors.New("controlled read failure")
		}
		return json.RawMessage(`{"id":17}`), nil
	}
	return json.Marshal(a.pr)
}
func (a *observedReadAPI) ResourcePages(context.Context, string, string) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func observerFixture(t *testing.T, n int) (*storage.KernelStore, map[int]*observedReadAPI) {
	t.Helper()
	ctx := context.Background()
	p, api, saved, id := publicationFixture(t)
	if _, err := p.Publish(ctx, id, "publisher"); err != nil {
		t.Fatal(err)
	}
	readers := map[int]*observedReadAPI{42: {pr: *api.pr}}
	// Seed additional admitted read resources. Mutation admission/PR capacity
	// are tested by Publisher; this fixture only exercises observer scheduling.
	for i := 1; i < n; i++ {
		target := saved.Value
		target.WorkItem.ID = fmt.Sprintf("work-%d", i)
		target.WorkItem.Branch = fmt.Sprintf("harness/work-%d", i)
		target.WorkItem.Candidate = fmt.Sprintf("candidate-%d", i)
		target.Candidate.ID = target.WorkItem.Candidate
		target.PRNumber = 42 + i
		if _, err := p.Store.SetProductTarget(ctx, "", target); err != nil {
			t.Fatal(err)
		}
		binding := storage.PRResourceBinding{ResourceKey: fmt.Sprintf("github:17:pull:%d", target.PRNumber), RepositoryID: 17, Number: target.PRNumber, WaveID: target.Target.WaveID, WorkItemID: target.WorkItem.ID, HeadBranch: target.WorkItem.Branch}
		if err := p.Store.Put(ctx, "product_pr_resource", binding.ResourceKey, binding.WaveID, binding); err != nil {
			t.Fatal(err)
		}
		if err := p.Store.Put(ctx, "product_work_item_pr", binding.WorkItemID, binding.WaveID, binding); err != nil {
			t.Fatal(err)
		}
		pr := *api.pr
		pr.Number = target.PRNumber
		pr.Head.Ref = target.WorkItem.Branch
		readers[target.PRNumber] = &observedReadAPI{pr: pr}
	}
	return p.Store, readers
}
func TestPRObserverBlockedReadDoesNotBlockIndependentResourceOrKernelReads(t *testing.T) {
	k, readers := observerFixture(t, 2)
	readers[42].block = true
	readers[42].started = make(chan struct{}, 1)
	mutated := make(chan struct{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o := Observer{Store: k, API: func(p storage.ProductTarget) ResourceAPI { return readers[p.PRNumber] }, Workers: 2, Every: time.Hour, OnMutation: func() { mutated <- struct{}{} }}
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()
	select {
	case <-readers[42].started:
	case <-time.After(3 * time.Second):
		t.Fatal("blocked resource never started")
	}
	select {
	case <-mutated:
	case <-time.After(3 * time.Second):
		t.Fatal("independent PR stalled behind blocked HTTP")
	}
	if _, err := k.PRResourceState(context.Background(), "github:17:pull:43"); err != nil {
		t.Fatal(err)
	}
	readCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := k.StateEvents(readCtx, 0, 16); err != nil {
		t.Fatal("read worker held a database transaction during external I/O", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("observer did not wait for canceled worker shutdown")
	}
}
func TestPRObserverFailuresDoNotStarveOthersAndReadOnlyRestartsDeduplicate(t *testing.T) {
	k, readers := observerFixture(t, 3)
	readers[42].failed = true
	ctx, cancel := context.WithCancel(context.Background())
	mutations := make(chan struct{}, 8)
	o := Observer{Store: k, API: func(p storage.ProductTarget) ResourceAPI { return readers[p.PRNumber] }, Workers: 1, Every: time.Hour, OnMutation: func() { mutations <- struct{}{} }}
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()
	for i := 0; i < 2; i++ {
		select {
		case <-mutations:
		case <-time.After(3 * time.Second):
			t.Fatal("failed first resource starved later resources")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var before int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_pr_scan' AND json_extract(payload,'$.notification_state')='pending'").Scan(&before); err != nil {
		t.Fatal(err)
	}

	scans := func(number int) int {
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_pr_scan' AND json_extract(payload,'$.resource_key')=?", fmt.Sprintf("github:17:pull:%d", number)).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	before43, before44 := scans(43), scans(44)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { done <- o.Run(ctx) }()
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	for scans(43) < before43+1 || scans(44) < before44+1 {
		select {
		case err := <-done:
			t.Fatal("restart stopped before both unchanged scans committed", err)
		case <-poll.C:
		}
	}
	// Observe the cooldown only after both successful restart scans exist.
	// A fixed total runtime conflates a slow initial read with a busy loop.
	select {
	case err := <-done:
		t.Fatal("restart stopped during cooldown observation", err)
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	var after int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_pr_scan' AND json_extract(payload,'$.notification_state')='pending'").Scan(&after); err != nil || after != before {
		t.Fatal("restart created duplicate changed observations", before, after, err)
	}
	if readers[43].calls.Load() != 6 || readers[44].calls.Load() != 6 {
		t.Fatal("completion loop spun without poll interval", readers[43].calls.Load(), readers[44].calls.Load())
	}
}

func TestPRObservationTargetsPaginateAndKeepAdmittedHeaderAfterCancel(t *testing.T) {
	k, _ := observerFixture(t, 2)
	ctx := context.Background()
	first, err := k.PRObservationTargets(ctx, "", 1)
	if err != nil || len(first.Targets) != 1 || first.NextAfter == "" {
		t.Fatal(first, err)
	}
	second, err := k.PRObservationTargets(ctx, first.NextAfter, 1)
	if err != nil || len(second.Targets) != 1 || second.NextAfter != "" || second.Targets[0].Resource.ResourceKey == first.Targets[0].Resource.ResourceKey {
		t.Fatal(second, err)
	}
	admitted := first.Targets[0]
	p, err := k.ReadProductTarget(ctx, admitted.Product.Target, admitted.Product.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	changed := p.Value
	changed.WorkItem.Branch = "new-canonical-branch"
	changed.WorkItem.Paused = true
	if _, err = k.SetProductTarget(ctx, p.Digest, changed); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "cancel", Target: changed.Target, Action: "cancel", Reason: "continue external fact reconciliation"}); err != nil {
		t.Fatal(err)
	}
	page, err := k.PRObservationTargets(ctx, "", 1)
	if err != nil || len(page.Targets) != 1 || page.Targets[0].Product.WorkItem.Branch != admitted.Resource.HeadBranch || page.Targets[0].Product.PRNumber != admitted.Resource.Number {
		t.Fatal(page, err)
	}
	canonical, err := k.ReadProductTarget(ctx, changed.Target, changed.WorkItem.ID)
	if err != nil || canonical.Value.WorkItem.Branch != "new-canonical-branch" || !canonical.Value.WorkItem.Paused {
		t.Fatal("read target mutated canonical facts", canonical, err)
	}
}
