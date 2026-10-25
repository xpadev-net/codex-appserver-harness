package product

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type serializedFinalSourceAPI struct {
	mu     sync.Mutex
	source *splitSourceFixtureAPI
}

func (a *serializedFinalSourceAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.source.ResourceGET(ctx, path)
}
func (a *serializedFinalSourceAPI) ResourcePages(ctx context.Context, path, field string) ([]json.RawMessage, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.source.ResourcePages(ctx, path, field)
}

type blockingFinalSourceAPI struct {
	active  atomic.Int32
	entered chan struct{}
}

func (a *blockingFinalSourceAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if path == "" {
		return json.Marshal(Repository{ID: 17, Name: "owner/repo"})
	}
	a.active.Add(1)
	defer a.active.Add(-1)
	a.entered <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (a *blockingFinalSourceAPI) ResourcePages(context.Context, string, string) ([]json.RawMessage, error) {
	return nil, errors.New("unexpected pagination")
}

func TestFinalSplitSourceObservationRefreshesWholeTreeWithoutGrantingAcceptance(t *testing.T) {
	observer, api, registered, _ := splitChildSourceFixture(t)
	ctx := context.Background()
	target := registered.Value.Request.Target
	now := observer.Store.Now
	// Controlled metadata/source acceptance uses a fixed source clock; it is not
	// a real-clock latency benchmark or evidence of completed split merges.
	fixed := time.Now().UTC()
	if now != nil {
		fixed = now()
	}
	observer.Store.Now = func() time.Time { return fixed }
	safe := &serializedFinalSourceAPI{source: api}
	gate := KernelSourceGate{Store: observer.Store, Policies: map[int64]IssueSourcePolicy{17: observer.Policy}, API: func(storage.ProductTarget) ResourceAPI { return safe }}
	before, err := observer.Store.WaveOperationBudget(ctx, target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.CheckFinalSplitSources(ctx, target); err != nil {
		t.Fatal("open tree did not refresh", err)
	}
	proof, err := observer.Store.ReadProductSplitFinalSources(ctx, target)
	if err != nil || proof == nil || len(proof.Value.Sources) != 2 {
		t.Fatal("whole tree source proof unavailable", proof, err)
	}
	coverage, err := observer.Store.ProductCoverage(ctx, target)
	if err != nil || len(coverage.Value.Merged) != 0 || len(coverage.Value.Residual) == 0 {
		t.Fatal("source observations created merge coverage", coverage, err)
	}
	for _, mode := range []string{"edited", "closed", "hold", "identity"} {
		t.Run(mode, func(t *testing.T) {
			api.mode = mode
			if err := gate.CheckFinalSplitSources(ctx, target); err == nil {
				t.Fatal("invalid child source accepted", mode)
			}
			api.mode = ""
			if err := gate.CheckFinalSplitSources(ctx, target); err != nil {
				t.Fatal("restored open sources unavailable", err)
			}
		})
	}
	t.Run("cancel_joins_every_GET_before_return", func(t *testing.T) {
		before, err := observer.Store.ReadProductSplitFinalSources(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		api := &blockingFinalSourceAPI{entered: make(chan struct{}, 4)}
		blocked := gate
		blocked.API = func(storage.ProductTarget) ResourceAPI { return api }
		run, cancel := context.WithCancel(ctx)
		defer cancel()
		finished := make(chan error, 1)
		go func() { finished <- blocked.CheckFinalSplitSources(run, target) }()
		for i := 0; i < 2; i++ {
			select {
			case <-api.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("parallel source GET did not start")
			}
		}
		cancel()
		select {
		case err := <-finished:
			if !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation lost", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("source cancellation did not join")
		}
		if api.active.Load() != 0 {
			t.Fatal("GET survived source gate return", api.active.Load())
		}
		after, err := observer.Store.ReadProductSplitFinalSources(ctx, target)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("canceled observation saved detached freshness", err)
		}
	})
	after, err := observer.Store.WaveOperationBudget(ctx, target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("source check changed budget", err)
	}
	// The final gate still requires real complete coverage before source routing.
	final := KernelFinalGate{Executor: Executor{Store: observer.Store}, Source: gate.Check, SplitSources: gate.CheckFinalSplitSources}
	if err := final.Check(ctx, target); !errors.Is(err, storage.ErrProductFinalPlanHeld) {
		t.Fatal("source-only check granted final acceptance", err)
	}
}
