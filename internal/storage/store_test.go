package storage

import (
	"context"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func fixture(t *testing.T) (*Store, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "state.db")
	s, e := Open(p)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, p
}
func work() domain.WorkItem {
	return domain.WorkItem{ID: "wi", RepositoryID: 1, Issue: 1, ScopeRevision: 1, PolicyRevision: 1, Phase: domain.Ready}
}
func TestConcurrentClaim(t *testing.T) {
	s, _ := fixture(t)
	var wg sync.WaitGroup
	success := make(chan bool, 10)
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := work()
			w.ID = fmt.Sprint(i)
			success <- s.Claim(context.Background(), w) == nil
		}()
	}
	wg.Wait()
	close(success)
	n := 0
	for ok := range success {
		if ok {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("claims=%d", n)
	}
}
func TestReplayAndRestart(t *testing.T) {
	s, p := fixture(t)
	ctx := context.Background()
	if err := s.Claim(ctx, work()); err != nil {
		t.Fatal(err)
	}
	phases := []struct{ kind, sha string }{{"start", ""}, {"implementation_ready", "impl"}, {"candidate_ready", "final"}, {"validation_passed", "final"}, {"gates_passed", "final"}, {"merge_confirmed", "final"}}
	for i, v := range phases {
		e := domain.Event{ID: fmt.Sprint(i), WorkItemID: "wi", Kind: v.kind, Candidate: v.sha, ScopeRevision: 1, PolicyRevision: 1}
		if err := s.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
		if err := s.Apply(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	s2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	w, err := s2.Get(ctx, "wi")
	if err != nil || w.Phase != domain.Merged {
		t.Fatalf("%+v %v", w, err)
	}
	ops, err := s2.Operations(ctx)
	if err != nil || len(ops) != 1 {
		t.Fatalf("ops %d %v", len(ops), err)
	}
}
func TestDispatchUnknownAndOperatorHold(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	s.Claim(ctx, work())
	s.Apply(ctx, domain.Event{ID: "start", WorkItemID: "wi", Kind: "start", ScopeRevision: 1, PolicyRevision: 1})
	s.Apply(ctx, domain.Event{ID: "pause", WorkItemID: "wi", Kind: "pause"})
	ok, err := s.BeginDispatch(ctx, "start:implement")
	if err != nil || ok {
		t.Fatal("paused dispatch")
	}
	s.Apply(ctx, domain.Event{ID: "resume", WorkItemID: "wi", Kind: "resume"})
	ok, err = s.BeginDispatch(ctx, "start:implement")
	if err != nil || !ok {
		t.Fatal("dispatch rejected", err)
	}
	if err = s.FinishOperation(ctx, "start:implement", "unknown", nil); err != nil {
		t.Fatal(err)
	}
	ok, err = s.BeginDispatch(ctx, "start:implement")
	if err != nil || ok {
		t.Fatal("unknown operation resent")
	}
}
func TestLeaseRequiresReconciliation(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	g, err := s.Acquire(ctx, "turn", "a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Acquire(ctx, "turn", "b", time.Minute); err == nil {
		t.Fatal("lease stolen")
	}
	if err = s.Release(ctx, "turn", "a", g+1); err == nil {
		t.Fatal("stale generation accepted")
	}
	if err = s.Release(ctx, "turn", "a", g); err != nil {
		t.Fatal(err)
	}
}
func TestStaleEventAndCanceledDispatch(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	s.Claim(ctx, work())
	s.Apply(ctx, domain.Event{ID: "stale", WorkItemID: "wi", Kind: "start", ScopeRevision: 2, PolicyRevision: 1})
	w, _ := s.Get(ctx, "wi")
	if w.Phase != domain.Ready {
		t.Fatal("stale event applied")
	}
	s.Apply(ctx, domain.Event{ID: "start", WorkItemID: "wi", Kind: "start", ScopeRevision: 1, PolicyRevision: 1})
	s.Apply(ctx, domain.Event{ID: "cancel", WorkItemID: "wi", Kind: "cancel"})
	ok, err := s.BeginDispatch(ctx, "start:implement")
	if ok || err != nil {
		t.Fatal("canceled dispatch")
	}
}

func TestLeaseGenerationSurvivesRelease(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	g1, err := s.Acquire(ctx, "r", "a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Release(ctx, "r", "a", g1); err != nil {
		t.Fatal(err)
	}
	g2, err := s.Acquire(ctx, "r", "a", time.Minute)
	if err != nil || g2 <= g1 {
		t.Fatal("generation reset", g1, g2, err)
	}
	if err = s.Release(ctx, "r", "a", g1); err == nil {
		t.Fatal("stale release succeeded")
	}
}
func TestInflightOperationBlocksRepair(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	s.Claim(ctx, work())
	s.Apply(ctx, domain.Event{ID: "start", WorkItemID: "wi", Kind: "start", ScopeRevision: 1, PolicyRevision: 1})
	s.Apply(ctx, domain.Event{ID: "impl", WorkItemID: "wi", Kind: "implementation_ready", Candidate: "sha", ScopeRevision: 1, PolicyRevision: 1})
	ok, err := s.BeginDispatch(ctx, "impl:verify")
	if !ok || err != nil {
		t.Fatal(err)
	}
	if err = s.Apply(ctx, domain.Event{ID: "repair", WorkItemID: "wi", Kind: "repair_tests", Candidate: "sha", ScopeRevision: 1, PolicyRevision: 1}); err == nil {
		t.Fatal("overlapping operation accepted")
	}
}
