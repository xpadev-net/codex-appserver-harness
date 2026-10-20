package storage

import (
	"context"
	"encoding/json"
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
		e := domain.Event{ID: fmt.Sprint(i), WorkItemID: "wi", Kind: v.kind, Attempt: i, Candidate: v.sha, ScopeRevision: 1, PolicyRevision: 1}
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
	s.Apply(ctx, domain.Event{ID: "impl", WorkItemID: "wi", Kind: "implementation_ready", Attempt: 1, Candidate: "sha", ScopeRevision: 1, PolicyRevision: 1})
	ok, err := s.BeginDispatch(ctx, "impl:verify")
	if !ok || err != nil {
		t.Fatal(err)
	}
	if err = s.Apply(ctx, domain.Event{ID: "repair", WorkItemID: "wi", Kind: "repair_tests", Attempt: 2, Candidate: "sha", ScopeRevision: 1, PolicyRevision: 1}); err == nil {
		t.Fatal("overlapping operation accepted")
	}
}
func TestReacquiredLeaseHasOwner(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	g, _ := s.Acquire(ctx, "r", "a", time.Minute)
	s.Release(ctx, "r", "a", g)
	g, err := s.Acquire(ctx, "r", "b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Acquire(ctx, "r", "c", time.Minute); err == nil {
		t.Fatal("reacquired lease stolen")
	}
	if err = s.Release(ctx, "r", "b", g); err != nil {
		t.Fatal(err)
	}
}
func TestPausedRepairAndStaleCompletion(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	s.Claim(ctx, work())
	s.Apply(ctx, domain.Event{ID: "start", WorkItemID: "wi", Kind: "start", ScopeRevision: 1, PolicyRevision: 1})
	s.Apply(ctx, domain.Event{ID: "impl", WorkItemID: "wi", Kind: "implementation_ready", Attempt: 1, Candidate: "sha", ScopeRevision: 1, PolicyRevision: 1})
	s.Apply(ctx, domain.Event{ID: "pause", WorkItemID: "wi", Kind: "pause"})
	if err := s.Apply(ctx, domain.Event{ID: "repair", WorkItemID: "wi", Kind: "repair_tests", Attempt: 2, Candidate: "sha", ScopeRevision: 1, PolicyRevision: 1}); err != nil {
		t.Fatal(err)
	}
	w, _ := s.Get(ctx, "wi")
	if w.Attempt != 3 || w.RepairRounds != 1 {
		t.Fatal("repair lost", w)
	}
	if err := s.Apply(ctx, domain.Event{ID: "old", WorkItemID: "wi", Kind: "candidate_ready", Attempt: 2, Candidate: "old", ScopeRevision: 1, PolicyRevision: 1}); err != nil {
		t.Fatal(err)
	}
	w, _ = s.Get(ctx, "wi")
	if w.Candidate != "sha" {
		t.Fatal("stale completion accepted")
	}
	s.Apply(ctx, domain.Event{ID: "resume", WorkItemID: "wi", Kind: "resume"})
	ok, err := s.BeginDispatch(ctx, "repair:verify")
	if err != nil || !ok {
		t.Fatal("repair not resumed", err)
	}
}
func TestPartialWorkItemClaimTransfer(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	w := work()
	s.Claim(ctx, w)
	next := work()
	next.ID = "next"
	if err := s.ClaimNext(ctx, w.ID, next); err == nil {
		t.Fatal("unmerged predecessor accepted")
	}
	w.Phase = domain.Merged
	b, _ := json.Marshal(w)
	if _, err := s.DB.Exec("UPDATE work_items SET data=? WHERE id=?", b, w.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimNext(ctx, w.ID, next); err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimNext(ctx, w.ID, next); err == nil {
		t.Fatal("claim transferred twice")
	}
	items, _ := s.List(ctx)
	if len(items) != 2 {
		t.Fatal("history lost")
	}
}
func TestRetryRequiresReconciledAbsence(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	s.Claim(ctx, work())
	s.Apply(ctx, domain.Event{ID: "start", WorkItemID: "wi", Kind: "start", ScopeRevision: 1, PolicyRevision: 1})
	s.BeginDispatch(ctx, "start:implement")
	if err := s.RetryUnexecuted(ctx, "start:implement", ""); err == nil {
		t.Fatal("missing evidence accepted")
	}
	if err := s.RetryUnexecuted(ctx, "start:implement", "thread/read proves no matching turn"); err != nil {
		t.Fatal(err)
	}
	ok, err := s.BeginDispatch(ctx, "start:implement")
	if !ok || err != nil {
		t.Fatal("retry unavailable", err)
	}
}
func TestDeliveryAckOnlyCapturedBatch(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	s.Receive(ctx, "first", "pull_request", []byte(`{}`))
	batch, err := s.PendingDeliveryIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Receive(ctx, "late", "pull_request", []byte(`{}`))
	if err = s.AcknowledgeDeliveries(ctx, batch); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingDeliveryIDs(ctx)
	if err != nil || len(pending) != 1 || pending[0] != "late" {
		t.Fatal("late delivery lost", pending, err)
	}
}
func TestChildRegistrationsRejectDuplicatesAndMutation(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	s.Claim(ctx, work())
	if err := s.Apply(ctx, domain.Event{ID: "plan", WorkItemID: "wi", Kind: "plan_split", Ref: "proposal", ScopeRevision: 1, PolicyRevision: 1}); err != nil {
		t.Fatal(err)
	}
	child := work()
	child.ID = "child"
	child.Issue = 2
	registration := ChildRegistration{Item: child, Dependencies: []string{}, Scope: map[string]string{"criterion": "AC"}}
	if err := s.RegisterChildren(ctx, "wi", []ChildRegistration{registration, registration}); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := s.RegisterChildren(ctx, "wi", []ChildRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterChildren(ctx, "wi", []ChildRegistration{registration}); err != nil {
		t.Fatal("replay rejected", err)
	}
	registration.Dependencies = []string{"other"}
	if err := s.RegisterChildren(ctx, "wi", []ChildRegistration{registration}); err == nil {
		t.Fatal("mutated replay accepted")
	}
}
func TestDependenciesBlockDispatch(t *testing.T) {
	s, _ := fixture(t)
	ctx := context.Background()
	w := work()
	s.Claim(ctx, w)
	dep := work()
	dep.ID = "dep"
	dep.Issue = 2
	s.Claim(ctx, dep)
	s.Put(ctx, "dependencies", "wi", "wi", []string{"dep"})
	s.Apply(ctx, domain.Event{ID: "start", WorkItemID: "wi", Kind: "start", ScopeRevision: 1, PolicyRevision: 1})
	ok, err := s.BeginDispatch(ctx, "start:implement")
	if err != nil || ok {
		t.Fatal("dependent work started", err)
	}
	dep.Phase = domain.Merged
	b, _ := json.Marshal(dep)
	s.DB.Exec("UPDATE work_items SET data=? WHERE id=?", b, dep.ID)
	ok, err = s.BeginDispatch(ctx, "start:implement")
	if err != nil || !ok {
		t.Fatal("merged dependency did not release dispatch", err)
	}
}
