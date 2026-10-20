package domain

import "testing"

func TestHoldReleaseAndBlockRecovery(t *testing.T) {
	w := WorkItem{ID: "w", Phase: Reviewing, ScopeRevision: 1, PolicyRevision: 1, Candidate: "sha", MergeHold: true}
	w, ops, err := Apply(w, Event{ID: "gate", WorkItemID: "w", Kind: "gates_passed", Candidate: "sha", ScopeRevision: 1, PolicyRevision: 1})
	if err != nil || len(ops) != 1 || CanDispatch(w, ops[0]) {
		t.Fatal("held merge dispatched", err)
	}
	w, _, _ = Apply(w, Event{ID: "release", WorkItemID: "w", Kind: "release"})
	if !CanDispatch(w, ops[0]) {
		t.Fatal("release lost operation")
	}
	w, _, _ = Apply(w, Event{ID: "block", WorkItemID: "w", Kind: "block", Reason: "auth"})
	if w.Phase != Blocked || CanDispatch(w, ops[0]) {
		t.Fatal("block failed")
	}
	w, _, _ = Apply(w, Event{ID: "unblock", WorkItemID: "w", Kind: "unblock"})
	if w.Phase != MergePending || !CanDispatch(w, ops[0]) {
		t.Fatal("block recovery failed")
	}
}
func TestCompletionWhileBlocked(t *testing.T) {
	w := WorkItem{ID: "w", Phase: Implementing, ScopeRevision: 1, PolicyRevision: 1, Attempt: 1}
	w, _, _ = Apply(w, Event{ID: "block", WorkItemID: "w", Kind: "block"})
	w, ops, err := Apply(w, Event{ID: "complete", WorkItemID: "w", Kind: "implementation_ready", Attempt: 1, ScopeRevision: 1, PolicyRevision: 1, Candidate: "sha"})
	if err != nil || w.Phase != Blocked || w.RecoveryPhase != Verifying || w.Candidate != "sha" || len(ops) != 1 {
		t.Fatal("completion lost", w, ops, err)
	}
	if CanDispatch(w, ops[0]) {
		t.Fatal("blocked dispatch")
	}
	w, _, _ = Apply(w, Event{ID: "unblock", WorkItemID: "w", Kind: "unblock"})
	if !CanDispatch(w, ops[0]) {
		t.Fatal("completion not resumed")
	}
}
func TestCanceledMergeConfirmation(t *testing.T) {
	w := WorkItem{ID: "w", Phase: MergePending, Candidate: "sha", ScopeRevision: 1, PolicyRevision: 1, Attempt: 4}
	w, _, _ = Apply(w, Event{ID: "cancel", WorkItemID: "w", Kind: "cancel"})
	w, ops, err := Apply(w, Event{ID: "merged", WorkItemID: "w", Kind: "merge_confirmed", Candidate: "sha", ScopeRevision: 1, PolicyRevision: 1, Attempt: 4})
	if err != nil || w.Phase != Merged || len(ops) != 0 {
		t.Fatal("external merge lost", w, err)
	}
}
