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
