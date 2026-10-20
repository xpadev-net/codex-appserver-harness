// Package domain contains workflow decisions without external side effects.
package domain

import (
	"errors"
	"fmt"
)

type Phase string

const (
	Ready        Phase = "READY"
	Implementing Phase = "IMPLEMENTING"
	Verifying    Phase = "VERIFYING"
	Validating   Phase = "VALIDATING"
	Reviewing    Phase = "REVIEWING"
	MergePending Phase = "MERGE_PENDING"
	Merged       Phase = "MERGED"
	Split        Phase = "SPLIT"
	Blocked      Phase = "BLOCKED"
	Canceled     Phase = "CANCELED"
)

type WorkItem struct {
	ID             string `json:"id"`
	RepositoryID   int64  `json:"repository_id"`
	Issue          int    `json:"issue"`
	ScopeRevision  int    `json:"scope_revision"`
	PolicyRevision int    `json:"policy_revision"`
	Phase          Phase  `json:"phase"`
	RecoveryPhase  Phase  `json:"recovery_phase,omitempty"`
	Paused         bool   `json:"paused"`
	MergeHold      bool   `json:"merge_hold"`
	Reason         string `json:"reason,omitempty"`
	Candidate      string `json:"candidate,omitempty"`
	Branch         string `json:"branch,omitempty"`
	Worktree       string `json:"worktree,omitempty"`
	RepairRounds   int    `json:"repair_rounds"`
	Attempt        int    `json:"attempt"`
	Version        int    `json:"version"`
}
type Event struct {
	ID             string `json:"id"`
	WorkItemID     string `json:"work_item_id"`
	Kind           string `json:"kind"`
	Attempt        int    `json:"attempt"`
	Candidate      string `json:"candidate,omitempty"`
	ScopeRevision  int    `json:"scope_revision"`
	PolicyRevision int    `json:"policy_revision"`
	Reason         string `json:"reason,omitempty"`
}
type Operation struct {
	ID             string `json:"id"`
	WorkItemID     string `json:"work_item_id"`
	Kind           string `json:"kind"`
	Candidate      string `json:"candidate"`
	ScopeRevision  int    `json:"scope_revision"`
	PolicyRevision int    `json:"policy_revision"`
	Attempt        int    `json:"attempt"`
	State          string `json:"state"`
}

func (w WorkItem) Terminal() bool {
	return w.Phase == Merged || w.Phase == Canceled || w.Phase == Split
}

// Apply accepts controller facts, never agent claims of external success.
func Apply(w WorkItem, e Event) (WorkItem, []Operation, error) {
	if e.WorkItemID != w.ID || e.ID == "" {
		return w, nil, errors.New("invalid event identity")
	}
	emit := ""
	switch e.Kind {
	case "pause":
		w.Paused = true
	case "resume":
		w.Paused = false
	case "hold":
		w.MergeHold = true
	case "release":
		w.MergeHold = false
	case "cancel":
		if w.Phase != Merged {
			w.Phase = Canceled
			w.Reason = e.Reason
		}
	case "unblock":
		if w.Phase == Blocked && w.RecoveryPhase != "" {
			w.Phase = w.RecoveryPhase
			w.RecoveryPhase = ""
			w.Reason = ""
		}
	case "block":
		if !w.Terminal() {
			if w.Phase != Blocked {
				w.RecoveryPhase = w.Phase
			}
			w.Phase = Blocked
			w.Reason = e.Reason
		}
	default:
		if w.Terminal() {
			return w, nil, nil
		}
		if e.Attempt != w.Attempt || e.ScopeRevision != w.ScopeRevision || e.PolicyRevision != w.PolicyRevision {
			return w, nil, nil
		}
		if e.Kind != "candidate_ready" && e.Kind != "implementation_ready" && e.Candidate != w.Candidate {
			return w, nil, nil
		}
		switch e.Kind {
		case "start":
			if w.Phase == Ready {
				w.Phase = Implementing
				emit = "implement"
			}
		case "implementation_ready":
			if w.Phase == Implementing && e.Candidate != "" {
				w.Candidate = e.Candidate
				w.Phase = Verifying
				emit = "verify"
			}
		case "candidate_ready":
			if w.Phase == Verifying && e.Candidate != "" {
				w.Candidate = e.Candidate
				w.Phase = Validating
				emit = "validate"
			}
		case "validation_passed":
			if w.Phase == Validating {
				w.Phase = Reviewing
				emit = "publish"
			}
		case "repair_implementation":
			if w.Phase == Reviewing || w.Phase == Verifying || w.Phase == Validating {
				w.RepairRounds++
				w.Phase = Implementing
				emit = "implement"
			}
		case "repair_tests":
			if w.Phase == Reviewing || w.Phase == Verifying || w.Phase == Validating {
				w.RepairRounds++
				w.Phase = Verifying
				emit = "verify"
			}
		case "gates_passed":
			if w.Phase == Reviewing {
				w.Phase = MergePending
				emit = "merge"
			}
		case "merge_confirmed":
			if w.Phase == MergePending {
				w.Phase = Merged
			}
		default:
			return w, nil, fmt.Errorf("unknown event %q", e.Kind)
		}
	}
	w.Version++
	if emit == "" {
		return w, nil, nil
	}
	w.Attempt++
	op := Operation{Attempt: w.Attempt, ID: e.ID + ":" + emit, WorkItemID: w.ID, Kind: emit, Candidate: w.Candidate, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, State: "pending"}
	return w, []Operation{op}, nil
}
func CanDispatch(w WorkItem, o Operation) bool {
	if o.Attempt != w.Attempt || w.Terminal() || w.ScopeRevision != o.ScopeRevision || w.PolicyRevision != o.PolicyRevision || w.Candidate != o.Candidate {
		return false
	}
	switch o.Kind {
	case "implement":
		return !w.Paused && w.Phase == Implementing
	case "verify":
		return !w.Paused && w.Phase == Verifying
	case "validate":
		return w.Phase == Validating
	case "publish", "review":
		return w.Phase == Reviewing
	case "merge":
		return w.Phase == MergePending && !w.MergeHold
	}
	return false
}
