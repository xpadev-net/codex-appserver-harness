// Package domain contains workflow decisions without external side effects.
package domain

import (
	"errors"
	"fmt"
	"time"
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
	CreatedAt       time.Time        `json:"created_at"`
	PhaseStartedAt  time.Time        `json:"phase_started_at"`
	MergeObservedAt time.Time        `json:"merge_observed_at"`
	PhaseDurations  map[string]int64 `json:"phase_duration_nanoseconds"`
	ID              string           `json:"id"`
	RepositoryID    int64            `json:"repository_id"`
	Issue           int              `json:"issue"`
	ScopeRevision   int              `json:"scope_revision"`
	PolicyRevision  int              `json:"policy_revision"`
	Phase           Phase            `json:"phase"`
	CanceledFrom    Phase            `json:"canceled_from,omitempty"`
	SplitPlanID     string           `json:"split_plan_id,omitempty"`
	LastCompletion  *Event           `json:"last_completion,omitempty"`
	RecoveryPhase   Phase            `json:"recovery_phase,omitempty"`
	Paused          bool             `json:"paused"`
	MergeHold       bool             `json:"merge_hold"`
	Reason          string           `json:"reason,omitempty"`
	Candidate       string           `json:"candidate,omitempty"`
	Branch          string           `json:"branch,omitempty"`
	Worktree        string           `json:"worktree,omitempty"`
	RepairRounds    int              `json:"repair_rounds"`
	Attempt         int              `json:"attempt"`
	Version         int              `json:"version"`
}
type Event struct {
	ObservedAt     time.Time `json:"observed_at,omitempty"`
	ID             string    `json:"id"`
	WorkItemID     string    `json:"work_item_id"`
	Kind           string    `json:"kind"`
	Attempt        int       `json:"attempt"`
	Candidate      string    `json:"candidate,omitempty"`
	ScopeRevision  int       `json:"scope_revision"`
	PolicyRevision int       `json:"policy_revision"`
	Reason         string    `json:"reason,omitempty"`
	Ref            string    `json:"ref,omitempty"`
}
type Operation struct {
	Reason         string `json:"reason,omitempty"`
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
		if !w.Terminal() {
			w.CanceledFrom = w.Phase
			if w.Phase == Blocked {
				w.CanceledFrom = w.RecoveryPhase
			}
			w.Phase = Canceled
			w.Reason = e.Reason
		}
	case "split_confirmed":
		if w.Phase == Blocked && w.Reason == "split_pending" && e.Ref == w.SplitPlanID && e.Attempt == w.Attempt && e.ScopeRevision == w.ScopeRevision && e.PolicyRevision == w.PolicyRevision {
			w.Phase = Split
			w.Version++
			return w, nil, nil
		}
		return w, nil, nil
	case "review_retry":
		if e.Reason == "" || !(w.Phase == Reviewing || (w.Phase == Blocked && w.RecoveryPhase == Reviewing)) {
			return w, nil, errors.New("review retry requires a review state and reason")
		}
		w.Phase = Reviewing
		w.Reason = ""
		w.RecoveryPhase = ""
	case "operator_retry":
		if w.Phase != Blocked || e.Reason == "" || w.Reason == "split_pending" {
			return w, nil, errors.New("retry requires a resolved ordinary blocker and reason")
		}
		w.Phase = w.RecoveryPhase
		w.RecoveryPhase = ""
		w.Reason = ""
		if w.Phase != Validating {
			w.RepairRounds++
		}
		switch w.Phase {
		case Implementing:
			emit = "implement"
		case Verifying:
			emit = "verify"
		case Validating:
			emit = "validate"
		case MergePending:
			w.Phase = Reviewing
		case Ready, Reviewing:
		default:
			return w, nil, errors.New("unsupported retry phase")
		}
	case "unblock":
		if w.Phase == Blocked && w.RecoveryPhase != "" && w.Reason != "split_pending" {
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
			if w.Phase == Canceled && e.Attempt == w.Attempt && e.ScopeRevision == w.ScopeRevision && e.PolicyRevision == w.PolicyRevision {
				switch e.Kind {
				case "merge_confirmed":
					if w.CanceledFrom == MergePending && e.Candidate == w.Candidate {
						w.Phase = Merged
						w.LastCompletion = &e
						w.Version++
					}
				case "implementation_ready", "candidate_ready", "validation_passed":
					w.LastCompletion = &e
					w.Version++
				}
			}
			return w, nil, nil
		}
		if e.Kind == "plan_split" && w.Phase == Blocked {
			return w, nil, errors.New("unblock before planning a split")
		}
		if e.Kind == "plan_split" && w.SplitPlanID != "" {
			return w, nil, errors.New("split already planned")
		}
		if w.Phase == Blocked {
			recovery := w
			recovery.Phase = w.RecoveryPhase
			next, ops, err := Apply(recovery, e)
			if err != nil {
				return w, nil, err
			}
			if !next.Terminal() {
				next.RecoveryPhase = next.Phase
				next.Phase = Blocked
			}
			return next, ops, nil
		}
		if e.Attempt != w.Attempt || e.ScopeRevision != w.ScopeRevision || e.PolicyRevision != w.PolicyRevision {
			return w, nil, nil
		}
		if e.Kind != "candidate_ready" && e.Kind != "implementation_ready" && e.Kind != "candidate_refreshed" && e.Kind != "implementation_validated" && e.Kind != "interim_requested" && e.Candidate != w.Candidate {
			return w, nil, nil
		}
		switch e.Kind {
		case "request_split":
			if w.Phase == Implementing {
				emit = "propose_split"
			}
		case "interim_requested":
			if w.Phase == Implementing && e.Candidate != "" {
				w.Candidate = e.Candidate
				w.Phase = Validating
				emit = "validate_interim"
			}
		case "interim_complete":
			if w.Phase == Validating {
				w.Phase = Implementing
				w.RepairRounds++
				emit = "implement"
			}
		case "plan_split":
			if e.Ref == "" {
				return w, nil, errors.New("split reference is required")
			}
			if e.Ref != "" {
				w.SplitPlanID = e.Ref
				w.RecoveryPhase = w.Phase
				w.Phase = Blocked
				w.Reason = "split_pending"
				emit = "split"
			}
		case "candidate_refreshed", "implementation_validated":
			if (w.Phase == Reviewing || w.Phase == Implementing) && e.Candidate != "" {
				w.Candidate = e.Candidate
				w.Phase = Validating
				emit = "validate"
			}
		case "return_to_review":
			if w.Phase == Verifying {
				w.Phase = Reviewing
			}
		case "start_assessment":
			if w.Phase == Ready && e.Ref != "" {
				w.Candidate = e.Ref
				w.Phase = Verifying
				emit = "verify"
			}
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
	op := Operation{Reason: e.Reason, Attempt: w.Attempt, ID: e.ID + ":" + emit, WorkItemID: w.ID, Kind: emit, Candidate: w.Candidate, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, State: "pending"}
	return w, []Operation{op}, nil
}
func CanDispatch(w WorkItem, o Operation) bool {
	if o.Attempt != w.Attempt || w.Terminal() || w.ScopeRevision != o.ScopeRevision || w.PolicyRevision != o.PolicyRevision || w.Candidate != o.Candidate {
		return false
	}
	switch o.Kind {
	case "split":
		return w.Phase == Blocked && w.Reason == "split_pending" && !w.Paused
	case "implement", "propose_split":
		return !w.Paused && w.Phase == Implementing
	case "verify":
		return !w.Paused && w.Phase == Verifying
	case "validate", "validate_interim":
		return w.Phase == Validating
	case "publish", "review":
		return w.Phase == Reviewing
	case "merge":
		return w.Phase == MergePending && !w.MergeHold
	}
	return false
}
