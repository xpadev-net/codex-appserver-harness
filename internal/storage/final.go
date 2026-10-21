package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"reflect"
	"strings"
	"time"
)

type FinalAcceptanceState struct {
	WorkItemID     string           `json:"work_item_id"`
	ScopeRevision  int              `json:"scope_revision"`
	State          string           `json:"state"`
	Reason         string           `json:"reason,omitempty"`
	Classification string           `json:"classification,omitempty"`
	RequiredAction string           `json:"required_action,omitempty"`
	Candidate      runner.Candidate `json:"candidate"`
	Attempt        int              `json:"attempt"`
	Retries        int              `json:"retries"`
	Failure        *runner.Result   `json:"failure,omitempty"`
	RecoveryID     string           `json:"recovery_work_item,omitempty"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

// transferFinal binds the final lifecycle to an explicitly registered successor
// or adopted scope in the same transaction as that change. Prior failures remain
// historical evidence and cannot trigger assessment against a different scope.
func transferFinal(ctx context.Context, tx *sql.Tx, previous, next domain.WorkItem) error {
	key := fmt.Sprintf("r%d-i%d", previous.RepositoryID, previous.Issue)
	var state FinalAcceptanceState
	if err := completionRecord(ctx, tx, "parent_final_state", key, &state); errors.Is(err, sql.ErrNoRows) {
		if next.ScopeRevision == previous.ScopeRevision {
			return nil
		}
		state = FinalAcceptanceState{WorkItemID: next.ID, ScopeRevision: next.ScopeRevision, State: "waiting", RecoveryID: next.ID, UpdatedAt: time.Now().UTC()}
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO records VALUES('parent_final_state',?,?,?)", key, next.ID, data)
		return err
	} else if err != nil {
		return err
	}
	if state.WorkItemID != previous.ID || state.ScopeRevision != previous.ScopeRevision {
		return nil
	}
	if next.ScopeRevision != previous.ScopeRevision {
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO records VALUES('parent_final_history',?,?,?)", fmt.Sprintf("%s:s%d:a%d", key, state.ScopeRevision, state.Attempt), previous.ID, data); err != nil {
			return err
		}
		state.ScopeRevision = next.ScopeRevision
		state.Attempt++
		state.Candidate = runner.Candidate{}
		state.Failure = nil
		state.Classification = ""
		state.Reason = ""
		state.RequiredAction = ""
		state.State = "waiting"
		state.RecoveryID = next.ID
	}
	state.WorkItemID = next.ID
	state.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE records SET work_item_id=?,payload=? WHERE kind='parent_final_state' AND id=?", next.ID, data, key)
	return err
}

// finalAcceptanceSubject accepts a newer revision only through the durable
// recovery/continuation lineage and the current issue claim. Sharing an Issue
// number or having a newer proof alone never makes revisions equivalent.
func finalAcceptanceSubject(ctx context.Context, q queryer, original domain.WorkItem) (domain.WorkItem, error) {
	var state FinalAcceptanceState
	key := fmt.Sprintf("r%d-i%d", original.RepositoryID, original.Issue)
	if err := completionRecord(ctx, q, "parent_final_state", key, &state); errors.Is(err, sql.ErrNoRows) {
		return original, nil
	} else if err != nil {
		return original, err
	}
	if state.RecoveryID == "" || state.WorkItemID == original.ID || state.ScopeRevision < original.ScopeRevision {
		return original, nil
	}
	var claim string
	if err := q.QueryRowContext(ctx, "SELECT work_item_id FROM issue_claims WHERE repository_id=? AND issue=?", original.RepositoryID, original.Issue).Scan(&claim); err != nil {
		return original, err
	}
	if claim != state.WorkItemID {
		return original, nil
	}
	subject, err := read(ctx, q, claim)
	if err != nil {
		return original, err
	}
	if subject.RepositoryID != original.RepositoryID || subject.Issue != original.Issue || subject.ScopeRevision != state.ScopeRevision || subject.PolicyRevision < original.PolicyRevision {
		return original, nil
	}
	current := subject
	seen := map[string]bool{}
	for current.ID != original.ID {
		if seen[current.ID] {
			return original, errors.New("persisted final recovery lineage cycle")
		}
		seen[current.ID] = true
		var audit struct {
			Previous string `json:"previous"`
		}
		err := completionRecord(ctx, q, "final_recovery_audit", current.ID, &audit)
		var previous string
		if err == nil {
			previous = audit.Previous
		} else if errors.Is(err, sql.ErrNoRows) {
			err = completionRecord(ctx, q, "continued_from", current.ID, &previous)
			if errors.Is(err, sql.ErrNoRows) {
				// Older deterministic partial continuations predate continued_from.
				// Require the exact residual scope before recognizing that legacy link.
				previous = strings.TrimSuffix(current.ID, "-remaining")
				if previous == current.ID {
					return original, nil
				}
				parent, err := read(ctx, q, previous)
				if errors.Is(err, sql.ErrNoRows) {
					return original, nil
				}
				if err != nil {
					return original, err
				}
				var before, after verification.Scope
				if err = completionRecord(ctx, q, "scope", parent.ID, &before); err != nil {
					return original, err
				}
				if err = completionRecord(ctx, q, "scope", current.ID, &after); err != nil {
					return original, err
				}
				expected := before
				expected.Included = append([]string{}, before.Remaining...)
				expected.Remaining = []string{}
				expected.PreviouslyMerged = append(append([]string{}, before.PreviouslyMerged...), before.Included...)
				if before.Validate() != nil || len(before.Remaining) == 0 || !reflect.DeepEqual(expected, after) {
					return original, nil
				}
			} else if err != nil {
				return original, err
			}
		} else {
			return original, err
		}
		if previous == "" {
			return original, nil
		}
		parent, err := read(ctx, q, previous)
		if err != nil {
			return original, err
		}
		if parent.RepositoryID != original.RepositoryID || parent.Issue != original.Issue || parent.ScopeRevision > current.ScopeRevision || parent.PolicyRevision > current.PolicyRevision {
			return original, nil
		}
		current = parent
	}
	return subject, nil
}
func (s *Store) FinalAcceptanceSubject(ctx context.Context, w domain.WorkItem) (domain.WorkItem, error) {
	return finalAcceptanceSubject(ctx, s.DB, w)
}
