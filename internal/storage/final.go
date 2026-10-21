package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
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
		return nil
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
		if state.RecoveryID != "" {
			state.RecoveryID = next.ID
		}
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
