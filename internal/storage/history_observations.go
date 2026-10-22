package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// HistoryObservation records what a read-only recovery process actually saw.
// Missing/ambiguous/error observations are not permission to discard or replay
// an unknown turn. Bodies and error text are retained only as SHA-256 digests.
type HistoryObservation struct {
	ID               string        `json:"id"`
	SessionID        string        `json:"session_id"`
	RunID            string        `json:"run_id"`
	ThreadID         string        `json:"thread_id"`
	PreparationID    string        `json:"preparation_id"`
	OwnerID          string        `json:"owner_id"`
	BaselineKnown    bool          `json:"baseline_known"`
	BaselineTurnIDs  []string      `json:"baseline_turn_ids"`
	SavedTurnID      string        `json:"saved_turn_id"`
	Status           string        `json:"status"`
	Turns            []HistoryTurn `json:"turns"`
	CandidateTurnIDs []string      `json:"candidate_turn_ids"`
	ResponseDigest   string        `json:"response_digest"`
	ErrorDigest      string        `json:"error_digest"`
	ObservedAt       time.Time     `json:"observed_at"`
}
type HistoryTurn struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	ItemsView string `json:"items_view"`
}

func (k *KernelStore) RecordHistoryRead(ctx context.Context, ownerID, threadID string, response json.RawMessage, errorDigest string) (HistoryObservation, error) {
	var out HistoryObservation
	if threadID == "" || (len(response) == 0) == (errorDigest == "") {
		return out, errors.New("one read response or error digest required")
	}
	if errorDigest != "" && (len(errorDigest) != 64 || strings.Trim(errorDigest, "0123456789abcdef") != "") {
		return out, errors.New("invalid read error digest")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	owner, err := managedProcessTx(ctx, tx, ownerID)
	if err != nil {
		return out, err
	}
	if owner.Request.Mode != "recovery" || owner.State != "started" {
		return out, ErrManagedProcessHeld
	}
	binding, err := sessionBindingTx(ctx, tx, owner.Request.Binding.ID)
	if err != nil {
		return out, err
	}
	if binding.ThreadID != threadID || binding.PreparationID != owner.PreparationID {
		return out, ErrManagedProcessHeld
	}
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", binding.ID).Scan(&active); err != nil {
		return out, err
	}
	if active != owner.Request.RunID {
		return out, ErrManagedProcessHeld
	}
	var run contract.SessionRun
	if err = completionRecord(ctx, tx, "agent_run", active, &run); err != nil {
		return out, err
	}
	if run.ID != active || run.ThreadID != threadID {
		return out, ErrManagedProcessHeld
	}
	out = HistoryObservation{SessionID: binding.ID, RunID: active, ThreadID: threadID, OwnerID: ownerID, PreparationID: owner.PreparationID, BaselineKnown: run.BaselineKnown, BaselineTurnIDs: append([]string{}, run.BaselineTurnIDs...), SavedTurnID: run.TurnID, Status: "read_error", Turns: []HistoryTurn{}, CandidateTurnIDs: []string{}, ErrorDigest: errorDigest, ObservedAt: k.clock()}
	if len(response) > 0 {
		out.ResponseDigest = fmt.Sprintf("%x", sha256.Sum256(response))
		var reply struct {
			Thread struct {
				ID, CWD string
				Turns   []struct {
					ID, Status string
					ItemsView  string `json:"itemsView"`
				}
			} `json:"thread"`
		}
		if err = json.Unmarshal(response, &reply); err != nil {
			out.Status = "invalid_response"
		} else if reply.Thread.ID != threadID || reply.Thread.CWD != binding.CWD {
			out.Status = "identity_mismatch"
		} else {
			out.Status = "missing_turn"
			for _, turn := range reply.Thread.Turns {
				out.Turns = append(out.Turns, HistoryTurn{ID: turn.ID, Status: turn.Status, ItemsView: turn.ItemsView})
				if run.BaselineKnown && !slices.Contains(run.BaselineTurnIDs, turn.ID) {
					out.CandidateTurnIDs = append(out.CandidateTurnIDs, turn.ID)
				}
			}
			selected := run.TurnID
			if selected == "" && run.BaselineKnown && len(out.CandidateTurnIDs) == 1 {
				selected = out.CandidateTurnIDs[0]
			}
			if selected == "" && len(out.CandidateTurnIDs) > 1 {
				out.Status = "ambiguous_turn"
			}
			for _, turn := range out.Turns {
				if selected != "" && turn.ID == selected {
					out.Status = "observed_turn"
					if turn.ItemsView != "" && turn.ItemsView != "full" {
						out.Status = "partial_history"
					}
				}
			}
		}
	}
	// Repeated classifications/history headers share their first attestation.
	// Body growth and changing error text cannot grow the journal every poll.
	out.ID, err = historyObservationID(out)
	if err != nil {
		return out, err
	}
	var previous HistoryObservation
	err = completionRecord(ctx, tx, "session_history_observation", out.ID, &previous)
	if err == nil {
		return previous, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	if err = kernelRecordTx(ctx, tx, "session_history_observation", out.ID, binding.ID, out); err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return HistoryObservation{}, err
	}
	return out, nil
}

func historyObservationID(observation HistoryObservation) (string, error) {
	observation.ID, observation.OwnerID, observation.PreparationID = "", "", ""
	observation.ResponseDigest, observation.ErrorDigest = "", ""
	observation.ObservedAt = time.Time{}
	digest, err := contentDigest(observation)
	return "history-" + digest, err
}
