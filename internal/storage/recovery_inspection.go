package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type HistoryObservationHeader struct {
	ID             string    `json:"id"`
	SessionID      string    `json:"session_id"`
	RunID          string    `json:"run_id"`
	ThreadID       string    `json:"thread_id"`
	OwnerID        string    `json:"owner_id"`
	PreparationID  string    `json:"preparation_id"`
	Status         string    `json:"status"`
	BaselineKnown  bool      `json:"baseline_known"`
	SavedTurnID    string    `json:"saved_turn_id"`
	BaselineTurns  int       `json:"baseline_turns"`
	CandidateTurns int       `json:"candidate_turns"`
	ObservedTurns  int       `json:"observed_turns"`
	ResponseDigest string    `json:"response_digest"`
	ErrorDigest    string    `json:"error_digest"`
	ObservedAt     time.Time `json:"observed_at"`
}

func historyHeader(o HistoryObservation) HistoryObservationHeader {
	return HistoryObservationHeader{o.ID, o.SessionID, o.RunID, o.ThreadID, o.OwnerID, o.PreparationID, o.Status, o.BaselineKnown, o.SavedTurnID, len(o.BaselineTurnIDs), len(o.CandidateTurnIDs), len(o.Turns), o.ResponseDigest, o.ErrorDigest, o.ObservedAt}
}

type SessionRecoveryInspection struct {
	Session      SessionInspection          `json:"session"`
	Observations []HistoryObservationHeader `json:"observations"`
	NextAfter    string                     `json:"next_after"`
}

func (k *KernelStore) InspectSessionRecovery(ctx context.Context, session, after string, limit int) (SessionRecoveryInspection, error) {
	out := SessionRecoveryInspection{Observations: []HistoryObservationHeader{}}
	if limit < 1 || limit > 128 {
		return out, errors.New("invalid history page limit")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	out.Session, err = inspectSessionTx(ctx, tx, session)
	if err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT id,payload FROM records WHERE kind='session_history_observation' AND work_item_id=? AND id>? ORDER BY id LIMIT ?", session, after, limit+1)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return out, err
		}
		if len(out.Observations) == limit {
			out.NextAfter = out.Observations[len(out.Observations)-1].ID
			break
		}
		var o HistoryObservation
		if err = json.Unmarshal(raw, &o); err != nil {
			return out, err
		}
		if o.ID != id || o.SessionID != session {
			return out, errors.New("history observation identity mismatch")
		}
		out.Observations = append(out.Observations, historyHeader(o))
	}
	return out, rows.Err()
}

type ObservedTurnInspection struct {
	HistoryTurn
	Candidate bool `json:"candidate"`
	Baseline  bool `json:"baseline"`
}
type HistoryInspection struct {
	Observation HistoryObservationHeader `json:"observation"`
	Turns       []ObservedTurnInspection `json:"turns"`
	NextAfter   string                   `json:"next_after"`
}

func (k *KernelStore) InspectHistoryObservation(ctx context.Context, id, after string, limit int) (HistoryInspection, error) {
	out := HistoryInspection{Turns: []ObservedTurnInspection{}}
	if limit < 1 || limit > 128 {
		return out, errors.New("invalid turn page limit")
	}
	var o HistoryObservation
	if err := k.Record(ctx, "session_history_observation", id, &o); err != nil {
		return out, err
	}
	if o.ID != id {
		return out, errors.New("history observation identity mismatch")
	}
	out.Observation = historyHeader(o)
	sort.Slice(o.Turns, func(i, j int) bool { return o.Turns[i].ID < o.Turns[j].ID })
	for _, turn := range o.Turns {
		if turn.ID <= after {
			continue
		}
		if len(out.Turns) == limit {
			out.NextAfter = out.Turns[len(out.Turns)-1].ID
			break
		}
		out.Turns = append(out.Turns, ObservedTurnInspection{turn, slices.Contains(o.CandidateTurnIDs, turn.ID), slices.Contains(o.BaselineTurnIDs, turn.ID)})
	}
	return out, nil
}

// SavedInputSnapshot resolves a host-supplied reference without creating input
// or extending a session's authority. Recovery ports revalidate scope in their
// own transaction. Payload stays out of CLI request/response JSON.
func (k *KernelStore) SavedInputSnapshot(ctx context.Context, id, digest string) (contract.InputSnapshot, error) {
	out := contract.InputSnapshot{ID: id}
	if id == "" || digest == "" {
		return out, errors.New("saved snapshot ID and digest required")
	}
	if err := k.DB.QueryRowContext(ctx, "SELECT digest,substr(CAST(data AS BLOB),1,?) FROM kernel_snapshots WHERE id=?", contract.MaxSessionInput+1, id).Scan(&out.Digest, &out.Input); err != nil {
		return out, err
	}
	if len(out.Input) > contract.MaxSessionInput || out.Digest != digest {
		return out, errors.New("saved snapshot reference mismatch or oversized input")
	}
	if out.Digest != fmt.Sprintf("%x", sha256.Sum256(out.Input)) {
		return out, errors.New("saved snapshot digest mismatch")
	}
	return out, contract.ValidateSessionInput(out.Input)
}
