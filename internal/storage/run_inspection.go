package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type RunDispositionInspection struct {
	ReasonPreviewComplete bool   `json:"reason_preview_complete"`
	Kind                  string `json:"kind"`
	State                 string `json:"state"`
	ResultID              string `json:"result_id"`
	Archived              bool   `json:"archived"`
	ReasonPreview         string `json:"reason_preview"`
	ReasonBytes           int64  `json:"reason_bytes"`
}
type RunHistoryEntry struct {
	Disposition       *RunDispositionInspection     `json:"disposition,omitempty"`
	Sequence          int64                         `json:"sequence"`
	SessionID         string                        `json:"session_id"`
	Run               RunInspection                 `json:"run"`
	BindingPresent    bool                          `json:"binding_present"`
	BudgetUnavailable bool                          `json:"budget_unavailable"`
	Disposed          bool                          `json:"disposed"`
	ProcessStopped    bool                          `json:"process_stopped"`
	Active            bool                          `json:"active"`
	ObservedUsage     *contract.RunUsageObservation `json:"observed_usage,omitempty"`
}
type RunHistoryPage struct {
	SessionID string            `json:"session_id"`
	Entries   []RunHistoryEntry `json:"entries"`
	Cursor    int64             `json:"cursor"`
	More      bool              `json:"more"`
}

func inspectRunTx(ctx context.Context, tx *sql.Tx, id string) (RunHistoryEntry, error) {
	var out RunHistoryEntry
	out.Run.ID = id
	var usage []byte
	err := tx.QueryRowContext(ctx, `SELECT r.rowid,r.work_item_id,coalesce(json_extract(r.payload,'$.thread_id'),''),coalesce(json_extract(r.payload,'$.turn_id'),''),json_extract(r.payload,'$.state'),coalesce(json_extract(r.payload,'$.started'),''),coalesce(length(CAST(json_extract(r.payload,'$.final') AS BLOB)),0),coalesce(json_extract(r.payload,'$.observed_usage'),'null'),b.run_id IS NOT NULL,coalesce(b.applied,0),coalesce(b.process_stopped,0),s.active_run_id=r.id FROM records r JOIN kernel_sessions s ON s.id=r.work_item_id LEFT JOIN kernel_run_bindings b ON b.run_id=r.id AND b.session_id=s.id WHERE r.kind='agent_run' AND r.id=?`, id).Scan(&out.Sequence, &out.SessionID, &out.Run.ThreadID, &out.Run.TurnID, &out.Run.State, &out.Run.Started, &out.Run.FinalBytes, &usage, &out.BindingPresent, &out.Disposed, &out.ProcessStopped, &out.Active)
	if err != nil {
		return out, err
	}
	if err = json.Unmarshal(usage, &out.ObservedUsage); err != nil {
		return out, err
	}
	var disposition RunDispositionInspection
	err = tx.QueryRowContext(ctx, `SELECT kind,CASE WHEN kind='session_run_disposition' THEN 'archived' ELSE coalesce(json_extract(payload,'$.state'),'') END,coalesce(json_extract(payload,'$.result_id'),''),(kind='session_run_disposition' OR coalesce(json_extract(payload,'$.archived'),0) OR (kind='main_result_application' AND json_extract(payload,'$.state')='archived')),coalesce(substr(json_extract(payload,'$.reason'),1,512),''),coalesce(length(CAST(json_extract(payload,'$.reason') AS BLOB)),0) FROM records WHERE id=? AND kind IN ('task_result_application','main_result_application','session_run_disposition') ORDER BY CASE kind WHEN 'task_result_application' THEN 0 WHEN 'main_result_application' THEN 1 ELSE 2 END LIMIT 1`, id).Scan(&disposition.Kind, &disposition.State, &disposition.ResultID, &disposition.Archived, &disposition.ReasonPreview, &disposition.ReasonBytes)
	if err == nil {
		disposition.ReasonPreviewComplete = int64(len(disposition.ReasonPreview)) == disposition.ReasonBytes
		out.Disposition = &disposition
	} else if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	var budget RunBudgetBinding
	err = completionRecord(ctx, tx, "run_budget", id, &budget)
	if errors.Is(err, sql.ErrNoRows) {
		out.BudgetUnavailable = true
		return out, nil
	}
	if err != nil {
		return out, err
	}
	if budget.RunID != id || budget.SessionID != out.SessionID {
		return out, errors.New("run budget identity mismatch")
	}
	out.Run.Budget = &budget
	return out, nil
}
func (k *KernelStore) InspectRuns(ctx context.Context, session string, after int64, limit int) (RunHistoryPage, error) {
	out := RunHistoryPage{SessionID: session, Entries: []RunHistoryEntry{}, Cursor: after}
	if after < 0 || limit < 1 || limit > 128 {
		return out, errors.New("invalid run history page")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if _, err = sessionBindingTx(ctx, tx, session); err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT id FROM records WHERE kind='agent_run' AND work_item_id=? AND rowid>? ORDER BY rowid LIMIT ?", session, after, limit+1)
	if err != nil {
		return out, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return out, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return out, err
	}
	if closeErr != nil {
		return out, closeErr
	}
	if len(ids) > limit {
		ids = ids[:limit]
		out.More = true
	}
	for _, id := range ids {
		entry, err := inspectRunTx(ctx, tx, id)
		if err != nil {
			return out, err
		}
		out.Entries = append(out.Entries, entry)
		out.Cursor = entry.Sequence
	}
	return out, nil
}

// InspectRunOutput returns saved bytes only. Disposed means the run has a
// recorded disposition; invalid/archived output is not an accepted result.
func (k *KernelStore) InspectRunOutput(ctx context.Context, id string, offset int64, limit int) (RunHistoryEntry, []byte, error) {
	var out RunHistoryEntry
	if offset < 0 || limit < 1 || limit > contract.MaxInline {
		return out, nil, errors.New("invalid run output excerpt bounds")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, nil, err
	}
	defer tx.Rollback()
	out, err = inspectRunTx(ctx, tx, id)
	if err != nil {
		return out, nil, err
	}
	if offset > out.Run.FinalBytes {
		return out, nil, errors.New("run output excerpt begins beyond content")
	}
	var data []byte
	if err = tx.QueryRowContext(ctx, `SELECT substr(CAST(coalesce(json_extract(payload,'$.final'),'') AS BLOB),?,?) FROM records WHERE kind='agent_run' AND id=?`, offset+1, limit, id).Scan(&data); err != nil {
		return out, nil, err
	}
	return out, data, nil
}
