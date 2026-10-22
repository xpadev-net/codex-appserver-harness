package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type runPlanTransition struct {
	OperationID    string          `json:"operation_id"`
	SessionID      string          `json:"session_id"`
	RunID          string          `json:"run_id"`
	ThreadID       string          `json:"thread_id"`
	SnapshotID     string          `json:"snapshot_id"`
	ArgumentDigest string          `json:"argument_digest"`
	PlanDigest     string          `json:"plan_digest"`
	From           contract.Target `json:"from"`
	To             contract.Target `json:"to"`
}

func saveRunPlanTransitionTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, admission PlanAdmission) error {
	if admission.RequestTarget == admission.Target {
		return nil
	}
	transition := runPlanTransition{OperationID: op.ID, SessionID: op.SessionID, RunID: op.RunID, ThreadID: op.ThreadID, SnapshotID: op.SnapshotID, ArgumentDigest: op.ArgumentDigest, PlanDigest: admission.Digest, From: admission.RequestTarget, To: admission.Target}
	return kernelRecordTx(ctx, tx, "run_plan_transition", op.ID, op.RunID, transition)
}

// The immutable snapshot and token allocation retain their original targets.
// Only same-run, committed controller plan acceptances can advance effective
// plan scope. Scope/policy, Wave set and caller identity never change here.
func effectiveRunTargetsTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, runID, snapshotID string, original []contract.Target) ([]contract.Target, error) {
	targets := append([]contract.Target{}, original...)
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM records WHERE kind='run_plan_transition' AND work_item_id=? ORDER BY rowid", runID)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		var transition runPlanTransition
		if err = completionRecord(ctx, tx, "run_plan_transition", id, &transition); err != nil {
			return nil, err
		}
		from, to := transition.From, transition.To
		if transition.OperationID != id || transition.SessionID != b.ID || transition.RunID != runID || transition.ThreadID != b.ThreadID || transition.SnapshotID != snapshotID || !from.Valid() || !to.Valid() || from.WaveID != to.WaveID || from.ScopeRevision != to.ScopeRevision || from.PolicyRevision != to.PolicyRevision || to.PlanRevision <= from.PlanRevision || to.PlanRevision-from.PlanRevision != 1 {
			return nil, errors.New("invalid same-run plan scope transition")
		}
		var op kernelToolOperation
		var raw []byte
		if err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_tool_operations WHERE id=? AND session_id=? AND name='wave_plan' AND state='completed'", id, b.ID).Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &op); err != nil {
			return nil, err
		}
		var planDigest string
		if err = tx.QueryRowContext(ctx, "SELECT digest FROM kernel_plans WHERE wave_id=? AND revision=? AND adoption_key=?", to.WaveID, to.PlanRevision, id).Scan(&planDigest); err != nil {
			return nil, err
		}
		if op.ID != id || op.RunID != runID || op.ThreadID != b.ThreadID || op.SnapshotID != snapshotID || op.Target != from || op.ArgumentDigest != transition.ArgumentDigest || planDigest != transition.PlanDigest {
			return nil, errors.New("plan transition has no matching committed acceptance")
		}
		found := false
		for i, target := range targets {
			if target.WaveID == from.WaveID {
				if target != from {
					return nil, errors.New("plan transition is outside the saved run scope")
				}
				targets[i] = to
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("plan transition invents a Wave")
		}
	}
	return targets, nil
}
func acceptedPlanReplayTargetTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, call contract.ToolInvocation, target contract.Target) (bool, error) {
	if call.Name != "wave_plan" {
		return false, nil
	}
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT payload FROM kernel_tool_operations WHERE session_id=? AND turn_id=? AND call_id=? AND name='wave_plan' AND state='completed'", b.ID, call.TurnID, call.CallID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var op kernelToolOperation
	if err = json.Unmarshal(raw, &op); err != nil {
		return false, err
	}
	if op.RunID != call.RunID || op.ThreadID != b.ThreadID || op.Target != target {
		return false, nil
	}
	var transition runPlanTransition
	err = completionRecord(ctx, tx, "run_plan_transition", op.ID, &transition)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return transition.OperationID == op.ID && transition.From == target && transition.SessionID == b.ID && transition.RunID == call.RunID && transition.ThreadID == b.ThreadID && transition.SnapshotID == op.SnapshotID && transition.ArgumentDigest == op.ArgumentDigest, nil
}
