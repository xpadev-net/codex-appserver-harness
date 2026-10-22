package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// The host already registered the immutable request and trusted policy when it
// allocated the Wave. A model request ID can only select that run's own saved
// input; it cannot invent requests, enlarge scope, or create a nested Wave.
func (k *KernelStore) registeredToolWaveTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, snapshotID, requestID string) (domain.Wave, error) {
	var out domain.Wave
	if b.Kind != "orchestrator" {
		return out, errors.New("Wave invocation requires main")
	}
	snapshot := contract.InputSnapshot{ID: snapshotID}
	if err := tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", snapshotID).Scan(&snapshot.Digest, &snapshot.Input); err != nil {
		return out, err
	}
	if snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) {
		return out, errors.New("Wave request snapshot digest conflict")
	}
	targets, err := mainBudgetTargets(b, snapshot)
	if err != nil {
		return out, err
	}
	matches := 0
	for _, target := range targets {
		w, err := loadWave(ctx, tx, target.WaveID)
		if err != nil {
			return out, err
		}
		if w.SourceID != requestID {
			continue
		}
		if w.OrchestratorSessionID != b.ID || w.Target() != target || w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) {
			return out, errors.New("registered Wave request is no longer admissible")
		}
		matches++
		out = w
	}
	if matches != 1 {
		return out, errors.New("request ID must identify exactly one registered request in this run")
	}
	return out, nil
}
func (k *KernelStore) acceptWaveToolTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation, b contract.SessionBinding, target contract.Target, admission *PlanAdmission) (contract.ToolReply, error) {
	reply := contract.ToolReply{SchemaVersion: 1, State: "accepted", OperationID: op.ID, ResourceID: target.WaveID, SubscriptionIDs: []string{}}
	if b.Kind != "orchestrator" {
		return reply, errors.New("Wave tool requires main ownership")
	}
	sub, err := k.subscribeRequestTx(ctx, tx, SubscriptionRequest{SessionID: b.ID, Target: target, Key: op.ID, Kind: "wave_terminal"})
	if err != nil {
		return reply, err
	}
	reply.SubscriptionIDs = []string{sub.ID}
	view := struct {
		Target       contract.Target `json:"target"`
		Accepted     bool            `json:"accepted"`
		CreatedTasks int             `json:"created_tasks"`
		CarriedTasks int             `json:"carried_tasks"`
	}{Target: target, Accepted: true}
	if admission != nil {
		view.CreatedTasks = len(admission.CreatedTaskIDs)
		view.CarriedTasks = len(admission.CarriedTaskIDs)
	}
	raw, err := json.Marshal(view)
	if err != nil {
		return reply, err
	}
	reply.Content = string(raw)
	return reply, nil
}

// A final structured plan may acknowledge the exact plan already committed by
// this run's wave_plan tool. It must not add another replan/Task budget charge or
// wait for the background Tasks to stop merely to acknowledge that acceptance.
func acceptedRunPlanTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, runID string, target contract.Target, proposal contract.PlanProposal) (*PlanAdmission, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT payload FROM kernel_plans WHERE wave_id=? AND revision=?", target.WaveID, target.PlanRevision).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var admission PlanAdmission
	if err = json.Unmarshal(raw, &admission); err != nil {
		return nil, err
	}
	digest, err := contentDigest(proposal)
	if err != nil {
		return nil, err
	}
	if admission.RequestTarget != target || admission.Target != target || admission.Digest != digest {
		return nil, nil
	}
	var op kernelToolOperation
	err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_tool_operations WHERE id=? AND session_id=? AND name='wave_plan' AND state='completed'", admission.Key, b.ID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &op); err != nil {
		return nil, err
	}
	requestDigest, err := contentDigest(&contract.PlanWaveRequest{SchemaVersion: 1, Target: target, Plan: proposal})
	if err != nil {
		return nil, err
	}
	if op.ID != admission.Key || op.SessionID != b.ID || op.RunID != runID || op.ThreadID != b.ThreadID || op.Target != target || op.ArgumentDigest != requestDigest {
		return nil, nil
	}
	return &admission, nil
}
