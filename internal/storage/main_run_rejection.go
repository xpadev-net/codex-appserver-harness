package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

var ErrMainRejectionHeld = errors.New("main rejection requires a known stopped unresolved run and reconciled operations")

type MainRunRejectionRequest struct {
	Key      string                  `json:"key"`
	Expected contract.SessionBinding `json:"expected"`
	RunID    string                  `json:"run_id"`
	Snapshot contract.InputSnapshot  `json:"snapshot"`
	Reason   string                  `json:"reason"`
}
type MainRunRejection struct {
	ID            string             `json:"id"`
	RequestDigest string             `json:"request_digest"`
	SessionID     string             `json:"session_id"`
	RunID         string             `json:"run_id"`
	RunState      string             `json:"run_state"`
	OldSnapshotID string             `json:"old_snapshot_id"`
	Targets       []contract.Target  `json:"targets"`
	Reason        string             `json:"reason"`
	Rotation      MainThreadRotation `json:"rotation"`
	CreatedAt     time.Time          `json:"created_at"`
}

// RejectStoppedMainRun is an explicit trusted operator decision, never a model
// tool or timeout policy. It rejects a known outcome, returns still-current
// inputs to pending and records thread replacement in one transaction. Unknown
// history, active processes and uncertain operations remain held. Physical run
// facts, prior outputs and cumulative budgets are retained without modification.
func (k *KernelStore) RejectStoppedMainRun(ctx context.Context, r MainRunRejectionRequest) (MainRunRejection, error) {
	var out MainRunRejection
	if strings.TrimSpace(r.Key) == "" || len(r.Key) > 256 || r.RunID == "" || strings.TrimSpace(r.Reason) == "" || len(r.Reason) > contract.MaxInline || !utf8.ValidString(r.Reason) || r.Snapshot.ID == "" || r.Snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(r.Snapshot.Input)) {
		return out, errors.New("bounded explicit rejection identity, reason and saved snapshot required")
	}
	if err := contract.ValidateSessionInput(r.Snapshot.Input); err != nil {
		return out, err
	}
	digest, err := contentDigest(r)
	if err != nil {
		return out, err
	}
	id := structureID("main-rejection-", r.Expected.ID, r.Key)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	err = completionRecord(ctx, tx, "main_run_rejection", id, &out)
	if err == nil {
		if out.ID != id || out.RequestDigest != digest || out.RunID != r.RunID || out.SessionID != r.Expected.ID {
			return out, errors.New("main rejection replay conflict")
		}
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	b, err := sessionBindingTx(ctx, tx, r.Expected.ID)
	if err != nil {
		return out, err
	}
	if b.Kind != "orchestrator" || b.Closed || b.ThreadID == "" || !reflect.DeepEqual(b, r.Expected) {
		return out, ErrMainRejectionHeld
	}
	state, err := sessionDispatchStateTx(ctx, tx, b.ID)
	if err != nil {
		return out, err
	}
	if state.Creating || !state.ProcessStopped || !state.RunStopped || state.Run == nil || state.Run.ID != r.RunID {
		return out, ErrMainRejectionHeld
	}
	run := *state.Run
	if run.ThreadID != b.ThreadID || run.State != "completed" && run.State != "failed" && run.State != "interrupted" && run.State != "blocked" {
		return out, ErrMainRejectionHeld
	}
	if run.State == "completed" && (!run.BaselineKnown || run.TurnID == "" || slices.Contains(run.BaselineTurnIDs, run.TurnID)) {
		return out, ErrMainRejectionHeld
	}
	var oldSnapshotID string
	if err = tx.QueryRowContext(ctx, "SELECT snapshot_id FROM kernel_run_bindings WHERE run_id=? AND session_id=?", run.ID, b.ID).Scan(&oldSnapshotID); err != nil {
		return out, err
	}
	var budget RunBudgetBinding
	if err = completionRecord(ctx, tx, "run_budget", run.ID, &budget); err != nil {
		return out, err
	}
	var oldDigest string
	var oldInput []byte
	if err = tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", oldSnapshotID).Scan(&oldDigest, &oldInput); err != nil {
		return out, err
	}
	if budget.RunID != run.ID || budget.SessionID != b.ID || budget.ThreadID != b.ThreadID || budget.SnapshotID != oldSnapshotID || budget.SnapshotDigest != oldDigest {
		return out, ErrMainRejectionHeld
	}
	if oldDigest != fmt.Sprintf("%x", sha256.Sum256(oldInput)) {
		return out, ErrMainRejectionHeld
	}
	originalTargets, err := mainBudgetTargets(b, contract.InputSnapshot{ID: oldSnapshotID, Digest: oldDigest, Input: oldInput})
	if err != nil {
		return out, err
	}
	if !slices.Equal(originalTargets, budget.Targets) {
		return out, ErrMainRejectionHeld
	}
	targets, err := effectiveRunTargetsTx(ctx, tx, b, run.ID, oldSnapshotID, budget.Targets)
	if err != nil {
		return out, err
	}
	nextTargets, err := k.rotationSnapshotTx(ctx, tx, b, r.Snapshot)
	if err != nil {
		return out, err
	}
	if !slices.Equal(targets, nextTargets) {
		return out, ErrMainRejectionHeld
	}
	var uncertain bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM kernel_tool_operations WHERE session_id=? AND state IN ('accepted','unknown'))", b.ID).Scan(&uncertain); err != nil {
		return out, err
	}
	if uncertain {
		return out, ErrMainRejectionHeld
	}
	receipt := MainResultApplication{SessionID: b.ID, RunID: run.ID, ThreadID: b.ThreadID, SnapshotID: oldSnapshotID, Targets: targets, State: "archived", Reason: "operator_rejected: " + r.Reason, Plans: []PlanAdmission{}}
	if err = k.finishMainResultEffectsTx(ctx, tx, b, receipt, "superseded"); err != nil {
		return out, err
	}
	rotation, err := k.beginMainRotationTx(ctx, tx, b, r.Snapshot, "operator_rejected: "+r.Reason)
	if err != nil {
		return out, err
	}
	out = MainRunRejection{ID: id, RequestDigest: digest, SessionID: b.ID, RunID: run.ID, RunState: run.State, OldSnapshotID: oldSnapshotID, Targets: targets, Reason: r.Reason, Rotation: rotation, CreatedAt: k.clock()}
	if err = kernelRecordTx(ctx, tx, "main_run_rejection", id, b.ID, out); err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return MainRunRejection{}, err
	}
	return out, nil
}
