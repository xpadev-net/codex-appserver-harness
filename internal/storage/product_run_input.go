package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// productRunInputTx authenticates the exact input snapshot actually consumed by
// a normal run. Current manifests or model-supplied input claims cannot replace it.
func (k *KernelStore) productRunInputTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, w domain.Wave, task domain.Task, runID, snapshotID, thread string) (contract.InputSnapshot, sessionManifest, error) {
	snapshot := contract.InputSnapshot{ID: snapshotID}
	var input sessionManifest
	if err := tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", snapshotID).Scan(&snapshot.Digest, &snapshot.Input); err != nil {
		return snapshot, input, err
	}
	if err := contract.ValidateSessionInput(snapshot.Input); err != nil {
		return snapshot, input, fmt.Errorf("%w: invalid input snapshot", ErrTaskEvidence)
	}
	if bytesDigest(snapshot.Input) != snapshot.Digest {
		return snapshot, input, fmt.Errorf("%w: input snapshot checksum mismatch", ErrTaskEvidence)
	}
	var budget RunBudgetBinding
	if err := taskEvidenceRecord(ctx, tx, "run_budget", runID, &budget); err != nil {
		return snapshot, input, err
	}
	if budget.RunID != runID || budget.SessionID != b.ID || budget.TaskID != task.ID || budget.ThreadID != thread || budget.SnapshotID != snapshotID || budget.SnapshotDigest != snapshot.Digest || !slices.Equal(budget.Targets, []contract.Target{w.Target()}) {
		return snapshot, input, fmt.Errorf("%w: run budget binding mismatch", ErrTaskEvidence)
	}
	var envelope struct {
		SchemaVersion int               `json:"schema_version"`
		SessionID     string            `json:"session_id"`
		Targets       []contract.Target `json:"targets"`
		Context       SessionContext    `json:"context"`
	}
	if err := json.Unmarshal(snapshot.Input, &envelope); err != nil {
		return snapshot, input, fmt.Errorf("%w: invalid input envelope", ErrTaskEvidence)
	}
	if envelope.SchemaVersion != 1 || envelope.SessionID != b.ID || !slices.Equal(envelope.Targets, []contract.Target{w.Target()}) || len(envelope.Context.Waves) != 1 || envelope.Context.Waves[0].Target != w.Target() {
		return snapshot, input, fmt.Errorf("%w: snapshot session or target mismatch", ErrTaskEvidence)
	}
	ref := envelope.Context.Waves[0].Manifest
	var grant inputReferenceGrant
	if err := taskEvidenceRecord(ctx, tx, "session_input_references", snapshotID, &grant); err != nil {
		return snapshot, input, err
	}
	if grant.SessionID != b.ID || !slices.Contains(grant.References, ref) || ref.Target != w.Target() || ref.Section != "manifest" {
		return snapshot, input, fmt.Errorf("%w: manifest grant mismatch", ErrTaskEvidence)
	}
	var manifest savedInputManifest
	if err := taskEvidenceRecord(ctx, tx, "session_input_manifest", ref.ID, &manifest); err != nil {
		return snapshot, input, err
	}
	if manifest.SessionID != b.ID || manifest.Reference != ref || int64(len(manifest.Data)) != ref.Bytes || bytesDigest(manifest.Data) != ref.Digest {
		return snapshot, input, fmt.Errorf("%w: manifest checksum mismatch", ErrTaskEvidence)
	}
	if err := json.Unmarshal(manifest.Data, &input); err != nil {
		return snapshot, input, fmt.Errorf("%w: invalid saved manifest", ErrTaskEvidence)
	}
	if input.SessionID != b.ID || input.Kind != "task" || input.Wave.Target() != w.Target() {
		return snapshot, input, fmt.Errorf("%w: workspace manifest assignment mismatch", ErrTaskEvidence)
	}
	owners := 0
	for _, assigned := range input.Tasks {
		if assigned.ID != task.ID {
			continue
		}
		owners++
		if assigned.Target() != task.Target() || assigned.InputDigest != task.InputDigest || !reflect.DeepEqual(assigned.Spec, task.Spec) {
			return snapshot, input, fmt.Errorf("%w: saved Task assignment mismatch", ErrTaskEvidence)
		}
	}
	if owners != 1 {
		return snapshot, input, fmt.Errorf("%w: saved Task owner is not unique", ErrTaskEvidence)
	}
	return snapshot, input, nil
}
