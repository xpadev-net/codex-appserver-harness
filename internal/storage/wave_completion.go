package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type WaveCompletion struct {
	ID        string                  `json:"id"`
	SessionID string                  `json:"session_id"`
	RunID     string                  `json:"run_id"`
	Proof     ArtifactAcceptanceProof `json:"proof"`
	Summary   string                  `json:"summary"`
	CreatedAt time.Time               `json:"created_at"`
}

// Called only from the exact normal/stopped main-result application transaction.
// No Task tool or model-supplied status can write this controller completion.
func (k *KernelStore) completeGenericWaveTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, runID string, target contract.Target, p contract.ArtifactAcceptance, summary string) (WaveCompletion, error) {
	var completion WaveCompletion
	wave, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return completion, err
	}
	if wave.OrchestratorSessionID != b.ID || b.Kind != "orchestrator" {
		return completion, ErrMainResultInvalid
	}
	var pending bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM kernel_tool_operations WHERE state IN ('accepted','unknown') AND json_extract(payload,'$.target.wave_id')=?)`, wave.ID).Scan(&pending); err != nil {
		return completion, err
	}
	if pending {
		return completion, ErrMainResultHeld
	}
	// Capacity intents also own runner/Git/external operations. Their trusted
	// release port requires terminal/stop proof; a live or unknown reservation
	// cannot be hidden by completing the Wave before its deadline stops it.
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM kernel_dispatch_intents WHERE wave_id=? AND state<>'released')", wave.ID).Scan(&pending); err != nil {
		return completion, err
	}
	if pending {
		return completion, ErrMainResultHeld
	}
	var creating, stopped bool
	if err = tx.QueryRowContext(ctx, "SELECT creating,process_stopped FROM kernel_sessions WHERE id=?", b.ID).Scan(&creating, &stopped); err != nil {
		return completion, err
	}
	if creating || !stopped {
		return completion, ErrMainResultHeld
	}
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records p WHERE p.kind='managed_process' AND json_extract(p.payload,'$.state')<>'stopped' AND (json_extract(p.payload,'$.request.binding.id')=? OR json_extract(p.payload,'$.request.binding.id') IN (SELECT s.id FROM kernel_sessions s JOIN kernel_tasks t ON t.id=s.task_id WHERE t.wave_id=?)))`, b.ID, wave.ID).Scan(&pending); err != nil {
		return completion, err
	}
	if pending {
		return completion, ErrMainResultHeld
	}
	proof, err := k.checkArtifactAcceptanceTx(ctx, tx, target, p)
	if err != nil {
		return completion, err
	}
	completion = WaveCompletion{ID: structureID("wave-completion-", wave.ID, fmt.Sprintf("%d:%d:%d", target.ScopeRevision, target.PlanRevision, target.PolicyRevision)), SessionID: b.ID, RunID: runID, Proof: proof, Summary: summary, CreatedAt: k.clock()}
	wave.State, wave.Reason, wave.CompletedAt = domain.WaveCompleted, summary, completion.CreatedAt
	if err = saveWave(ctx, tx, wave); err != nil {
		return completion, err
	}
	if err = kernelRecordTx(ctx, tx, "wave_completion", completion.ID, wave.ID, completion); err != nil {
		return completion, err
	}
	if err = k.waveEventTx(ctx, tx, wave, "wave_completed", "", completion.ID, completion); err != nil {
		return completion, err
	}
	return completion, nil
}
