package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrProductAcceptanceHeld = errors.New("product acceptance requires exact parent input and current stopped final verification")

type ProductAcceptanceProof struct {
	Target         contract.Target            `json:"target"`
	Proposal       contract.ProductAcceptance `json:"proposal"`
	SnapshotID     string                     `json:"snapshot_id"`
	SnapshotDigest string                     `json:"snapshot_digest"`
	Manifest       InputReference             `json:"manifest"`
	PlanDigest     string                     `json:"plan_digest"`
	Selection      ProductFinalPlanSelection  `json:"selection"`
}
type ProductWaveCompletion struct {
	ID        string                 `json:"id"`
	SessionID string                 `json:"session_id"`
	RunID     string                 `json:"run_id"`
	Proof     ProductAcceptanceProof `json:"proof"`
	Summary   string                 `json:"summary"`
	CreatedAt time.Time              `json:"created_at"`
}

// The actual run snapshot and its immutable grant must both name the exact
// parent manifest. A caller-selected current manifest cannot stand in for it.
func productAcceptanceInputTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, snapshot contract.InputSnapshot, target contract.Target) (sessionManifest, InputReference, error) {
	var empty sessionManifest
	var envelope struct {
		SessionID string         `json:"session_id"`
		Context   SessionContext `json:"context"`
	}
	if json.Unmarshal(snapshot.Input, &envelope) != nil || envelope.SessionID != b.ID || len(envelope.Context.Waves) < 1 || len(envelope.Context.Waves) > contract.MaxMainWaves {
		return empty, InputReference{}, ErrProductAcceptanceHeld
	}
	var ref InputReference
	found := false
	for _, wave := range envelope.Context.Waves {
		if wave.Target == target {
			if found {
				return empty, ref, ErrProductAcceptanceHeld
			}
			found = true
			ref = wave.Manifest
		}
	}
	if !found || ref.Target != target || ref.Section != "manifest" || ref.Bytes < 1 || ref.Bytes > maxInputManifest {
		return empty, ref, ErrProductAcceptanceHeld
	}
	var grant inputReferenceGrant
	if err := completionRecord(ctx, tx, "session_input_references", snapshot.ID, &grant); err != nil {
		return empty, ref, errors.Join(ErrProductAcceptanceHeld, err)
	}
	if grant.SessionID != b.ID || !slices.Contains(grant.References, ref) {
		return empty, ref, ErrProductAcceptanceHeld
	}
	var saved savedInputManifest
	if err := completionRecord(ctx, tx, "session_input_manifest", ref.ID, &saved); err != nil {
		return empty, ref, errors.Join(ErrProductAcceptanceHeld, err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(saved.Data))
	if saved.SessionID != b.ID || saved.Reference != ref || int64(len(saved.Data)) != ref.Bytes || digest != ref.Digest || ref.ID != structureID("input-manifest-", b.ID, digest) || json.Unmarshal(saved.Data, &empty) != nil || empty.SessionID != b.ID || empty.Kind != "orchestrator" || empty.Wave.Target() != target {
		return empty, ref, ErrProductAcceptanceHeld
	}
	return empty, ref, nil
}
func (k *KernelStore) checkProductAcceptanceTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, snapshot contract.InputSnapshot, target contract.Target, p contract.ProductAcceptance) (ProductAcceptanceProof, error) {
	proof := ProductAcceptanceProof{Target: target, Proposal: p, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest}
	if p.Validate() != nil || !target.Valid() {
		return proof, ErrMainResultInvalid
	}
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return proof, err
	}
	if w.Target() != target || w.OrchestratorSessionID != b.ID || b.Kind != "orchestrator" || w.Policy.Stage != "P" || !w.Policy.Product {
		return proof, ErrProductAcceptanceHeld
	}
	if snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(snapshot.Input)) {
		return proof, ErrProductAcceptanceHeld
	}
	input, ref, err := productAcceptanceInputTx(ctx, tx, b, snapshot, target)
	if err != nil {
		return proof, err
	}
	proof.Manifest = ref
	if input.ProductMergeCoverage == nil || input.ProductFinalVerification == nil || input.UnavailableProductCoverage != "" || input.UnavailableProductFinalVerification != "" {
		return proof, ErrProductAcceptanceHeld
	}
	coverage, evidence := input.ProductMergeCoverage, input.ProductFinalVerification
	coverageDigest, err := contentDigest(coverage.Value)
	if err != nil {
		return proof, err
	}
	evidenceDigest, err := contentDigest(evidence.Value)
	if err != nil {
		return proof, err
	}
	if coverageDigest != coverage.Digest || evidenceDigest != evidence.Digest || coverage.Value.Target != target || evidence.Value.Target != target || coverage.Digest != p.CoverageDigest || evidence.Value.CoverageDigest != p.CoverageDigest || evidence.Digest != p.FinalVerificationDigest || evidence.Value.Plan.ID != p.FinalPlanID || evidence.Value.Plan.Digest != p.FinalPlanDigest || !evidence.Value.Current || !evidence.Value.Complete || !evidence.Value.Passed || evidence.Value.State != "passed" {
		return proof, ErrProductAcceptanceHeld
	}
	current, err := k.productFinalVerificationEvidenceTx(ctx, tx, w)
	if err != nil {
		return proof, err
	}
	if current.Digest != evidence.Digest || !current.Value.Current || !current.Value.Complete || !current.Value.Passed {
		return proof, ErrProductAcceptanceHeld
	}
	if input.Wave.Source != w.Source || input.Wave.SourceID != w.SourceID || input.Wave.OriginalRequest != w.OriginalRequest || !reflect.DeepEqual(input.Wave.Criteria, w.Criteria) {
		return proof, ErrProductAcceptanceHeld
	}
	criteria := make([]string, 0, len(w.Criteria))
	for _, criterion := range w.Criteria {
		criteria = append(criteria, criterion.ID)
	}
	selected := slices.Clone(p.CriterionIDs)
	sort.Strings(criteria)
	sort.Strings(selected)
	if !slices.Equal(selected, criteria) {
		return proof, ErrProductAcceptanceHeld
	}
	plan, err := k.readProductFinalPlanTx(ctx, tx, p.FinalPlanID)
	if err != nil {
		return proof, errors.Join(ErrProductAcceptanceHeld, err)
	}
	selection, _, err := k.finalRunnerPreflightTx(ctx, tx, plan)
	if err != nil {
		return proof, errors.Join(ErrProductAcceptanceHeld, err)
	}
	proof.Selection = selection
	progress, _, _, err := k.activePhaseTx(ctx, tx, target)
	if err != nil {
		return proof, err
	}
	if progress.State != "execution_completed" {
		return proof, ErrProductAcceptanceHeld
	}
	if err := tx.QueryRowContext(ctx, "SELECT digest FROM kernel_plans WHERE wave_id=? AND revision=?", w.ID, w.PlanRevision).Scan(&proof.PlanDigest); err != nil {
		return proof, err
	}
	if err := checkWaveCompletionIdleTx(ctx, tx, b, w); err != nil {
		return proof, err
	}
	return proof, nil
}

// Only ApplyMainResult calls this after authenticating its normal stopped run.
func (k *KernelStore) completeProductWaveTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, runID string, snapshot contract.InputSnapshot, target contract.Target, p contract.ProductAcceptance, summary string) (ProductWaveCompletion, error) {
	var completion ProductWaveCompletion
	proof, err := k.checkProductAcceptanceTx(ctx, tx, b, snapshot, target, p)
	if err != nil {
		return completion, err
	}
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return completion, err
	}
	completion = ProductWaveCompletion{ID: structureID("product-wave-completion-", w.ID, fmt.Sprintf("%d:%d:%d", target.ScopeRevision, target.PlanRevision, target.PolicyRevision)), SessionID: b.ID, RunID: runID, Proof: proof, Summary: summary, CreatedAt: k.clock()}
	w.State, w.Reason, w.CompletedAt = domain.WaveCompleted, summary, completion.CreatedAt
	if err := saveWave(ctx, tx, w); err != nil {
		return completion, err
	}
	if err := kernelRecordTx(ctx, tx, "product_wave_completion", completion.ID, w.ID, completion); err != nil {
		return completion, err
	}
	if err := k.waveEventTx(ctx, tx, w, "wave_completed", "", completion.ID, completion); err != nil {
		return completion, err
	}
	return completion, nil
}
