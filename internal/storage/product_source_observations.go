package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type ProductSourceFacts struct {
	RepositoryID  int64     `json:"repository_id"`
	IssueNumber   int       `json:"issue_number"`
	PolicyDigest  string    `json:"policy_digest"`
	Available     bool      `json:"available"`
	IssueState    string    `json:"issue_state"`
	Ready         bool      `json:"ready"`
	Hold          bool      `json:"hold"`
	RequestDigest string    `json:"request_digest"`
	UpdatedAt     time.Time `json:"updated_at"`
	Failure       string    `json:"failure"`
}

type ProductSourceObservation struct {
	WaveID         string             `json:"wave_id"`
	SourceID       string             `json:"source_id"`
	ScopeRevision  int                `json:"scope_revision"`
	PolicyRevision int                `json:"policy_revision"`
	Status         string             `json:"status"`
	Facts          ProductSourceFacts `json:"facts"`
}
type ProductSourceSnapshot struct {
	ID         string                   `json:"id"`
	Version    int64                    `json:"version"`
	ObservedAt time.Time                `json:"observed_at"`
	Value      ProductSourceObservation `json:"value"`
	Digest     string                   `json:"digest"`
}
type ProductSourceUpdate struct {
	Snapshot ProductSourceSnapshot  `json:"snapshot"`
	Hold     *domain.WaveSourceHold `json:"hold,omitempty"`
	Changed  bool                   `json:"changed"`
}

func ProductSourceRequestDigest(request string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(request)))
}

func validSourceDigest(value string) bool {
	return len(value) == 64 && strings.Trim(value, "0123456789abcdef") == ""
}

func (f ProductSourceFacts) validate() error {
	if f.RepositoryID < 1 || f.IssueNumber < 1 || !validSourceDigest(f.PolicyDigest) {
		return errors.New("pinned product source identity and policy digest required")
	}
	if f.Available {
		if (f.IssueState != "open" && f.IssueState != "closed") || !validSourceDigest(f.RequestDigest) || f.UpdatedAt.IsZero() || f.Failure != "" {
			return errors.New("complete product source facts required")
		}
	} else {
		if f.IssueState != "" || f.RequestDigest != "" || f.Ready || f.Hold || !f.UpdatedAt.IsZero() || (f.Failure != "read_failed" && f.Failure != "repository_mismatch" && f.Failure != "invalid_source") {
			return errors.New("explicit unavailable product source required")
		}
	}
	return nil
}

func validSourceStatus(status string) bool {
	switch status {
	case "ready", "unavailable", "closed", "hold", "not_ready", "request_changed", "scope_unconfirmed", "policy_changed":
		return true
	}
	return false
}

// Immutable per policy revision, independent of scope and plan changes. The
// second record is retained as evidence if the authoritative pin is lost.
type productSourcePolicy struct {
	WaveID         string `json:"wave_id"`
	SourceID       string `json:"source_id"`
	PolicyRevision int    `json:"policy_revision"`
	PolicyDigest   string `json:"policy_digest"`
}
type productSourcePolicyRecord struct {
	Value  productSourcePolicy `json:"value"`
	Digest string              `json:"digest"`
}

func productSourcePolicyID(w domain.Wave) string {
	return structureID("product-source-policy-", w.ID, strconv.Itoa(w.Policy.Revision))
}
func saveProductSourcePolicyTx(ctx context.Context, tx *sql.Tx, w domain.Wave, digest string) error {
	value := productSourcePolicy{WaveID: w.ID, SourceID: w.SourceID, PolicyRevision: w.Policy.Revision, PolicyDigest: digest}
	checksum, err := contentDigest(value)
	if err != nil {
		return err
	}
	record := productSourcePolicyRecord{Value: value, Digest: checksum}
	for _, kind := range []string{"product_source_policy", "product_source_policy_history"} {
		if err := kernelRecordTx(ctx, tx, kind, productSourcePolicyID(w), w.ID, record); err != nil {
			return err
		}
	}
	return nil
}
func productSourcePolicyTx(ctx context.Context, tx *sql.Tx, w domain.Wave) (string, error) {
	var record productSourcePolicyRecord
	err := taskEvidenceRecord(ctx, tx, "product_source_policy", productSourcePolicyID(w), &record)
	if errors.Is(err, sql.ErrNoRows) {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE (kind='product_source_policy_history' AND id=?) OR (kind IN ('product_source_observation','product_source_observation_history') AND work_item_id=? AND json_extract(payload,'$.value.policy_revision')=?)", productSourcePolicyID(w), w.ID, w.Policy.Revision).Scan(&count); err != nil {
			return "", err
		}
		if count != 0 {
			return "", fmt.Errorf("%w: source policy pin missing", ErrTaskEvidence)
		}
		return "", nil
	}
	if err != nil {
		return "", err
	}
	digest, err := contentDigest(record.Value)
	if err != nil {
		return "", err
	}
	if record.Digest != digest || record.Value.WaveID != w.ID || record.Value.SourceID != w.SourceID || record.Value.PolicyRevision != w.Policy.Revision || !validSourceDigest(record.Value.PolicyDigest) {
		return "", fmt.Errorf("%w: source policy pin integrity conflict", ErrTaskEvidence)
	}
	return record.Value.PolicyDigest, nil
}

func loadProductSourceSnapshot(ctx context.Context, q queryer, waveID string) (ProductSourceSnapshot, error) {
	var saved ProductSourceSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_source_observation", waveID, &saved); err != nil {
		return saved, err
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if saved.ID == "" || saved.Version < 1 || saved.ObservedAt.IsZero() || saved.Value.WaveID != waveID || saved.Value.ScopeRevision < 1 || saved.Value.PolicyRevision < 1 || !validSourceStatus(saved.Value.Status) || saved.Value.SourceID != fmt.Sprintf("r%d-i%d", saved.Value.Facts.RepositoryID, saved.Value.Facts.IssueNumber) || saved.Digest != digest || saved.Value.Facts.validate() != nil {
		return saved, fmt.Errorf("%w: product source observation integrity conflict", ErrTaskEvidence)
	}
	return saved, nil
}

func (k *KernelStore) ReadProductSourceObservation(ctx context.Context, waveID string) (ProductSourceSnapshot, error) {
	return loadProductSourceSnapshot(ctx, k.DB, waveID)
}

func sourceStatusTx(ctx context.Context, tx *sql.Tx, w domain.Wave, facts ProductSourceFacts) (string, error) {
	switch {
	case !facts.Available:
		return "unavailable", nil
	case facts.IssueState == "closed":
		return "closed", nil
	case facts.Hold:
		return "hold", nil
	case !facts.Ready:
		return "not_ready", nil
	case facts.RequestDigest != ProductSourceRequestDigest(w.OriginalRequest):
		return "request_changed", nil
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, w.SourceID)
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrTaskEvidence) {
		return "scope_unconfirmed", nil
	}
	if err != nil {
		return "", err
	}
	if !productRegistrationScope(registration, w) || registration.Value.Definition.RepositoryID != facts.RepositoryID {
		return "scope_unconfirmed", nil
	}
	return "ready", nil
}

func sameSourceHold(a, b *domain.WaveSourceHold) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && a.Reason == b.Reason && a.Digest == b.Digest)
}

// ObserveProductSource accepts host-verified GET facts, never a model tool.
// The CAS is captured before the external read. It can change only this
// source prohibition; manual pause, directions, scope and acceptance stay owned
// by their existing controllers. Source "ready" is not a quality approval.
func (k *KernelStore) ObserveProductSource(ctx context.Context, target contract.Target, expected string, facts ProductSourceFacts) (ProductSourceUpdate, error) {
	var result ProductSourceUpdate
	if !target.Valid() {
		return result, errors.New("current product source target required")
	}
	if err := facts.validate(); err != nil {
		return result, err
	}
	facts.UpdatedAt = facts.UpdatedAt.UTC().Round(0)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return result, err
	}
	if w.Policy.Stage != "P" || !w.Policy.Product || w.Source != "github" || w.ScopeRevision != target.ScopeRevision || w.Policy.Revision != target.PolicyRevision || w.SourceID != fmt.Sprintf("r%d-i%d", facts.RepositoryID, facts.IssueNumber) {
		return result, errors.New("product source ownership or scope conflict")
	}
	prior, priorErr := loadProductSourceSnapshot(ctx, tx, w.ID)
	if errors.Is(priorErr, ErrTaskEvidence) {
		return k.prohibitInvalidSourceTx(ctx, tx, w, priorErr)
	}
	if priorErr != nil && !errors.Is(priorErr, sql.ErrNoRows) {
		return result, priorErr
	}
	if errors.Is(priorErr, sql.ErrNoRows) {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='product_source_observation_history' AND work_item_id=?", w.ID).Scan(&count); err != nil {
			return result, err
		}
		if count != 0 {
			return k.prohibitInvalidSourceTx(ctx, tx, w, fmt.Errorf("%w: current source observation missing", ErrTaskEvidence))
		}
	}
	status, err := sourceStatusTx(ctx, tx, w, facts)
	if err != nil {
		return result, err
	}
	pinnedPolicy, err := productSourcePolicyTx(ctx, tx, w)
	if errors.Is(err, ErrTaskEvidence) {
		return k.prohibitInvalidSourceTx(ctx, tx, w, err)
	}
	if err != nil {
		return result, err
	}
	if pinnedPolicy != "" && pinnedPolicy != facts.PolicyDigest {
		status = "policy_changed"
	}
	value := ProductSourceObservation{WaveID: w.ID, SourceID: w.SourceID, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, Status: status, Facts: facts}
	digest, err := contentDigest(value)
	if err != nil {
		return result, err
	}
	var hold *domain.WaveSourceHold
	if status != "ready" && w.State != domain.WaveCanceled && w.State != domain.WaveCompleted {
		hold = &domain.WaveSourceHold{Reason: status, Digest: digest, ObservedAt: k.clock()}
	}
	if priorErr == nil && prior.Digest == digest && sameSourceHold(w.SourceHold, hold) {
		return ProductSourceUpdate{Snapshot: prior, Hold: w.SourceHold}, tx.Commit()
	}
	if (priorErr == nil && expected != prior.Digest) || (priorErr != nil && expected != "") {
		return result, errors.New("product source observation CAS conflict")
	}
	if pinnedPolicy == "" {
		if err := saveProductSourcePolicyTx(ctx, tx, w, facts.PolicyDigest); err != nil {
			return result, err
		}
	}
	saved := prior
	if priorErr != nil || prior.Digest != digest {
		if prior.Version == math.MaxInt64 {
			return result, errors.New("source observation version exhausted")
		}
		id, err := opaqueID("source-observation-")
		if err != nil {
			return result, err
		}
		saved = ProductSourceSnapshot{ID: id, Version: prior.Version + 1, ObservedAt: k.clock(), Value: value, Digest: digest}
		if err := kernelRecordTx(ctx, tx, "product_source_observation", w.ID, w.ID, saved); err != nil {
			return result, err
		}
		if err := kernelRecordTx(ctx, tx, "product_source_observation_history", saved.ID, w.ID, saved); err != nil {
			return result, err
		}
	}
	changed := !sameSourceHold(w.SourceHold, hold)
	if changed {
		w.SourceHold = hold
		if err := saveWave(ctx, tx, w); err != nil {
			return result, err
		}
		if err := sourceControlEventTx(ctx, tx, w); err != nil {
			return result, err
		}
	}
	result = ProductSourceUpdate{Snapshot: saved, Hold: w.SourceHold, Changed: changed}
	if err := tx.Commit(); err != nil {
		return ProductSourceUpdate{}, err
	}
	return result, nil
}

// Preserve corrupt evidence and install only a prohibition. No ordinary
// observation can silently reconstruct or authorize a missing/corrupt head.
func (k *KernelStore) prohibitInvalidSourceTx(ctx context.Context, tx *sql.Tx, w domain.Wave, cause error) (ProductSourceUpdate, error) {
	result := ProductSourceUpdate{Hold: w.SourceHold}
	if w.State == domain.WaveCanceled || w.State == domain.WaveCompleted {
		return result, cause
	}
	digest, err := contentDigest(struct {
		Wave, Source  string
		Scope, Policy int
	}{w.ID, w.SourceID, w.ScopeRevision, w.Policy.Revision})
	if err != nil {
		return result, err
	}
	hold := &domain.WaveSourceHold{Reason: "state_invalid", Digest: digest, ObservedAt: k.clock()}
	if sameSourceHold(w.SourceHold, hold) {
		return result, cause
	}
	id, err := opaqueID("source-fault-")
	if err != nil {
		return result, err
	}
	w.SourceHold = hold
	if err := kernelRecordTx(ctx, tx, "product_source_observation_fault", id, w.ID, hold); err != nil {
		return result, err
	}
	if err := saveWave(ctx, tx, w); err != nil {
		return result, err
	}
	if err := sourceControlEventTx(ctx, tx, w); err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, err
	}
	return ProductSourceUpdate{Hold: hold, Changed: true}, cause
}

// State/watch evidence is appended without a notification intent. Observed
// source controls cannot create an unconditional model judgment turn.
func sourceControlEventTx(ctx context.Context, tx *sql.Tx, w domain.Wave) error {
	id, err := opaqueID("source-control-")
	if err != nil {
		return err
	}
	raw, err := json.Marshal(struct {
		SourceID string                 `json:"source_id"`
		Hold     *domain.WaveSourceHold `json:"hold"`
	}{w.SourceID, w.SourceHold})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO kernel_events(id,wave_id,task_id,kind,scope_revision,plan_revision,payload) VALUES(?,?,'','product_source_hold_changed',?,?,?)", id, w.ID, w.ScopeRevision, w.PlanRevision, raw)
	return err
}
