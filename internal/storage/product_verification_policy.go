package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
)

var ErrProductVerificationPolicyConflict = errors.New("verification policy is already pinned for this Wave policy revision")

// ProductVerificationPolicy is host configuration, outside candidate trees and
// model tools. Full profile argv is never copied into a model input manifest.
type ProductVerificationPolicy struct {
	WaveID         string              `json:"wave_id"`
	PolicyRevision int                 `json:"policy_revision"`
	RepositoryID   int64               `json:"repository_id"`
	Repository     string              `json:"repository"`
	VerifierPaths  []string            `json:"verifier_paths"`
	Verification   policy.Verification `json:"verification"`
}
type ProductVerificationPolicySnapshot struct {
	ID     string                    `json:"id"`
	Value  ProductVerificationPolicy `json:"value"`
	Digest string                    `json:"digest"`
}

// ProductVerificationPolicyInput contains only public scope/profile names; it
// is data for planning tests, not execution permission or runner success.
type ProductVerificationPolicyInput struct {
	ID                string   `json:"id"`
	Digest            string   `json:"digest"`
	VerifierPaths     []string `json:"verifier_paths"`
	RequiredProfiles  []string `json:"required_profiles"`
	AvailableProfiles []string `json:"available_profiles"`
}

func (value ProductVerificationPolicy) Validate() error {
	_, err := normalizeProductVerificationPolicy(value)
	return err
}

func productVerificationPolicyID(waveID string, revision int, repositoryID int64) string {
	return structureID("product-verification-policy-", waveID, fmt.Sprintf("%d:%d", revision, repositoryID))
}
func normalizeProductVerificationPolicy(value ProductVerificationPolicy) (ProductVerificationPolicy, error) {
	if value.WaveID == "" || !utf8.ValidString(value.WaveID) || len(value.WaveID) > contract.MaxInline || value.PolicyRevision < 1 || value.RepositoryID < 1 || !filepath.IsAbs(value.Repository) || filepath.Clean(value.Repository) != value.Repository || len(value.Repository) > contract.MaxInline || !utf8.ValidString(value.Repository) || strings.ContainsAny(value.Repository, "\x00\r\n") || len(value.VerifierPaths) == 0 || len(value.VerifierPaths) > 128 || len(value.Verification.Profiles) == 0 || len(value.Verification.Profiles) > 64 || len(value.Verification.RequiredProfiles) > 64 || value.Verification.Revision != value.PolicyRevision {
		return value, errors.New("bounded host verification policy and repository required")
	}
	if err := value.Verification.Validate(); err != nil {
		return value, err
	}
	seen := map[string]bool{}
	for _, rule := range value.VerifierPaths {
		clean := strings.TrimSuffix(rule, "/")
		if !utf8.ValidString(rule) || len(rule) > contract.MaxInline || clean == "" || clean == "." || clean == ".." || path.IsAbs(clean) || path.Clean(clean) != clean || strings.HasPrefix(clean, "../") || strings.ContainsAny(rule, "\x00\r\n\\*?[]") || seen[rule] {
			return value, errors.New("test paths must be unique canonical relative exact paths or directory prefixes")
		}
		for _, part := range strings.Split(clean, "/") {
			if strings.EqualFold(part, ".git") {
				return value, errors.New("verification cannot write Git metadata")
			}
		}
		seen[rule] = true
	}
	for _, profile := range value.Verification.Profiles {
		if len(profile.Argv) > 128 || len(profile.SuccessPattern) > contract.MaxInline || !utf8.ValidString(profile.SuccessPattern) || profile.TimeoutSeconds > 86400 {
			return value, errors.New("bounded verification profile required")
		}
		for _, arg := range profile.Argv {
			if !utf8.ValidString(arg) || len(arg) > contract.MaxInline || strings.ContainsRune(arg, '\x00') {
				return value, errors.New("bounded UTF-8 verification argv required")
			}
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return value, err
	}
	if len(raw) > 1<<20 {
		return value, errors.New("verification policy exceeds storage bound")
	}
	// Detach maps and slices from the caller, then canonicalize path-set order.
	var detached ProductVerificationPolicy
	if err := json.Unmarshal(raw, &detached); err != nil {
		return value, err
	}
	value = detached
	sort.Strings(value.VerifierPaths)
	return value, nil
}

func loadProductVerificationPolicy(ctx context.Context, q queryer, waveID string, revision int, repositoryID int64) (ProductVerificationPolicySnapshot, error) {
	var saved ProductVerificationPolicySnapshot
	id := productVerificationPolicyID(waveID, revision, repositoryID)
	err := taskEvidenceRecord(ctx, q, "product_verification_policy", id, &saved)
	if errors.Is(err, sql.ErrNoRows) {
		var history bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_verification_policy_history' AND id=?)", id).Scan(&history); err != nil {
			return saved, err
		}
		if history {
			return saved, ErrTaskEvidence
		}
		return saved, sql.ErrNoRows
	}
	if err != nil {
		return saved, err
	}
	value, err := normalizeProductVerificationPolicy(saved.Value)
	if err != nil {
		return saved, ErrTaskEvidence
	}
	digest, err := contentDigest(value)
	if err != nil {
		return saved, err
	}
	if saved.ID != id || saved.Value.WaveID != waveID || saved.Value.PolicyRevision != revision || saved.Value.RepositoryID != repositoryID || saved.Digest != digest || !reflect.DeepEqual(value, saved.Value) {
		return saved, ErrTaskEvidence
	}
	var history ProductVerificationPolicySnapshot
	if err := taskEvidenceRecord(ctx, q, "product_verification_policy_history", id, &history); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return saved, ErrTaskEvidence
		}
		return saved, err
	}
	if !reflect.DeepEqual(history, saved) {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

// RegisterProductVerificationPolicy pins one immutable host policy per Wave
// policy revision/repository. It never grants write/dispatch or invokes Git.
func (k *KernelStore) RegisterProductVerificationPolicy(ctx context.Context, target contract.Target, value ProductVerificationPolicy) (ProductVerificationPolicySnapshot, error) {
	var saved ProductVerificationPolicySnapshot
	value, err := normalizeProductVerificationPolicy(value)
	if err != nil {
		return saved, err
	}
	canonical, err := resourcePath(value.Repository)
	if err != nil {
		return saved, err
	}
	if canonical != value.Repository {
		return saved, errors.New("canonical verification repository required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return saved, err
	}
	if w.Target() != target || w.Policy.Stage != "P" || !w.Policy.Product || value.WaveID != w.ID || value.PolicyRevision != w.Policy.Revision {
		return saved, errors.New("verification policy requires current trusted P revision")
	}
	saved, err = loadProductVerificationPolicy(ctx, tx, w.ID, w.Policy.Revision, value.RepositoryID)
	if err == nil {
		if !reflect.DeepEqual(saved.Value, value) {
			return saved, ErrProductVerificationPolicyConflict
		}
		return saved, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return saved, err
	}
	if w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) {
		return saved, ErrProductSubjectHeld
	}
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM records WHERE kind='product_work_item_registration' AND work_item_id=? ORDER BY id", w.ID)
	if err != nil {
		return saved, err
	}
	owned := false
	for _, id := range ids {
		registration, err := loadProductWorkItemRegistration(ctx, tx, id)
		if err != nil {
			return saved, err
		}
		owned = owned || productRegistrationScope(registration, w) && registration.Value.Definition.RepositoryID == value.RepositoryID
	}
	if !owned {
		return saved, errors.New("verification repository is outside registered product scope")
	}
	digest, err := contentDigest(value)
	if err != nil {
		return saved, err
	}
	saved = ProductVerificationPolicySnapshot{ID: productVerificationPolicyID(w.ID, w.Policy.Revision, value.RepositoryID), Value: value, Digest: digest}
	for _, kind := range []string{"product_verification_policy", "product_verification_policy_history"} {
		if err := kernelRecordTx(ctx, tx, kind, saved.ID, w.ID, saved); err != nil {
			return saved, err
		}
	}
	return saved, tx.Commit()
}

func (k *KernelStore) ReadProductVerificationPolicy(ctx context.Context, target contract.Target, repositoryID int64) (ProductVerificationPolicySnapshot, error) {
	w, err := k.Wave(ctx, target.WaveID)
	if err != nil {
		return ProductVerificationPolicySnapshot{}, err
	}
	if w.Target() != target || w.Policy.Stage != "P" || !w.Policy.Product {
		return ProductVerificationPolicySnapshot{}, ErrTaskEvidence
	}
	return loadProductVerificationPolicy(ctx, k.DB, w.ID, w.Policy.Revision, repositoryID)
}

func publicProductVerificationPolicy(saved ProductVerificationPolicySnapshot) ProductVerificationPolicyInput {
	available := make([]string, 0, len(saved.Value.Verification.Profiles))
	for id := range saved.Value.Verification.Profiles {
		available = append(available, id)
	}
	sort.Strings(available)
	return ProductVerificationPolicyInput{ID: saved.ID, Digest: saved.Digest, VerifierPaths: append([]string{}, saved.Value.VerifierPaths...), RequiredProfiles: append([]string{}, saved.Value.Verification.RequiredProfiles...), AvailableProfiles: available}
}

func (k *KernelStore) productVerificationPolicyInputTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) (*ProductVerificationPolicyInput, error) {
	if !UsesProductVerificationOutput(w, task) {
		return nil, nil
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, task.Spec.WorkItemIDs[0])
	if err != nil {
		return nil, err
	}
	if !productRegistrationScope(registration, w) {
		return nil, ErrTaskEvidence
	}
	saved, err := loadProductVerificationPolicy(ctx, tx, w.ID, w.Policy.Revision, registration.Value.Definition.RepositoryID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	input := publicProductVerificationPolicy(saved)
	return &input, nil
}
