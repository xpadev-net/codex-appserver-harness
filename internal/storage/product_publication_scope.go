package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sort"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrProductPublicationScopeHeld = errors.New("product publication requires current tracked residual scope")

// ProductPublicationScope records scope, never implementation, merge or parent
// acceptance. Outside criteria retain their original Wave IDs and requirements;
// each must have a separately registered, persistent tracking WorkItem.
type ProductPublicationScopeValue struct {
	Target          contract.Target                       `json:"target"`
	Source          string                                `json:"source"`
	SourceID        string                                `json:"source_id"`
	RequestDigest   string                                `json:"request_digest"`
	Criteria        []domain.WaveCriterion                `json:"criteria"`
	Included        ProductWorkItemRegistrationSnapshot   `json:"included"`
	Outside         []ProductWorkItemRegistrationSnapshot `json:"outside"`
	OutsideCriteria []string                              `json:"outside_criteria"`
}

type ProductPublicationScope struct {
	ID     string                       `json:"id"`
	Value  ProductPublicationScopeValue `json:"value"`
	Digest string                       `json:"digest"`
}

func productPublicationScopeKey(target contract.Target, workID string) string {
	digest, _ := contentDigest(target)
	return structureID("product-publication-scope-head-", digest, workID)
}

func productPublicationFullScope(w domain.Wave, r ProductWorkItemRegistrationSnapshot) (bool, error) {
	if !productRegistrationScope(r, w) {
		return false, ErrProductPublicationScopeHeld
	}
	known := map[string]bool{}
	for _, c := range w.Criteria {
		known[c.ID] = true
	}
	seen := map[string]bool{}
	if len(r.Value.Definition.Criteria) == 0 {
		return false, ErrTaskEvidence
	}
	for _, c := range r.Value.Definition.Criteria {
		if !known[c] || seen[c] {
			return false, ErrTaskEvidence
		}
		seen[c] = true
	}
	return len(seen) == len(known), nil
}

func publicationScopeRegistrationTx(ctx context.Context, tx *sql.Tx, w domain.Wave, id string) (ProductWorkItemRegistrationSnapshot, error) {
	r, err := loadProductWorkItemRegistration(ctx, tx, id)
	if err != nil {
		return r, err
	}
	if !productRegistrationScope(r, w) {
		return r, ErrProductPublicationScopeHeld
	}
	var version ProductWorkItemRegistrationSnapshot
	if err := taskEvidenceRecord(ctx, tx, "product_work_item_registration_version", structureID("product-work-", id, r.Digest), &version); err != nil {
		return r, err
	}
	if !reflect.DeepEqual(version, r) {
		return r, ErrTaskEvidence
	}
	var member struct {
		Target      contract.Target `json:"target"`
		CandidateID string          `json:"candidate_id"`
	}
	if err := taskEvidenceRecord(ctx, tx, "wave_work_item", id, &member); err != nil {
		return r, err
	}
	if member.Target.WaveID != w.ID || member.Target.ScopeRevision != w.ScopeRevision || member.Target.PolicyRevision != w.Policy.Revision {
		return r, ErrProductPublicationScopeHeld
	}
	_, err = productPublicationFullScope(w, r)
	return r, err
}

// Derive only from authenticated current heads and immutable registration
// versions. Independent indexes discover missing heads, never replace them.
func productPublicationScopeTx(ctx context.Context, tx *sql.Tx, w domain.Wave, workID string) (ProductPublicationScope, error) {
	var saved ProductPublicationScope
	if w.Policy.Stage != "P" || !w.Policy.Product || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked {
		return saved, ErrProductPublicationScopeHeld
	}
	included, err := publicationScopeRegistrationTx(ctx, tx, w, workID)
	if err != nil {
		return saved, err
	}
	saved.Value = ProductPublicationScopeValue{Target: w.Target(), Source: w.Source, SourceID: w.SourceID, Criteria: slices.Clone(w.Criteria), Included: included, Outside: []ProductWorkItemRegistrationSnapshot{}, OutsideCriteria: []string{}}
	sort.Slice(saved.Value.Criteria, func(i, j int) bool { return saved.Value.Criteria[i].ID < saved.Value.Criteria[j].ID })
	saved.Value.RequestDigest, err = contentDigest(w.OriginalRequest)
	if err != nil {
		return saved, err
	}
	covered := map[string]bool{}
	for _, c := range included.Value.Definition.Criteria {
		covered[c] = true
	}
	for _, c := range saved.Value.Criteria {
		if !covered[c.ID] {
			saved.Value.OutsideCriteria = append(saved.Value.OutsideCriteria, c.ID)
		}
	}
	// Full-scope publications do not acquire unrelated peer dependencies.
	if len(saved.Value.OutsideCriteria) > 0 {
		ids, err := inputIDsTx(ctx, tx, "SELECT DISTINCT id FROM records WHERE work_item_id=? AND kind IN ('product_work_item_registration','wave_work_item') ORDER BY id LIMIT ?", w.ID, maxProductWorkItems+1)
		if err != nil {
			return saved, err
		}
		historyIDs, err := inputIDsTx(ctx, tx, "SELECT id FROM records WHERE work_item_id=? AND kind='product_work_item_registration_version' ORDER BY id LIMIT ?", w.ID, maxProductCoverageMerges+1)
		if err != nil {
			return saved, err
		}
		if len(historyIDs) > maxProductCoverageMerges {
			return saved, ErrProductPublicationScopeHeld
		}
		set := map[string]bool{}
		for _, id := range ids {
			set[id] = true
		}
		for _, id := range historyIDs {
			var r ProductWorkItemRegistrationSnapshot
			if err := taskEvidenceRecord(ctx, tx, "product_work_item_registration_version", id, &r); err != nil {
				return saved, err
			}
			digest, err := contentDigest(r.Value)
			if err != nil {
				return saved, err
			}
			if r.Digest != digest || r.Value.WaveID != w.ID || r.Value.Definition.ID == "" || id != structureID("product-work-", r.Value.Definition.ID, digest) {
				return saved, ErrTaskEvidence
			}
			set[r.Value.Definition.ID] = true
		}
		if len(set) > maxProductWorkItems {
			return saved, ErrProductPublicationScopeHeld
		}
		ids = ids[:0]
		for id := range set {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if id == workID {
				continue
			}
			r, err := publicationScopeRegistrationTx(ctx, tx, w, id)
			if err != nil {
				return saved, err
			}
			// Keep all contributors touching outside criteria. They may already
			// be merged; this static scope never invents their completion state.
			if slices.ContainsFunc(r.Value.Definition.Criteria, func(c string) bool { return slices.Contains(saved.Value.OutsideCriteria, c) }) {
				saved.Value.Outside = append(saved.Value.Outside, r)
				for _, c := range r.Value.Definition.Criteria {
					covered[c] = true
				}
			}
		}
		for _, c := range saved.Value.OutsideCriteria {
			if !covered[c] {
				return saved, ErrProductPublicationScopeHeld
			}
		}
	}
	raw, err := json.Marshal(saved.Value)
	if err != nil {
		return saved, err
	}
	if len(raw) > 1<<20 {
		return saved, ErrProductPublicationScopeHeld
	}
	saved.Digest = bytesDigest(raw)
	saved.ID = structureID("product-publication-scope-", productPublicationScopeKey(w.Target(), workID), saved.Digest)
	return saved, nil
}

func loadProductPublicationScope(ctx context.Context, q queryer, target contract.Target, workID string) (ProductPublicationScope, error) {
	var saved, history ProductPublicationScope
	if err := taskEvidenceRecord(ctx, q, "product_publication_scope", productPublicationScopeKey(target, workID), &saved); err != nil {
		return saved, err
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.ID != structureID("product-publication-scope-", productPublicationScopeKey(target, workID), digest) || saved.Value.Target != target || saved.Value.Included.Value.Definition.ID != workID {
		return saved, ErrTaskEvidence
	}
	if err := taskEvidenceRecord(ctx, q, "product_publication_scope_history", saved.ID, &history); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return saved, ErrTaskEvidence
		}
		return saved, err
	}
	if !reflect.DeepEqual(saved, history) {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

// PrepareProductPublicationScope durably saves the residual mapping before any
// later publication. It neither creates WorkItems/Tasks nor spends/reset budgets.
// A lost selector/history is held, never reconstructed by an exact retry.
func (k *KernelStore) PrepareProductPublicationScope(ctx context.Context, target contract.Target, workID string) (ProductPublicationScope, error) {
	var saved ProductPublicationScope
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	saved, err = k.prepareProductPublicationScopeTx(ctx, tx, target, workID)
	if err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

func (k *KernelStore) prepareProductPublicationScopeTx(ctx context.Context, tx *sql.Tx, target contract.Target, workID string) (ProductPublicationScope, error) {
	var saved ProductPublicationScope
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return saved, err
	}
	if w.Target() != target || !k.clock().Before(w.Deadline) {
		return saved, ErrProductPublicationScopeHeld
	}
	saved, err = productPublicationScopeTx(ctx, tx, w, workID)
	if err != nil {
		return saved, err
	}
	prior, readErr := loadProductPublicationScope(ctx, tx, target, workID)
	if readErr == nil {
		if !reflect.DeepEqual(prior, saved) {
			return saved, ErrProductPublicationScopeHeld
		}
		return prior, nil
	}
	if !errors.Is(readErr, sql.ErrNoRows) {
		return saved, readErr
	}
	var known bool
	key := productPublicationScopeKey(target, workID)
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_publication_scope' AND id=?) OR EXISTS(SELECT 1 FROM records WHERE kind='product_publication_scope_history' AND work_item_id=?)", key, key).Scan(&known); err != nil {
		return saved, err
	}
	if known {
		return saved, ErrTaskEvidence
	}
	if err := kernelRecordTx(ctx, tx, "product_publication_scope_history", saved.ID, key, saved); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_publication_scope", key, key, saved); err != nil {
		return saved, err
	}
	return saved, nil
}

// ReadProductPublicationScope authenticates saved scope and rechecks all current
// source/criterion/WorkItem mappings in the same transaction.
func (k *KernelStore) ReadProductPublicationScope(ctx context.Context, target contract.Target, workID string) (ProductPublicationScope, error) {
	var saved ProductPublicationScope
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	return k.readProductPublicationScopeTx(ctx, tx, target, workID)
}

func (k *KernelStore) readProductPublicationScopeTx(ctx context.Context, tx *sql.Tx, target contract.Target, workID string) (ProductPublicationScope, error) {
	var saved ProductPublicationScope
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return saved, err
	}
	if w.Target() != target || !k.clock().Before(w.Deadline) {
		return saved, ErrProductPublicationScopeHeld
	}
	saved, err = loadProductPublicationScope(ctx, tx, target, workID)
	if err != nil {
		return saved, err
	}
	current, err := productPublicationScopeTx(ctx, tx, w, workID)
	if err != nil {
		return saved, err
	}
	if !reflect.DeepEqual(current, saved) {
		return saved, ErrProductPublicationScopeHeld
	}
	return saved, nil
}
