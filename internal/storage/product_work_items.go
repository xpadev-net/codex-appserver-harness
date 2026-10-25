package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

const maxProductWorkItems = 128

// Ownership and criterion mapping precede candidate preparation. Registration
// is a trusted source port, not a model tool, validation result or dispatch grant.
type ProductWorkItemDefinition struct {
	ID           string   `json:"id"`
	Repository   string   `json:"repository"`
	RepositoryID int64    `json:"repository_id"`
	Criteria     []string `json:"criteria"`
}
type ProductWorkItemRegistration struct {
	SplitScope     *ProductSplitChildScopeReference `json:"split_scope,omitempty"`
	WaveID         string                           `json:"wave_id"`
	ScopeRevision  int                              `json:"scope_revision"`
	PolicyRevision int                              `json:"policy_revision"`
	Source         string                           `json:"source"`
	SourceID       string                           `json:"source_id"`
	Definition     ProductWorkItemDefinition        `json:"definition"`
}
type ProductWorkItemRegistrationSnapshot struct {
	Value  ProductWorkItemRegistration `json:"value"`
	Digest string                      `json:"digest"`
}
type UnavailableProductWorkItem struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// ReadProductWorkItemRegistration is a trusted controller read. It validates
// the saved content digest; it does not grant a model access to source metadata.
func (k *KernelStore) ReadProductWorkItemRegistration(ctx context.Context, id string) (ProductWorkItemRegistrationSnapshot, error) {
	return loadProductWorkItemRegistration(ctx, k.DB, id)
}

func loadProductWorkItemRegistration(ctx context.Context, q queryer, id string) (ProductWorkItemRegistrationSnapshot, error) {
	var saved ProductWorkItemRegistrationSnapshot
	if err := taskEvidenceRecord(ctx, q, "product_work_item_registration", id, &saved); err != nil {
		return saved, err
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.Value.Definition.ID != id || saved.Value.WaveID == "" || saved.Value.ScopeRevision < 1 || saved.Value.PolicyRevision < 1 {
		return saved, fmt.Errorf("%w: product registration integrity conflict", ErrTaskEvidence)
	}
	return saved, nil
}

// Exact retries are read-only. New scope/policy definitions require the old
// digest, retain history and invalidate any previous candidate membership.
func (k *KernelStore) RegisterProductWorkItem(ctx context.Context, target contract.Target, expected string, definition ProductWorkItemDefinition) (ProductWorkItemRegistrationSnapshot, error) {
	var saved ProductWorkItemRegistrationSnapshot
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	saved, err = k.registerProductWorkItemTx(ctx, tx, target, expected, definition)
	if err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

func (k *KernelStore) registerProductWorkItemTx(ctx context.Context, tx *sql.Tx, target contract.Target, expected string, definition ProductWorkItemDefinition) (ProductWorkItemRegistrationSnapshot, error) {
	return k.registerProductWorkItemSourceTx(ctx, tx, target, expected, definition, false)
}

// continuationRefresh is private to the receipt-authenticated continuation port.
// It permits metadata adoption while retaining the source hold, never dispatch.
func (k *KernelStore) registerProductWorkItemSourceTx(ctx context.Context, tx *sql.Tx, target contract.Target, expected string, definition ProductWorkItemDefinition, continuationRefresh bool) (ProductWorkItemRegistrationSnapshot, error) {
	var saved ProductWorkItemRegistrationSnapshot
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return saved, err
	}
	if w.Target() != target || w.Policy.Stage != "P" || !w.Policy.Product {
		return saved, errors.New("current trusted P scope required for product registration")
	}
	definition.Criteria = append([]string{}, definition.Criteria...)
	known := map[string]bool{}
	for _, criterion := range w.Criteria {
		known[criterion.ID] = true
	}
	seen := map[string]bool{}
	for _, id := range definition.Criteria {
		if !known[id] || seen[id] {
			return saved, errors.New("product criteria must be a unique subset of the registered request")
		}
		seen[id] = true
	}
	parts := strings.Split(definition.Repository, "/")
	if definition.RepositoryID < 1 || len(parts) != 2 || parts[0] == "" || parts[1] == "" || len(definition.Criteria) == 0 {
		return saved, errors.New("product identity and criterion coverage required")
	}
	for _, value := range []string{definition.ID, definition.Repository} {
		if value == "" || len(value) > 1024 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n\t ") {
			return saved, errors.New("bounded product identity required")
		}
	}
	if err := rejectProductSplitChildRoot(ctx, tx, definition.ID); err != nil {
		return saved, err
	}
	sort.Strings(definition.Criteria)
	raw, err := json.Marshal(definition)
	if err != nil {
		return saved, err
	}
	if len(raw) > 64<<10 {
		return saved, errors.New("product definition exceeds metadata bound")
	}
	saved.Value = ProductWorkItemRegistration{WaveID: w.ID, ScopeRevision: w.ScopeRevision, PolicyRevision: w.Policy.Revision, Source: w.Source, SourceID: w.SourceID, Definition: definition}
	encoded, err := json.Marshal(saved.Value)
	if err != nil {
		return saved, err
	}
	if len(encoded) > 64<<10 {
		return saved, errors.New("product source registration exceeds metadata bound")
	}
	saved.Digest, err = contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	var membership struct {
		Target      contract.Target `json:"target"`
		CandidateID string          `json:"candidate_id"`
	}
	memberErr := completionRecord(ctx, tx, "wave_work_item", definition.ID, &membership)
	if memberErr != nil && !errors.Is(memberErr, sql.ErrNoRows) {
		return saved, memberErr
	}
	if memberErr == nil && membership.Target.WaveID != w.ID {
		return saved, errors.New("product WorkItem already belongs to another Wave")
	}
	var projection ProductTargetSnapshot
	projectionErr := completionRecord(ctx, tx, "product_target", productTargetID(target, definition.ID), &projection)
	if projectionErr != nil && !errors.Is(projectionErr, sql.ErrNoRows) {
		return saved, projectionErr
	}
	if projectionErr == nil {
		digest, err := contentDigest(projection.Value)
		if err != nil {
			return saved, err
		}
		if err := projection.Value.validate(); err != nil {
			return saved, err
		}
		if projection.Digest != digest || projection.Value.Target.WaveID != w.ID || projection.Value.WorkItem.ID != definition.ID || projection.Value.WorkItem.RepositoryID != definition.RepositoryID {
			return saved, errors.New("existing product projection ownership conflict")
		}
		if projection.Value.Target.ScopeRevision == w.ScopeRevision && projection.Value.Target.PolicyRevision == w.Policy.Revision && projection.Value.Repository != definition.Repository {
			return saved, errors.New("existing product repository differs from source definition")
		}
	}
	prior, priorErr := loadProductWorkItemRegistration(ctx, tx, definition.ID)
	if priorErr != nil && !errors.Is(priorErr, sql.ErrNoRows) {
		return saved, priorErr
	}
	if priorErr == nil {
		if prior.Value.SplitScope != nil {
			return saved, ErrProductSplitHeld
		}
		if memberErr != nil || prior.Value.WaveID != w.ID || prior.Value.Definition.RepositoryID != definition.RepositoryID {
			return saved, errors.New("product registration ownership conflict")
		}
		if prior.Digest == saved.Digest {
			return prior, nil
		}
		if expected != prior.Digest || (prior.Value.ScopeRevision == w.ScopeRevision && prior.Value.PolicyRevision == w.Policy.Revision) {
			return saved, errors.New("product registration requires a versioned source change and matching CAS")
		}
	} else {
		if expected != "" {
			return saved, errors.New("product registration CAS expected an existing definition")
		}
		var reserved bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_work_item_seat' AND id=?)", definition.ID).Scan(&reserved); err != nil {
			return saved, err
		}
		if reserved {
			return saved, errors.New("split child requires authenticated split registration")
		}
		ids, err := productWorkItemLifetimeIDsTx(ctx, tx, w)
		if err != nil {
			return saved, err
		}
		if ids[definition.ID] {
			return saved, errors.New("product WorkItem identity already reserved or missing registration evidence")
		}
		if len(ids) >= maxProductWorkItems {
			return saved, errors.New("product WorkItem lifetime registration limit reached")
		}
	}
	// Source verification may be waiting for this exact versioned metadata
	// refresh. Permit only the CAS-protected existing definition to catch up;
	// retain the source prohibition and all manual/direction/terminal barriers.
	sourceRefresh := priorErr == nil && expected == prior.Digest && (prior.Value.ScopeRevision != w.ScopeRevision || prior.Value.PolicyRevision != w.Policy.Revision)
	sourceRefresh = sourceRefresh || continuationRefresh && errors.Is(priorErr, sql.ErrNoRows) && expected == ""
	if w.Paused || w.PendingDirectionID != "" || (w.SourceHold != nil && !sourceRefresh) || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return saved, errors.New("product source registration is held")
	}
	if memberErr != nil || membership.Target.ScopeRevision != w.ScopeRevision || membership.Target.PolicyRevision != w.Policy.Revision {
		membership.CandidateID = ""
	}
	membership.Target = w.Target()
	if err := kernelRecordTx(ctx, tx, "wave_work_item", definition.ID, w.ID, membership); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_work_item_registration", definition.ID, w.ID, saved); err != nil {
		return saved, err
	}
	if err := kernelRecordTx(ctx, tx, "product_work_item_registration_version", structureID("product-work-", definition.ID, saved.Digest), w.ID, saved); err != nil {
		return saved, err
	}
	return saved, nil
}

func productRegistrationScope(saved ProductWorkItemRegistrationSnapshot, w domain.Wave) bool {
	return saved.Value.WaveID == w.ID && saved.Value.ScopeRevision == w.ScopeRevision && saved.Value.PolicyRevision == w.Policy.Revision && saved.Value.Source == w.Source && saved.Value.SourceID == w.SourceID
}

func (k *KernelStore) productWorkItemInputsTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, w domain.Wave) ([]ProductWorkItemRegistrationSnapshot, []UnavailableProductWorkItem, error) {
	if w.Policy.Stage != "P" || !w.Policy.Product {
		return nil, nil, nil
	}
	var ids []string
	var err error
	if b.Kind == "task" {
		task, e := loadTask(ctx, tx, b.TaskID)
		if e != nil {
			return nil, nil, e
		}
		ids = append(ids, task.Spec.WorkItemIDs...)
		sort.Strings(ids)
	} else {
		ids, err = inputIDsTx(ctx, tx, "SELECT id FROM records WHERE kind='product_work_item_registration' AND work_item_id=? ORDER BY id LIMIT ?", w.ID, maxProductWorkItems+1)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(ids) > maxProductWorkItems {
		return nil, nil, errors.New("product input metadata exceeds WorkItem bound")
	}
	var items []ProductWorkItemRegistrationSnapshot
	var unavailable []UnavailableProductWorkItem
	for _, id := range ids {
		saved, err := loadProductWorkItemRegistration(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			unavailable = append(unavailable, UnavailableProductWorkItem{ID: id, Reason: "source_not_registered"})
			continue
		}
		if b.Kind == "orchestrator" && errors.Is(err, ErrTaskEvidence) {
			unavailable = append(unavailable, UnavailableProductWorkItem{ID: id, Reason: "invalid_source_registration"})
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if !productRegistrationScope(saved, w) {
			unavailable = append(unavailable, UnavailableProductWorkItem{ID: id, Reason: "source_scope_changed"})
			continue
		}
		items = append(items, saved)
	}
	return items, unavailable, nil
}
