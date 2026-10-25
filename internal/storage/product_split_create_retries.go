package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"unicode/utf8"
)

type splitCreateRetryBudget struct {
	CreationID        string `json:"creation_id"`
	InitialCreationID string `json:"initial_creation_id"`
	PreviousID        string `json:"previous_id"`
	PreviousDigest    string `json:"previous_digest"`
	WaveID            string `json:"wave_id"`
	ProposalID        string `json:"proposal_id"`
	ParentHeadID      string `json:"parent_head_id"`
	ChildID           string `json:"child_id"`
	Attempt           int    `json:"attempt"`
}

func splitCreateRetryID(previousID string) string {
	return structureID("split-create-retry-", previousID, "v1")
}
func validSplitCreateAttempt(job ProductSplitCreate) bool {
	v := job.Value
	if v.RetryOf == "" {
		return v.Attempt == 0 && v.RetryOfDigest == "" && job.ID == splitCreateID(v.Child.ID)
	}
	return v.Attempt >= 1 && v.Attempt <= 32 && validSourceDigest(v.RetryOfDigest) && job.ID == splitCreateRetryID(v.RetryOf)
}
func splitRetryBudget(job ProductSplitCreate) splitCreateRetryBudget {
	v := job.Value
	return splitCreateRetryBudget{CreationID: job.ID, InitialCreationID: splitCreateID(v.Child.ID), PreviousID: v.RetryOf, PreviousDigest: v.RetryOfDigest, WaveID: v.Request.Target.WaveID, ProposalID: v.Request.ProposalID, ParentHeadID: v.ParentHeadID, ChildID: v.Child.ID, Attempt: v.Attempt}
}
func validateSplitCreateRetryTx(ctx context.Context, tx *sql.Tx, job ProductSplitCreate, depth int) error {
	if job.Value.RetryOf == "" {
		return nil
	}
	prior, err := loadSplitCreateDepth(ctx, tx, job.Value.RetryOf, depth+1)
	if err != nil {
		return splitEvidenceErr(err)
	}
	priorDigest, err := contentDigest(prior)
	if err != nil {
		return err
	}
	expected := prior.Value
	expected.RetryOf, expected.RetryOfDigest, expected.Attempt = prior.ID, priorDigest, prior.Value.Attempt+1
	expected.OwnerID, expected.CreatedAt = job.Value.OwnerID, job.Value.CreatedAt
	if prior.State != "aborted" || prior.HTTP != nil || prior.Abort == nil || job.Value.CreatedAt.Before(prior.Abort.VerifiedAt) || !reflect.DeepEqual(expected, job.Value) {
		return ErrProductSplitHeld
	}
	want := splitRetryBudget(job)
	var saved splitCreateRetryBudget
	if err := taskEvidenceRecord(ctx, tx, "product_split_create_retry_budget", job.ID, &saved); err != nil {
		return splitEvidenceErr(err)
	}
	var raw []byte
	var amount, unknown int
	if err := tx.QueryRowContext(ctx, "SELECT amount,unknown,payload FROM kernel_budget_ledger WHERE wave_id=? AND operation_key=? AND kind='split_create_retry'", want.WaveID, job.ID).Scan(&amount, &unknown, &raw); err != nil {
		return splitEvidenceErr(err)
	}
	var ledger splitCreateRetryBudget
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	if saved != want || ledger != want || amount != 1 || unknown != 0 {
		return ErrProductSplitHeld
	}
	return nil
}

// Count all independent admissions, never only surviving budget ledger rows.
func splitCreateRetryCountTx(ctx context.Context, tx *sql.Tx, waveID string) (int, error) {
	malformed, err := splitScopeUnreadableForWave(ctx, tx, waveID)
	if err != nil {
		return 0, err
	}
	if malformed {
		return 0, ErrProductSplitHeld
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM records WHERE work_item_id=? AND kind IN ('product_split_create','product_split_create_admission','product_split_create_gate','product_split_create_retry_budget') AND id LIKE 'split-create-retry-%'
 UNION SELECT id FROM records WHERE kind='product_split_create_history' AND id LIKE 'split-create-retry-%' AND json_extract(payload,'$.request.target.wave_id')=?
 UNION SELECT work_item_id FROM records WHERE kind='product_split_create_event' AND work_item_id LIKE 'split-create-retry-%' AND json_extract(payload,'$.value.value.request.target.wave_id')=?
 UNION SELECT work_item_id FROM records WHERE kind='product_split_create_gate_history' AND work_item_id LIKE 'split-create-retry-%' AND json_extract(payload,'$.value.request.target.wave_id')=?
 UNION SELECT json_extract(payload,'$.creation_id') FROM records WHERE kind='product_split_created_resource' AND work_item_id=? AND json_extract(payload,'$.creation_id') LIKE 'split-create-retry-%'
 UNION SELECT operation_key FROM kernel_budget_ledger WHERE wave_id=? AND kind='split_create_retry'
 UNION SELECT run_id FROM kernel_resource_leases WHERE wave_id=? AND run_id LIKE 'split-create-retry-%'
 UNION SELECT json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id') FROM records r,json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.creations') c WHERE r.kind IN ('product_split_child_scope','product_split_child_scope_history','product_split_child_scope_admission') AND json_extract(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.request.target.wave_id')=? AND json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.attempt')>0
 UNION SELECT json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id') FROM records r,json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.children') b,json_each(CASE WHEN json_valid(b.value) THEN b.value ELSE '{}' END,'$.value.creations') c WHERE r.kind IN ('product_split_child_scopes','product_split_child_scopes_history','product_split_child_scopes_admission','product_split_registrations','product_split_registrations_history','product_split_registrations_admission','product_split_completion') AND json_extract(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.request.target.wave_id')=? AND json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.attempt')>0
 UNION SELECT json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id') FROM records r,json_each(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.creations') c WHERE r.kind='product_split_child_scope_source' AND r.work_item_id=? AND json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.attempt')>0
 UNION SELECT json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id') FROM records r,json_each(coalesce(json_extract(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.value.split_scope.creations'),json_extract(CASE WHEN json_valid(r.payload) THEN r.payload ELSE '{}' END,'$.split_scope.creations'))) c WHERE r.kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND r.work_item_id=? AND json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.attempt')>0 LIMIT 33`, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID, waveID)
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(ids) > 32 {
		return 0, ErrProductSplitHeld
	}
	for _, id := range ids {
		job, err := loadSplitCreate(ctx, tx, id)
		if err != nil {
			return 0, splitEvidenceErr(err)
		}
		if job.Value.Request.Target.WaveID != waveID || job.Value.Attempt < 1 {
			return 0, ErrProductSplitHeld
		}
	}
	return len(ids), nil
}

// Only an atomically sealed unactivated attempt may get a bounded successor.
// The marker/child/allocation/lifetime WorkItem seats never change. Unknown,
// activated, stopped or observed outcomes cannot be retried from GET absence.
func (k *KernelStore) ReserveProductSplitCreateUnsentRetry(ctx context.Context, previousID, ownerID string) (ProductSplitCreate, error) {
	var empty ProductSplitCreate
	if previousID == "" || strings.TrimSpace(ownerID) == "" || len(ownerID) > 256 || !utf8.ValidString(ownerID) || strings.ContainsAny(ownerID, "\x00\r\n") {
		return empty, ErrProductSplitHeld
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	previous, err := loadSplitCreate(ctx, tx, previousID)
	if err != nil {
		return empty, err
	}
	if previous.State != "aborted" || previous.HTTP != nil || previous.Abort == nil || previous.Value.Attempt >= 32 {
		return empty, ErrProductSplitHeld
	}
	id := splitCreateRetryID(previousID)
	existing, err := loadSplitCreate(ctx, tx, id)
	if err == nil {
		if existing.Value.RetryOf != previousID || existing.Value.OwnerID != ownerID {
			return empty, ErrProductSplitHeld
		}
		return existing, nil
	}
	if err != sql.ErrNoRows {
		return empty, err
	}
	r := previous.Value.Request
	if err := k.splitCreatePreflightTx(ctx, tx, r); err != nil {
		return empty, err
	}
	limits, err := waveOperationLimitsTx(ctx, tx, r.Target.WaveID)
	if err != nil {
		return empty, splitEvidenceErr(err)
	}
	used, err := splitCreateRetryCountTx(ctx, tx, r.Target.WaveID)
	if err != nil {
		return empty, err
	}
	if used >= limits.SplitCreateRetries {
		return empty, ErrOperationBudget
	}
	value := previous.Value
	value.RetryOf, value.Attempt, value.OwnerID, value.CreatedAt = previousID, value.Attempt+1, ownerID, k.clock()
	value.RetryOfDigest, err = contentDigest(previous)
	if err != nil {
		return empty, err
	}
	if value.CreatedAt.Before(previous.Abort.VerifiedAt) {
		return empty, ErrProductSplitHeld
	}
	job := ProductSplitCreate{ID: id, Value: value, State: "prepared"}
	job.Digest, err = contentDigest(value)
	if err != nil {
		return empty, err
	}
	job.Lease, err = claimResourceTx(ctx, tx, previous.Lease.Resource, id, r.Target.WaveID)
	if err != nil {
		return empty, err
	}
	binding := splitRetryBudget(job)
	if err := kernelRecordTx(ctx, tx, "product_split_create_retry_budget", id, r.Target.WaveID, binding); err != nil {
		return empty, err
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		return empty, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'split_create_retry',1,0,?)", r.Target.WaveID, id, raw); err != nil {
		return empty, err
	}
	if err := kernelRecordTx(ctx, tx, "product_split_create_history", id, r.ProposalID, value); err != nil {
		return empty, err
	}
	if err := kernelRecordTx(ctx, tx, "product_split_create_admission", id, r.Target.WaveID, splitCreateAdmission{r.ProposalID, value.ParentHeadID, value.Child.ID, job.Digest}); err != nil {
		return empty, err
	}
	job, err = saveSplitCreateTx(ctx, tx, job, nil)
	if err != nil {
		return empty, err
	}
	return job, tx.Commit()
}

// A missing admitted successor never falls back to an earlier aborted attempt.
func (k *KernelStore) ReadLatestProductSplitCreate(ctx context.Context, childID string) (ProductSplitCreate, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSplitCreate{}, err
	}
	defer tx.Rollback()
	return loadLatestProductSplitCreate(ctx, tx, childID)
}
func loadLatestProductSplitCreate(ctx context.Context, tx *sql.Tx, childID string) (ProductSplitCreate, error) {
	job, err := loadSplitCreate(ctx, tx, splitCreateID(childID))
	if err != nil {
		return job, err
	}
	for attempt := 0; attempt <= 32; attempt++ {
		next, err := loadSplitCreate(ctx, tx, splitCreateRetryID(job.ID))
		if err == sql.ErrNoRows {
			return job, nil
		}
		if err != nil {
			return ProductSplitCreate{}, err
		}
		if attempt == 32 || next.Value.RetryOf != job.ID {
			return ProductSplitCreate{}, ErrProductSplitHeld
		}
		job = next
	}
	return job, nil
}
