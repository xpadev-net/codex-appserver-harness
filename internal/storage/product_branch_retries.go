package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
)

func productBranchRetryID(previous string) string {
	return structureID("product-branch-retry-", previous, "v1")
}

func validProductBranchAttempt(job ProductBranchPublication) bool {
	if job.Value.RetryOf == "" {
		return job.Value.Attempt == 0 && job.ID == productBranchID(job.Value.OperationID)
	}
	return job.Value.Attempt >= 1 && job.Value.Attempt <= 32 && job.ID == productBranchRetryID(job.Value.RetryOf)
}

func productBranchUnsent(job ProductBranchPublication) bool {
	return job.State == "stopped" && job.Stop != nil && !job.Stop.Observation.Attempted && job.MayHaveSentAt.IsZero() && job.Published == nil
}

func productBranchRetryMatches(previous ProductBranchPublication, value ProductBranchValue) bool {
	if !productBranchUnsent(previous) {
		return false
	}
	expected := previous.Value
	expected.RetryOf, expected.Attempt = previous.ID, previous.Value.Attempt+1
	return reflect.DeepEqual(expected, value)
}

func productBranchAdmitted(ctx context.Context, q queryer, id string) (bool, error) {
	var admitted bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_branch_history','product_branch_claim','product_branch_send_claim','product_branch_stop','product_branch_remote','product_branch_cancel','product_branch_retry_budget')) OR EXISTS(SELECT 1 FROM kernel_dispatch_intents WHERE run_id=?) OR EXISTS(SELECT 1 FROM kernel_budget_ledger WHERE operation_key=? AND kind='branch_retry')`, id, id, id).Scan(&admitted)
	return admitted, err
}

// Derive successors rather than trusting a mutable latest pointer. Missing any
// admitted head/history/budget cannot fall back to an earlier stopped attempt.
func latestProductBranch(ctx context.Context, q queryer, operationID string) (ProductBranchPublication, error) {
	id := productBranchID(operationID)
	job, err := loadProductBranch(ctx, q, id)
	if errors.Is(err, sql.ErrNoRows) {
		if admitted, e := productBranchAdmitted(ctx, q, id); e != nil {
			return job, e
		} else if admitted {
			return job, ErrTaskEvidence
		}
	}
	if err != nil {
		return job, err
	}
	for count := 0; count <= 32; count++ {
		id = productBranchRetryID(job.ID)
		next, err := loadProductBranch(ctx, q, id)
		if errors.Is(err, sql.ErrNoRows) {
			if admitted, e := productBranchAdmitted(ctx, q, id); e != nil {
				return job, e
			} else if admitted {
				return job, ErrTaskEvidence
			}
			return job, nil
		}
		if err != nil {
			return job, err
		}
		if !productBranchRetryMatches(job, next.Value) {
			return job, ErrTaskEvidence
		}
		job = next
	}
	return job, ErrTaskEvidence
}

func validateProductBranchRetryBudget(ctx context.Context, q queryer, job ProductBranchPublication) error {
	expected := OperationBudgetBinding{OperationID: job.ID, WaveID: job.Value.Product.Value.Target.WaveID, Kind: "git", RootID: productBranchID(job.Value.OperationID), RetryNumber: job.Value.Attempt}
	var binding OperationBudgetBinding
	if err := taskEvidenceRecord(ctx, q, "product_branch_retry_budget", job.ID, &binding); err != nil {
		return err
	}
	var raw []byte
	var amount, unknown int
	if err := q.QueryRowContext(ctx, "SELECT payload,amount,unknown FROM kernel_budget_ledger WHERE wave_id=? AND operation_key=? AND kind='branch_retry'", expected.WaveID, job.ID).Scan(&raw, &amount, &unknown); err != nil {
		return err
	}
	var ledger OperationBudgetBinding
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return err
	}
	if binding != expected || ledger != expected || amount != 1 || unknown != 0 {
		return ErrTaskEvidence
	}
	return nil
}

// Count the union of durable admission evidence, not only surviving ledger
// rows. A missing receipt from another operation must not free Wave budget.
func branchRetryBudgetCountTx(ctx context.Context, q *sql.Tx, waveID string) (int, error) {
	rows, err := q.QueryContext(ctx, `SELECT id FROM records WHERE work_item_id=? AND (kind='product_branch_retry_budget' OR kind='product_branch_history' AND coalesce(json_extract(payload,'$.retry_of'),'')!='' OR kind='product_branch_publication' AND coalesce(json_extract(payload,'$.publication.value.retry_of'),'')!='') UNION SELECT operation_key FROM kernel_budget_ledger WHERE wave_id=? AND kind='branch_retry' UNION SELECT run_id FROM kernel_dispatch_intents WHERE wave_id=? AND run_id LIKE 'product-branch-retry-%' LIMIT 33`, waveID, waveID, waveID)
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
		return 0, ErrTaskEvidence
	}
	for _, id := range ids {
		job, err := loadProductBranch(ctx, q, id)
		if err != nil {
			return 0, errors.Join(ErrTaskEvidence, err)
		}
		if job.Value.RetryOf == "" || job.Value.Product.Value.Target.WaveID != waveID {
			return 0, ErrTaskEvidence
		}
	}
	return len(ids), nil
}

// ReserveProductBranchUnsentRetry is a bounded new physical intent, never a
// reset of old history. A send-fenced, attempted, unjoined or published owner
// cannot be retried here, even when a current remote read reports no branch.
func (k *KernelStore) ReserveProductBranchUnsentRetry(ctx context.Context, operationID, ownerID, previousDigest string) (ProductBranchPublication, error) {
	var job ProductBranchPublication
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	previous, err := latestProductBranch(ctx, tx, operationID)
	if err != nil {
		return job, err
	}
	if previous.Value.OwnerID != ownerID {
		return job, ErrProductBranchHeld
	}
	if previous.Digest != previousDigest {
		if previous.Value.RetryOf != "" {
			old, err := loadProductBranch(ctx, tx, previous.Value.RetryOf)
			if err != nil {
				return job, err
			}
			if old.Digest == previousDigest {
				return previous, tx.Commit()
			}
		}
		return job, ErrProductBranchHeld
	}
	if !productBranchUnsent(previous) {
		return job, ErrProductBranchHeld
	}
	claim, ready, err := k.branchDispatchTx(ctx, tx, operationID, ownerID)
	if err != nil {
		return job, err
	}
	if !reflect.DeepEqual(claim.Product, previous.Value.Product) || ready.ID != previous.Value.ReadinessID || ready.Digest != previous.Value.ReadinessDigest {
		return job, ErrProductBranchHeld
	}
	limits, err := waveOperationLimitsTx(ctx, tx, claim.Product.Value.Target.WaveID)
	if err != nil {
		return job, err
	}
	count, err := branchRetryBudgetCountTx(ctx, tx, claim.Product.Value.Target.WaveID)
	if err != nil {
		return job, err
	}
	if count >= limits.BranchRetries || previous.Value.Attempt >= 32 {
		return job, ErrOperationBudget
	}
	value := previous.Value
	value.RetryOf, value.Attempt = previous.ID, previous.Value.Attempt+1
	job = ProductBranchPublication{ID: productBranchRetryID(previous.ID), Value: value, State: "prepared", CreatedAt: k.clock()}
	job.Digest, err = contentDigest(value)
	if err != nil {
		return job, err
	}
	binding := OperationBudgetBinding{OperationID: job.ID, WaveID: value.Product.Value.Target.WaveID, Kind: "git", RootID: productBranchID(operationID), RetryNumber: value.Attempt}
	raw, err := json.Marshal(binding)
	if err != nil {
		return job, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'branch_retry',1,0,?)", binding.WaveID, job.ID, raw); err != nil {
		return job, err
	}
	r, err := k.reserveOperationCapacityTx(ctx, tx, OperationCapacityRequest{ID: job.ID, Kind: "git", SessionID: value.SessionID, Target: value.Product.Value.Target, Repository: value.Source.Repository, Worktree: value.Source.CWD})
	if err != nil {
		return job, err
	}
	if r.State != "held" {
		return job, ErrProductBranchHeld
	}
	if err := kernelRecordTx(ctx, tx, "product_branch_retry_budget", job.ID, binding.WaveID, binding); err != nil {
		return job, err
	}
	if err := kernelRecordTx(ctx, tx, "product_branch_history", job.ID, binding.WaveID, value); err != nil {
		return job, err
	}
	if err := saveProductBranchTx(ctx, tx, job); err != nil {
		return job, err
	}
	return job, tx.Commit()
}
