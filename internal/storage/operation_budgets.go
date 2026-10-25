package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrOperationBudget = errors.New("Wave operation budget exhausted")

// Adopted once from trusted configuration when a Wave is admitted. Profiles
// retain their separately trusted definitions and timeout policy.
type WaveOperationLimits struct {
	BranchRetries      int   `json:"branch_retries,omitempty"`
	SplitDepth         int   `json:"split_depth,omitempty"`
	SplitChildren      int   `json:"split_children,omitempty"`
	SplitCreateRetries int   `json:"split_create_retries,omitempty"`
	ValidationJobs     int   `json:"validation_jobs"`
	RunnerRetries      int   `json:"runner_retries"`
	CandidateReviews   int   `json:"candidate_reviews"`
	TurnSeconds        int64 `json:"turn_seconds"`
	ReviewSeconds      int64 `json:"review_seconds"`
}

func DefaultWaveOperationLimits() WaveOperationLimits {
	return WaveOperationLimits{BranchRetries: 1, SplitDepth: 2, SplitChildren: 8, SplitCreateRetries: 1, ValidationJobs: 100, RunnerRetries: 1, CandidateReviews: 2, TurnSeconds: 1800, ReviewSeconds: 3600}
}
func (l WaveOperationLimits) Validate() error {
	const maxSeconds = int64((1<<63 - 1) / time.Second)
	if l.BranchRetries < 0 || l.BranchRetries > 32 || l.SplitCreateRetries < 0 || l.SplitCreateRetries > 32 || l.ValidationJobs < 1 || l.RunnerRetries < 0 || l.CandidateReviews < 1 || l.TurnSeconds < 1 || l.TurnSeconds > maxSeconds || l.ReviewSeconds < 1 || l.ReviewSeconds > maxSeconds {
		return errors.New("invalid Wave operation limits")
	}
	// Zero/zero is the old persisted format, with splitting disabled. Do not
	// infer new permissions from defaults when reading an existing Wave.
	if l.SplitDepth < 0 || l.SplitDepth > maxProductWorkItems || l.SplitChildren < 0 || l.SplitChildren > maxProductWorkItems || (l.SplitDepth == 0) != (l.SplitChildren == 0) || l.SplitChildren == 1 {
		return errors.New("split limits must both be disabled or bounded depth and at least two children")
	}
	return nil
}
func waveOperationLimitsTx(ctx context.Context, q queryer, waveID string) (WaveOperationLimits, error) {
	// Do not overlay new defaults: older records deliberately retain zero
	// automatic branch retries and/or disabled splitting. New admission only.
	var l WaveOperationLimits
	if err := taskEvidenceRecord(ctx, q, "wave_operation_limits", waveID, &l); err != nil {
		return l, err
	}
	if err := l.Validate(); err != nil {
		return l, fmt.Errorf("%w: %v", ErrTaskEvidence, err)
	}
	return l, nil
}
func (k *KernelStore) WaveOperationLimits(ctx context.Context, waveID string) (WaveOperationLimits, error) {
	return waveOperationLimitsTx(ctx, k.DB, waveID)
}

type OperationBudgetBinding struct {
	OperationID   string `json:"operation_id"`
	WaveID        string `json:"wave_id"`
	Kind          string `json:"kind"`
	RootID        string `json:"root_id"`
	RetryNumber   int    `json:"retry_number"`
	ReviewSubject string `json:"review_subject"`
}

func operationBudgetCountTx(ctx context.Context, tx *sql.Tx, waveID, kind, subject string) (int, error) {
	rows, err := tx.QueryContext(ctx, "SELECT payload FROM kernel_budget_ledger WHERE wave_id=? AND kind=?", waveID, kind)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return 0, err
		}
		var b OperationBudgetBinding
		if err := json.Unmarshal(raw, &b); err != nil {
			return 0, err
		}
		if b.WaveID != waveID || b.OperationID == "" {
			return 0, errors.New("invalid operation budget binding")
		}
		if subject == "" || kind == "runner_retry" && b.RootID == subject || kind == "candidate_review" && b.ReviewSubject == subject {
			count++
		}
	}
	return count, rows.Err()
}
func reserveOperationBudgetTx(ctx context.Context, tx *sql.Tx, r OperationCapacityRequest) error {
	if r.Kind != "runner" && r.Kind != "cloud_review" {
		return nil
	}
	limits, err := waveOperationLimitsTx(ctx, tx, r.Target.WaveID)
	if err != nil {
		return err
	}
	b := OperationBudgetBinding{OperationID: r.ID, WaveID: r.Target.WaveID, Kind: r.Kind, RootID: r.ID}
	kind := "validation_job"
	if r.Kind == "runner" {
		count, err := operationBudgetCountTx(ctx, tx, b.WaveID, kind, "")
		if err != nil {
			return err
		}
		if count >= limits.ValidationJobs {
			return fmt.Errorf("%w: validation jobs", ErrOperationBudget)
		}
		if r.RetryOf != "" {
			previous, err := loadOperationReservationTx(ctx, tx, r.RetryOf)
			if err != nil {
				return err
			}
			sameWork, err := productRunnerRetryWorkspaceTx(ctx, tx, previous, r)
			if err != nil {
				return err
			}
			if previous.Kind != "runner" || previous.WaveID != b.WaveID || previous.SessionID != r.SessionID || previous.Request.Target != r.Target || !sameWork || previous.State != "released" || previous.Proof == nil || previous.Proof.Disposition != "process_stopped" || !previous.Proof.RetryableInfrastructureFailure {
				return errors.New("runner retry requires a stopped, classified infrastructure failure of the same job")
			}
			var old OperationBudgetBinding
			if err := completionRecord(ctx, tx, "operation_budget", previous.ID, &old); err != nil {
				return err
			}
			if old.OperationID != previous.ID || old.Kind != "runner" || old.WaveID != b.WaveID || old.RootID == "" || old.RetryNumber < 0 || old.RetryNumber >= limits.RunnerRetries {
				return fmt.Errorf("%w: runner retry lineage", ErrOperationBudget)
			}
			b.RootID, b.RetryNumber = old.RootID, old.RetryNumber+1
			retries, err := operationBudgetCountTx(ctx, tx, b.WaveID, "runner_retry", b.RootID)
			if err != nil {
				return err
			}
			if b.RetryNumber > limits.RunnerRetries || retries >= limits.RunnerRetries {
				return fmt.Errorf("%w: runner retries", ErrOperationBudget)
			}
		}
	} else {
		kind = "candidate_review"
		b.ReviewSubject = r.ResourceKey + ":" + r.CandidateSHA
		count, err := operationBudgetCountTx(ctx, tx, b.WaveID, kind, b.ReviewSubject)
		if err != nil {
			return err
		}
		if count >= limits.CandidateReviews {
			return fmt.Errorf("%w: same-candidate reviews", ErrOperationBudget)
		}
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,?,1,0,?)", b.WaveID, r.ID, kind, raw); err != nil {
		return err
	}
	if b.RetryNumber > 0 {
		if _, err := tx.ExecContext(ctx, "INSERT INTO kernel_budget_ledger(wave_id,operation_key,kind,amount,unknown,payload) VALUES(?,?,'runner_retry',1,0,?)", b.WaveID, r.ID, raw); err != nil {
			return err
		}
	}
	return kernelRecordTx(ctx, tx, "operation_budget", r.ID, r.SessionID, b)
}

// Like run accounting, older unaccounted operations need explicit offline
// reconciliation. Check on the read-only connection before changing the DB.
func validateKernelOperationBudgetFormat(db *sql.DB) error {
	var missing int
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM kernel_waves w LEFT JOIN records r ON r.kind='wave_operation_limits' AND r.id=w.id WHERE r.id IS NULL)`).Scan(&missing); err != nil {
		return err
	}
	if missing != 0 {
		return errors.New("Wave operation limits missing; explicit offline reconciliation/migration required")
	}
	rows, err := db.Query("SELECT run_id,payload FROM kernel_dispatch_intents")
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return err
		}
		var header struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &header); err != nil {
			rows.Close()
			return err
		}
		if header.Kind == "runner" || header.Kind == "cloud_review" {
			ids = append(ids, id)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := db.QueryRow("SELECT count(*) FROM records WHERE kind='operation_budget' AND id=?", id).Scan(&missing); err != nil {
			return err
		}
		if missing != 1 {
			return fmt.Errorf("operation %s budget binding missing; explicit offline reconciliation/migration required", strings.TrimSpace(id))
		}
	}
	return nil
}

type WaveOperationBudget struct {
	SplitCreateRetries int                 `json:"split_create_retries,omitempty"`
	BranchRetries      int                 `json:"branch_retries,omitempty"`
	WaveID             string              `json:"wave_id"`
	Limits             WaveOperationLimits `json:"limits"`
	ValidationJobs     int                 `json:"validation_jobs"`
	RunnerRetries      int                 `json:"runner_retries"`
	CandidateReviews   map[string]int      `json:"candidate_reviews"`
}

func (k *KernelStore) WaveOperationBudget(ctx context.Context, waveID string) (WaveOperationBudget, error) {
	result := WaveOperationBudget{WaveID: waveID, CandidateReviews: map[string]int{}}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	result.Limits, err = waveOperationLimitsTx(ctx, tx, waveID)
	if err != nil {
		return result, err
	}
	result.ValidationJobs, err = operationBudgetCountTx(ctx, tx, waveID, "validation_job", "")
	if err != nil {
		return result, err
	}
	result.BranchRetries, err = branchRetryBudgetCountTx(ctx, tx, waveID)
	if err != nil {
		return result, err
	}
	result.SplitCreateRetries, err = splitCreateRetryCountTx(ctx, tx, waveID)
	if err != nil {
		return result, err
	}
	result.RunnerRetries, err = operationBudgetCountTx(ctx, tx, waveID, "runner_retry", "")
	if err != nil {
		return result, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT payload FROM kernel_budget_ledger WHERE wave_id=? AND kind='candidate_review'", waveID)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var raw []byte
		var b OperationBudgetBinding
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return result, err
		}
		if err = json.Unmarshal(raw, &b); err != nil {
			rows.Close()
			return result, err
		}
		if b.WaveID != waveID || b.Kind != "cloud_review" || b.ReviewSubject == "" {
			rows.Close()
			return result, errors.New("invalid review budget binding")
		}
		result.CandidateReviews[b.ReviewSubject]++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	return result, tx.Commit()
}
