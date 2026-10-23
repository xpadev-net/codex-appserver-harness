package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

var ErrExecutionCapacity = errors.New("controller execution capacity is exhausted")
var ErrResourceBusy = errors.New("execution resource requires owner reconciliation")

type ExecutionPolicy struct {
	Revision int                    `json:"revision"`
	Limits   domain.ExecutionLimits `json:"limits"`
}
type ResourceLease struct {
	Resource   string `json:"resource"`
	Generation int64  `json:"generation"`
}
type ProcessReservation struct {
	Kind           string          `json:"kind"`
	ID             string          `json:"id"`
	SessionID      string          `json:"session_id"`
	WaveID         string          `json:"wave_id"`
	RunID          string          `json:"run_id"`
	State          string          `json:"state"`
	PolicyRevision int             `json:"policy_revision"`
	Resources      []ResourceLease `json:"resources"`
	CreatedAt      time.Time       `json:"created_at"`
	StoppedAt      time.Time       `json:"stopped_at"`
}
type ExecutionCapacity struct {
	Policy                   ExecutionPolicy `json:"policy"`
	HeldProcessSlots         int             `json:"held_process_slots"`
	UnreconciledPreparations int             `json:"unreconciled_preparations"`
}

func executionPolicyTx(ctx context.Context, q queryer) (ExecutionPolicy, error) {
	p := ExecutionPolicy{}
	err := completionRecord(ctx, q, "kernel_execution_policy", "controller", &p)
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionPolicy{Revision: 1, Limits: domain.DefaultExecutionLimits()}, nil
	}
	if err != nil {
		return p, err
	}
	if p.Revision < 1 {
		return p, errors.New("invalid persisted execution policy")
	}
	return p, p.Limits.Validate()
}

// ConfigureExecutionLimits persists trusted configuration. Changing capacities
// requires all processes stopped; it never steals or drops an existing owner.
func (k *KernelStore) ConfigureExecutionLimits(ctx context.Context, limits domain.ExecutionLimits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	p, err := executionPolicyTx(ctx, tx)
	if err != nil {
		return err
	}
	if p.Limits != limits {
		var live int
		if err = tx.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM kernel_sessions WHERE process_stopped=0)+(SELECT count(*) FROM kernel_dispatch_intents WHERE state='held')+(SELECT count(*) FROM kernel_resource_leases WHERE state='held')").Scan(&live); err != nil {
			return err
		}
		if live != 0 {
			return errors.New("stop and reconcile all processes before changing execution capacities")
		}
		if p.Revision == int(^uint(0)>>1) {
			return errors.New("execution policy revision exhausted")
		}
		if err = kernelRecordTx(ctx, tx, "kernel_execution_policy_history", fmt.Sprint(p.Revision), "controller", p); err != nil {
			return err
		}
		p.Revision++
		p.Limits = limits
	}
	if err = kernelRecordTx(ctx, tx, "kernel_execution_policy", "controller", "controller", p); err != nil {
		return err
	}
	return tx.Commit()
}
func (k *KernelStore) ExecutionCapacity(ctx context.Context) (ExecutionCapacity, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ExecutionCapacity{}, err
	}
	defer tx.Rollback()
	p, err := executionPolicyTx(ctx, tx)
	if err != nil {
		return ExecutionCapacity{}, err
	}
	c := ExecutionCapacity{Policy: p}
	err = tx.QueryRowContext(ctx, "SELECT count(*),coalesce(sum(CASE WHEN s.active_run_id='' OR r.state IN ('unknown','blocked') THEN 1 ELSE 0 END),0) FROM kernel_sessions s LEFT JOIN kernel_run_bindings r ON r.run_id=s.active_run_id WHERE s.process_stopped=0").Scan(&c.HeldProcessSlots, &c.UnreconciledPreparations)
	if err != nil {
		return c, err
	}
	return c, tx.Commit()
}

func checkWaveCapacityTx(ctx context.Context, tx *sql.Tx) error {
	p, err := executionPolicyTx(ctx, tx)
	if err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "kernel_execution_policy", "controller", "controller", p); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "SELECT id,payload FROM kernel_waves")
	if err != nil {
		return err
	}
	defer rows.Close()
	live := 0
	for rows.Next() {
		var id string
		var raw []byte
		var w domain.Wave
		if err = rows.Scan(&id, &raw); err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &w); err != nil {
			return err
		}
		if err = w.Validate(); err != nil {
			return err
		}
		if w.ID != id {
			return errors.New("Wave identity differs while checking capacity")
		}
		if w.State != domain.WaveCompleted && w.State != domain.WaveCanceled {
			live++
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if live >= p.Limits.Waves {
		return ErrExecutionCapacity
	}
	return nil
}

// Resolve aliases even before a controller-created Task workspace exists.
func resourcePath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("resource path must be absolute")
	}
	// Resolve components in filesystem order, including dangling symlinks.
	// Cleaning before following links would give the wrong target for link/..
	// and resolving only the existing parent would discard dangling targets.
	resolved := string(filepath.Separator)
	pending := strings.Split(path, string(filepath.Separator))
	links := 0
	for len(pending) > 0 {
		part := pending[0]
		pending = pending[1:]
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			resolved = filepath.Dir(resolved)
			continue
		}
		next := filepath.Join(resolved, part)
		info, err := os.Lstat(next)
		if errors.Is(err, os.ErrNotExist) {
			resolved = next
			continue
		}
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			links++
			if links > 255 {
				return "", errors.New("too many resource path symlinks")
			}
			target, err := os.Readlink(next)
			if err != nil {
				return "", err
			}
			if filepath.IsAbs(target) {
				resolved = string(filepath.Separator)
			}
			pending = append(strings.Split(target, string(filepath.Separator)), pending...)
			continue
		}
		if !info.IsDir() {
			for _, rest := range pending {
				if rest != "" {
					return "", errors.New("resource path crosses a non-directory")
				}
			}
		}
		resolved = next
	}
	return resolved, nil
}

// A subdirectory shares the checkout/index with its containing worktree. A
// linked worktree has its own git-dir, while shared common-dir operations use
// the separate repository lease in the operation scheduler.
func worktreeResource(ctx context.Context, path string) (string, error) {
	canonical, err := resourcePath(path)
	if err != nil {
		return "", err
	}
	existing := canonical
	for {
		info, err := os.Stat(existing)
		if err == nil {
			if !info.IsDir() {
				return "", errors.New("workspace resource is not a directory")
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			return "", err
		}
		existing = parent
	}
	for directory := existing; ; directory = filepath.Dir(directory) {
		marker := filepath.Join(directory, ".git")
		info, err := os.Lstat(marker)
		if err == nil && info.IsDir() {
			entries, readErr := os.ReadDir(marker)
			if readErr != nil {
				return "", readErr
			}
			// An empty .git placeholder is not a repository. Git discovery
			// also skips it; preserve standalone K workspaces beneath it.
			if len(entries) == 0 {
				err = os.ErrNotExist
			}
		}
		if err == nil {
			identity, err := workspace.Git(ctx, existing, "rev-parse", "--absolute-git-dir")
			if err != nil {
				return "", fmt.Errorf("resolve worktree at %s from %s: %w", directory, existing, err)
			}
			identity = strings.TrimSpace(identity)
			if !filepath.IsAbs(identity) {
				return "", errors.New("Git returned a non-absolute worktree identity")
			}
			return resourcePath(identity)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if filepath.Dir(directory) == directory {
			return canonical, nil
		}
	}
}
func claimResourceTx(ctx context.Context, tx *sql.Tx, resource, ownerID, waveID string) (ResourceLease, error) {
	lease := ResourceLease{Resource: resource, Generation: 1}
	var owner, state string
	var generation int64
	err := tx.QueryRowContext(ctx, "SELECT run_id,state,generation FROM kernel_resource_leases WHERE resource=?", resource).Scan(&owner, &state, &generation)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return lease, err
	}
	if err == nil {
		if state == "held" {
			if owner != ownerID {
				return lease, ErrResourceBusy
			}
			lease.Generation = generation
			return lease, nil
		}
		if state != "released" || generation == int64(^uint64(0)>>1) {
			return lease, errors.New("invalid/exhausted resource lease generation")
		}
		lease.Generation = generation + 1
	}
	raw, err := json.Marshal(lease)
	if err != nil {
		return lease, err
	}
	result, err := tx.ExecContext(ctx, "INSERT INTO kernel_resource_leases(resource,run_id,wave_id,state,generation,payload) VALUES(?,?,?,'held',?,?) ON CONFLICT(resource) DO UPDATE SET run_id=excluded.run_id,wave_id=excluded.wave_id,state=excluded.state,generation=excluded.generation,payload=excluded.payload WHERE kernel_resource_leases.state='released'", resource, ownerID, waveID, lease.Generation, raw)
	if err != nil {
		return lease, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return lease, err
	}
	if n != 1 {
		return lease, ErrResourceBusy
	}
	return lease, nil
}
func loadProcessReservationTx(ctx context.Context, q queryer, id string) (ProcessReservation, error) {
	var r ProcessReservation
	var raw []byte
	var session, wave, state string
	err := q.QueryRowContext(ctx, "SELECT session_id,wave_id,state,payload FROM kernel_dispatch_intents WHERE run_id=?", id).Scan(&session, &wave, &state, &raw)
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	if r.Kind != "app_server" || r.ID != id || r.SessionID != session || r.WaveID != wave || r.State != state {
		return r, errors.New("process reservation identity conflict")
	}
	return r, nil
}
func saveProcessReservationTx(ctx context.Context, tx *sql.Tx, r ProcessReservation) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE kernel_dispatch_intents SET state=?,payload=? WHERE run_id=?", r.State, raw, r.ID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("process reservation disappeared before update")
	}
	return nil
}
func (k *KernelStore) reserveSessionResourcesTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding) error {
	p, err := executionPolicyTx(ctx, tx)
	if err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "kernel_execution_policy", "controller", "controller", p); err != nil {
		return err
	}
	// The caller has already atomically set this preparation's process latch.
	// Counting sessions also fences live preparations saved before this feature.
	var live int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_sessions WHERE process_stopped=0").Scan(&live); err != nil {
		return err
	}
	if live > p.Limits.Turns {
		return ErrExecutionCapacity
	}
	r := ProcessReservation{Kind: "app_server", ID: b.PreparationID, SessionID: b.ID, WaveID: b.Target.WaveID, State: "held", PolicyRevision: p.Revision, Resources: []ResourceLease{}, CreatedAt: k.clock()}
	if err := checkWorkspacePreparationTx(ctx, tx, b.CWD); err != nil {
		return err
	}
	path, err := worktreeResource(ctx, b.CWD)
	if err != nil {
		return err
	}
	if err = checkLiveWorkspaceTx(ctx, tx, path, b.ID); err != nil {
		return err
	}
	resources := []string{"worktree:" + path}
	if b.ThreadID != "" {
		resources = append(resources, "thread:"+b.ThreadID)
	}
	for _, resource := range resources {
		lease, err := claimResourceTx(ctx, tx, resource, r.ID, r.WaveID)
		if err != nil {
			return err
		}
		r.Resources = append(r.Resources, lease)
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO kernel_dispatch_intents(run_id,session_id,wave_id,state,payload) VALUES(?,?,?,'held',?)", r.ID, r.SessionID, r.WaveID, raw)
	return err
}
func bindSessionResourcesTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, thread, runID string) error {
	r, err := loadProcessReservationTx(ctx, tx, b.PreparationID)
	if err != nil {
		return err
	}
	if r.SessionID != b.ID || r.State != "held" || (r.RunID != "" && r.RunID != runID) {
		return errors.New("process preparation reservation changed")
	}
	if thread != "" {
		resource := "thread:" + thread
		exists := false
		for _, lease := range r.Resources {
			exists = exists || lease.Resource == resource
		}
		if !exists {
			lease, err := claimResourceTx(ctx, tx, resource, r.ID, r.WaveID)
			if err != nil {
				return err
			}
			r.Resources = append(r.Resources, lease)
		}
	}
	if runID != "" {
		r.RunID = runID
	}
	return saveProcessReservationTx(ctx, tx, r)
}
func (k *KernelStore) releaseSessionResourcesTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, proofID string) error {
	r, err := loadProcessReservationTx(ctx, tx, b.PreparationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} // An old untracked process can still be stopped safely.
	if err != nil {
		return err
	}
	if r.SessionID != b.ID || (r.RunID != "" && proofID != r.RunID) || (r.RunID == "" && proofID != r.ID) {
		return errors.New("process stopping proof does not own reservation")
	}
	if r.State == "released" {
		return nil
	}
	if r.State != "held" {
		return errors.New("invalid process reservation state")
	}
	for _, lease := range r.Resources {
		result, err := tx.ExecContext(ctx, "UPDATE kernel_resource_leases SET state='released' WHERE resource=? AND run_id=? AND state='held' AND generation=?", lease.Resource, r.ID, lease.Generation)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return errors.New("resource owner/generation changed before stopping proof")
		}
	}
	r.State, r.StoppedAt = "released", k.clock()
	return saveProcessReservationTx(ctx, tx, r)
}

// Untracked preparations from earlier versions still exclude their workspace.
func checkLiveWorkspaceTx(ctx context.Context, tx *sql.Tx, path, excludeSessionID string) error {
	rows, err := tx.QueryContext(ctx, "SELECT payload FROM kernel_sessions WHERE process_stopped=0 AND id<>?", excludeSessionID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var raw []byte
		var other contract.SessionBinding
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		if err = json.Unmarshal(raw, &other); err != nil {
			rows.Close()
			return err
		}
		otherPath, err := worktreeResource(ctx, other.CWD)
		if err != nil {
			rows.Close()
			return err
		}
		if otherPath == path {
			rows.Close()
			return ErrResourceBusy
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	return nil
}
