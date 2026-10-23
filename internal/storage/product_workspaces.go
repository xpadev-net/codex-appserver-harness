package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrProductWorkspaceHeld = errors.New("product workspace preparation requires owner reconciliation")

// ProductWorkspaceRequest is host-authored, never a model tool or Issue field.
// Repository is the canonical local repository mapped to RepositoryID by host
// configuration; BaseSHA is a fixed local commit, not a branch-name guess.
type ProductWorkspaceRequest struct {
	TaskID       string `json:"task_id"`
	RepositoryID int64  `json:"repository_id"`
	Repository   string `json:"repository"`
	BaseSHA      string `json:"base_sha"`
}
type ProductWorkspaceValue struct {
	ParentSessionID    string                  `json:"parent_session_id"`
	Request            ProductWorkspaceRequest `json:"request"`
	Target             contract.Target         `json:"target"`
	WorkItemID         string                  `json:"work_item_id"`
	RegistrationDigest string                  `json:"registration_digest"`
	InputDigest        string                  `json:"input_digest"`
	SessionID          string                  `json:"session_id"`
	CWD                string                  `json:"cwd"`
	Branch             string                  `json:"branch"`
}

// Only the synchronous Git owner submits physical-stop and filesystem facts.
type ProductWorkspaceProof struct {
	CWD       string `json:"cwd,omitempty"`
	Branch    string `json:"branch,omitempty"`
	ProofID   string `json:"proof_id"`
	Outcome   string `json:"outcome"` // prepared or failed; both require a stopped owner
	Reason    string `json:"reason"`
	HeadSHA   string `json:"head_sha,omitempty"`
	GitDir    string `json:"git_dir,omitempty"`
	CommonDir string `json:"common_dir,omitempty"`
	Clean     bool   `json:"clean"`
}
type ProductWorkspace struct {
	ID         string                 `json:"id"`
	Value      ProductWorkspaceValue  `json:"value"`
	Digest     string                 `json:"digest"`
	State      string                 `json:"state"` // prepared, launching, ready, failed, stale
	Reason     string                 `json:"reason,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
	ClaimedAt  time.Time              `json:"claimed_at"`
	FinishedAt time.Time              `json:"finished_at"`
	Proof      *ProductWorkspaceProof `json:"proof,omitempty"`
}
type ProductWorkspaceClaim struct {
	Workspace ProductWorkspace `json:"workspace"`
	Start     bool             `json:"start"`
}

type productWorkspaceRecord struct {
	Workspace ProductWorkspace `json:"workspace"`
	Digest    string           `json:"digest"`
}

func saveProductWorkspaceTx(ctx context.Context, tx *sql.Tx, kind string, saved ProductWorkspace) error {
	digest, err := contentDigest(saved)
	if err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, kind, saved.ID, saved.Value.Target.WaveID, productWorkspaceRecord{Workspace: saved, Digest: digest})
}

func productWorkspaceID(taskID string) string { return structureID("product-workspace-", taskID, "v1") }
func (k *KernelStore) productWorkspaceTx(ctx context.Context, q queryer, id string) (ProductWorkspace, error) {
	var record productWorkspaceRecord
	if err := taskEvidenceRecord(ctx, q, "product_workspace", id, &record); err != nil {
		return record.Workspace, err
	}
	saved := record.Workspace
	checksum, err := contentDigest(saved)
	if err != nil {
		return saved, err
	}
	if checksum != record.Digest {
		return saved, ErrTaskEvidence
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if saved.ID != id || saved.ID != productWorkspaceID(saved.Value.Request.TaskID) || saved.Digest != digest || !saved.Value.Target.Valid() || saved.Value.ParentSessionID == "" || saved.Value.WorkItemID == "" || !validSourceDigest(saved.Value.RegistrationDigest) || !validSourceDigest(saved.Value.InputDigest) || saved.Value.SessionID == "" || saved.Value.SessionID == "." || saved.Value.SessionID == ".." || filepath.Base(saved.Value.SessionID) != saved.Value.SessionID || saved.Value.CWD != filepath.Join(k.Root, "workspaces", saved.Value.SessionID) || saved.Value.Branch != "harness/"+saved.Value.SessionID || saved.CreatedAt.IsZero() {
		return saved, ErrTaskEvidence
	}
	switch saved.State {
	case "prepared":
		if !saved.ClaimedAt.IsZero() || saved.Proof != nil || !saved.FinishedAt.IsZero() {
			return saved, ErrTaskEvidence
		}
	case "launching":
		if saved.ClaimedAt.IsZero() || saved.Proof != nil || !saved.FinishedAt.IsZero() {
			return saved, ErrTaskEvidence
		}
	case "ready", "failed", "stale":
		if saved.FinishedAt.IsZero() || saved.Proof == nil || (saved.ClaimedAt.IsZero() && saved.Proof.Outcome != "not_started") {
			return saved, ErrTaskEvidence
		}
		if ((saved.State == "ready" || saved.State == "stale") && saved.Proof.Outcome != "prepared") || (saved.State == "failed" && saved.Proof.Outcome != "failed" && saved.Proof.Outcome != "not_started") {
			return saved, ErrTaskEvidence
		}
	default:
		return saved, ErrTaskEvidence
	}
	return saved, nil
}
func (k *KernelStore) ProductWorkspace(ctx context.Context, taskID string) (ProductWorkspace, error) {
	return k.productWorkspaceTx(ctx, k.DB, productWorkspaceID(taskID))
}
func (k *KernelStore) productWorkspaceOwnerTx(ctx context.Context, tx *sql.Tx, value ProductWorkspaceValue) (domain.Wave, domain.Task, contract.SessionBinding, error) {
	var b contract.SessionBinding
	task, err := loadTask(ctx, tx, value.Request.TaskID)
	if err != nil {
		return domain.Wave{}, task, b, err
	}
	w, err := loadWave(ctx, tx, task.WaveID)
	if err != nil {
		return w, task, b, err
	}
	if err := w.Validate(); err != nil {
		return w, task, b, err
	}
	if w.Policy.Stage != "P" || !w.Policy.Product || w.Target() != task.Target() || value.Target.Valid() && value.Target != w.Target() || task.State != contract.TaskReady || task.Attempts != 0 || (task.Spec.Kind != "work" && task.Spec.Kind != "fix") || len(task.Spec.WorkItemIDs) != 1 {
		return w, task, b, ErrProductWorkspaceHeld
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return w, task, b, err
	}
	inputs, err := k.taskInputDigestTx(ctx, tx, w, task.Spec)
	if err != nil {
		return w, task, b, err
	}
	if inputs != task.InputDigest || value.ParentSessionID != "" && value.ParentSessionID != w.OrchestratorSessionID {
		return w, task, b, ErrProductWorkspaceHeld
	}
	b, err = sessionBindingTx(ctx, tx, task.SessionID)
	if err != nil {
		return w, task, b, err
	}
	var stopped, creating bool
	var active string
	if err := tx.QueryRowContext(ctx, "SELECT process_stopped,creating,active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&stopped, &creating, &active); err != nil {
		return w, task, b, err
	}
	if value.SessionID != "" && (value.SessionID != b.ID || value.CWD != b.CWD || value.WorkItemID != task.Spec.WorkItemIDs[0]) {
		return w, task, b, ErrProductWorkspaceHeld
	}
	if b.ThreadID != "" || b.Closed || !stopped || creating || active != "" || b.CWD != filepath.Join(k.Root, "workspaces", b.ID) {
		return w, task, b, ErrProductWorkspaceHeld
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, task.Spec.WorkItemIDs[0])
	if err != nil {
		return w, task, b, err
	}
	if !productRegistrationScope(registration, w) || registration.Value.Definition.RepositoryID != value.Request.RepositoryID || value.RegistrationDigest != "" && value.RegistrationDigest != registration.Digest || value.InputDigest != "" && value.InputDigest != task.InputDigest {
		return w, task, b, ErrProductWorkspaceHeld
	}
	for _, criterion := range task.Spec.Criteria {
		if !slices.Contains(registration.Value.Definition.Criteria, criterion) {
			return w, task, b, ErrProductWorkspaceHeld
		}
	}
	return w, task, b, nil
}
func (k *KernelStore) ReserveProductWorkspace(ctx context.Context, request ProductWorkspaceRequest) (ProductWorkspace, error) {
	var saved ProductWorkspace
	for _, value := range []string{request.TaskID, request.Repository, request.BaseSHA} {
		if !utf8.ValidString(value) || len(value) > contract.MaxInline || strings.ContainsAny(value, "\x00\r\n") {
			return saved, errors.New("bounded product workspace request required")
		}
	}
	if request.TaskID == "" || request.RepositoryID < 1 || !filepath.IsAbs(request.Repository) || filepath.Clean(request.Repository) != request.Repository || len(request.BaseSHA) != 40 || strings.Trim(request.BaseSHA, "0123456789abcdef") != "" {
		return saved, errors.New("trusted product Task, canonical repository and fixed SHA required")
	}
	canonical, err := resourcePath(request.Repository)
	if err != nil {
		return saved, err
	}
	if canonical != request.Repository {
		return saved, errors.New("canonical local repository required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	id := productWorkspaceID(request.TaskID)
	saved, err = k.productWorkspaceTx(ctx, tx, id)
	if err == nil {
		if saved.Value.Request != request {
			return saved, errors.New("product workspace identity reused with different inputs")
		}
		return saved, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return saved, err
	}
	value := ProductWorkspaceValue{Request: request}
	w, task, b, err := k.productWorkspaceOwnerTx(ctx, tx, value)
	if err != nil {
		return saved, err
	}
	if task.Paused {
		return saved, ErrProductWorkspaceHeld
	}
	registration, err := loadProductWorkItemRegistration(ctx, tx, task.Spec.WorkItemIDs[0])
	if err != nil {
		return saved, err
	}
	value.ParentSessionID = w.OrchestratorSessionID
	value.Target, value.WorkItemID, value.RegistrationDigest, value.InputDigest = w.Target(), task.Spec.WorkItemIDs[0], registration.Digest, task.InputDigest
	value.SessionID, value.CWD, value.Branch = b.ID, b.CWD, "harness/"+b.ID
	operation := OperationCapacityRequest{ID: id, Kind: "git", SessionID: w.OrchestratorSessionID, Target: w.Target(), Repository: request.Repository, Worktree: b.CWD}
	reservation, err := k.reserveOperationCapacityTx(ctx, tx, operation)
	if err != nil {
		return saved, err
	}
	if reservation.State != "held" {
		return saved, ErrProductWorkspaceHeld
	}
	digest, err := contentDigest(value)
	if err != nil {
		return saved, err
	}
	saved = ProductWorkspace{ID: id, Value: value, Digest: digest, State: "prepared", CreatedAt: k.clock()}
	if err := saveProductWorkspaceTx(ctx, tx, "product_workspace", saved); err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}

// ClaimProductWorkspace permits exactly one physical start. Reopen/retry of a
// launching intent never grants another start, even if no PID was recorded.
func (k *KernelStore) ClaimProductWorkspace(ctx context.Context, taskID, digest string) (ProductWorkspaceClaim, error) {
	var result ProductWorkspaceClaim
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	saved, err := k.productWorkspaceTx(ctx, tx, productWorkspaceID(taskID))
	if err != nil {
		return result, err
	}
	if saved.Digest != digest {
		return result, ErrProductWorkspaceHeld
	}
	result.Workspace = saved
	if saved.State != "prepared" {
		return result, tx.Commit()
	}
	w, task, _, err := k.productWorkspaceOwnerTx(ctx, tx, saved.Value)
	if err != nil {
		return result, err
	}
	if task.Paused || w.DispatchPaused() || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || !k.clock().Before(w.Deadline) {
		return result, ErrProductWorkspaceHeld
	}
	reservation, err := loadOperationReservationTx(ctx, tx, saved.ID)
	if err != nil {
		return result, err
	}
	if reservation.Kind != "git" || reservation.SessionID != saved.Value.ParentSessionID || reservation.State != "held" || reservation.Request.Target != saved.Value.Target || reservation.Request.Worktree != saved.Value.CWD || reservation.Request.Repository != saved.Value.Request.Repository {
		return result, ErrProductWorkspaceHeld
	}
	saved.State, saved.ClaimedAt = "launching", k.clock()
	if err := saveProductWorkspaceTx(ctx, tx, "product_workspace", saved); err != nil {
		return result, err
	}
	result.Workspace = saved
	if err := tx.Commit(); err != nil {
		return result, err
	}
	result.Start = true
	return result, nil
}
func (k *KernelStore) FinishProductWorkspace(ctx context.Context, taskID string, proof ProductWorkspaceProof) (ProductWorkspace, error) {
	var saved ProductWorkspace
	for _, value := range []string{proof.ProofID, proof.Reason, proof.CWD, proof.Branch, proof.GitDir, proof.CommonDir, proof.HeadSHA} {
		if !utf8.ValidString(value) || len(value) > contract.MaxInline || strings.ContainsAny(value, "\x00\r\n") {
			return saved, errors.New("bounded workspace stopping proof required")
		}
	}
	if proof.Outcome != "prepared" && (proof.CWD != "" || proof.Branch != "" || proof.HeadSHA != "" || proof.GitDir != "" || proof.CommonDir != "" || proof.Clean) {
		return saved, errors.New("failed preparation cannot claim successful Git facts")
	}
	if strings.TrimSpace(proof.ProofID) == "" || len(proof.ProofID) > contract.MaxInline || strings.TrimSpace(proof.Reason) == "" || len(proof.Reason) > contract.MaxInline || (proof.Outcome != "prepared" && proof.Outcome != "failed" && proof.Outcome != "not_started") {
		return saved, errors.New("trusted stopped workspace owner proof required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return saved, err
	}
	defer tx.Rollback()
	saved, err = k.productWorkspaceTx(ctx, tx, productWorkspaceID(taskID))
	if err != nil {
		return saved, err
	}
	if saved.Proof != nil {
		if !reflect.DeepEqual(*saved.Proof, proof) {
			return saved, errors.New("workspace stopping proof replay conflict")
		}
		return saved, tx.Commit()
	}
	if (saved.State != "launching" || proof.Outcome == "not_started") && (saved.State != "prepared" || proof.Outcome != "not_started") {
		return saved, ErrProductWorkspaceHeld
	}
	reservation, err := loadOperationReservationTx(ctx, tx, saved.ID)
	if err != nil {
		return saved, err
	}
	if reservation.Kind != "git" || reservation.SessionID != saved.Value.ParentSessionID || reservation.State != "held" || reservation.Request.Target != saved.Value.Target || reservation.Request.Repository != saved.Value.Request.Repository || reservation.Request.Worktree != saved.Value.CWD {
		return saved, ErrProductWorkspaceHeld
	}
	saved.State, saved.Reason = "failed", proof.Reason
	if proof.Outcome == "prepared" {
		common := ""
		for _, lease := range reservation.Resources {
			if strings.HasPrefix(lease.Resource, "repository_git:") {
				common = strings.TrimPrefix(lease.Resource, "repository_git:")
			}
		}
		if proof.CWD != saved.Value.CWD || proof.Branch != saved.Value.Branch || proof.HeadSHA != saved.Value.Request.BaseSHA || !proof.Clean || !filepath.IsAbs(proof.GitDir) || filepath.Clean(proof.GitDir) != proof.GitDir || !strings.HasPrefix(proof.GitDir, common+string(filepath.Separator)+"worktrees"+string(filepath.Separator)) || proof.CommonDir != common {
			return saved, errors.New("workspace proof does not match pinned Git resources/base")
		}
		_, task, b, ownerErr := k.productWorkspaceOwnerTx(ctx, tx, saved.Value)
		if ownerErr != nil {
			if !errors.Is(ownerErr, ErrProductWorkspaceHeld) && !errors.Is(ownerErr, ErrTaskBinding) && !errors.Is(ownerErr, ErrTaskEvidence) && !errors.Is(ownerErr, sql.ErrNoRows) {
				return saved, ownerErr
			}
			saved.State, saved.Reason = "stale", "workspace stopped after its Task/source changed"
		} else {
			if task.SessionID != saved.Value.SessionID || b.CWD != saved.Value.CWD {
				return saved, ErrProductWorkspaceHeld
			}
			b.Sandbox = "workspace-write"
			raw, err := json.Marshal(b)
			if err != nil {
				return saved, err
			}
			if _, err := tx.ExecContext(ctx, "UPDATE kernel_sessions SET payload=? WHERE id=?", raw, b.ID); err != nil {
				return saved, err
			}
			saved.State, saved.Reason = "ready", ""
		}
	}
	saved.Proof, saved.FinishedAt = &proof, k.clock()
	disposition := "process_stopped"
	if proof.Outcome == "not_started" {
		disposition = "not_started"
	}
	if err := k.releaseOperationCapacityTx(ctx, tx, OperationReleaseProof{OperationID: saved.ID, ProofID: proof.ProofID, Disposition: disposition, Reason: proof.Reason}); err != nil {
		return saved, err
	}
	if err := saveProductWorkspaceTx(ctx, tx, "product_workspace", saved); err != nil {
		return saved, err
	}
	if err := saveProductWorkspaceTx(ctx, tx, "product_workspace_history", saved); err != nil {
		return saved, err
	}
	return saved, tx.Commit()
}
