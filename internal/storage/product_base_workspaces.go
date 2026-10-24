package storage

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"

	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

type ProductBaseWorkspaceClaim struct {
	Selection ProductBaseSelection `json:"selection"`
	ClaimedAt time.Time            `json:"claimed_at"`
}
type ProductWorkspaceFetchSend struct {
	WorkspaceID     string                  `json:"workspace_id"`
	WorkspaceDigest string                  `json:"workspace_digest"`
	Selection       ProductBaseSelection    `json:"selection"`
	Source          workspace.RepositoryPin `json:"source"`
	At              time.Time               `json:"at"`
}

func (k *KernelStore) baseWorkspacePlanTx(ctx context.Context, q queryer, v ProductWorkspaceValue) (ProductBasePlan, error) {
	r := v.Request
	if r.BasePlanID == "" || !validSourceDigest(r.BasePlanDigest) {
		return ProductBasePlan{}, ErrProductWorkspaceHeld
	}
	plan, err := k.readProductBasePlanTx(ctx, q, r.BasePlanID)
	if err != nil {
		return plan, err
	}
	input := plan.Value.Input.Value
	task := input.Preparation.Task
	base := plan.Value.Head.HeadSHA
	if current := input.Preparation.Product; current != nil {
		base = current.Value.Candidate.SHA
		if v.Target.Valid() && v.ComparisonBaseSHA != plan.Value.Head.HeadSHA {
			return plan, ErrProductWorkspaceHeld
		}
	} else if v.ComparisonBaseSHA != "" {
		return plan, ErrProductWorkspaceHeld
	}
	if plan.Digest != r.BasePlanDigest || task.ID != r.TaskID || base != r.BaseSHA || input.Policy.Value.RepositoryID != r.RepositoryID || input.Policy.Value.Repository != r.Repository || task.Spec.Kind != "work" && task.Spec.Kind != "fix" {
		return plan, ErrProductWorkspaceHeld
	}
	if v.Target.Valid() && (task.Target() != v.Target || task.SessionID != v.SessionID || input.Preparation.Registration.Digest != v.RegistrationDigest || task.InputDigest != v.InputDigest) {
		return plan, ErrProductWorkspaceHeld
	}
	return plan, nil
}
func (k *KernelStore) baseWorkspaceOwnerTx(ctx context.Context, tx *sql.Tx, v ProductWorkspaceValue) error {
	r := v.Request
	if r.BasePlanID == "" {
		if r.BasePlanDigest != "" {
			return ErrProductWorkspaceHeld
		}
		return nil
	}
	plan, err := k.baseWorkspacePlanTx(ctx, tx, v)
	if err != nil {
		return errors.Join(ErrProductWorkspaceHeld, err)
	}
	current, err := k.currentProductBasePlanTx(ctx, tx, r.TaskID)
	if err != nil {
		return errors.Join(ErrProductWorkspaceHeld, err)
	}
	if current.ID != plan.ID || current.Digest != plan.Digest {
		return ErrProductWorkspaceHeld
	}
	return nil
}
func (k *KernelStore) validateBaseWorkspaceEvidenceTx(ctx context.Context, q queryer, s ProductWorkspace) (retErr error) {
	if s.Value.Request.BasePlanID == "" {
		if s.Value.Request.BasePlanDigest != "" || s.BaseClaim != nil || !baseWorkspaceFetchTime(s).IsZero() || s.BaseMergeAt != nil || s.Proof != nil && (s.Proof.Fetch != nil || s.Proof.BaseMerge != nil) {
			return ErrTaskEvidence
		}
		return nil
	}
	// A dependent missing record never means that this operation was unadmitted.
	defer func() {
		if errors.Is(retErr, sql.ErrNoRows) {
			retErr = errors.Join(ErrTaskEvidence, retErr)
		}
	}()
	plan, err := k.baseWorkspacePlanTx(ctx, q, s.Value)
	if err != nil {
		return err
	}
	var admission ProductWorkspace
	if err := taskEvidenceRecord(ctx, q, "product_base_workspace_admission", s.ID, &admission); err != nil {
		return err
	}
	if admission.ID != s.ID || admission.Digest != s.Digest || !reflect.DeepEqual(admission.Value, s.Value) || admission.CreatedAt != s.CreatedAt || admission.State != "prepared" || !admission.ClaimedAt.IsZero() || admission.BaseClaim != nil || !baseWorkspaceFetchTime(admission).IsZero() || admission.BaseMergeAt != nil || admission.Proof != nil || !admission.FinishedAt.IsZero() {
		return ErrTaskEvidence
	}
	if s.ClaimedAt.IsZero() {
		if s.BaseClaim != nil || !baseWorkspaceFetchTime(s).IsZero() || s.BaseMergeAt != nil {
			return ErrTaskEvidence
		}
		var claimed bool
		if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE id=? AND kind IN ('product_base_workspace_claim','product_workspace_fetch_send','product_workspace_base_merge_send'))", s.ID).Scan(&claimed); err != nil {
			return err
		}
		if claimed {
			return ErrTaskEvidence
		}
	} else {
		var claim ProductBaseWorkspaceClaim
		if err := taskEvidenceRecord(ctx, q, "product_base_workspace_claim", s.ID, &claim); err != nil {
			return err
		}
		if s.BaseClaim == nil || !reflect.DeepEqual(claim, *s.BaseClaim) || claim.ClaimedAt != s.ClaimedAt || claim.Selection.TaskID != s.Value.Request.TaskID || claim.Selection.Plan.ID != s.Value.Request.BasePlanID || claim.Selection.Plan.Digest != s.Value.Request.BasePlanDigest || !reflect.DeepEqual(claim.Selection.Observation.Head, plan.Value.Head) || claim.Selection.Observation.ObservedAt.After(claim.ClaimedAt) || claim.ClaimedAt.Sub(claim.Selection.Observation.ObservedAt) > 30*time.Second {
			return ErrTaskEvidence
		}
		sid, err := baseSelectionIdentity(claim.Selection)
		if err != nil {
			return err
		}
		var selected ProductBaseSelection
		if err := taskEvidenceRecord(ctx, q, "product_base_selection_history", sid, &selected); err != nil {
			return err
		}
		if !reflect.DeepEqual(claim.Selection, selected) {
			return ErrTaskEvidence
		}
	}
	var send ProductWorkspaceFetchSend
	err = taskEvidenceRecord(ctx, q, "product_workspace_fetch_send", s.ID, &send)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if baseWorkspaceFetchTime(s).IsZero() {
		if err == nil {
			return ErrTaskEvidence
		}
	} else {
		if err != nil {
			return err
		}
		if s.BaseClaim == nil || send.WorkspaceID != s.ID || send.WorkspaceDigest != s.Digest || send.At != baseWorkspaceFetchTime(s) || send.At.Before(s.ClaimedAt) || send.At.After(k.clock()) || send.Selection.TaskID != s.Value.Request.TaskID || send.Selection.Plan.ID != s.Value.Request.BasePlanID || send.Selection.Plan.Digest != s.Value.Request.BasePlanDigest || !reflect.DeepEqual(send.Selection.Observation.Head, plan.Value.Head) || send.At.Sub(send.Selection.Observation.ObservedAt) > 30*time.Second || send.Selection.Observation.ObservedAt.After(send.At) || send.Source.Repository != s.Value.Request.Repository {
			return ErrTaskEvidence
		}
		sid, err := baseSelectionIdentity(send.Selection)
		if err != nil {
			return err
		}
		var selected ProductBaseSelection
		if err := taskEvidenceRecord(ctx, q, "product_base_selection_history", sid, &selected); err != nil {
			return err
		}
		if !reflect.DeepEqual(send.Selection, selected) {
			return ErrTaskEvidence
		}
	}
	if err := k.validateWorkspaceBaseMergeTx(ctx, q, s, plan); err != nil {
		return err
	}
	if s.Proof != nil {
		var terminal ProductWorkspace
		if err := taskEvidenceRecord(ctx, q, "product_base_workspace_terminal", s.ID, &terminal); err != nil {
			return err
		}
		if !reflect.DeepEqual(terminal, s) {
			return ErrTaskEvidence
		}
		return k.validateBaseWorkspaceProofTx(ctx, q, s)
	}
	var finished bool
	if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_base_workspace_terminal' AND id=?)", s.ID).Scan(&finished); err != nil {
		return err
	}
	if finished {
		return ErrTaskEvidence
	}
	return nil
}
func (k *KernelStore) validateBaseWorkspaceProofTx(ctx context.Context, q queryer, s ProductWorkspace) error {
	if s.Value.Request.BasePlanID == "" {
		if s.Proof != nil && (s.Proof.Fetch != nil || s.Proof.BaseMerge != nil) {
			return ErrTaskEvidence
		}
		return nil
	}
	plan, err := k.baseWorkspacePlanTx(ctx, q, s.Value)
	if err != nil {
		return err
	}
	if s.Proof == nil {
		return ErrTaskEvidence
	}
	if err := k.validateWorkspaceBaseMergeTx(ctx, q, s, plan); err != nil {
		return err
	}
	fetch := s.Proof.Fetch
	if fetch == nil {
		if s.Proof.Outcome == "prepared" || !baseWorkspaceFetchTime(s).IsZero() {
			return ErrTaskEvidence
		}
		return nil
	}
	if fetch.SHA != plan.Value.Head.HeadSHA || fetch.Remote != plan.Value.Head.Remote || fetch.Source.Repository != s.Value.Request.Repository || fetch.Available && s.Proof.Outcome != "prepared" && s.Proof.Outcome != "failed" {
		return ErrTaskEvidence
	}
	for _, p := range []string{fetch.Source.Repository, fetch.Source.GitDir, fetch.Source.CommonDir} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return ErrTaskEvidence
		}
	}
	r, err := loadOperationReservationTx(ctx, q, s.ID)
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(r.Resources, func(l ResourceLease) bool { return l.Resource == "repository_git:"+fetch.Source.CommonDir }) {
		return ErrTaskEvidence
	}
	if !baseWorkspaceFetchTime(s).IsZero() {
		var send ProductWorkspaceFetchSend
		if err := taskEvidenceRecord(ctx, q, "product_workspace_fetch_send", s.ID, &send); err != nil {
			return err
		}
		if send.Source != fetch.Source {
			return ErrTaskEvidence
		}
	} else if fetch.Attempted {
		return ErrTaskEvidence
	}
	if s.Proof.Outcome == "prepared" && (!fetch.Available || !baseWorkspaceFetchTime(s).IsZero() && !fetch.Attempted) {
		return ErrTaskEvidence
	}
	return nil
}

// MarkProductWorkspaceFetchMaySend consumes one durable fence for the actual
// claimed synchronous owner. A restart or missing record never grants a resend.
func (k *KernelStore) MarkProductWorkspaceFetchMaySend(ctx context.Context, taskID, digest string, source workspace.RepositoryPin) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	saved, err := k.productWorkspaceTx(ctx, tx, productWorkspaceID(taskID))
	if err != nil {
		return err
	}
	if saved.State != "launching" || saved.Digest != digest || saved.BaseClaim == nil || saved.BaseMergeAt != nil || !baseWorkspaceFetchTime(saved).IsZero() || source.Repository != saved.Value.Request.Repository {
		return ErrProductWorkspaceHeld
	}
	selection, err := k.checkProductWorkspaceBaseFetchTx(ctx, tx, saved, source)
	if err != nil {
		return err
	}
	at := k.clock()
	saved.BaseFetchAt = &at
	send := ProductWorkspaceFetchSend{WorkspaceID: saved.ID, WorkspaceDigest: saved.Digest, Selection: selection, Source: source, At: at}
	if err := kernelRecordTx(ctx, tx, "product_workspace_fetch_send", saved.ID, saved.Value.Target.WaveID, send); err != nil {
		return err
	}
	if err := saveProductWorkspaceTx(ctx, tx, "product_workspace", saved); err != nil {
		return err
	}
	return tx.Commit()
}

func baseWorkspaceFetchTime(s ProductWorkspace) time.Time {
	if s.BaseFetchAt == nil {
		return time.Time{}
	}
	return *s.BaseFetchAt
}

func (k *KernelStore) productModelBaseSelectionTx(ctx context.Context, tx *sql.Tx, task domain.Task, s ProductWorkspace, host ProductModelRepository) (*ProductBaseSelection, error) {
	if s.Value.Request.BasePlanID == "" {
		return nil, nil
	}
	plan, err := k.baseWorkspacePlanTx(ctx, tx, s.Value)
	if err != nil {
		return nil, err
	}
	head := plan.Value.Head
	if host.InitialSHA != head.InitialSHA || host.Name != head.Repository || host.BaseBranch != head.BaseBranch || host.Policy.RepositoryID != head.RepositoryID || host.Policy.Repository != s.Value.Request.Repository {
		return nil, ErrProductWorkspaceHeld
	}
	original := plan.Value.Input.Value.Preparation.Task
	if original.Target() != task.Target() || original.SessionID != task.SessionID || original.ParentSessionID != task.ParentSessionID || original.InputDigest != s.Value.InputDigest || !reflect.DeepEqual(original.Spec, task.Spec) {
		return nil, ErrProductWorkspaceHeld
	}
	if err := k.productWorkspaceOriginCurrentTx(ctx, tx, s); err != nil {
		return nil, err
	}
	selection, _, err := k.readProductBaseSelectionTx(ctx, tx, task.ID)
	if err != nil {
		return nil, err
	}
	if selection.Plan.ID != plan.ID || selection.Plan.Digest != plan.Digest {
		return nil, ErrProductWorkspaceHeld
	}
	if task.Attempts == 0 && k.clock().Sub(selection.Observation.ObservedAt) > 30*time.Second {
		return nil, ErrProductWorkspaceHeld
	}
	return &selection, nil
}

func sameProductExecutionOrigin(current, original ProductTargetSnapshot) bool {
	normalized := current.Value
	normalized.WorkItem.MergeHold = original.Value.WorkItem.MergeHold
	return reflect.DeepEqual(normalized, original.Value)
}

func (k *KernelStore) productWorkspaceOriginCurrentTx(ctx context.Context, tx *sql.Tx, s ProductWorkspace) error {
	if s.Value.Request.BasePlanID == "" {
		return nil
	}
	plan, err := k.baseWorkspacePlanTx(ctx, tx, s.Value)
	if err != nil {
		return err
	}
	current, currentErr := loadProductTargetTx(ctx, tx, s.Value.Target, s.Value.WorkItemID)
	old := plan.Value.Input.Value.Preparation.Product
	if old == nil {
		if currentErr != sql.ErrNoRows {
			return errors.Join(ErrProductWorkspaceHeld, currentErr)
		}
	} else if currentErr != nil || !sameProductExecutionOrigin(current, *old) {
		return errors.Join(ErrProductWorkspaceHeld, currentErr)
	}
	return nil
}

// A stopped prepared checkout may refresh read-only source observations after
// initial thread creation, before any model run. This grants no Git start:
// workspaceOwner still requires an unused, stopped session for mutations.
func (k *KernelStore) preparedBaseTaskReadableTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) (bool, error) {
	if task.State != contract.TaskReady || task.Attempts != 0 {
		return false, nil
	}
	fixed, err := k.fixedProductWorkspaceInputTx(ctx, tx, w, task.Spec)
	if err != nil {
		return false, err
	}
	if fixed == nil || fixed.Value.Request.BasePlanID == "" {
		return false, nil
	}
	var active string
	var used bool
	if err := tx.QueryRowContext(ctx, "SELECT active_run_id,EXISTS(SELECT 1 FROM kernel_run_bindings WHERE session_id=?) FROM kernel_sessions WHERE id=?", task.SessionID, task.SessionID).Scan(&active, &used); err != nil {
		return false, err
	}
	if active != "" || used {
		return false, nil
	}
	if err := currentObservedTaskTx(ctx, tx, w, task); err != nil {
		return false, err
	}
	return true, nil
}

func (k *KernelStore) checkProductWorkspaceBaseFetchTx(ctx context.Context, tx *sql.Tx, saved ProductWorkspace, source workspace.RepositoryPin) (ProductBaseSelection, error) {
	var empty ProductBaseSelection
	if saved.State != "launching" || saved.BaseClaim == nil || source.Repository != saved.Value.Request.Repository {
		return empty, ErrProductWorkspaceHeld
	}
	for _, p := range []string{source.Repository, source.GitDir, source.CommonDir} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return empty, ErrProductWorkspaceHeld
		}
	}
	if _, _, _, err := k.productWorkspaceOwnerTx(ctx, tx, saved.Value); err != nil {
		return empty, err
	}
	r, err := loadOperationReservationTx(ctx, tx, saved.ID)
	if err != nil {
		return empty, err
	}
	if r.Kind != "git" || r.State != "held" || r.Request.Repository != source.Repository || r.Request.Worktree != saved.Value.CWD || !slices.ContainsFunc(r.Resources, func(l ResourceLease) bool { return l.Resource == "repository_git:"+source.CommonDir }) {
		return empty, ErrProductWorkspaceHeld
	}
	for _, lease := range r.Resources {
		var owner, state string
		var generation int64
		if err := tx.QueryRowContext(ctx, "SELECT run_id,state,generation FROM kernel_resource_leases WHERE resource=?", lease.Resource).Scan(&owner, &state, &generation); err != nil {
			return empty, err
		}
		if owner != saved.ID || state != "held" || generation != lease.Generation {
			return empty, ErrProductWorkspaceHeld
		}
	}
	selected, _, err := k.readProductBaseSelectionTx(ctx, tx, saved.Value.Request.TaskID)
	return selected, err
}

// CheckProductWorkspaceBaseFetch checks the actual local pin and current leases
// before any object import or checkout, including already-local commits.
func (k *KernelStore) CheckProductWorkspaceBaseFetch(ctx context.Context, taskID, digest string, source workspace.RepositoryPin) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	saved, err := k.productWorkspaceTx(ctx, tx, productWorkspaceID(taskID))
	if err != nil {
		return err
	}
	if saved.Digest != digest {
		return ErrProductWorkspaceHeld
	}
	_, err = k.checkProductWorkspaceBaseFetchTx(ctx, tx, saved, source)
	return err
}
