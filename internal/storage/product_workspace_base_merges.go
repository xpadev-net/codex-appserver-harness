package storage

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// ProductWorkspaceBaseMergeSend is an independent one-use mutation fence. It
// records the original candidate checkout, never an adopted replacement.
type ProductWorkspaceBaseMergeSend struct {
	WorkspaceID     string                   `json:"workspace_id"`
	WorkspaceDigest string                   `json:"workspace_digest"`
	Selection       ProductBaseSelection     `json:"selection"`
	Source          workspace.FrozenCheckout `json:"source"`
	At              time.Time                `json:"at"`
}

func baseMergeSourceMatches(s ProductWorkspace, plan ProductBasePlan, source workspace.FrozenCheckout) bool {
	old := plan.Value.Input.Value.Preparation.Product
	if old == nil {
		return false
	}
	for _, path := range []string{source.Repository, source.CWD, source.GitDir, source.CommonDir} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\x00\r\n") {
			return false
		}
	}
	return source.Repository == s.Value.Request.Repository && source.CWD == s.Value.CWD && source.Branch == s.Value.Branch && source.SHA == old.Value.Candidate.SHA && source.BaseSHA == old.Value.Candidate.BaseSHA && strings.HasPrefix(source.GitDir, source.CommonDir+string(filepath.Separator)+"worktrees"+string(filepath.Separator))
}

func (k *KernelStore) validateWorkspaceBaseMergeTx(ctx context.Context, q queryer, s ProductWorkspace, plan ProductBasePlan) error {
	var send ProductWorkspaceBaseMergeSend
	err := taskEvidenceRecord(ctx, q, "product_workspace_base_merge_send", s.ID, &send)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	sent := s.BaseMergeAt != nil
	if sent != (err == nil) {
		return ErrTaskEvidence
	}
	old := plan.Value.Input.Value.Preparation.Product
	if old == nil {
		if sent || s.Proof != nil && s.Proof.BaseMerge != nil {
			return ErrTaskEvidence
		}
		return nil
	}
	if sent {
		if !baseWorkspaceFetchTime(s).IsZero() && s.BaseMergeAt.Before(baseWorkspaceFetchTime(s)) {
			return ErrTaskEvidence
		}
		if s.BaseClaim == nil || !baseMergeSourceMatches(s, plan, send.Source) || send.WorkspaceID != s.ID || send.WorkspaceDigest != s.Digest || send.At != *s.BaseMergeAt || send.At.Before(s.ClaimedAt) || send.At.After(k.clock()) || send.Selection.TaskID != s.Value.Request.TaskID || send.Selection.Plan.ID != plan.ID || send.Selection.Plan.Digest != plan.Digest || !reflect.DeepEqual(send.Selection.Observation.Head, plan.Value.Head) || send.Selection.Observation.ObservedAt.After(send.At) || send.At.Sub(send.Selection.Observation.ObservedAt) > 30*time.Second {
			return ErrTaskEvidence
		}
		id, err := baseSelectionIdentity(send.Selection)
		if err != nil {
			return err
		}
		var selected ProductBaseSelection
		if err := taskEvidenceRecord(ctx, q, "product_base_selection_history", id, &selected); err != nil {
			return errors.Join(ErrTaskEvidence, err)
		}
		if !reflect.DeepEqual(selected, send.Selection) {
			return ErrTaskEvidence
		}
		reservation, err := loadOperationReservationTx(ctx, q, s.ID)
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(reservation.Resources, func(l ResourceLease) bool { return l.Resource == "repository_git:"+send.Source.CommonDir }) {
			return ErrTaskEvidence
		}
	}
	if s.Proof == nil {
		return nil
	}
	merge := s.Proof.BaseMerge
	if merge == nil {
		if sent || s.Proof.Outcome == "prepared" {
			return ErrTaskEvidence
		}
		return nil
	}
	if !baseMergeSourceMatches(s, plan, merge.Source) || merge.BaseSHA != plan.Value.Head.HeadSHA || merge.Attempted && !sent || sent && merge.Source != send.Source {
		return ErrTaskEvidence
	}
	if s.Proof.Outcome == "not_started" {
		return ErrTaskEvidence
	}
	switch merge.Outcome {
	case "merged":
		if !sent || !merge.Attempted || !fixedSHA(merge.HeadSHA) || merge.HeadSHA == merge.Source.SHA || merge.Failure != nil || len(merge.Conflicts) != 0 {
			return ErrTaskEvidence
		}
	case "already_current":
		if sent || merge.Attempted || merge.HeadSHA != merge.Source.SHA || merge.Failure != nil || len(merge.Conflicts) != 0 {
			return ErrTaskEvidence
		}
	case "conflict":
		if !sent || !merge.Attempted || merge.HeadSHA != merge.Source.SHA || merge.Failure != nil || len(merge.Conflicts) == 0 || !validBaseMergePaths(merge.Conflicts) || s.Proof.Outcome != "failed" {
			return ErrTaskEvidence
		}
	case "":
		if merge.HeadSHA != "" || len(merge.Conflicts) != 0 || s.Proof.Outcome != "failed" {
			return ErrTaskEvidence
		}
		if failure := merge.Failure; failure != nil {
			if !merge.Attempted || !fixedSHA(failure.HeadSHA) || failure.MergeHeadSHA != "" && !fixedSHA(failure.MergeHeadSHA) || !validBaseMergePaths(failure.Unresolved) {
				return ErrTaskEvidence
			}
		}
	default:
		return ErrTaskEvidence
	}
	if s.Proof.Outcome == "prepared" && ((merge.Outcome != "merged" && merge.Outcome != "already_current") || s.Proof.HeadSHA != merge.HeadSHA || !s.Proof.Clean || s.Proof.GitDir != merge.Source.GitDir || s.Proof.CommonDir != merge.Source.CommonDir) {
		return ErrTaskEvidence
	}
	return nil
}

func validBaseMergePaths(paths []string) bool {
	if len(paths) > 256 {
		return false
	}
	seen := map[string]bool{}
	for _, path := range paths {
		if !utf8.ValidString(path) || path == "" || path == "." || len(path) > 4096 || filepath.IsAbs(path) || filepath.Clean(path) != path || path == ".." || strings.HasPrefix(path, "../") || strings.ContainsAny(path, "\x00\r\n") || seen[path] {
			return false
		}
		seen[path] = true
	}
	return true
}

// MarkProductWorkspaceBaseMergeMaySend authorizes exactly one ordinary merge
// after the synchronous owner has imported the fixed base and pinned checkout.
// All current source/task/control and real lease generations are checked again.
func (k *KernelStore) MarkProductWorkspaceBaseMergeMaySend(ctx context.Context, taskID, digest string, source workspace.FrozenCheckout) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	saved, err := k.productWorkspaceTx(ctx, tx, productWorkspaceID(taskID))
	if err != nil {
		return err
	}
	if saved.Digest != digest || saved.BaseMergeAt != nil || saved.State != "launching" || saved.BaseClaim == nil {
		return ErrProductWorkspaceHeld
	}
	plan, err := k.baseWorkspacePlanTx(ctx, tx, saved.Value)
	if err != nil {
		return err
	}
	if !baseMergeSourceMatches(saved, plan, source) {
		return ErrProductWorkspaceHeld
	}
	pin := workspace.RepositoryPin{Repository: source.Repository, GitDir: source.GitDir, CommonDir: source.CommonDir}
	selection, err := k.checkProductWorkspaceBaseFetchTx(ctx, tx, saved, pin)
	if err != nil {
		return err
	}
	at := k.clock()
	saved.BaseMergeAt = &at
	send := ProductWorkspaceBaseMergeSend{WorkspaceID: saved.ID, WorkspaceDigest: saved.Digest, Selection: selection, Source: source, At: at}
	if err := kernelRecordTx(ctx, tx, "product_workspace_base_merge_send", saved.ID, saved.Value.Target.WaveID, send); err != nil {
		return err
	}
	if err := saveProductWorkspaceTx(ctx, tx, "product_workspace", saved); err != nil {
		return err
	}
	return tx.Commit()
}
