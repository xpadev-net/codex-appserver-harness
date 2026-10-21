package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func completionRecord(ctx context.Context, q queryer, kind, id string, out any) error {
	var data []byte
	if err := q.QueryRowContext(ctx, "SELECT payload FROM records WHERE kind=? AND id=?", kind, id).Scan(&data); err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

// dependencyComplete runs on the same transaction as dispatch. SPLIT is a
// durable history state, so completion must follow the tree and the controller's
// whole-issue acceptance proof rather than changing its phase to MERGED.
func dependencyComplete(ctx context.Context, q queryer, w domain.WorkItem, seen map[string]bool) (bool, error) {
	if seen[w.ID] {
		return false, errors.New("persisted dependency completion cycle")
	}
	seen[w.ID] = true
	if w.Phase != domain.Merged && w.Phase != domain.Split {
		return false, nil
	}
	var scope verification.Scope
	if err := completionRecord(ctx, q, "scope", w.ID, &scope); err != nil {
		return false, err
	}
	if err := scope.Validate(); err != nil {
		return false, err
	}
	if scope.Revision != w.ScopeRevision {
		return false, nil
	}
	if w.Phase == domain.Merged {
		if len(scope.Remaining) == 0 {
			return true, nil
		}
		next, err := read(ctx, q, w.ID+"-remaining")
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		var actual verification.Scope
		if err = completionRecord(ctx, q, "scope", next.ID, &actual); err != nil {
			return false, err
		}
		expected := scope
		expected.Included = append([]string{}, scope.Remaining...)
		expected.Remaining = []string{}
		expected.PreviouslyMerged = append(append([]string{}, scope.PreviouslyMerged...), scope.Included...)
		if next.RepositoryID != w.RepositoryID || next.Issue != w.Issue || next.PolicyRevision != w.PolicyRevision || !reflect.DeepEqual(expected, actual) {
			return false, nil
		}
		return dependencyComplete(ctx, q, next, seen)
	}
	var progress struct {
		Children []struct {
			Key, State string
			Issue      int
		}
	}
	if err := completionRecord(ctx, q, "split_progress", w.ID, &progress); err != nil {
		return false, err
	}
	if len(progress.Children) == 0 {
		return false, nil
	}
	for _, child := range progress.Children {
		if child.Key == "" || child.State != "created" || child.Issue < 1 {
			return false, nil
		}
		nested, err := read(ctx, q, w.ID+"-"+child.Key)
		if err != nil {
			return false, err
		}
		if nested.RepositoryID != w.RepositoryID || nested.Issue != child.Issue || nested.PolicyRevision != w.PolicyRevision {
			return false, nil
		}
		ok, err := dependencyComplete(ctx, q, nested, seen)
		if err != nil || !ok {
			return ok, err
		}
		ok, err = acceptedIssue(ctx, q, nested)
		if err != nil || !ok {
			return ok, err
		}
	}
	return acceptedIssue(ctx, q, w)
}

// Only the trusted controller writes parent_completion, after checking the full
// configured profile set. Retain support for existing proofs and check their
// current identity; their result list alone never creates a completion proof.
func acceptedIssue(ctx context.Context, q queryer, w domain.WorkItem) (bool, error) {
	subject, err := finalAcceptanceSubject(ctx, q, w)
	if err != nil {
		return false, err
	}
	w = subject
	if w.Phase != domain.Merged && w.Phase != domain.Split {
		return false, nil
	}
	var proof struct {
		ScopeRevision int              `json:"scope_revision"`
		Candidate     runner.Candidate `json:"candidate"`
		Results       []runner.Result  `json:"results"`
	}
	if err := completionRecord(ctx, q, "parent_completion", fmt.Sprintf("r%d-i%d", w.RepositoryID, w.Issue), &proof); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if proof.ScopeRevision != w.ScopeRevision || proof.Candidate.ScopeRevision != w.ScopeRevision || proof.Candidate.PolicyRevision != w.PolicyRevision || proof.Candidate.ID == "" || proof.Candidate.SHA == "" || len(proof.Results) == 0 {
		return false, nil
	}
	for _, r := range proof.Results {
		if r.Candidate != proof.Candidate || r.TestedSHA != proof.Candidate.SHA || r.Status != "passed" || r.ExitCode == nil || *r.ExitCode != 0 || r.ProfileID == "" || r.ProfileRevision < 1 || r.Started.IsZero() || r.Finished.Before(r.Started) || r.LogPath == "" {
			return false, nil
		}
	}
	return true, nil
}
