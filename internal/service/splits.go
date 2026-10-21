package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func splittingResidual(w domain.WorkItem, s verification.Scope) splitting.Residual {
	return splitting.Residual{ID: w.ID + "-remaining", ParentWorkItemID: w.ID, ScopeRevision: s.Revision, Criteria: append([]string{}, s.Remaining...), DependsOn: []string{w.ID}, State: "planned"}
}
func (c *Controller) split(ctx context.Context, w domain.WorkItem, op domain.Operation) error {
	lock, err := c.splitLease(w.ID)
	if errors.Is(err, ErrSplitBusy) {
		return nil
	}
	if err != nil {
		return err
	}
	defer lock.Close()

	if w.Terminal() {
		return c.done(ctx, op, "split canceled")
	}
	scope, err := c.current(ctx, w)
	if err != nil {
		return err
	}
	var proposal splitting.Proposal
	if err = c.Store.Record(ctx, "split_proposal", w.SplitPlanID, &proposal); err != nil {
		return err
	}
	var depth int
	err = c.Store.Record(ctx, "split_depth", w.ID, &depth)
	if err != nil && !missing(err) {
		return err
	}
	if err = proposal.Validate(w.ID, scope, depth, c.Config.Limits.SplitDepth, c.Config.Limits.SplitChildren); err != nil {
		return err
	}
	progress, err := splitting.CreateChildren(ctx, c.Store, scopedCreator{GitHub: c.GitHub, Context: fmt.Sprintf("Parent Issue #%d\nOriginal parent scope:\n%s", w.Issue, scope.OriginalRequest)}, proposal)
	if err != nil {
		return err
	}
	registrations := []storage.ChildRegistration{}
	for i, child := range proposal.Children {
		state := progress.Children[i]
		issue, err := c.GitHub.GetIssue(ctx, state.Issue)
		if err != nil {
			return err
		}
		if issue.State != "open" {
			return errors.New("split child is not open")
		}
		id := w.ID + "-" + child.Key
		criteria := []string{}
		for _, criterion := range child.Criteria {
			criteria = append(criteria, criterion.ID)
		}
		scope := verification.Scope{Revision: 1, OriginalRequest: fmt.Sprintf("Original parent requirements:\n%s\nThis child scope:\n%s", scope.OriginalRequest, child.Requirement), Criteria: child.Criteria, Included: criteria, Remaining: []string{}, PreviouslyMerged: []string{}}
		deps := []string{}
		for _, key := range child.DependsOn {
			deps = append(deps, w.ID+"-"+key)
		}
		registrations = append(registrations, storage.ChildRegistration{Depth: depth + 1, Item: domain.WorkItem{ID: id, RepositoryID: w.RepositoryID, Issue: state.Issue, ScopeRevision: 1, PolicyRevision: w.PolicyRevision, Phase: domain.Ready}, Dependencies: deps, Scope: scope, Source: Source{digest(issue)}, PolicySnapshot: c.policyDigest()})

	}
	if err = c.Store.RegisterChildren(ctx, w.ID, registrations); err != nil {
		return err
	}
	return c.complete(ctx, w, op, progress, "split_confirmed", w.Candidate, "children created and registered")
}
func (c *Controller) afterMerge(ctx context.Context, w domain.WorkItem) error {
	var scope verification.Scope
	if err := c.Store.Record(ctx, "scope", w.ID, &scope); err != nil {
		return err
	}
	if len(scope.Remaining) > 0 {
		residual := splittingResidual(w, scope)
		var next domain.WorkItem
		if c.Store.GetInto(ctx, residual.ID, &next) == nil {
			return nil
		}
		var hold map[string]any
		if err := c.Store.Record(ctx, "continuation_hold", w.ID, &hold); err == nil {
			return nil
		} else if !missing(err) {
			return err
		}
		if _, err := c.current(ctx, w); err != nil {
			var conflict currentConflict
			if !errors.As(err, &conflict) {
				return err
			}
			return c.Store.Put(ctx, "continuation_hold", w.ID, w.ID, map[string]any{"reason": err.Error(), "required_action": "adopt a new scope revision for the successor", "observed_at": time.Now().UTC()})
		}
		var source Source
		if err := c.Store.Record(ctx, "source", w.ID, &source); err != nil {
			return err
		}
		nextScope, err := splitting.NextScope(scope)
		if err != nil {
			return err
		}
		next = domain.WorkItem{ID: residual.ID, RepositoryID: w.RepositoryID, Issue: w.Issue, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, Phase: domain.Ready}
		return c.claimContinuation(ctx, w, next, map[string]any{"scope": nextScope, "source": source, "policy_snapshot": c.policyDigest()})
	}
	return c.finalAcceptance(ctx, w, scope)
}
func (c *Controller) splitComplete(ctx context.Context, w domain.WorkItem, seen map[string]bool) (bool, error) {
	if seen[w.ID] {
		return false, errors.New("persisted split cycle")
	}
	seen[w.ID] = true
	if w.Phase == domain.Merged {
		var scope verification.Scope
		if err := c.Store.Record(ctx, "scope", w.ID, &scope); err != nil {
			return false, err
		}
		if err := scope.Validate(); err != nil {
			return false, err
		}
		if len(scope.Remaining) == 0 {
			return true, nil
		}
		next, err := c.Store.Get(ctx, splittingResidual(w, scope).ID)
		if missing(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		var nextScope verification.Scope
		if err = c.Store.Record(ctx, "scope", next.ID, &nextScope); err != nil {
			return false, err
		}
		expected, err := splitting.NextScope(scope)
		if err != nil {
			return false, err
		}
		// A newer or unrelated scope cannot prove the old residual complete.
		a, _ := json.Marshal(expected)
		b, _ := json.Marshal(nextScope)
		if next.Issue != w.Issue || next.RepositoryID != w.RepositoryID || string(a) != string(b) {
			return false, nil
		}
		return c.splitComplete(ctx, next, seen)
	}
	if w.Phase != domain.Split {
		return false, nil
	}
	var progress splitting.Progress
	if err := c.Store.Record(ctx, "split_progress", w.ID, &progress); err != nil {
		return false, err
	}
	for _, child := range progress.Children {
		nested, err := c.Store.Get(ctx, w.ID+"-"+child.Key)
		if err != nil {
			return false, err
		}
		ok, err := c.splitComplete(ctx, nested, seen)
		if err != nil || !ok {
			return ok, err
		}
		var proof struct {
			ScopeRevision int              `json:"scope_revision"`
			Candidate     runner.Candidate `json:"candidate"`
			Results       []runner.Result  `json:"results"`
		}
		key := fmt.Sprintf("r%d-i%d", nested.RepositoryID, nested.Issue)
		if err = c.Store.Record(ctx, "parent_completion", key, &proof); missing(err) {
			return false, nil
		} else if err != nil {
			return false, err
		}
		subject, err := c.Store.FinalAcceptanceSubject(ctx, nested)
		if err != nil {
			return false, err
		}
		if (subject.Phase != domain.Merged && subject.Phase != domain.Split) || proof.ScopeRevision != subject.ScopeRevision || proof.Candidate.ScopeRevision != subject.ScopeRevision || proof.Candidate.PolicyRevision != subject.PolicyRevision || !runner.Passed(proof.Candidate, c.Config.Verification, proof.Results) {
			return false, nil
		}
	}
	return len(progress.Children) > 0, nil
}

type scopedCreator struct {
	GitHub  GitHub
	Context string
}

func (s scopedCreator) FindChild(ctx context.Context, marker string) (int, error) {
	return s.GitHub.FindChild(ctx, marker)
}
func (s scopedCreator) CreateChild(ctx context.Context, title, body, marker string) (int, error) {
	return s.GitHub.CreateChild(ctx, title, body+"\n\n"+s.Context, marker)
}

// AdoptContinuation preserves the merged PR and starts a separately versioned
// successor. Old completion/approval evidence is not assigned to the new scope.
func (c *Controller) AdoptContinuation(ctx context.Context, w domain.WorkItem, scope verification.Scope, reason string) error {
	lock, err := c.splitLease("final:" + issueKey(w))
	if err != nil {
		return err
	}
	defer lock.Close()

	if w.Phase != domain.Merged || scope.Revision != w.ScopeRevision+1 || strings.TrimSpace(reason) == "" || len(scope.PreviouslyMerged) != 0 || c.Config.Verification.Revision < w.PolicyRevision {
		return errors.New("continuation adoption requires a new scope and fresh coverage")
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	var previous verification.Scope
	if err := c.Store.Record(ctx, "scope", w.ID, &previous); err != nil {
		return err
	}
	var previousPolicy string
	if err := c.Store.Record(ctx, "policy_snapshot", w.ID, &previousPolicy); err != nil {
		return err
	}
	if previousPolicy != c.policyDigest() && c.Config.Verification.Revision <= w.PolicyRevision {
		return errors.New("policy changes require an increased verification.revision")
	}
	issue, err := c.GitHub.GetIssue(ctx, w.Issue)
	if err != nil {
		return err
	}
	if issue.State != "open" {
		return errors.New("continuation issue is not open")
	}
	for _, label := range issue.Labels {
		if label.Name == c.Config.HoldLabel {
			return errors.New("continuation issue is on hold")
		}
	}
	next := domain.WorkItem{ID: splittingResidual(w, previous).ID, RepositoryID: w.RepositoryID, Issue: w.Issue, ScopeRevision: scope.Revision, PolicyRevision: c.Config.Verification.Revision, Phase: domain.Ready}
	return c.claimContinuation(ctx, w, next, map[string]any{"scope": scope, "source": Source{digest(issue)}, "policy_snapshot": c.policyDigest(), "adoption_reason": reason, "continued_from": w.ID})
}

func (c *Controller) RetrySplitChild(ctx context.Context, id, operation, child, evidence, reason string) error {
	lock, err := c.splitLease(id)
	if err != nil {
		return err
	}
	defer lock.Close()

	if strings.TrimSpace(evidence) == "" || strings.TrimSpace(reason) == "" || child == "" {
		return errors.New("child key, absence evidence and reason are required")
	}
	w, err := c.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	if _, err = c.current(ctx, w); err != nil {
		return err
	}
	ops, err := c.Store.Operations(ctx)
	if err != nil {
		return err
	}
	found := false
	for _, op := range ops {
		if op.ID == operation && op.WorkItemID == id && op.Kind == "split" {
			found = true
		}
	}
	if !found {
		return errors.New("split operation is not associated with this work item")
	}
	issue, err := c.GitHub.FindChild(ctx, "harness-child:"+id+":"+child)
	if err != nil {
		return err
	}
	if issue != 0 {
		return errors.New("child already exists; reconcile its result instead of retrying creation")
	}
	return c.Store.RetrySplitChildBounded(ctx, operation, child, evidence+": "+reason, c.Config.Limits.NetworkRetries)
}

var ErrSplitBusy = errors.New("split reconciliation is active; retry after it finishes")

// CLI and controller share a host-local lease across the complete read/lookup/
// write cycle. OS ownership ends on crash, without an expiry-based takeover.
func (c *Controller) splitLease(id string) (*os.File, error) {
	root := filepath.Join(c.Config.Root, "split-locks")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	key := sha256.Sum256([]byte(id))
	file, err := os.OpenFile(filepath.Join(root, fmt.Sprintf("%x.lock", key)), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrSplitBusy
		}
		return nil, err
	}
	return file, nil
}
