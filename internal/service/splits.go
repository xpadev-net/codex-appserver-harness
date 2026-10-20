package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
	"strings"
	"time"
)

func splittingResidual(w domain.WorkItem, s verification.Scope) splitting.Residual {
	return splitting.Residual{ID: w.ID + "-remaining", ParentWorkItemID: w.ID, ScopeRevision: s.Revision, Criteria: append([]string{}, s.Remaining...), DependsOn: []string{w.ID}, State: "planned"}
}
func (c *Controller) split(ctx context.Context, w domain.WorkItem, op domain.Operation) error {
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
		registrations = append(registrations, storage.ChildRegistration{Item: domain.WorkItem{ID: id, RepositoryID: w.RepositoryID, Issue: state.Issue, ScopeRevision: 1, PolicyRevision: w.PolicyRevision, Phase: domain.Ready}, Dependencies: deps, Scope: scope, Source: Source{digest(issue)}, PolicySnapshot: c.policyDigest()})
		if err = c.Store.Put(ctx, "split_depth", id, id, depth+1); err != nil {
			return err
		}
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
		nextScope, err := splitting.NextScope(scope)
		if err != nil {
			return err
		}
		next = domain.WorkItem{ID: residual.ID, RepositoryID: w.RepositoryID, Issue: w.Issue, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, Phase: domain.Ready}
		issue, err := c.GitHub.GetIssue(ctx, w.Issue)
		if err != nil {
			return err
		}
		return c.Store.ClaimNextRecords(ctx, w.ID, next, map[string]any{"scope": nextScope, "source": Source{digest(issue)}, "policy_snapshot": c.policyDigest()})
	}
	return c.finalAcceptance(ctx, w, scope)
}
func (c *Controller) splitComplete(ctx context.Context, w domain.WorkItem, seen map[string]bool) (bool, error) {
	if seen[w.ID] {
		return false, errors.New("persisted split cycle")
	}
	seen[w.ID] = true
	if w.Phase == domain.Merged {
		return true, nil
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
	}
	return len(progress.Children) > 0, nil
}
func (c *Controller) finalAcceptance(ctx context.Context, w domain.WorkItem, s verification.Scope) error {
	key := fmt.Sprintf("r%d-i%d", w.RepositoryID, w.Issue)
	var closed map[string]any
	if c.Store.Record(ctx, "parent_completion", key, &closed) == nil {
		return nil
	}
	items, err := c.Store.List(ctx)
	if err != nil {
		return err
	}
	completion := []splitting.Completion{}
	for _, item := range items {
		if item.Issue != w.Issue || item.RepositoryID != w.RepositoryID {
			continue
		}
		var childScope verification.Scope
		if err = c.Store.Record(ctx, "scope", item.ID, &childScope); err != nil {
			return err
		}
		ok, err := c.splitComplete(ctx, item, map[string]bool{})
		if err != nil {
			return err
		}
		if ok {
			completion = append(completion, splitting.Completion{ScopeRevision: childScope.Revision, Phase: domain.Merged, ParentCriteria: childScope.Included})
		}
	}
	if !splitting.ParentComplete(s, completion, s.Revision, true) {
		return nil
	}
	issue, err := c.GitHub.GetIssue(ctx, w.Issue)
	if err != nil {
		return err
	}
	var source Source
	if err = c.Store.Record(ctx, "source", w.ID, &source); err != nil {
		return err
	}
	if digest(issue) != source.Digest {
		return errors.New("parent scope edited before final acceptance")
	}
	if _, err = workspace.Git(ctx, c.Config.RepositoryPath, "fetch", "origin", c.Config.BaseBranch); err != nil {
		return err
	}
	sha, err := workspace.Resolve(ctx, c.Config.RepositoryPath, "FETCH_HEAD")
	if err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%d", key, sha, s.Revision, c.Config.Verification.Revision)))
	candidate := runner.Candidate{ID: fmt.Sprintf("final-%x", hash[:16]), SHA: sha, BaseSHA: sha, ScopeRevision: s.Revision, PolicyRevision: c.Config.Verification.Revision}
	results := []runner.Result{}
	for _, profile := range c.Config.Verification.RequiredProfiles {
		jobKey := candidate.ID + "-" + profile
		spec := runner.Spec{JobID: jobKey, Repository: c.Config.RepositoryPath, Root: c.Config.Root + "/jobs", Candidate: candidate, ProfileID: profile, Profile: c.Config.Verification.Profiles[profile], Launcher: c.Config.Launcher, ForbiddenPaths: forbiddenPaths(c.Config)}
		var intent JobIntent
		err = c.Store.Record(ctx, "job_intent", jobKey, &intent)
		if err != nil && !missing(err) {
			return err
		}
		if err != nil {
			busy, err := c.runnerBusy(ctx, "")
			if err != nil {
				return err
			}
			if busy {
				return nil
			}
			if err = c.jobBudget(ctx, w.ID); err != nil {
				return err
			}
			if err = c.artifactBudget(); err != nil {
				return err
			}
			if !c.Config.IsolationVerified {
				return errors.New("final acceptance requires verified isolation")
			}
			intent = JobIntent{Spec: spec, Started: time.Now().UTC()}
			if err = c.Store.Put(ctx, "job_intent", jobKey, w.ID, intent); err != nil {
				return err
			}
			if err = c.Jobs.Start(ctx, spec); err != nil {
				return err
			}
		}
		r, err := c.Jobs.Read(spec)
		if err != nil {
			if strings.Contains(err.Error(), "no such file") {
				return nil
			}
			return err
		}
		if err = c.Store.Put(ctx, "validation", r.JobID, w.ID, r); err != nil {
			return err
		}
		if r.Status != "passed" {
			return errors.New("parent final acceptance failed or unavailable")
		}
		results = append(results, r)
	}
	if !runner.Passed(candidate, c.Config.Verification, results) {
		return errors.New("parent final acceptance incomplete")
	}
	freshIssue, err := c.GitHub.GetIssue(ctx, w.Issue)
	if err != nil {
		return err
	}
	if digest(freshIssue) != source.Digest || w.PolicyRevision != c.Config.Verification.Revision {
		return errors.New("scope or policy changed during final acceptance")
	}
	var savedPolicy string
	if err = c.Store.Record(ctx, "policy_snapshot", w.ID, &savedPolicy); err != nil {
		return err
	}
	if savedPolicy != c.policyDigest() {
		return errors.New("policy changed during final acceptance")
	}
	for _, label := range freshIssue.Labels {
		if label.Name == c.Config.HoldLabel && freshIssue.State == "open" {
			return errors.New("parent completion is on hold")
		}
	}
	ctx = c.writeContext(ctx, w, "close_issue")
	if freshIssue.State != "closed" {
		if err = c.GitHub.CloseIssue(ctx, w.Issue); err != nil {
			return err
		}
	}
	return c.Store.Put(context.WithoutCancel(ctx), "parent_completion", key, w.ID, map[string]any{"scope_revision": s.Revision, "candidate": candidate, "results": results, "closed_at": time.Now().UTC()})
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
