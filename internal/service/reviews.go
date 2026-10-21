package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
	"strings"
	"time"
)

func (c *Controller) publish(ctx context.Context, w domain.WorkItem, op domain.Operation) error {
	if _, live := c.GitHub.(github.Client); live {
		if err := c.VerifyRuntime(ctx); err != nil {
			return c.reject(ctx, w, op, err.Error())
		}
	}
	if w.Terminal() {
		var found *github.PR
		var err error
		found, err = c.GitHub.FindPR(ctx, w.Branch, w.ID)
		if err != nil {
			return err
		}
		return c.done(ctx, op, found)
	}
	scope, err := c.current(ctx, w)
	if err != nil {
		return c.reject(ctx, w, op, err.Error())
	}
	candidate, err := c.candidate(ctx, w)
	if err != nil {
		return err
	}
	var plan verification.Plan
	if err = c.Store.Record(ctx, "plan", w.ID, &plan); err != nil {
		return err
	}
	results, err := c.results(ctx, candidate, w)
	if err != nil {
		return err
	}
	if !verification.PassedPlan(candidate, c.Config.Verification, plan, results) {
		return c.reject(ctx, w, op, "validation gates changed before publication")
	}
	if len(scope.Remaining) > 0 {
		var residual splitting.Residual
		if err = c.Store.Record(ctx, "residual", w.ID, &residual); err != nil || residual.Validate(w, scope) != nil {
			return c.reject(ctx, w, op, "remaining work is not persisted")
		}
		messages, err := workspace.Git(ctx, w.Worktree, "log", "--format=%B", candidate.BaseSHA+".."+candidate.SHA)
		if err != nil {
			return err
		}
		if err = github.CheckClosingText(messages); err != nil {
			return c.reject(ctx, w, op, err.Error())
		}
	}
	var publicationIntent map[string]any
	prior := c.Store.Record(ctx, "publication_intent", op.ID, &publicationIntent)
	if prior != nil && !missing(prior) {
		return prior
	}
	if op.State == "unknown" || prior == nil {
		found, err := c.GitHub.FindPR(ctx, w.Branch, w.ID)
		if err != nil {
			return err
		}
		if found == nil {
			return c.block(ctx, w, "PR creation outcome unknown; reconcile before retry")
		}
		if err = c.Store.Put(ctx, "pull_request", w.ID, w.ID, *found); err != nil {
			return err
		}
		return c.done(ctx, op, found)
	}
	latest, err := c.Store.Get(ctx, w.ID)
	if err != nil {
		return err
	}
	if !domain.CanDispatch(latest, op) {
		return c.Store.FinishOperation(ctx, op.ID, "canceled", "publication permission changed")
	}
	if err = c.Store.Put(ctx, "publication_intent", op.ID, w.ID, map[string]any{"candidate": candidate, "attempted_at": time.Now().UTC()}); err != nil {
		return err
	}
	if err = workspace.Push(ctx, w.Worktree, w.Branch, candidate.SHA); err != nil {
		return err
	}
	latest, err = c.Store.Get(ctx, w.ID)
	if err != nil {
		return err
	}
	if !domain.CanDispatch(latest, op) {
		return c.Store.FinishOperation(ctx, op.ID, "canceled", "branch uploaded; PR publication canceled before request")
	}
	body := fmt.Sprintf("Parent Issue #%d\n\nWorkItem: %s\nScope revision: %d; policy revision: %d\nCandidate: %s\nIncluded criteria: %s\nRemaining criteria: %s\n\nValidation evidence is stored in the controller job journal (private logs are not published).\n", w.Issue, w.ID, w.ScopeRevision, w.PolicyRevision, candidate.SHA, strings.Join(scope.Included, ", "), strings.Join(scope.Remaining, ", "))
	pr, err := c.GitHub.Publish(ctx, c.Config.BaseBranch, w.Branch, w.ID, "Harness implementation "+w.ID, body)
	if errors.Is(err, contract.ErrUnsent) {
		return c.Store.ReturnUnsent(context.WithoutCancel(ctx), op.ID, err.Error())
	}
	if err != nil {
		_ = c.Store.FinishOperation(context.WithoutCancel(ctx), op.ID, "unknown", nil)
		return err
	}
	if pr.Head.SHA != candidate.SHA || pr.Head.Repo.ID != w.RepositoryID {
		return c.reject(ctx, w, op, "published PR head or repository mismatched")
	}
	if err = c.Store.Put(ctx, "pull_request", w.ID, w.ID, pr); err != nil {
		return err
	}
	return c.done(ctx, op, pr)
}
func (c *Controller) reviewPolicy() review.Policy {
	p := c.Config.Review
	verified := func(key string) bool {
		capability := c.Capabilities.Capabilities[key]
		return capability.State == "verified" && capability.Evidence != "" && c.Capabilities.RepositoryID == c.Config.RepositoryID
	}
	p.VerifiedRequest = p.VerifiedRequest && verified("cloud_request")
	p.VerifiedNativeApproval = p.VerifiedNativeApproval && verified("cloud_native_approval")
	p.VerifiedSHAMapping = p.VerifiedSHAMapping && verified("review_sha")
	return p
}
func (c *Controller) observeReview(ctx context.Context, w domain.WorkItem) error {
	ctx = c.writeContext(ctx, w, "review")
	scope, err := c.current(ctx, w)
	if err != nil {
		return c.block(ctx, w, err.Error())
	}
	candidate, err := c.candidate(ctx, w)
	if err != nil {
		return err
	}
	var saved github.PR
	if err = c.Store.Record(ctx, "pull_request", w.ID, &saved); err != nil {
		return err
	}
	pr, err := c.GitHub.Pull(ctx, saved.Number)
	if err != nil {
		return err
	}
	if pr.Merged {
		return c.block(ctx, w, "PR merged externally; reconcile completion explicitly")
	}
	if pr.State != "open" || pr.Draft {
		return c.block(ctx, w, "PR closed or draft")
	}
	if pr.Head.SHA != candidate.SHA || pr.Head.Repo.ID != w.RepositoryID || pr.Base.Repo.ID != w.RepositoryID || pr.Base.Ref != c.Config.BaseBranch {
		return c.block(ctx, w, "remote PR head changed outside harness")
	}
	if pr.Base.SHA != candidate.BaseSHA {
		return c.refreshBase(ctx, w, pr.Base.SHA)
	}
	p := c.reviewPolicy()
	if !p.VerifiedRequest {
		return c.block(ctx, w, "Cloud review request capability is unverified")
	}
	var round review.Round
	err = c.Store.Record(ctx, "review_round", candidate.ID, &round)
	if err != nil && !missing(err) {
		return err
	}
	if err != nil {
		if c.Config.Limits.ReviewRequests < 1 {
			return c.block(ctx, w, "review request budget exhausted")
		}
		round = review.Round{Number: 1, ID: candidate.ID + "-round1", Candidate: candidate, PR: pr.Number, RequestedAt: time.Now().UTC(), RequesterID: c.Config.RequesterID}
		if err = c.Store.Put(ctx, "review_round", candidate.ID, w.ID, round); err != nil {
			return err
		}
	}
	if round.RequestCommentID == 0 && round.RequestAttempted {
		id, err := c.GitHub.FindReviewRequest(ctx, pr.Number, round.ID, c.Config.RequesterID)
		if err != nil {
			return err
		}
		if id == 0 {
			return c.block(ctx, w, "review request outcome unknown; no duplicate request sent")
		}
		round.RequestCommentID = id
		if err = c.Store.Put(ctx, "review_round", candidate.ID, w.ID, round); err != nil {
			return err
		}
	}
	if round.RequestCommentID == 0 && !round.RequestAttempted {
		found, err := c.GitHub.FindReviewRequest(ctx, pr.Number, round.ID, c.Config.RequesterID)
		if err != nil {
			return err
		}
		if found > 0 {
			round.RequestCommentID = found
			if err = c.Store.Put(ctx, "review_round", candidate.ID, w.ID, round); err != nil {
				return err
			}
		}
	}
	if round.RequestCommentID == 0 {
		latest, err := c.Store.Get(ctx, w.ID)
		if err != nil {
			return err
		}
		if latest.Terminal() || latest.Phase != domain.Reviewing || latest.Candidate != candidate.ID || latest.ScopeRevision != candidate.ScopeRevision || latest.PolicyRevision != candidate.PolicyRevision {
			return nil
		}
		round.RequestAttempted = true
		if err = c.Store.Put(ctx, "review_round", candidate.ID, w.ID, round); err != nil {
			return err
		}
		id, err := c.GitHub.RequestReview(ctx, pr.Number, round.ID, c.Config.ReviewTrigger, c.Config.RequesterID)
		if errors.Is(err, contract.ErrUnsent) {
			round.RequestAttempted = false
			if saveErr := c.Store.Put(context.WithoutCancel(ctx), "review_round", candidate.ID, w.ID, round); saveErr != nil {
				return saveErr
			}
		}
		if err != nil {
			return err
		}
		round.RequestCommentID = id
		if err = c.Store.Put(context.WithoutCancel(ctx), "review_round", candidate.ID, w.ID, round); err != nil {
			return err
		}
	}
	reviews, err := c.GitHub.Reviews(ctx, pr.Number)
	if err != nil {
		return err
	}
	comments, err := c.GitHub.Comments(ctx, pr.Number)
	if err != nil {
		return err
	}
	findings := review.Findings(candidate, round, p, reviews, comments)
	unresolved := []review.Finding{}
	for _, f := range findings {
		var resolved map[string]string
		if err = c.Store.Record(ctx, "resolved_feedback", f.ID, &resolved); missing(err) {
			unresolved = append(unresolved, f)
		} else if err != nil {
			return err
		}
	}

	decision := review.Evaluate(candidate, round, p, reviews)
	snapshot, err := c.snapshot(ctx, w, scope, pr, decision)
	if err != nil {
		return err
	}
	if handled, err := c.routeCIFailures(ctx, w, snapshot, unresolved...); handled || err != nil {
		return err
	}
	if err = c.Store.Put(ctx, "gate", w.ID, w.ID, gate.Reasons(snapshot)); err != nil {
		return err
	}
	if len(gate.Reasons(snapshot)) == 0 {
		if !c.Config.MergeEnabled {
			return c.block(ctx, w, "automatic merge is disabled")
		}
		return c.emit(ctx, w, "gates_passed", "all fresh candidate gates passed")
	}
	if time.Since(round.RequestedAt) > time.Duration(c.Config.Limits.ReviewSeconds)*time.Second {
		return c.block(ctx, w, "review timeout or missing formal approval")
	}
	return nil
}
func (c *Controller) snapshot(ctx context.Context, w domain.WorkItem, scope verification.Scope, pr github.PR, decision review.Decision) (gate.Snapshot, error) {
	candidate, err := c.candidate(ctx, w)
	if err != nil {
		return gate.Snapshot{}, err
	}
	var plan verification.Plan
	if err = c.Store.Record(ctx, "plan", w.ID, &plan); err != nil {
		return gate.Snapshot{}, err
	}
	results, err := c.results(ctx, candidate, w)
	if err != nil {
		return gate.Snapshot{}, err
	}
	target := candidate.SHA
	mapping := gate.CIRevision{}
	if c.Config.CIRefMode == "test_merge" {
		if pr.MergeCommitSHA == "" {
			return gate.Snapshot{}, errors.New("test merge SHA is unavailable")
		}
		parents, err := c.GitHub.CommitParents(ctx, pr.MergeCommitSHA)
		if err != nil {
			return gate.Snapshot{}, err
		}
		verified := len(parents) == 2 && ((parents[0] == candidate.SHA && parents[1] == candidate.BaseSHA) || (parents[1] == candidate.SHA && parents[0] == candidate.BaseSHA))
		if !verified {
			return gate.Snapshot{}, errors.New("test merge SHA parents do not match current head/base")
		}
		target = pr.MergeCommitSHA
		mapping = gate.CIRevision{HeadSHA: candidate.SHA, BaseSHA: candidate.BaseSHA, TestedSHA: target, Verified: true}
	}
	checks, err := c.GitHub.Checks(ctx, target)
	if err != nil {
		return gate.Snapshot{}, err
	}
	conditions, err := c.GitHub.Conditions(ctx, pr.Number)
	if err != nil {
		return gate.Snapshot{}, err
	}
	residualOK := len(scope.Remaining) == 0
	if !residualOK {
		var residual splitting.Residual
		residualOK = c.Store.Record(ctx, "residual", w.ID, &residual) == nil && residual.Validate(w, scope) == nil
	}
	return gate.Snapshot{CI: mapping, WorkItem: w, Candidate: candidate, PR: pr, BaseBranch: c.Config.BaseBranch, Scope: scope, Plan: plan, Verification: c.Config.Verification, Results: results, Review: decision, RepositoryConditions: conditions, Checks: checks, RequiredChecks: c.Config.RequiredChecks, ResidualPersisted: residualOK, CurrentScopeRevision: scope.Revision, CurrentPolicyRevision: c.Config.Verification.Revision, ObservedAt: time.Now().UTC()}, nil
}
func (c *Controller) merge(ctx context.Context, w domain.WorkItem, op domain.Operation) error {
	observedAt := time.Now().UTC()
	var saved github.PR
	if err := c.Store.Record(ctx, "pull_request", w.ID, &saved); err != nil {
		return err
	}
	pr, err := c.GitHub.Pull(ctx, saved.Number)
	if err != nil {
		return err
	}
	if pr.Merged {
		return c.complete(ctx, w, op, pr, "merge_confirmed", w.Candidate, "GitHub reports merged")
	}
	var mergeIntent map[string]any
	prior := c.Store.Record(ctx, "merge_intent", op.ID, &mergeIntent)
	if prior != nil && !missing(prior) {
		return prior
	}
	if op.State == "unknown" || prior == nil {
		return nil
	}
	scope, err := c.current(ctx, w)
	if err != nil {
		return c.reject(ctx, w, op, err.Error())
	}
	candidate, err := c.candidate(ctx, w)
	if err != nil {
		return err
	}
	var round review.Round
	if err = c.Store.Record(ctx, "review_round", candidate.ID, &round); err != nil {
		return err
	}
	reviews, err := c.GitHub.Reviews(ctx, pr.Number)
	if err != nil {
		return err
	}
	comments, err := c.GitHub.Comments(ctx, pr.Number)
	if err != nil {
		return err
	}
	p := c.reviewPolicy()
	for _, f := range review.Findings(candidate, round, p, reviews, comments) {
		var resolved map[string]string
		if err = c.Store.Record(ctx, "resolved_feedback", f.ID, &resolved); err != nil {
			return c.reject(ctx, w, op, "review feedback changed before merge")
		}
	}
	snapshot, err := c.snapshot(ctx, w, scope, pr, review.Evaluate(candidate, round, p, reviews))
	if err != nil {
		return err
	}
	latest, err := c.Store.Get(ctx, w.ID)
	if err != nil {
		return err
	}
	snapshot.WorkItem = latest
	snapshot.ObservedAt = observedAt
	finalPR, err := c.GitHub.Pull(ctx, pr.Number)
	if err != nil {
		return err
	}
	if finalPR.Head.SHA != pr.Head.SHA || finalPR.Base.SHA != pr.Base.SHA || finalPR.State != pr.State || finalPR.Draft != pr.Draft || finalPR.MergeCommitSHA != pr.MergeCommitSHA {
		return c.reject(ctx, w, op, "PR changed during final merge reconciliation")
	}
	if _, err = c.current(ctx, latest); err != nil {
		return c.reject(ctx, w, op, err.Error())
	}
	if !c.Config.MergeEnabled || !c.Capabilities.AutoMergeReady() || len(gate.Reasons(snapshot)) > 0 {
		return c.reject(ctx, w, op, "merge gates changed or unverified")
	}
	if err = c.Store.Put(ctx, "merge_intent", op.ID, w.ID, map[string]any{"candidate": candidate, "attempted_at": time.Now().UTC()}); err != nil {
		return err
	}
	err = c.GitHub.Merge(ctx, pr.Number, candidate.SHA, c.Config.MergeMethod)
	if errors.Is(err, contract.ErrUnsent) {
		return c.Store.ReturnUnsent(context.WithoutCancel(ctx), op.ID, err.Error())
	}
	_ = c.Store.FinishOperation(context.WithoutCancel(ctx), op.ID, "unknown", map[string]string{"state": "submitted_or_unknown"})
	return err
}
func (c *Controller) refreshBase(ctx context.Context, w domain.WorkItem, base string) error {
	if _, err := workspace.Git(ctx, w.Worktree, "fetch", "origin", c.Config.BaseBranch); err != nil {
		return err
	}
	resolved, err := workspace.Resolve(ctx, w.Worktree, "FETCH_HEAD")
	if err != nil || resolved != base {
		return errors.New("base changed during refresh")
	}
	if _, err = workspace.Git(ctx, w.Worktree, "-c", "core.hooksPath=/dev/null", "merge", "--no-edit", base); err != nil {
		if saveErr := c.Store.Put(ctx, "base_override", w.ID, w.ID, base); saveErr != nil {
			return saveErr
		}
		return c.emit(ctx, w, "repair_implementation", "base merge conflict; preserve and resolve current worktree")
	}
	candidate, err := c.freeze(ctx, w, base)
	if err != nil {
		return err
	}
	scope, err := c.current(ctx, w)
	if err != nil {
		return err
	}
	var plan verification.Plan
	if err = c.Store.Record(ctx, "plan", w.ID, &plan); err != nil {
		return err
	}
	plan, err = verification.Rebind(plan, w.ID, candidate, scope, c.Config.Verification)
	if err != nil {
		return err
	}
	if err = c.Store.Put(ctx, "plan", w.ID, w.ID, plan); err != nil {
		return err
	}
	return c.Store.Apply(ctx, domain.Event{ID: candidate.ID + ":base", WorkItemID: w.ID, Kind: "candidate_refreshed", Candidate: candidate.ID, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, Attempt: w.Attempt})
}
func (c *Controller) Rereview(ctx context.Context, id, reason string) error {
	if reason == "" {
		return errors.New("rereview requires a reason")
	}
	w, err := c.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	candidate, err := c.candidate(ctx, w)
	if err != nil {
		return err
	}
	var old review.Round
	if err = c.Store.Record(ctx, "review_round", candidate.ID, &old); err != nil {
		return err
	}
	if old.RequestCommentID == 0 {
		return errors.New("previous review request must be reconciled before rereview")
	}
	number := old.Number
	if number < 1 {
		number = 1
	}
	if number >= c.Config.Limits.ReviewRequests {
		return errors.New("review request budget exhausted")
	}
	reviews, err := c.GitHub.Reviews(ctx, old.PR)
	if err != nil {
		return err
	}
	finished := false
	trusted := map[int64]bool{}
	for _, id := range c.Config.Review.TrustedReviewerIDs {
		trusted[id] = true
	}
	for _, r := range reviews {
		if trusted[r.User.ID] && r.CommitID == candidate.SHA && !r.SubmittedAt.Before(old.RequestedAt) && r.State != "PENDING" {
			finished = true
		}
	}
	if !finished && time.Since(old.RequestedAt) < time.Duration(c.Config.Limits.ReviewSeconds)*time.Second {
		return errors.New("review is still pending within its deadline")
	}
	next := review.Round{ID: fmt.Sprintf("%s-round%d", candidate.ID, number+1), Number: number + 1, Candidate: candidate, PR: old.PR, RequestedAt: time.Now().UTC(), RequesterID: c.Config.RequesterID}
	e := domain.Event{ID: next.ID + ":operator", WorkItemID: w.ID, Kind: "review_retry", Reason: reason}
	return c.Store.EventRecords(ctx, e, map[string]map[string]any{"review_round_history": {old.ID: old}, "review_round": {candidate.ID: next}, "operator_audit": {e.ID: map[string]string{"reason": reason, "action": "rereview"}}})
}
