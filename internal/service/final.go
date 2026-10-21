package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

type FinalAcceptanceState = storage.FinalAcceptanceState

type BudgetBaseline struct {
	ValidationJobs int   `json:"validation_jobs"`
	ObservedTokens int64 `json:"observed_tokens"`
}

func issueKey(w domain.WorkItem) string { return fmt.Sprintf("r%d-i%d", w.RepositoryID, w.Issue) }

func (c *Controller) saveFinal(ctx context.Context, w domain.WorkItem, state FinalAcceptanceState) error {
	state.UpdatedAt = time.Now().UTC()
	return c.Store.Put(ctx, "parent_final_state", issueKey(w), state.WorkItemID, state)
}

func (c *Controller) blockFinal(ctx context.Context, w domain.WorkItem, state FinalAcceptanceState, classification, reason, action string) error {
	state.State, state.Classification, state.Reason, state.RequiredAction = "blocked", classification, reason, action
	return c.saveFinal(ctx, w, state)
}

func (c *Controller) finalAcceptance(ctx context.Context, w domain.WorkItem, s verification.Scope) error {
	lock, err := c.splitLease("final:" + issueKey(w))
	if errors.Is(err, ErrSplitBusy) {
		return nil
	}
	if err != nil {
		return err
	}
	defer lock.Close()
	var proof struct {
		ScopeRevision int              `json:"scope_revision"`
		Candidate     runner.Candidate `json:"candidate"`
	}
	if err = c.Store.Record(ctx, "parent_completion", issueKey(w), &proof); err == nil && proof.ScopeRevision == s.Revision && proof.Candidate.PolicyRevision == c.Config.Verification.Revision {
		var state FinalAcceptanceState
		if err = c.Store.Record(ctx, "parent_final_state", issueKey(w), &state); err != nil && !missing(err) {
			return err
		}
		if state.State == "completed" {
			return nil
		}
		if state.WorkItemID == "" {
			state.WorkItemID = w.ID
		}
		state.State = "completed"
		state.ScopeRevision = s.Revision
		state.Candidate = proof.Candidate
		state.Classification = ""
		state.Reason = ""
		state.RequiredAction = ""
		state.Failure = nil
		return c.saveFinal(ctx, w, state)
	}
	if err != nil && !missing(err) {
		return err
	}
	state := FinalAcceptanceState{WorkItemID: w.ID, ScopeRevision: s.Revision, State: "waiting"}
	var previous FinalAcceptanceState
	if err = c.Store.Record(ctx, "parent_final_state", issueKey(w), &previous); err == nil {
		if previous.ScopeRevision > s.Revision || (previous.ScopeRevision == s.Revision && previous.WorkItemID != w.ID) {
			return nil
		}
		if previous.ScopeRevision == s.Revision {
			state = previous
		} else {
			state.Retries = previous.Retries
			state.Attempt = previous.Attempt + 1
		}
	} else if !missing(err) {
		return err
	}
	if state.State == "blocked" {
		return nil
	}
	if w.Paused || w.MergeHold {
		return c.blockFinal(ctx, w, state, "hold", "final acceptance is paused or held", "resume/release, then retry-final with evidence and reason")
	}

	if state.State == "waiting" {
		if err = c.saveFinal(ctx, w, state); err != nil {
			return err
		}
	}
	ownDone, err := c.splitComplete(ctx, w, map[string]bool{})
	if err != nil || !ownDone {
		return err
	}
	items, err := c.Store.List(ctx)
	if err != nil {
		return err
	}
	completion := []splitting.Completion{}
	for _, item := range items {
		if item.Issue != w.Issue || item.RepositoryID != w.RepositoryID || item.ScopeRevision != s.Revision {
			continue
		}
		var scope verification.Scope
		if err = c.Store.Record(ctx, "scope", item.ID, &scope); err != nil {
			return err
		}
		ok, err := c.splitComplete(ctx, item, map[string]bool{})
		if err != nil {
			return err
		}
		if ok {
			completion = append(completion, splitting.Completion{ScopeRevision: scope.Revision, Phase: domain.Merged, ParentCriteria: scope.Included})
		}
	}
	if !splitting.ParentComplete(s, completion, s.Revision, true) {
		return nil
	}
	// Closure recovery may observe an already closed issue after a lost response.
	issue, err := c.GitHub.GetIssue(ctx, w.Issue)
	if err != nil {
		return err
	}
	var source Source
	if err = c.Store.Record(ctx, "source", w.ID, &source); err != nil {
		return err
	}
	var savedPolicy string
	if err = c.Store.Record(ctx, "policy_snapshot", w.ID, &savedPolicy); err != nil {
		return err
	}
	if digest(issue) != source.Digest || savedPolicy != c.policyDigest() || w.PolicyRevision != c.Config.Verification.Revision {
		return c.blockFinal(ctx, w, state, "scope_or_policy_conflict", "scope or policy changed before final acceptance", "adopt the changed requirements before resuming")
	}
	for _, label := range issue.Labels {
		if label.Name == c.Config.HoldLabel && issue.State == "open" {
			return c.blockFinal(ctx, w, state, "hold", "parent completion is on hold", "release hold, then retry-final with evidence and reason")
		}
	}
	if _, err = workspace.Git(ctx, c.Config.RepositoryPath, "fetch", "origin", c.Config.BaseBranch); err != nil {
		return err
	}
	sha, err := workspace.Resolve(ctx, c.Config.RepositoryPath, "FETCH_HEAD")
	if err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%d", issueKey(w), sha, s.Revision, c.Config.Verification.Revision)))
	candidate := runner.Candidate{ID: fmt.Sprintf("final-%x", hash[:16]), SHA: sha, BaseSHA: sha, ScopeRevision: s.Revision, PolicyRevision: c.Config.Verification.Revision}
	if state.Candidate != candidate {
		blocked, err := c.finalOrphans(ctx, w, state)
		if err != nil || blocked {
			return err
		}
		// A changing base cannot hide an unresolved runner from the old candidate.
		busy, err := c.runnerBusy(ctx, "")
		if err != nil {
			return err
		}
		if busy {
			return nil
		}
		state.Candidate = candidate
		state.Failure = nil
	}
	state.State = "running"
	if err = c.saveFinal(ctx, w, state); err != nil {
		return err
	}
	results := []runner.Result{}
	for _, profile := range c.Config.Verification.RequiredProfiles {
		jobKey := fmt.Sprintf("%s-%s-a%d", candidate.ID, profile, state.Attempt)
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
				return c.blockFinal(ctx, w, state, "budget", err.Error(), "inspect retained validation job budget")
			}
			if err = c.artifactBudget(); err != nil {
				return c.blockFinal(ctx, w, state, "budget", err.Error(), "inspect retention and run gc")
			}
			if !c.Config.IsolationVerified {
				return c.blockFinal(ctx, w, state, "isolation", "final acceptance requires verified isolation", "verify isolation before retry-final")
			}
			intent = JobIntent{Spec: runner.Spec{JobID: jobKey, Repository: c.Config.RepositoryPath, Root: c.Config.Root + "/jobs", Candidate: candidate, ProfileID: profile, Profile: c.Config.Verification.Profiles[profile], Launcher: c.Config.Launcher, LauncherContract: c.Config.LauncherContract, ForbiddenPaths: forbiddenPaths(c.Config)}, Started: time.Now().UTC()}
			if err = c.Store.Put(ctx, "job_intent", jobKey, w.ID, intent); err != nil {
				return err
			}
			if err = c.Jobs.Start(ctx, intent.Spec); err != nil {
				return c.blockFinal(ctx, w, state, "unknown", "runner launch outcome unknown: "+err.Error(), "inspect runner and reconcile-job before retry-final")
			}
			intent.Launched = true
			if err = c.Store.Put(context.WithoutCancel(ctx), "job_intent", jobKey, w.ID, intent); err != nil {
				return err
			}
		}
		r, err := c.Jobs.Read(intent.Spec)
		if os.IsNotExist(err) {
			if api, ok := c.Jobs.(JobReconciler); ok {
				life, err := api.Inspect(ctx, intent.Spec)
				if err != nil {
					return err
				}
				if err = c.Store.Put(ctx, "runner_lifecycle", jobKey, w.ID, life); err != nil {
					return err
				}
				if life.State == "not_started" || life.State == "stopped" || life.State == "unknown" {
					return c.blockFinal(ctx, w, state, "unknown", "runner "+life.State, "reconcile-job with quiescence evidence before retry-final")
				}
			}
			if time.Since(intent.Started) > time.Duration(intent.Spec.Profile.TimeoutSeconds+300)*time.Second {
				return c.blockFinal(ctx, w, state, "unknown", "runner result unknown", "reconcile-job with quiescence evidence before retry-final")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if r.JobID != jobKey || r.Candidate != candidate || r.ProfileID != profile || r.ProfileRevision != intent.Spec.Profile.Revision || (r.TestedSHA != candidate.SHA && !(r.Status == "infrastructure_failure" && r.TestedSHA == "")) {
			return c.blockFinal(ctx, w, state, "unknown", "final runner evidence identity mismatch", "inspect and reconcile evidence before retry-final")
		}
		if err = c.Store.Put(ctx, "validation", r.JobID, w.ID, r); err != nil {
			return err
		}
		if r.Status != "passed" {
			state.Failure = &r
			classification := "unknown"
			switch r.Status {
			case "failed":
				classification = "requires_assessment"
			case "infrastructure_failure":
				classification = "infrastructure_failure"
			case "timeout":
				classification = "timeout"
			case "canceled":
				classification = "canceled"
			}
			if err = c.blockFinal(ctx, w, state, classification, "final acceptance "+r.Status+": "+r.Reason, "retry-final with evidence and reason; failed assertions go to an independent assessment work item"); err != nil {
				return err
			}
			if classification == "requires_assessment" {
				if err = c.recoverFinalAssertion(ctx, w, s, "runner:"+r.JobID, "independent assessment of final acceptance failure"); err != nil {
					return c.blockFinal(ctx, w, state, "requires_assessment", err.Error(), "inspect repair budget and retry-final with evidence and reason")
				}
			}
			return nil
		}
		results = append(results, r)
	}
	if !runner.Passed(candidate, c.Config.Verification, results) {
		return c.blockFinal(ctx, w, state, "unknown", "parent final acceptance incomplete", "inspect runner evidence before retry-final")
	}
	fresh, err := c.GitHub.GetIssue(ctx, w.Issue)
	if err != nil {
		return err
	}
	if digest(fresh) != source.Digest || savedPolicy != c.policyDigest() {
		return c.blockFinal(ctx, w, state, "scope_or_policy_conflict", "scope or policy changed during final acceptance", "adopt changed requirements before resuming")
	}
	for _, label := range fresh.Labels {
		if label.Name == c.Config.HoldLabel && fresh.State == "open" {
			return c.blockFinal(ctx, w, state, "hold", "parent completion is on hold", "release hold, then retry-final with evidence and reason")
		}
	}
	ctx = c.writeContext(ctx, w, "close_issue")
	if fresh.State != "closed" {
		if err = c.GitHub.CloseIssue(ctx, w.Issue); err != nil {
			return err
		}
	}
	if err = c.Store.Put(context.WithoutCancel(ctx), "parent_completion", issueKey(w), w.ID, map[string]any{"scope_revision": s.Revision, "candidate": candidate, "results": results, "closed_at": time.Now().UTC()}); err != nil {
		return err
	}
	state.State, state.Reason, state.RequiredAction, state.Classification = "completed", "", "", ""
	state.Failure = nil
	return c.saveFinal(context.WithoutCancel(ctx), w, state)
}

// RetryFinal resumes only an explicitly reconciled final lifecycle. A failed
// assertion is assessed in a new WorkItem; infrastructure retries consume the
// same retained counter and never reopen a merged PR.
func (c *Controller) RetryFinal(ctx context.Context, id, evidence, reason string) error {
	if evidence == "" || reason == "" {
		return errors.New("final recovery requires evidence and reason")
	}
	w, err := c.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	lock, err := c.splitLease("final:" + issueKey(w))
	if err != nil {
		return err
	}
	defer lock.Close()
	var state FinalAcceptanceState
	if err = c.Store.Record(ctx, "parent_final_state", issueKey(w), &state); err != nil {
		return err
	}
	if state.WorkItemID != w.ID || state.State != "blocked" || state.ScopeRevision != w.ScopeRevision || !w.Terminal() || w.Phase == domain.Canceled {
		return errors.New("work item does not own a blocked final acceptance")
	}
	scope, err := c.current(ctx, w)
	if err != nil {
		return err
	}
	busy, err := c.runnerBusy(ctx, "")
	if err != nil {
		return err
	}
	if busy {
		return errors.New("runner must complete or be reconciled with quiescence evidence first")
	}
	if state.Classification == "requires_assessment" {
		return c.recoverFinalAssertion(ctx, w, scope, evidence, reason)
	}
	retryJob := true
	switch state.Classification {
	case "hold", "budget", "isolation", "scope_or_policy_conflict":
		retryJob = false
	}
	if retryJob {
		if state.Retries >= c.Config.Limits.RunnerRetries {
			return errors.New("final runner retry budget exhausted")
		}
		state.Retries++
		state.Attempt++
	}
	state.State = "waiting"
	state.Reason = reason
	state.RequiredAction = ""
	state.Failure = nil
	if err = c.Store.Put(ctx, "operator_audit", fmt.Sprintf("retry-final:%s:%d", issueKey(w), time.Now().UnixNano()), w.ID, map[string]string{"evidence": evidence, "reason": reason}); err != nil {
		return err
	}
	return c.saveFinal(ctx, w, state)
}

func (c *Controller) startFinalAssessment(ctx context.Context, w domain.WorkItem, failure runner.Result) error {
	if _, err := c.current(ctx, w); err != nil {
		return c.block(ctx, w, err.Error())
	}
	if failure.Candidate.ScopeRevision != w.ScopeRevision || failure.Candidate.PolicyRevision != w.PolicyRevision {
		return c.block(ctx, w, "final failure revisions changed; adopt fresh recovery scope")
	}
	w, base, err := c.workspace(ctx, w)
	if err != nil {
		return err
	}
	if base != failure.Candidate.SHA {
		return c.block(ctx, w, "final recovery checkout does not match failed SHA")
	}
	candidate, err := c.freeze(ctx, w, base)
	if err != nil {
		return err
	}
	e := domain.Event{ID: "final-assessment:" + w.ID, WorkItemID: w.ID, Kind: "start_assessment", ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, Attempt: w.Attempt, Ref: candidate.ID, Reason: "classify final acceptance failure"}
	return c.Store.EventRecords(ctx, e, map[string]map[string]any{"correction": {w.ID: Correction{Mode: "assessment", Reason: "classify final acceptance failure", EvidenceIDs: []string{failure.JobID}, FeedbackIDs: []string{}}}})
}

func (c *Controller) finalBlocked(ctx context.Context, w domain.WorkItem) (bool, error) {
	lock, err := c.splitLease("final:" + issueKey(w))
	if errors.Is(err, ErrSplitBusy) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer lock.Close()
	var state FinalAcceptanceState
	if err = c.Store.Record(ctx, "parent_final_state", issueKey(w), &state); missing(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if state.RecoveryID != "" && state.State != "completed" && state.State != "blocked" {
		owner, err := c.Store.Get(ctx, state.WorkItemID)
		if err != nil {
			return false, err
		}
		if owner.Phase == domain.Canceled {
			if err = c.blockFinal(ctx, owner, state, "canceled_recovery", "final recovery was canceled: "+owner.Reason, "inspect canceled recovery; issue claim and merged history are retained"); err != nil {
				return false, err
			}
			state.State = "blocked"
		}
	}
	return state.ScopeRevision >= w.ScopeRevision && state.State == "blocked", nil
}

func (c *Controller) recoverFinalAssertion(ctx context.Context, w domain.WorkItem, scope verification.Scope, evidence, reason string) error {
	var state FinalAcceptanceState
	if err := c.Store.Record(ctx, "parent_final_state", issueKey(w), &state); err != nil {
		return err
	}
	if state.WorkItemID != w.ID || state.State != "blocked" || state.Classification != "requires_assessment" {
		return errors.New("final assertion recovery state changed")
	}
	var err error
	if state.Failure == nil {
		return errors.New("failed assertion evidence is missing")
	}
	if w.RepairRounds >= c.Config.Limits.RepairRounds {
		return errors.New("repair budget exhausted")
	}
	next := domain.WorkItem{ID: fmt.Sprintf("%s-final-repair-%d", issueKey(w), state.Attempt+1), RepositoryID: w.RepositoryID, Issue: w.Issue, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, Phase: domain.Ready, RepairRounds: w.RepairRounds + 1}
	full := scope
	full.Included = []string{}
	for _, criterion := range scope.Criteria {
		full.Included = append(full.Included, criterion.ID)
	}
	full.Remaining = []string{}
	full.PreviouslyMerged = []string{}
	var source Source
	if err = c.Store.Record(ctx, "source", w.ID, &source); err != nil {
		return err
	}
	jobs, err := c.jobAttempts(ctx, w.ID)
	if err != nil {
		return err
	}
	metrics, err := c.Metrics(ctx, w.ID)
	if err != nil {
		return err
	}
	updated := state
	updated.State = "repairing"
	updated.WorkItemID = next.ID
	updated.RecoveryID = next.ID
	updated.Attempt++
	updated.Reason = reason
	updated.RequiredAction = "inspect recovery work item " + next.ID
	updated.UpdatedAt = time.Now().UTC()
	records := map[string]any{"scope": full, "source": source, "policy_snapshot": c.policyDigest(), "workspace_base": state.Failure.Candidate.SHA, "final_failure": *state.Failure, "budget_baseline": BudgetBaseline{ValidationJobs: jobs, ObservedTokens: metrics["observed_tokens"].(int64)}, "final_recovery_audit": map[string]string{"previous": w.ID, "evidence": evidence, "reason": reason}}
	return c.Store.ClaimFinalRepair(ctx, w.ID, next, records, issueKey(w), state, updated)
}

func (c *Controller) finalOrphans(ctx context.Context, w domain.WorkItem, state FinalAcceptanceState) (bool, error) {
	raw, err := c.Store.Records(ctx, "job_intent", w.ID)
	if err != nil {
		return false, err
	}
	for _, data := range raw {
		var intent JobIntent
		if err = json.Unmarshal(data, &intent); err != nil {
			return false, err
		}
		if !strings.HasPrefix(intent.Spec.Candidate.ID, "final-") {
			continue
		}
		if _, err = c.Jobs.Read(intent.Spec); !os.IsNotExist(err) {
			if err != nil {
				return false, err
			}
			continue
		}
		reason := ""
		if api, ok := c.Jobs.(JobReconciler); ok {
			life, err := api.Inspect(ctx, intent.Spec)
			if err != nil {
				return false, err
			}
			if life.State == "not_started" || life.State == "stopped" || life.State == "unknown" {
				reason = "old final runner " + life.State
			}
		}
		if reason == "" && time.Since(intent.Started) > time.Duration(intent.Spec.Profile.TimeoutSeconds+300)*time.Second {
			reason = "old final runner result unknown"
		}
		if reason != "" {
			return true, c.blockFinal(ctx, w, state, "unknown", reason, "reconcile-job with quiescence evidence before retry-final")
		}
	}
	return false, nil
}

// A continuation of a final repair is part of the same recovery budget, even
// though its PR and remaining criteria belong to a separate WorkItem.
func (c *Controller) claimContinuation(ctx context.Context, previous, next domain.WorkItem, records map[string]any) error {
	var state FinalAcceptanceState
	if err := c.Store.Record(ctx, "parent_final_state", issueKey(previous), &state); err == nil {
		if state.WorkItemID == previous.ID && state.RecoveryID != "" {
			next.RepairRounds = previous.RepairRounds
			jobs, err := c.jobAttempts(ctx, previous.ID)
			if err != nil {
				return err
			}
			metrics, err := c.Metrics(ctx, previous.ID)
			if err != nil {
				return err
			}
			records["budget_baseline"] = BudgetBaseline{ValidationJobs: jobs, ObservedTokens: metrics["observed_tokens"].(int64)}
		}
	} else if !missing(err) {
		return err
	}
	return c.Store.ClaimNextRecords(ctx, previous.ID, next, records)
}
