package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

type failingFinalJobs struct {
	*fakeJobs
	status                string
	remaining, finalReads int
	when                  func() bool
}

func (j *failingFinalJobs) Start(ctx context.Context, s runner.Spec) error {
	if err := j.fakeJobs.Start(ctx, s); err != nil {
		return err
	}
	if strings.HasPrefix(s.Candidate.ID, "final-") && j.remaining > 0 && (j.when == nil || j.when()) {
		j.remaining--
		r := j.results[s.JobID]
		r.Status = j.status
		r.Reason = "final profile diagnostic"
		code := 1
		r.ExitCode = &code
		j.results[s.JobID] = r
	}
	return nil
}
func (j *failingFinalJobs) Read(s runner.Spec) (runner.Result, error) {
	if strings.HasPrefix(s.Candidate.ID, "final-") {
		j.finalReads++
	}
	return j.fakeJobs.Read(s)
}
func finalState(t *testing.T, c *Controller) FinalAcceptanceState {
	t.Helper()
	var state FinalAcceptanceState
	if err := c.Store.Record(context.Background(), "parent_final_state", "r1-i1", &state); err != nil {
		t.Fatal(err)
	}
	return state
}
func TestFinalInfrastructureAndTimeoutRecoveryIsBounded(t *testing.T) {
	for _, status := range []string{"infrastructure_failure", "timeout"} {
		t.Run(status, func(t *testing.T) {
			c, gh, agents, fake := setup(t)
			ctx := context.Background()
			jobs := &failingFinalJobs{fakeJobs: fake, status: status, remaining: 2}
			c.Jobs = jobs
			drive(t, c, 22)
			state := finalState(t, c)
			w, _ := c.Store.Get(ctx, "w")
			if state.State != "blocked" || state.Classification != status || state.Failure == nil || state.RequiredAction == "" || w.Phase != domain.Merged || gh.closed != 0 {
				t.Fatal(state, w, gh.closed)
			}
			reads, calls := jobs.finalReads, agents.calls
			poolCalls := gh.poolCalls
			drive(t, c, 8)
			if gh.poolCalls <= poolCalls {
				t.Fatal("blocked terminal final prevented new issue acquisition")
			}
			if jobs.finalReads != reads || agents.calls != calls {
				t.Fatal("blocked final kept polling or starting models", jobs.finalReads, reads, agents.calls)
			}
			if err := c.RetryFinal(ctx, w.ID, "audit:environment-repaired", "retry the final profile"); err != nil {
				t.Fatal(err)
			}
			drive(t, c, 12)
			state = finalState(t, c)
			if state.State != "blocked" || state.Retries != 1 || state.Attempt != 1 || state.Failure.JobID == "" {
				t.Fatal(state)
			}
			if err := c.RetryFinal(ctx, w.ID, "audit:again", "budget cannot reset"); err == nil {
				t.Fatal("retry budget reset")
			}
			if gh.merges != 1 || gh.closed != 0 || agents.calls != 2 || fake.calls != 3 {
				t.Fatal(gh.merges, gh.closed, agents.calls, fake.calls)
			}
		})
	}
}
func TestFinalInfrastructureRecoveryCanCloseIssue(t *testing.T) {
	c, gh, agents, fake := setup(t)
	ctx := context.Background()
	jobs := &failingFinalJobs{fakeJobs: fake, status: "infrastructure_failure", remaining: 1}
	c.Jobs = jobs
	drive(t, c, 20)
	before := finalState(t, c)
	if err := c.RetryFinal(ctx, "w", "audit:fixed", "infrastructure restored"); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 18)
	after := finalState(t, c)
	if after.State != "completed" || after.Attempt != before.Attempt+1 || gh.closed != 1 || gh.merges != 1 || agents.calls != 2 {
		t.Fatal(after, gh.closed, gh.merges, agents.calls)
	}
	records, _ := c.Store.Records(ctx, "validation", "w")
	if len(records) != 3 {
		t.Fatal("failed evidence was overwritten", len(records))
	}
}
func TestFinalFailureCreatesIndependentRepairWithoutReopeningMergedPR(t *testing.T) {
	for _, classification := range []string{"implementation_defect", "test_defect"} {
		t.Run(classification, func(t *testing.T) {
			c, gh, agents, fake := setup(t)
			ctx := context.Background()
			agents.classification = classification
			c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1}
			drive(t, c, 50)
			original, _ := c.Store.Get(ctx, "w")
			state := finalState(t, c)
			repair, err := c.Store.Get(ctx, state.RecoveryID)
			if err != nil || original.Phase != domain.Merged || repair.Phase != domain.Merged || state.State != "completed" || gh.merges != 2 || gh.closed != 1 {
				t.Fatal(original, repair, state, gh.merges, gh.closed, err)
			}
			audit, _ := c.Store.Records(ctx, "assessment", repair.ID)
			if len(audit) != 1 {
				t.Fatal("missing independent assessment", len(audit))
			}
			var a verification.Assessment
			json.Unmarshal(audit[0], &a)
			if a.Classification != classification || len(a.EvidenceIDs) != 1 {
				t.Fatal(a)
			}
			tokens, err := c.Metrics(ctx, repair.ID)
			if err != nil || tokens["observed_tokens"].(int64) < 40 {
				t.Fatal(tokens, err)
			}
			jobs, err := c.jobAttempts(ctx, repair.ID)
			if err != nil || jobs != 4 {
				t.Fatal("validation budget reset", jobs, err)
			}
			foundFailure := false
			for _, prompt := range agents.prompts {
				if strings.Contains(prompt, "final_acceptance_failure") && strings.Contains(prompt, "final profile diagnostic") {
					foundFailure = true
				}
			}
			if !foundFailure {
				t.Fatal("assessment did not receive final evidence")
			}
			expectedCalls := 5
			if classification == "test_defect" {
				expectedCalls = 4
			}
			if agents.calls != expectedCalls {
				t.Fatal("repair dispatched wrong role", classification, agents.calls)
			}
		})
	}
}
func TestSplitParentFinalFailureCanCreateRepairSuccessor(t *testing.T) {
	c, gh, agents, fake := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	scope.Included = []string{"AC1", "AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	agents.autoSplit = true
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1, when: func() bool { return gh.closed == 2 }}
	drive(t, c, 90)
	root, _ := c.Store.Get(ctx, "w")
	state := finalState(t, c)
	if root.Phase != domain.Split || state.State != "completed" || state.RecoveryID == "" || gh.merges != 3 || gh.closed != 3 {
		t.Fatal(root, state, gh.merges, gh.closed)
	}
}
func TestFinalUnknownLaunchRequiresQuiescenceBeforeRetry(t *testing.T) {
	c, gh, agents, fake := setup(t)
	ctx := context.Background()
	for range 20 {
		drive(t, c, 1)
		w, _ := c.Store.Get(ctx, "w")
		if w.Phase == domain.Merged {
			break
		}
	}
	jobs := &recoverableJobs{FileJobs: FileJobs{Executable: "/missing-runner-executable"}, fake: fake}
	c.Jobs = jobs
	drive(t, c, 2)
	state := finalState(t, c)
	if state.State != "blocked" || state.Classification != "unknown" {
		t.Fatal(state)
	}
	if err := c.RetryFinal(ctx, "w", "audit:unproven", "do not duplicate runner"); err == nil {
		t.Fatal("unresolved intent retried")
	}
	raw, _ := c.Store.Records(ctx, "job_intent", "w")
	var job string
	for _, data := range raw {
		var intent JobIntent
		json.Unmarshal(data, &intent)
		if strings.HasPrefix(intent.Spec.Candidate.ID, "final-") {
			job = intent.Spec.JobID
		}
	}
	if job == "" {
		t.Fatal("missing final intent")
	}
	if err := c.ResolveJob(ctx, "w", job, "not_started", "audit:no-process", "launch was never started"); err != nil {
		t.Fatal(err)
	}
	jobs.allow = true
	if err := c.RetryFinal(ctx, "w", "audit:launcher-fixed", "use a new job identity"); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 16)
	state = finalState(t, c)
	if state.State != "completed" || state.Attempt != 1 || gh.closed != 1 || agents.calls != 2 {
		t.Fatal(state, gh.closed, agents.calls)
	}
}

type splitRepairAgent struct{ *fakeAgents }

func (a splitRepairAgent) Run(ctx context.Context, w domain.WorkItem, op domain.Operation, input string, schema any) (worker.Run, error) {
	r, err := a.fakeAgents.Run(ctx, w, op, input, schema)
	if strings.Contains(w.ID, "final-repair") && op.Kind == "implement" {
		// Restore the assessment checkpoint; request decomposition without making a fix.
		workspace.Git(ctx, w.Worktree, "reset", "--hard", "HEAD")
		body, _ := json.Marshal(worker.Handoff{SchemaVersion: 1, WorkItemID: w.ID, ScopeRevision: w.ScopeRevision, InputCandidateID: w.Candidate, Outcome: "split", Summary: "needs decomposition", CoveredCriteria: []string{}, RemainingWork: []string{"fix"}, RequestedProfiles: []string{}, Blockers: []string{}})
		r.Final = string(body)
	}
	return r, err
}
func TestRepairSplitMustWaitForChildren(t *testing.T) {
	c, gh, a, fake := setup(t)
	var initial verification.Scope
	c.Store.Record(context.Background(), "scope", "w", &initial)
	initial.Criteria = append(initial.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	initial.Included = []string{"AC1", "AC2"}
	c.Store.Put(context.Background(), "scope", "w", "w", initial)
	c.Agents = splitRepairAgent{a}
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1}
	for i := 0; i < 40; i++ {
		drive(t, c, 1)
		stateErr := func() bool {
			var s FinalAcceptanceState
			if c.Store.Record(context.Background(), "parent_final_state", "r1-i1", &s) != nil || s.RecoveryID == "" {
				return false
			}
			w, _ := c.Store.Get(context.Background(), s.RecoveryID)
			return w.Phase == domain.Split
		}()
		if stateErr {
			break
		}
	}
	// Call the parent's acceptance check before giving any split child a turn.
	s := finalState(t, c)
	w, _ := c.Store.Get(context.Background(), s.RecoveryID)
	if w.Phase != domain.Split {
		t.Fatal("probe did not split", w)
	}
	var scope verification.Scope
	c.Store.Record(context.Background(), "scope", w.ID, &scope)
	if err := c.finalAcceptance(context.Background(), w, scope); err != nil {
		t.Fatal(err)
	}
	if gh.issue.State == "closed" {
		t.Fatal("parent closed while repair children are still READY; merges:", gh.merges)
	}
}
func TestAdoptFinalRepairRetiresFailure(t *testing.T) {
	c, gh, a, fake := setup(t)
	a.classification = "spec_ambiguity"
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1}
	drive(t, c, 25)
	ctx := context.Background()
	state := finalState(t, c)
	w, _ := c.Store.Get(ctx, state.RecoveryID)
	if w.Phase != domain.Blocked {
		t.Fatal("probe not blocked", w)
	}
	var scope verification.Scope
	c.Store.Record(ctx, "scope", w.ID, &scope)
	scope.Revision++
	gh.issue.Body = "clarified requirements"
	scope.OriginalRequest = gh.issue.Body
	if err := c.Store.Adopt(ctx, w.ID, scope.Revision, w.PolicyRevision, map[string]any{"scope": scope, "source": Source{digest(gh.issue)}, "policy_snapshot": c.policyDigest()}); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 1)
	w, _ = c.Store.Get(ctx, w.ID)
	if w.Phase == domain.Blocked {
		t.Fatal("adopted final repair cannot start:", w.Reason)
	}
	drive(t, c, 40)
	if state = finalState(t, c); state.State != "completed" || state.ScopeRevision != scope.Revision || gh.closed != 1 {
		t.Fatal(state, gh.closed)
	}
	var stale runner.Result
	if err := c.Store.Record(ctx, "final_failure", w.ID, &stale); !missing(err) {
		t.Fatal("adoption retained active failure", err)
	}
	history, err := c.Store.Records(ctx, "final_failure_history", w.ID)
	if err != nil || len(history) != 1 {
		t.Fatal(history, err)
	}
}
func TestCancelFinalRepairAllowsPool(t *testing.T) {
	c, gh, _, fake := setup(t)
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1}
	ctx := context.Background()
	var state FinalAcceptanceState
	for i := 0; i < 30; i++ {
		drive(t, c, 1)
		c.Store.Record(ctx, "parent_final_state", "r1-i1", &state)
		if state.RecoveryID != "" {
			break
		}
	}
	if state.RecoveryID == "" {
		t.Fatal("missing repair")
	}
	if err := c.Store.Apply(ctx, domain.Event{ID: "cancel-repair", WorkItemID: state.RecoveryID, Kind: "cancel", Reason: "operator abandons repair"}); err != nil {
		t.Fatal(err)
	}
	before := gh.poolCalls
	drive(t, c, 8)
	if gh.poolCalls == before {
		t.Fatal("canceled repair permanently prevents unrelated issue acquisition")
	}
}

func TestFinalCompletionReconcilesInterruptedLifecycleWrite(t *testing.T) {
	c, gh, _, fake := setup(t)
	ctx := context.Background()
	drive(t, c, 24)
	state := finalState(t, c)
	state.State = "running"
	if err := c.saveFinal(ctx, domain.WorkItem{RepositoryID: 1, Issue: 1}, state); err != nil {
		t.Fatal(err)
	}
	calls := fake.calls
	drive(t, c, 2)
	state = finalState(t, c)
	if state.State != "completed" || fake.calls != calls || gh.closed != 1 {
		t.Fatal(state, fake.calls, calls, gh.closed)
	}
}

func TestFinalRepairPartialContinuationTransfersFinalOwnership(t *testing.T) {
	c, gh, _, fake := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	scope.Included = []string{"AC1", "AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1}
	var state FinalAcceptanceState
	for range 25 {
		drive(t, c, 1)
		c.Store.Record(ctx, "parent_final_state", "r1-i1", &state)
		if state.RecoveryID != "" {
			break
		}
	}
	repair, err := c.Store.Get(ctx, state.RecoveryID)
	if err != nil {
		t.Fatal(err)
	}
	c.Store.Record(ctx, "scope", repair.ID, &scope)
	scope.Revision++
	scope.Included = []string{"AC1"}
	scope.Remaining = []string{"AC2"}
	if err = c.Store.Adopt(ctx, repair.ID, scope.Revision, repair.PolicyRevision, map[string]any{"scope": scope, "source": Source{digest(gh.issue)}, "policy_snapshot": c.policyDigest()}); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 65)
	state = finalState(t, c)
	if state.State != "completed" || state.WorkItemID != repair.ID+"-remaining" || gh.merges != 3 || gh.closed != 1 {
		t.Fatal(state, gh.merges, gh.closed)
	}
	next, _ := c.Store.Get(ctx, state.WorkItemID)
	previous, _ := c.Store.Get(ctx, repair.ID)
	var baseline BudgetBaseline
	if err = c.Store.Record(ctx, "budget_baseline", next.ID, &baseline); err != nil || baseline.ValidationJobs != 3 || baseline.ObservedTokens < 20 || next.RepairRounds != previous.RepairRounds || next.RepairRounds < 1 {
		t.Fatal("continuation reset recovery budgets", baseline, next, previous, err)
	}
	jobs, err := c.jobAttempts(ctx, next.ID)
	if err != nil || jobs != 5 {
		t.Fatal("cumulative validation count", jobs, err)
	}
}

func TestFinalAdministrativeHoldDoesNotSpendRunnerRetry(t *testing.T) {
	c, gh, agents, fake := setup(t)
	ctx := context.Background()
	c.Config.Limits.RunnerRetries = 0
	c.Store.Put(ctx, "policy_snapshot", "w", "w", c.policyDigest())
	for range 20 {
		drive(t, c, 1)
		w, _ := c.Store.Get(ctx, "w")
		if w.Phase == domain.Merged {
			break
		}
	}
	if err := c.Store.Apply(ctx, domain.Event{ID: "hold-final", WorkItemID: "w", Kind: "hold"}); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 3)
	state := finalState(t, c)
	if state.State != "blocked" || state.Classification != "hold" || fake.calls != 1 {
		t.Fatal(state, fake.calls)
	}
	if err := c.Store.Apply(ctx, domain.Event{ID: "release-final", WorkItemID: "w", Kind: "release"}); err != nil {
		t.Fatal(err)
	}
	if err := c.RetryFinal(ctx, "w", "audit:hold-released", "resume the first final run"); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 12)
	state = finalState(t, c)
	if state.State != "completed" || state.Retries != 0 || state.Attempt != 0 || gh.closed != 1 || fake.calls != 2 || agents.calls != 2 {
		t.Fatal(state, gh.closed, fake.calls, agents.calls)
	}
}

func TestSingleFinalRepairRoundAllowsCorrection(t *testing.T) {
	for _, classification := range []string{"implementation_defect", "test_defect"} {
		t.Run(classification, func(t *testing.T) {
			c, gh, agents, fake := setup(t)
			ctx := context.Background()
			c.Config.Limits.RepairRounds = 1
			c.Store.Put(ctx, "policy_snapshot", "w", "w", c.policyDigest())
			agents.classification = classification
			c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1}
			drive(t, c, 45)
			state := finalState(t, c)
			repair, _ := c.Store.Get(ctx, state.WorkItemID)
			if state.State != "completed" || repair.RepairRounds != 1 || gh.merges != 2 || gh.closed != 1 {
				t.Fatal(state, repair, gh.merges, gh.closed)
			}
		})
	}
}

func TestFinalRepairSplitCompletes(t *testing.T) {
	c, gh, a, fake := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	scope.Included = []string{"AC1", "AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	c.Agents = rootRepairSplitAgent{a}
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1}
	drive(t, c, 100)
	state := finalState(t, c)
	if state.State != "completed" {
		items, _ := c.Store.List(ctx)
		t.Fatalf("state=%+v items=%+v merges=%d closed=%d", state, items, gh.merges, gh.closed)
	}
}

func TestBlockedSplitChildFinalAllowsPool(t *testing.T) {
	c, gh, a, fake := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	scope.Included = []string{"AC1", "AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	a.autoSplit = true
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "infrastructure_failure", remaining: 1}
	drive(t, c, 70)
	if gh.poolCalls == 0 {
		items, _ := c.Store.List(ctx)
		states, _ := c.Store.Records(ctx, "parent_final_state", "")
		t.Fatalf("pool never checked items=%+v final=%s", items, states)
	}
	if err := c.RetryFinal(ctx, "w-part0", "incident:environment-fixed", "restore descendant final acceptance"); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 25)
	if state := finalState(t, c); state.State != "completed" || gh.issue.State != "closed" {
		t.Fatal("ancestor did not resume after descendant recovery", state, gh.issue.State)
	}
}

type rootRepairSplitAgent struct{ *fakeAgents }

func (a rootRepairSplitAgent) Run(ctx context.Context, w domain.WorkItem, op domain.Operation, input string, schema any) (worker.Run, error) {
	if w.ID == "r1-i1-final-repair-1" && op.Kind == "implement" {
		return (splitRepairAgent{a.fakeAgents}).Run(ctx, w, op, input, schema)
	}
	return a.fakeAgents.Run(ctx, w, op, input, schema)
}

func TestAdoptChildRepairCompletesAncestor(t *testing.T) {
	c, gh, a, fake := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	scope.Included = []string{"AC1", "AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	a.autoSplit = true
	a.classification = "spec_ambiguity"
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1, when: func() bool { return gh.closed == 1 }}
	drive(t, c, 45)
	var state FinalAcceptanceState
	if err := c.Store.Record(ctx, "parent_final_state", "r1-i101", &state); err != nil {
		t.Fatal(err)
	}
	repair, err := c.Store.Get(ctx, state.RecoveryID)
	if err != nil || repair.Phase != domain.Blocked {
		t.Fatal(state, repair, err)
	}
	c.Store.Record(ctx, "scope", repair.ID, &scope)
	scope.Revision++
	issue := gh.children[101]
	issue.Body += "\nclarified requirements"
	gh.children[101] = issue
	if err = c.Store.Adopt(ctx, repair.ID, scope.Revision, repair.PolicyRevision, map[string]any{"scope": scope, "source": Source{digest(issue)}, "policy_snapshot": c.policyDigest()}); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 60)
	c.Store.Record(ctx, "parent_final_state", "r1-i101", &state)
	if state.State != "completed" {
		t.Fatal("repair failed", state)
	}
	if root := finalState(t, c); root.State != "completed" {
		t.Fatalf("ancestor stuck after successful child repair adoption: %+v", root)
	}
	dependent := domain.WorkItem{ID: "dependent", RepositoryID: 1, Issue: 999, ScopeRevision: 1, PolicyRevision: 1, Phase: domain.Ready}
	if err = c.Store.ClaimRecords(ctx, dependent, map[string]any{"dependencies": []string{"w"}}); err != nil {
		t.Fatal(err)
	}
	if ready, err := c.Store.DependenciesReady(ctx, dependent.ID); err != nil || !ready {
		t.Fatal("adopted descendant did not release dependency", ready, err)
	}
	var audit map[string]string
	c.Store.Record(ctx, "final_recovery_audit", repair.ID, &audit)
	previous := audit["previous"]
	audit["previous"] = "w-part0"
	c.Store.Put(ctx, "final_recovery_audit", repair.ID, repair.ID, audit)
	root, _ := c.Store.Get(ctx, "w")
	if complete, err := c.splitComplete(ctx, root, map[string]bool{}); err != nil || complete {
		t.Fatal("unrelated revision accepted as a descendant repair", complete, err)
	}
	if ready, err := c.Store.DependenciesReady(ctx, dependent.ID); err != nil || ready {
		t.Fatal("unrelated revision released dependency", ready, err)
	}
	audit["previous"] = previous
	c.Store.Put(ctx, "final_recovery_audit", repair.ID, repair.ID, audit)
}

type scriptedFinalJobs struct {
	*fakeJobs
	statuses []string
}

func (j *scriptedFinalJobs) Start(ctx context.Context, s runner.Spec) error {
	if err := j.fakeJobs.Start(ctx, s); err != nil {
		return err
	}
	if strings.HasPrefix(s.Candidate.ID, "final-") && len(j.statuses) > 0 {
		r := j.results[s.JobID]
		r.Status = j.statuses[0]
		j.statuses = j.statuses[1:]
		code := 1
		r.ExitCode = &code
		r.Reason = "diagnostic"
		j.results[s.JobID] = r
	}
	return nil
}
func TestBlockedRecoverySplitAllowsPool(t *testing.T) {
	c, gh, a, fake := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	scope.Included = []string{"AC1", "AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	c.Agents = rootRepairSplitAgent{a}
	c.Jobs = &scriptedFinalJobs{fakeJobs: fake, statuses: []string{"failed", "infrastructure_failure"}}
	drive(t, c, 100)
	if gh.poolCalls == 0 {
		items, _ := c.Store.List(ctx)
		states, _ := c.Store.Records(ctx, "parent_final_state", "")
		t.Fatalf("pool blocked: items=%+v states=%s", items, states)
	}
}

func TestFinalRepairRetainsSplitDepth(t *testing.T) {
	c, _, a, fake := setup(t)
	ctx := context.Background()
	c.Store.Put(ctx, "split_depth", "w", "w", c.Config.Limits.SplitDepth)
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1}
	a.classification = "spec_ambiguity"
	drive(t, c, 25)
	state := finalState(t, c)
	var depth int
	err := c.Store.Record(ctx, "split_depth", state.RecoveryID, &depth)
	if err != nil || depth != c.Config.Limits.SplitDepth {
		t.Fatalf("depth reset: %d %v", depth, err)
	}
}

func TestFinalRepairCannotSplitPastInheritedLimit(t *testing.T) {
	c, gh, agents, fake := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	scope.Included = []string{"AC1", "AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	c.Store.Put(ctx, "split_depth", "w", "w", c.Config.Limits.SplitDepth)
	c.Agents = rootRepairSplitAgent{agents}
	c.Jobs = &failingFinalJobs{fakeJobs: fake, status: "failed", remaining: 1}
	drive(t, c, 45)
	state := finalState(t, c)
	repair, _ := c.Store.Get(ctx, state.RecoveryID)
	if repair.Phase != domain.Blocked || !strings.Contains(repair.Reason, "limits") || len(gh.children) != 0 || gh.merges != 1 || gh.closed != 0 {
		t.Fatal(repair, gh.children, gh.merges, gh.closed)
	}
}
