package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
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
