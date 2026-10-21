package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/policy"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeGH struct {
	requestIDs               map[string]int64
	children                 map[int]github.Issue
	posts                    int
	controller               *Controller
	issue                    github.Issue
	pr                       github.PR
	requests, merges, closed int
	requestTime              time.Time
	approved                 bool
	checkResults             []github.Check
	ciFailureSHA             string
	ciFailFirst              bool
	diagnosticsMissing       bool
	onDiagnostics            func()
	reviewFinding            bool
}

func (f *fakeGH) GetIssue(_ context.Context, n int) (github.Issue, error) {
	if n == 1 {
		return f.issue, nil
	}
	issue, ok := f.children[n]
	if !ok {
		return issue, fmt.Errorf("unknown issue %d", n)
	}
	return issue, nil
}
func (f *fakeGH) Pool(context.Context) ([]github.Issue, error) { return nil, nil }
func (f *fakeGH) Pull(context.Context, int) (github.PR, error) { return f.pr, nil }
func (f *fakeGH) FindPR(context.Context, string, string) (*github.PR, error) {
	if f.pr.Number == 0 {
		return nil, nil
	}
	p := f.pr
	return &p, nil
}
func (f *fakeGH) Publish(ctx context.Context, base, branch, id, title, body string) (github.PR, error) {
	w, _ := f.controller.Store.Get(ctx, id)
	c, _ := f.controller.candidate(ctx, w)
	f.posts++
	f.pr = github.PR{Number: 9 + f.posts, State: "open", Body: body}
	f.pr.Head.SHA = c.SHA
	f.pr.Head.Ref = branch
	f.pr.Head.Repo.ID = 1
	f.pr.Base.SHA = c.BaseSHA
	f.pr.Base.Ref = base
	f.pr.Base.Repo.ID = 1
	return f.pr, nil
}
func (f *fakeGH) Reviews(context.Context, int) ([]github.Review, error) {
	if f.reviewFinding {
		return []github.Review{{ID: 99, User: github.User{ID: 7}, State: "CHANGES_REQUESTED", CommitID: f.pr.Head.SHA, SubmittedAt: f.requestTime.Add(time.Second), Body: "Fix the implementation edge case"}}, nil
	}

	if !f.approved {
		return nil, nil
	}
	return []github.Review{{ID: 1, User: github.User{ID: 7}, State: "APPROVED", CommitID: f.pr.Head.SHA, SubmittedAt: f.requestTime.Add(time.Second)}}, nil
}
func (f *fakeGH) Comments(context.Context, int) ([]github.Comment, error) { return nil, nil }
func (f *fakeGH) RequestReview(_ context.Context, _ int, round, _ string, _ int64) (int64, error) {
	f.requests++
	f.requestTime = time.Now()
	if f.requestIDs == nil {
		f.requestIDs = map[string]int64{}
	}
	f.requestIDs[round] = int64(49 + f.requests)
	return f.requestIDs[round], nil
}
func (f *fakeGH) FindReviewRequest(_ context.Context, _ int, round string, _ int64) (int64, error) {
	return f.requestIDs[round], nil
}
func (f *fakeGH) Merge(ctx context.Context, _ int, head, method string) error {
	if head != f.pr.Head.SHA {
		return fmt.Errorf("wrong SHA")
	}
	f.merges++
	repo := f.controller.Config.RepositoryPath
	for _, a := range [][]string{{"merge", "--ff-only", f.pr.Head.Ref}, {"push", "origin", "trunk"}} {
		if _, err := workspace.Git(ctx, repo, a...); err != nil {
			return err
		}
	}
	f.pr.Merged = true
	f.pr.State = "closed"
	return nil
}
func (f *fakeGH) Checks(_ context.Context, target string) ([]github.Check, error) {
	if f.ciFailFirst {
		if f.ciFailureSHA == "" {
			f.ciFailureSHA = target
		}
		check := github.Check{ID: 100, Name: "required", HeadSHA: target, Status: "completed", Conclusion: "success", DetailsURL: "https://github.com/owner/repo/actions/runs/100"}
		check.App.ID = 9
		if target == f.ciFailureSHA {
			check.Conclusion = "failure"
			check.Output.Summary = "acceptance assertion failed"
		}
		return []github.Check{check}, nil
	}
	return f.checkResults, nil
}
func (f *fakeGH) Conditions(context.Context, int) (string, error) { return "CLEAN", nil }
func (f *fakeGH) FindChild(_ context.Context, marker string) (int, error) {
	for n, issue := range f.children {
		if strings.Contains(issue.Body, "<!-- "+marker+" -->") {
			return n, nil
		}
	}
	return 0, nil
}
func (f *fakeGH) CreateChild(_ context.Context, title, body, marker string) (int, error) {
	if f.children == nil {
		f.children = map[int]github.Issue{}
	}
	n := 100 + len(f.children)
	f.children[n] = github.Issue{Number: n, Title: title, Body: "<!-- " + marker + " -->\n" + body, State: "open"}
	return n, nil
}
func (f *fakeGH) CloseIssue(_ context.Context, n int) error {
	f.closed++
	if n == 1 {
		f.issue.State = "closed"
	} else {
		issue := f.children[n]
		issue.State = "closed"
		f.children[n] = issue
	}
	return nil
}

type fakeAgents struct {
	totals         map[string]int
	calls          int
	prompts        []string
	classification string
	autoSplit      bool
	partialChild   bool
	interim        bool
}

func (f *fakeAgents) Run(_ context.Context, w domain.WorkItem, op domain.Operation, input string, _ any) (worker.Run, error) {
	f.calls++
	f.prompts = append(f.prompts, input)
	var in struct {
		Scope      verification.Scope `json:"scope"`
		Correction Correction         `json:"correction"`
	}
	json.Unmarshal([]byte(input), &in)
	var output any
	if f.autoSplit && w.ID == "w" && op.Kind == "implement" {
		output = worker.Handoff{SchemaVersion: 1, WorkItemID: w.ID, ScopeRevision: w.ScopeRevision, InputCandidateID: w.Candidate, Outcome: "split", Summary: "needs decomposition", CoveredCriteria: []string{}, RemainingWork: []string{"two independent criteria"}, RequestedProfiles: []string{}, Blockers: []string{}}
	} else if op.Kind == "propose_split" {
		children := []splitting.Child{}
		for i, criterion := range in.Scope.Criteria {
			deps := []string{}
			if i > 0 {
				deps = append(deps, "part0")
			}
			childID := "child-" + criterion.ID
			children = append(children, splitting.Child{Key: fmt.Sprint("part", i), Title: criterion.ID, Requirement: criterion.Requirement, Criteria: []verification.Criterion{{ID: childID, Requirement: criterion.Requirement}}, Coverage: []splitting.Coverage{{ParentCriterion: criterion.ID, ChildCriterion: childID}}, DependsOn: deps})
			if f.partialChild && i == 0 {
				child := &children[len(children)-1]
				child.Criteria = append(child.Criteria, verification.Criterion{ID: childID + "-extra", Requirement: "second part of " + criterion.Requirement})
				child.Coverage[0].PartID = "first"
				child.Coverage[0].PartCount = 2
				child.Coverage[0].Basis = "two required behaviors"
				child.Coverage = append(child.Coverage, splitting.Coverage{ParentCriterion: criterion.ID, ChildCriterion: childID + "-extra", PartID: "second", PartCount: 2, Basis: "two required behaviors"})
			}
		}
		output = splitting.Proposal{SchemaVersion: 1, WorkItemID: w.ID, ScopeRevision: w.ScopeRevision, Outcome: "split", Children: children}
	} else if op.Kind == "implement" {
		if err := os.WriteFile(filepath.Join(w.Worktree, "app.txt"), []byte(fmt.Sprint("implementation ", f.calls)), 0600); err != nil {
			return worker.Run{}, err
		}
		outcome := "candidate"
		profiles := []string{}
		if f.interim && f.calls == 1 {
			outcome = "validation_needed"
			profiles = []string{"unit"}
		}
		output = worker.Handoff{SchemaVersion: 1, WorkItemID: w.ID, ScopeRevision: w.ScopeRevision, InputCandidateID: w.Candidate, Outcome: outcome, Summary: "implemented", CoveredCriteria: in.Scope.Included, RemainingWork: []string{}, RequestedProfiles: profiles, Blockers: []string{}}
	} else if in.Correction.Mode == "assessment" && !in.Correction.Consumed {
		output = verification.Assessment{SchemaVersion: 1, WorkItemID: w.ID, CandidateID: w.Candidate, ScopeRevision: w.ScopeRevision, Classification: f.classification, EvidenceIDs: in.Correction.EvidenceIDs, Reason: "evidence shows defect", ExpectationBasis: "original acceptance", TestChanges: []string{}}
	} else {
		os.MkdirAll(filepath.Join(w.Worktree, "tests"), 0700)
		os.WriteFile(filepath.Join(w.Worktree, "tests", "test.txt"), []byte("expected behavior"), 0600)
		mappings := []verification.Mapping{}
		for _, id := range in.Scope.Included {
			mappings = append(mappings, verification.Mapping{CriterionID: id, TestRef: "tests/test.txt", ExpectationBasis: "original " + id, ProfileID: "unit"})
		}
		output = verification.Plan{SchemaVersion: 1, WorkItemID: w.ID, ScopeRevision: w.ScopeRevision, PolicyRevision: w.PolicyRevision, InputCandidateID: w.Candidate, Revision: 1, CriteriaTests: mappings, RequestedProfiles: []string{}, UnverifiedCriteria: []string{}, Blockers: []string{}, OnSuccess: "publish_pr"}
	}
	body, _ := json.Marshal(output)
	if f.totals == nil {
		f.totals = map[string]int{}
	}
	f.totals[op.Kind] += 10
	usage := json.RawMessage(fmt.Sprintf(`{"last":{"totalTokens":10},"total":{"totalTokens":%d,"inputTokens":%d,"outputTokens":0}}`, f.totals[op.Kind], f.totals[op.Kind]))
	return worker.Run{ID: op.ID, ThreadID: "role-" + op.Kind, TurnID: fmt.Sprint(f.calls), State: "completed", Reason: op.Kind, Started: time.Now(), Final: string(body), Usage: usage}, nil
}

type fakeJobs struct {
	infrastructureFirst bool
	onRead              func()
	results             map[string]runner.Result
	waiting             bool
	failFirst           bool
	calls               int
}

func (f *fakeJobs) Start(_ context.Context, s runner.Spec) error {
	f.calls++
	code := 0
	status := "passed"
	if f.failFirst && f.calls == 1 {
		code = 1
		status = "failed"
	}
	if f.infrastructureFirst && f.calls == 1 {
		status = "infrastructure_failure"
	}
	r := runner.Result{JobID: s.JobID, Candidate: s.Candidate, TestedSHA: s.Candidate.SHA, ProfileID: s.ProfileID, ProfileRevision: s.Profile.Revision, Status: status, ExitCode: &code, Started: time.Now(), Finished: time.Now(), LogPath: filepath.Join(s.Root, s.JobID+".log")}
	f.results[s.JobID] = r
	return nil
}
func (f *fakeJobs) Read(s runner.Spec) (runner.Result, error) {
	if f.onRead != nil {
		callback := f.onRead
		f.onRead = nil
		callback()
	}
	r, ok := f.results[s.JobID]
	if !ok || f.waiting {
		return r, os.ErrNotExist
	}
	return r, nil
}
func setup(t *testing.T) (*Controller, *fakeGH, *fakeAgents, *fakeJobs) {
	t.Helper()
	ctx := context.Background()
	base := t.TempDir()
	repo := filepath.Join(base, "repo")
	origin := filepath.Join(base, "origin")
	os.Mkdir(repo, 0700)
	os.Mkdir(origin, 0700)
	if _, err := workspace.Git(ctx, origin, "init", "--bare"); err != nil {
		t.Fatal(err)
	}
	for _, a := range [][]string{{"init", "--initial-branch=trunk"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"config", "commit.gpgsign", "false"}, {"commit", "--allow-empty", "-m", "base"}, {"remote", "add", "origin", origin}, {"push", "-u", "origin", "trunk"}} {
		if _, err := workspace.Git(ctx, repo, a...); err != nil {
			t.Fatal(err)
		}
	}
	root := filepath.Join(base, "state")
	os.Mkdir(root, 0700)
	s, err := storage.Open(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cfg := Config{LauncherContract: "argv_passthrough_v1", CIRefMode: "head", PriorityLabels: []string{"high", "normal", "low"}, LogRetentionDays: 30, MaxArtifactBytes: 1 << 30, Version: 1, Repository: "owner/repo", RepositoryID: 1, RepositoryPath: repo, BaseBranch: "trunk", Root: root, Model: "configured", CodexBinary: "/native/codex", WorkerCodexHome: filepath.Join(base, "auth"), Launcher: []string{"/isolation"}, IsolationVerified: true, ReadyLabel: "ready", HoldLabel: "hold", VerifierPaths: []string{"tests/"}, Verification: policy.Verification{Revision: 1, RequiredProfiles: []string{"unit"}, Profiles: map[string]policy.Profile{"unit": {Revision: 1, Argv: []string{"true"}, TimeoutSeconds: 1, SuccessPattern: "PASS"}}}, Review: review.Policy{VerifiedRequest: true, VerifiedNativeApproval: true, VerifiedSHAMapping: true, TrustedReviewerIDs: []int64{7}, RequiredApprovals: 1}, RequesterID: 2, ReviewTrigger: "@codex review", MergeEnabled: true, MergeMethod: "squash", Limits: Limits{NetworkRetries: 3, ValidationJobs: 100, RepairRounds: 5, RunnerRetries: 1, ReviewRequests: 2, SplitDepth: 2, SplitChildren: 8, Tokens: 1000, TurnSeconds: 10, ReviewSeconds: 3600}, ReconcileSeconds: 1}
	c := &Controller{Config: cfg, Store: s, Capabilities: contract.NewReport("owner/repo")}
	c.Capabilities.RepositoryID = 1
	c.Capabilities.DefaultBranch = "trunk"
	for _, name := range contract.MergeRequirements {
		c.Capabilities.Capabilities[name] = contract.Capability{State: contract.Verified, Evidence: "contract fixture"}
	}
	gh := &fakeGH{controller: c, issue: github.Issue{Number: 1, Title: "implement", Body: "original requirements", State: "open"}, approved: true}
	agents := &fakeAgents{classification: "implementation_defect"}
	jobs := &fakeJobs{results: map[string]runner.Result{}}
	c.GitHub = gh
	c.Agents = agents
	c.Jobs = jobs
	scope := verification.Scope{Revision: 1, OriginalRequest: gh.issue.Body, Criteria: []verification.Criterion{{ID: "AC1", Requirement: "behavior"}}, Included: []string{"AC1"}, Remaining: []string{}, PreviouslyMerged: []string{}}
	if err = c.Enqueue(ctx, domain.WorkItem{ID: "w", RepositoryID: 1, Issue: 1, ScopeRevision: 1, PolicyRevision: 1, Phase: domain.Ready}, scope, gh.issue); err != nil {
		t.Fatal(err)
	}
	return c, gh, agents, jobs
}
func drive(t *testing.T, c *Controller, count int) {
	t.Helper()
	for range count {
		if err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
func TestEndToEndFakeMergeAndRestart(t *testing.T) {
	c, gh, agents, _ := setup(t)
	drive(t, c, 18)
	w, err := c.Store.Get(context.Background(), "w")
	if err != nil || w.Phase != domain.Merged || gh.merges != 1 || gh.closed != 1 || agents.calls != 2 {
		t.Fatal(w, gh.merges, gh.closed, agents.calls, err)
	}
	path := filepath.Join(c.Config.Root, "state.db")
	c.Store.Close()
	c.Store, err = storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	drive(t, c, 5)
	if gh.merges != 1 || gh.closed != 1 || agents.calls != 2 {
		t.Fatal("replay duplicated work")
	}
	metrics, err := c.Metrics(context.Background(), "w")
	if err != nil || metrics["turn_starts_during_wait"] != 0 || metrics["observed_tokens"] != int64(20) {
		t.Fatal(metrics, err)
	}
}
func TestWaitsNeverStartTurns(t *testing.T) {
	c, gh, agents, jobs := setup(t)
	jobs.waiting = true
	drive(t, c, 7)
	calls := agents.calls
	drive(t, c, 6)
	if agents.calls != calls || calls != 2 {
		t.Fatal("runner wait used model", agents.calls, calls)
	}
	jobs.waiting = false
	gh.approved = false
	drive(t, c, 10)
	calls = agents.calls
	drive(t, c, 6)
	if agents.calls != calls || gh.merges != 0 {
		t.Fatal("review wait used model or merged")
	}
}
func TestFailureRoutesToImplementationWithPlanReuse(t *testing.T) {
	c, gh, agents, jobs := setup(t)
	jobs.failFirst = true
	drive(t, c, 26)
	w, _ := c.Store.Get(context.Background(), "w")
	if w.Phase != domain.Merged || gh.merges != 1 || agents.calls != 4 {
		t.Fatal("repair flow or plan reuse failed", w, agents.calls)
	}
}

func TestScopeEditBlocksBeforeAnyTurn(t *testing.T) {
	c, gh, agents, _ := setup(t)
	gh.issue.Body = "changed requirement"
	drive(t, c, 3)
	w, _ := c.Store.Get(context.Background(), "w")
	if w.Phase != domain.Blocked || agents.calls != 0 {
		t.Fatal(w, agents.calls)
	}
}
func TestPauseMergeHoldAndCancel(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	drive(t, c, 1)
	c.Store.Apply(ctx, domain.Event{ID: "pause", WorkItemID: "w", Kind: "pause"})
	drive(t, c, 4)
	if agents.calls != 0 {
		t.Fatal("paused turn dispatched")
	}
	c.Store.Apply(ctx, domain.Event{ID: "resume", WorkItemID: "w", Kind: "resume"})
	c.Store.Apply(ctx, domain.Event{ID: "hold", WorkItemID: "w", Kind: "hold"})
	drive(t, c, 12)
	if gh.merges != 0 || agents.calls != 2 {
		t.Fatal("hold ignored", gh.merges, agents.calls)
	}
	c.Store.Apply(ctx, domain.Event{ID: "release", WorkItemID: "w", Kind: "release"})
	drive(t, c, 8)
	if gh.merges != 1 {
		t.Fatal("release did not merge")
	}
}
func TestCanceledValidationRetainsResultWithoutPublishing(t *testing.T) {
	c, gh, agents, jobs := setup(t)
	ctx := context.Background()
	jobs.waiting = true
	drive(t, c, 7)
	c.Store.Apply(ctx, domain.Event{ID: "cancel", WorkItemID: "w", Kind: "cancel"})
	jobs.waiting = false
	drive(t, c, 7)
	w, _ := c.Store.Get(ctx, "w")
	results, err := c.Store.Records(ctx, "validation", "w")
	if err != nil || w.Phase != domain.Canceled || len(results) != 1 || gh.pr.Number != 0 || agents.calls != 2 {
		t.Fatal(w, len(results), gh.pr.Number, agents.calls, err)
	}
}

type lostMerge struct{ *fakeGH }

func (f lostMerge) Merge(ctx context.Context, n int, head, method string) error {
	if err := f.fakeGH.Merge(ctx, n, head, method); err != nil {
		return err
	}
	return fmt.Errorf("merge response lost")
}
func TestLostMergeResponseReconcilesExactlyOnce(t *testing.T) {
	c, gh, agents, _ := setup(t)
	c.GitHub = lostMerge{gh}
	errorsSeen := 0
	for range 24 {
		if err := c.Tick(context.Background()); err != nil {
			errorsSeen++
		}
	}
	w, _ := c.Store.Get(context.Background(), "w")
	if errorsSeen != 1 || w.Phase != domain.Merged || gh.merges != 1 || agents.calls != 2 {
		t.Fatal(w, errorsSeen, gh.merges, agents.calls)
	}
}

type lostPublication struct {
	*fakeGH
	calls int
}

func (f *lostPublication) Publish(ctx context.Context, base, branch, id, title, body string) (github.PR, error) {
	f.calls++
	p, err := f.fakeGH.Publish(ctx, base, branch, id, title, body)
	if err != nil {
		return p, err
	}
	return p, fmt.Errorf("PR response lost")
}
func TestLostPRCreationResponseUsesExistingPR(t *testing.T) {
	c, gh, _, _ := setup(t)
	api := &lostPublication{fakeGH: gh}
	c.GitHub = api
	for range 23 {
		_ = c.Tick(context.Background())
	}
	w, _ := c.Store.Get(context.Background(), "w")
	if w.Phase != domain.Merged || api.calls != 1 {
		t.Fatal(w, api.calls)
	}
}

type lostRequest struct{ *fakeGH }

func (f lostRequest) RequestReview(ctx context.Context, n int, round, trigger string, requester int64) (int64, error) {
	_, err := f.fakeGH.RequestReview(ctx, n, round, trigger, requester)
	if err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("review request response lost")
}
func TestLostReviewRequestDoesNotDuplicateTrigger(t *testing.T) {
	c, gh, _, _ := setup(t)
	c.GitHub = lostRequest{gh}
	for range 24 {
		_ = c.Tick(context.Background())
	}
	w, _ := c.Store.Get(context.Background(), "w")
	if w.Phase != domain.Merged || gh.requests != 1 {
		t.Fatal(w, gh.requests)
	}
}
func TestTokenBudgetAppliesAcrossRoles(t *testing.T) {
	c, _, agents, _ := setup(t)
	c.Config.Limits.Tokens = 5
	c.Store.Put(context.Background(), "policy_snapshot", "w", "w", c.policyDigest())
	drive(t, c, 8)
	w, _ := c.Store.Get(context.Background(), "w")
	if w.Phase != domain.Blocked || agents.calls != 1 {
		t.Fatal(w, agents.calls)
	}
}
func TestBaseRefreshInvalidatesEvidenceWithoutNewTurns(t *testing.T) {
	c, gh, agents, jobs := setup(t)
	gh.approved = false
	drive(t, c, 8)
	ctx := context.Background()
	for _, args := range [][]string{{"commit", "--allow-empty", "-m", "base update"}, {"push", "origin", "trunk"}} {
		if _, err := workspace.Git(ctx, c.Config.RepositoryPath, args...); err != nil {
			t.Fatal(err)
		}
	}
	newBase, err := workspace.Resolve(ctx, c.Config.RepositoryPath, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	gh.pr.Base.SHA = newBase
	gh.approved = true
	drive(t, c, 17)
	w, _ := c.Store.Get(ctx, "w")
	if w.Phase != domain.Merged || agents.calls != 2 || jobs.calls < 3 {
		t.Fatal(w, agents.calls, jobs.calls)
	}
}
func TestPolicyChangeInvalidatesOldWork(t *testing.T) {
	c, gh, agents, _ := setup(t)
	drive(t, c, 5)
	c.Config.Verification.Revision++
	drive(t, c, 4)
	w, _ := c.Store.Get(context.Background(), "w")
	if w.Phase != domain.Blocked || gh.merges != 0 || agents.calls != 2 {
		t.Fatal(w, gh.merges, agents.calls)
	}
}
func TestPartialPRsRetainParentUntilFinalAcceptance(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "remaining"})
	scope.Remaining = []string{"AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	drive(t, c, 35)
	items, err := c.Store.List(ctx)
	if err != nil || len(items) != 2 || gh.merges != 2 || gh.closed != 1 || agents.calls != 4 {
		t.Fatal(items, gh.merges, gh.closed, agents.calls, err)
	}
	for _, w := range items {
		if w.Phase != domain.Merged {
			t.Fatal(w)
		}
	}
}
func TestAutomaticSplitCreatesDependentChildrenAndCompletesParent(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	scope.Included = []string{"AC1", "AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	agents.autoSplit = true
	drive(t, c, 55)
	root, _ := c.Store.Get(ctx, "w")
	a, err := c.Store.Get(ctx, "w-part0")
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Store.Get(ctx, "w-part1")
	if err != nil {
		t.Fatal(err)
	}
	if root.Phase != domain.Split || a.Phase != domain.Merged || b.Phase != domain.Merged || gh.merges != 2 || gh.closed != 3 || agents.calls != 6 {
		t.Fatal(root, a, b, gh.merges, gh.closed, agents.calls)
	}
}
func TestInterimValidationResumesOnlyRequestingImplementation(t *testing.T) {
	c, gh, agents, jobs := setup(t)
	agents.interim = true
	drive(t, c, 26)
	w, _ := c.Store.Get(context.Background(), "w")
	if w.Phase != domain.Merged || gh.merges != 1 || agents.calls != 3 || jobs.calls != 3 {
		t.Fatal(w, agents.calls, jobs.calls)
	}
}
func (f *fakeGH) CommitParents(context.Context, string) ([]string, error) {
	return []string{f.pr.Base.SHA, f.pr.Head.SHA}, nil
}
func TestReviewRetryBudgetRetainsOldRound(t *testing.T) {
	c, gh, agents, _ := setup(t)
	gh.approved = false
	drive(t, c, 8)
	ctx := context.Background()
	w, _ := c.Store.Get(ctx, "w")
	var round review.Round
	c.Store.Record(ctx, "review_round", w.Candidate, &round)
	round.RequestedAt = time.Now().Add(-2 * time.Hour)
	c.Store.Put(ctx, "review_round", w.Candidate, w.ID, round)
	if err := c.Rereview(ctx, "w", "retry timed-out accepted review"); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 2)
	if gh.requests != 2 || agents.calls != 2 {
		t.Fatal(gh.requests, agents.calls)
	}
	if err := c.Rereview(ctx, "w", "third request"); err == nil {
		t.Fatal("review budget reset")
	}
	history, err := c.Store.Records(ctx, "review_round_history", "w")
	if err != nil || len(history) != 1 {
		t.Fatal(history, err)
	}
}

type recoveringAgents struct {
	base  *fakeAgents
	saved map[string]worker.Run
	reads map[string]int
}

func (f *recoveringAgents) Run(ctx context.Context, w domain.WorkItem, op domain.Operation, prompt string, schema any) (worker.Run, error) {
	if saved, ok := f.saved[op.ID]; ok {
		f.reads[op.ID]++
		if f.reads[op.ID] < 2 {
			saved.State = "unknown"
			saved.Final = ""
			return saved, fmt.Errorf("history not visible yet")
		}
		return saved, nil
	}
	r, err := f.base.Run(ctx, w, op, prompt, schema)
	if err != nil {
		return r, err
	}
	f.saved[op.ID] = r
	r.State = "unknown"
	r.Final = ""
	return r, fmt.Errorf("turn response lost")
}
func TestUnknownTurnsReconcileWithoutRepeatingModelWork(t *testing.T) {
	c, gh, base, _ := setup(t)
	c.Agents = &recoveringAgents{base: base, saved: map[string]worker.Run{}, reads: map[string]int{}}
	drive(t, c, 30)
	w, _ := c.Store.Get(context.Background(), "w")
	if w.Phase != domain.Merged || gh.merges != 1 || base.calls != 2 {
		t.Fatal("model work repeated during reconciliation", w, base.calls, gh.merges)
	}
}
func TestCancelOnRunnerCompletionDoesNotRetryInfrastructure(t *testing.T) {
	c, gh, agents, jobs := setup(t)
	ctx := context.Background()
	jobs.infrastructureFirst = true
	jobs.waiting = true
	drive(t, c, 7)
	jobs.waiting = false
	jobs.onRead = func() {
		if err := c.Store.Apply(ctx, domain.Event{ID: "cancel-completing-job", WorkItemID: "w", Kind: "cancel"}); err != nil {
			t.Fatal(err)
		}
	}
	drive(t, c, 5)
	w, _ := c.Store.Get(ctx, "w")
	if w.Phase != domain.Canceled || jobs.calls != 1 || gh.pr.Number != 0 || agents.calls != 2 {
		t.Fatal("canceled work advanced", w, jobs.calls, gh.pr.Number, agents.calls)
	}
}
func TestWorkerLauncherEnvironmentIsExplicit(t *testing.T) {
	c, _, _, _ := setup(t)
	t.Setenv("HOME", "/controller/home")
	t.Setenv("GH_TOKEN", "must-not-forward")
	live := LiveAgents{Config: c.Config, Store: c.Store}
	values := map[string]string{}
	for _, entry := range live.environment(domain.WorkItem{Worktree: "/candidate"}, "/dedicated/home", "/readonly/git") {
		key, value, _ := strings.Cut(entry, "=")
		values[key] = value
	}
	if values["HOME"] != "/dedicated/home" || values["HARNESS_SANDBOX_HOME"] != "/dedicated/home" || values["HARNESS_LAUNCH_MODE"] != "worker" || values["CODEX_HOME"] != c.Config.WorkerCodexHome || values["HARNESS_GIT_DIR"] != "/readonly/git" || values["HARNESS_CODEX_BINARY"] != c.Config.CodexBinary || values["GH_TOKEN"] != "" {
		t.Fatal("worker environment contract mismatch")
	}
}

func TestIgnoredTestsCannotBecomeCandidateEvidence(t *testing.T) {
	c, gh, _, jobs := setup(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(c.Config.RepositoryPath, ".gitignore"), []byte("tests/\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.Freeze(ctx, c.Config.RepositoryPath, "ignore tests"); err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.Git(ctx, c.Config.RepositoryPath, "push", "origin", "trunk"); err != nil {
		t.Fatal(err)
	}
	drive(t, c, 8)
	w, err := c.Store.Get(ctx, "w")
	if err != nil || w.Phase != domain.Blocked || !strings.Contains(w.Reason, "not a tracked regular file") {
		t.Fatal(w, err)
	}
	if gh.posts != 0 || gh.merges != 0 || jobs.calls != 0 {
		t.Fatal("uncommitted test evidence escaped to validation/publication")
	}
}

func TestSplitParentWaitsForChildResidualAndFinalAcceptance(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "second"})
	scope.Included = []string{"AC1", "AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	agents.autoSplit, agents.partialChild = true, true
	for range 15 {
		drive(t, c, 1)
		root, _ := c.Store.Get(ctx, "w")
		if root.Phase == domain.Split {
			break
		}
	}
	child, err := c.Store.Get(ctx, "w-part0")
	if err != nil {
		t.Fatal(err)
	}
	var childScope verification.Scope
	c.Store.Record(ctx, "scope", child.ID, &childScope)
	childScope.Included = []string{childScope.Criteria[0].ID}
	childScope.Remaining = []string{childScope.Criteria[1].ID}
	c.Store.Put(ctx, "scope", child.ID, child.ID, childScope)
	for range 25 {
		drive(t, c, 1)
		child, _ = c.Store.Get(ctx, child.ID)
		if child.Phase == domain.Merged {
			break
		}
	}
	root, _ := c.Store.Get(ctx, "w")
	if err = c.afterMerge(ctx, child); err != nil {
		t.Fatal(err)
	}
	next, err := c.Store.Get(ctx, child.ID+"-remaining")
	if err != nil || next.Phase != domain.Ready {
		t.Fatal(next, err)
	}
	if err = c.finalAcceptance(ctx, root, scope); err != nil {
		t.Fatal(err)
	}
	if gh.issue.State != "open" {
		t.Fatal("parent closed before residual merge")
	}
	if ok, err := c.splitComplete(ctx, child, map[string]bool{}); err != nil || ok {
		t.Fatal("partial child counted complete", ok, err)
	}
	drive(t, c, 70)
	if gh.issue.State != "closed" || gh.merges != 3 || gh.closed != 3 {
		t.Fatal(gh.issue.State, gh.merges, gh.closed)
	}
}

func TestEditedIssueBetweenMergeAndContinuationRequiresAdoption(t *testing.T) {
	c, gh, _, _ := setup(t)
	ctx := context.Background()
	var scope verification.Scope
	c.Store.Record(ctx, "scope", "w", &scope)
	scope.Criteria = append(scope.Criteria, verification.Criterion{ID: "AC2", Requirement: "old remaining"})
	scope.Remaining = []string{"AC2"}
	c.Store.Put(ctx, "scope", "w", "w", scope)
	var w domain.WorkItem
	for range 20 {
		drive(t, c, 1)
		w, _ = c.Store.Get(ctx, "w")
		if w.Phase == domain.Merged {
			break
		}
	}
	if w.Phase != domain.Merged {
		t.Fatal(w)
	}
	gh.issue.Body = "new acceptance requirements"
	if err := c.afterMerge(ctx, w); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Store.Get(ctx, "w-remaining"); !missing(err) {
		t.Fatal("stale scope successor registered", err)
	}
	var hold map[string]any
	if err := c.Store.Record(ctx, "continuation_hold", w.ID, &hold); err != nil {
		t.Fatal(err)
	}
	var source Source
	c.Store.Record(ctx, "source", w.ID, &source)
	if source.Digest == digest(gh.issue) {
		t.Fatal("new digest bound to old scope")
	}
	scope.Revision++
	scope.OriginalRequest = gh.issue.Body
	scope.Criteria[1].Requirement = "new remaining"
	scope.Included, scope.Remaining = []string{"AC1", "AC2"}, []string{}
	if err := c.AdoptContinuation(ctx, w, scope, "adopt edited acceptance"); err != nil {
		t.Fatal(err)
	}
	var adopted verification.Scope
	c.Store.Record(ctx, "scope", "w-remaining", &adopted)
	if adopted.Revision != 2 || adopted.Criteria[1].Requirement != "new remaining" {
		t.Fatal(adopted)
	}
	drive(t, c, 30)
	if gh.issue.State != "closed" || gh.merges != 2 {
		t.Fatal(gh.issue, gh.merges)
	}
	saved, _ := c.Store.Get(ctx, w.ID)
	if saved.Phase != domain.Merged || saved.ScopeRevision != 1 || saved.Candidate != w.Candidate {
		t.Fatal("merged history changed", saved)
	}
}

func TestRequiredCIFailureRepairsSameRoleAndPreservesEvidence(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	c.Config.RequiredChecks = []gate.RequiredCheck{{Name: "required", AppID: 9, AllowedConclusions: []string{"success"}}}
	c.Store.Put(ctx, "policy_snapshot", "w", "w", c.policyDigest())
	gh.ciFailFirst = true
	drive(t, c, 32)
	w, _ := c.Store.Get(ctx, "w")
	if w.Phase != domain.Merged || agents.calls != 4 || gh.merges != 1 {
		t.Fatal(w, agents.calls, gh.merges)
	}
	raw, err := c.Store.Records(ctx, "ci_evidence", "w")
	if err != nil || len(raw) != 1 {
		t.Fatal(raw, err)
	}
	var evidence CIEvidence
	json.Unmarshal(raw[0], &evidence)
	if evidence.Check.Conclusion != "failure" || evidence.TestedSHA != gh.ciFailureSHA || evidence.Reference == "" {
		t.Fatal(evidence)
	}
	var input struct {
		CIEvidence []CIEvidence `json:"ci_evidence"`
	}
	json.Unmarshal([]byte(agents.prompts[2]), &input)
	if len(input.CIEvidence) != 1 || input.CIEvidence[0].ID != evidence.ID || input.CIEvidence[0].Check.Output.Summary == "" || input.CIEvidence[0].Diagnostics == "" {
		t.Fatal("CI evidence missing from existing verification thread input", agents.prompts[2])
	}
}

func TestCIFailureSelectionAndMechanicalHold(t *testing.T) {
	for _, tc := range []struct {
		name, mode, conclusion string
		stale                  bool
		assess, hold           bool
	}{
		{"old SHA", "head", "failure", true, false, false},
		{"failure", "head", "failure", false, true, false},
		{"test merge failure", "test_merge", "failure", false, true, false},
		{"timeout", "head", "timed_out", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, gh, agents, _ := setup(t)
			ctx := context.Background()
			gh.approved = false
			drive(t, c, 8)
			w, _ := c.Store.Get(ctx, "w")
			candidate, _ := c.candidate(ctx, w)
			target := candidate.SHA
			mapping := gate.CIRevision{}
			if tc.mode == "test_merge" {
				target = strings.Repeat("b", 40)
				mapping = gate.CIRevision{HeadSHA: candidate.SHA, BaseSHA: candidate.BaseSHA, TestedSHA: target, Verified: true}
			}
			check := github.Check{ID: 100, Name: "required", HeadSHA: target, Status: "completed", Conclusion: tc.conclusion}
			check.App.ID = 9
			if tc.stale {
				check.HeadSHA = "old"
			}
			snapshot := gate.Snapshot{Candidate: candidate, CI: mapping, RequiredChecks: []gate.RequiredCheck{{Name: "required", AppID: 9, AllowedConclusions: []string{"success"}}}, Checks: []github.Check{check}}
			handled, err := c.routeCIFailures(ctx, w, snapshot)
			if err != nil || handled != (tc.assess || tc.hold) {
				t.Fatal(handled, err)
			}
			after, _ := c.Store.Get(ctx, w.ID)
			if (after.Phase == domain.Verifying) != tc.assess || (after.Phase == domain.Blocked) != tc.hold || agents.calls != 2 {
				t.Fatal(after, agents.calls)
			}
			if tc.assess {
				var correction Correction
				c.Store.Record(ctx, "correction", w.ID, &correction)
				var evidence CIEvidence
				c.Store.Record(ctx, "ci_evidence", correction.EvidenceIDs[0], &evidence)
				if evidence.TestedSHA != target || evidence.Candidate.SHA != candidate.SHA {
					t.Fatal(evidence)
				}
			}
		})
	}
}

type repairWithTests struct{ fake *fakeAgents }

func (a repairWithTests) Run(ctx context.Context, w domain.WorkItem, op domain.Operation, input string, schema any) (worker.Run, error) {
	r, err := a.fake.Run(ctx, w, op, input, schema)
	if err == nil && op.Kind == "implement" && a.fake.calls > 1 {
		err = os.WriteFile(filepath.Join(w.Worktree, "tests", "test.txt"), []byte("updated test for implementation repair"), 0600)
	}
	return r, err
}

func TestCIRepairChangingTestsRetiresOldCorrection(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	c.Config.RequiredChecks = []gate.RequiredCheck{{Name: "required", AppID: 9, AllowedConclusions: []string{"success"}}}
	if err := c.Store.Put(ctx, "policy_snapshot", "w", "w", c.policyDigest()); err != nil {
		t.Fatal(err)
	}
	gh.ciFailFirst = true
	c.Agents = repairWithTests{agents}
	drive(t, c, 32)
	w, _ := c.Store.Get(ctx, "w")
	if w.Phase != domain.Merged || agents.calls != 5 {
		t.Fatalf("phase=%s reason=%s agent_calls=%d", w.Phase, w.Reason, agents.calls)
	}
	history, err := c.Store.Records(ctx, "correction_history", "w")
	if err != nil || len(history) != 1 {
		t.Fatal("correction history lost", history, err)
	}
}

func (f *fakeGH) CheckDiagnostics(context.Context, github.Check) (string, error) {
	if f.onDiagnostics != nil {
		callback := f.onDiagnostics
		f.onDiagnostics = nil
		callback()
	}

	if f.diagnosticsMissing {
		return "", fmt.Errorf("private diagnostics access unavailable")
	}
	return "expected behavior assertion failed in CI", nil
}

type testRepair struct{ fake *fakeAgents }

func (a testRepair) Run(ctx context.Context, w domain.WorkItem, op domain.Operation, input string, schema any) (worker.Run, error) {
	r, err := a.fake.Run(ctx, w, op, input, schema)
	if err == nil && a.fake.calls == 4 {
		err = os.WriteFile(filepath.Join(w.Worktree, "tests/test.txt"), []byte("corrected expected behavior"), 0600)
	}
	return r, err
}
func TestVerificationCIRepairRetiresCorrectionBeforeBaseConflict(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	c.Config.RequiredChecks = []gate.RequiredCheck{{Name: "required", AppID: 9, AllowedConclusions: []string{"success"}}}
	c.Store.Put(ctx, "policy_snapshot", "w", "w", c.policyDigest())
	gh.ciFailFirst, gh.approved = true, false
	agents.classification = "test_defect"
	c.Agents = testRepair{agents}
	var w domain.WorkItem
	for range 25 {
		drive(t, c, 1)
		w, _ = c.Store.Get(ctx, "w")
		candidate, _ := c.candidate(ctx, w)
		if agents.calls == 4 && w.Phase == domain.Reviewing && gh.pr.Head.SHA == candidate.SHA {
			break
		}
	}
	if agents.calls != 4 || w.Phase != domain.Reviewing {
		t.Fatal(w, agents.calls)
	}
	var correction Correction
	c.Store.Record(ctx, "correction", "w", &correction)
	if len(correction.EvidenceIDs) != 0 {
		t.Fatal("old correction remains active", correction)
	}
	os.WriteFile(filepath.Join(c.Config.RepositoryPath, "app.txt"), []byte("upstream implementation"), 0600)
	for _, args := range [][]string{{"add", "app.txt"}, {"commit", "-m", "upstream"}, {"push", "origin", "trunk"}} {
		if _, err := workspace.Git(ctx, c.Config.RepositoryPath, args...); err != nil {
			t.Fatal(err)
		}
	}
	base, err := workspace.Resolve(ctx, c.Config.RepositoryPath, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	gh.pr.Base.SHA = base
	drive(t, c, 2)
	after, _ := c.Store.Get(ctx, "w")
	if after.Phase == domain.Blocked || agents.calls != 5 {
		t.Fatal(after, agents.calls)
	}
}

func TestMissingCIDiagnosticsBlocksWithoutAssessment(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	gh.approved = false
	drive(t, c, 8)
	w, _ := c.Store.Get(ctx, "w")
	candidate, _ := c.candidate(ctx, w)
	check := github.Check{ID: 123, Name: "required", HeadSHA: candidate.SHA, Status: "completed", Conclusion: "failure"}
	check.App.ID = 9
	gh.diagnosticsMissing = true
	handled, err := c.routeCIFailures(ctx, w, gate.Snapshot{Candidate: candidate, RequiredChecks: []gate.RequiredCheck{{Name: "required", AppID: 9, AllowedConclusions: []string{"success"}}}, Checks: []github.Check{check}})
	after, _ := c.Store.Get(ctx, "w")
	if err != nil || !handled || after.Phase != domain.Blocked || !strings.Contains(after.Reason, "diagnostics unavailable") || agents.calls != 2 {
		t.Fatal(after, handled, err, agents.calls)
	}
}

func TestBaseRefreshRetiresConsumedCICorrection(t *testing.T) {
	c, gh, _, _ := setup(t)
	ctx := context.Background()
	gh.approved = false
	drive(t, c, 8)
	w, _ := c.Store.Get(ctx, "w")
	candidate, _ := c.candidate(ctx, w)
	evidence := CIEvidence{ID: "old-ci", Candidate: candidate}
	c.Store.Put(ctx, "ci_evidence", evidence.ID, w.ID, evidence)
	c.Store.Put(ctx, "correction", w.ID, w.ID, Correction{Mode: "repair", Consumed: true, EvidenceIDs: []string{evidence.ID}, FeedbackIDs: []string{}})
	for _, args := range [][]string{{"commit", "--allow-empty", "-m", "base update"}, {"push", "origin", "trunk"}} {
		if _, err := workspace.Git(ctx, c.Config.RepositoryPath, args...); err != nil {
			t.Fatal(err)
		}
	}
	base, err := workspace.Resolve(ctx, c.Config.RepositoryPath, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.refreshBase(ctx, w, base); err != nil {
		t.Fatal(err)
	}
	var correction Correction
	c.Store.Record(ctx, "correction", w.ID, &correction)
	if len(correction.EvidenceIDs) != 0 || !correction.Consumed {
		t.Fatal(correction)
	}
	history, err := c.Store.Records(ctx, "correction_history", w.ID)
	if err != nil || len(history) != 1 {
		t.Fatal(history, err)
	}
	if candidateID(w, candidate.SHA, base) == candidateID(w, candidate.SHA, candidate.BaseSHA) {
		t.Fatal("base change reused candidate identity")
	}
}

func TestReviewFeedbackAndCIFailureAreBatched(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	c.Config.RequiredChecks = []gate.RequiredCheck{{Name: "required", AppID: 9, AllowedConclusions: []string{"success"}}}
	c.Store.Put(ctx, "policy_snapshot", "w", "w", c.policyDigest())
	gh.reviewFinding, gh.ciFailFirst = true, true
	var w domain.WorkItem
	for range 15 {
		drive(t, c, 1)
		w, _ = c.Store.Get(ctx, "w")
		if w.Phase == domain.Verifying && agents.calls == 2 {
			break
		}
	}
	var correction Correction
	if err := c.Store.Record(ctx, "correction", "w", &correction); err != nil {
		t.Fatal(err)
	}
	if len(correction.EvidenceIDs) != 2 || len(correction.FeedbackIDs) != 1 || correction.Mode != "assessment" || agents.calls != 2 {
		t.Fatal(w, correction, agents.calls)
	}
	evidence, err := c.Store.Records(ctx, "ci_evidence", "w")
	if err != nil || len(evidence) != 1 {
		t.Fatal(evidence, err)
	}
}

type unchangedRepair struct{ fake *fakeAgents }

func (a unchangedRepair) Run(ctx context.Context, w domain.WorkItem, op domain.Operation, input string, schema any) (worker.Run, error) {
	before, readErr := os.ReadFile(filepath.Join(w.Worktree, "app.txt"))
	r, err := a.fake.Run(ctx, w, op, input, schema)
	if err == nil && readErr == nil && op.Kind == "implement" {
		err = os.WriteFile(filepath.Join(w.Worktree, "app.txt"), before, 0600)
	}
	return r, err
}
func TestUnchangedReviewRepairsReachBudgetInsteadOfStalling(t *testing.T) {
	c, gh, agents, _ := setup(t)
	ctx := context.Background()
	c.Config.Limits.RepairRounds = 2
	c.Store.Put(ctx, "policy_snapshot", "w", "w", c.policyDigest())
	gh.reviewFinding = true
	c.Agents = unchangedRepair{agents}
	drive(t, c, 50)
	w, _ := c.Store.Get(ctx, "w")
	if w.Phase != domain.Blocked || !strings.Contains(w.Reason, "budget") || agents.calls > 8 {
		t.Fatal("unchanged batch stalled or bypassed budget", w, agents.calls)
	}
}

func TestStaleDiagnosticHoldCannotBlockAdoptedScope(t *testing.T) {
	c, gh, _, _ := setup(t)
	ctx := context.Background()
	gh.approved = false
	drive(t, c, 8)
	w, _ := c.Store.Get(ctx, "w")
	candidate, _ := c.candidate(ctx, w)
	check := github.Check{ID: 101, Name: "required", HeadSHA: candidate.SHA, Status: "completed", Conclusion: "failure"}
	check.App.ID = 9
	gh.diagnosticsMissing = true
	gh.onDiagnostics = func() {
		if err := c.Store.Adopt(ctx, w.ID, 2, 1, map[string]any{"scope": "adopted new requirements"}); err != nil {
			t.Fatal(err)
		}
	}
	handled, err := c.routeCIFailures(ctx, w, gate.Snapshot{Candidate: candidate, RequiredChecks: []gate.RequiredCheck{{Name: "required", AppID: 9, AllowedConclusions: []string{"success"}}}, Checks: []github.Check{check}})
	after, _ := c.Store.Get(ctx, w.ID)
	if err != nil || !handled || after.Phase != domain.Ready || after.ScopeRevision != 2 {
		t.Fatal(after, handled, err)
	}
}
