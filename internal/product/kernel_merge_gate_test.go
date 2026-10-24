package product

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type mergeReadFixture struct {
	*cloudReadAPI
	checks          []github.Check
	comments        []github.Comment
	parents         []string
	conditions      string
	onConditions    func()
	conditionsReads int
}

func (a *mergeReadFixture) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if strings.HasPrefix(path, "commits/") {
		parents := []map[string]string{}
		for _, sha := range a.parents {
			parents = append(parents, map[string]string{"sha": sha})
		}
		return json.Marshal(map[string]any{"sha": a.pr.MergeCommitSHA, "parents": parents})
	}
	return a.cloudReadAPI.ResourceGET(ctx, path)
}
func (a *mergeReadFixture) ResourcePages(ctx context.Context, path, field string) ([]json.RawMessage, error) {
	var values []any
	switch {
	case strings.Contains(path, "check-runs"):
		for _, v := range a.checks {
			values = append(values, v)
		}
	case path == "pulls/42/comments?per_page=100":
		for _, v := range a.comments {
			values = append(values, v)
		}
	default:
		return a.cloudReadAPI.ResourcePages(ctx, path, field)
	}
	var out []json.RawMessage
	for _, v := range values {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}
func (a *mergeReadFixture) ResourceConditions(ctx context.Context, n int) (string, error) {
	a.conditionsReads++
	if a.onConditions != nil {
		a.onConditions()
	}
	return a.conditions, nil
}

func mergeGateFixture(t *testing.T) (KernelMergeGate, storage.ProductTargetSnapshot, *mergeReadFixture, string, *contract.Report) {
	t.Helper()
	worker, api, id := completionWorkerFixture(t)
	round, err := worker.Reader.Store.ReadProductCloudRound(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	api.reviews[0].State = "APPROVED"
	report := contract.NewReport(round.Product.Value.Repository)
	report.RepositoryID, report.DefaultBranch = 17, round.Product.Value.BaseBranch
	for _, name := range contract.MergeRequirements {
		report.Capabilities[name] = contract.Capability{State: contract.Verified, Evidence: "fixture verified " + name}
	}
	readReport := func(context.Context) (contract.Report, error) { return report, nil }
	cloud := CloudReviewReader{Store: worker.Reader.Store, Repositories: map[int64]CloudReviewPolicy{17: {Policy: review.Policy{VerifiedRequest: true, VerifiedNativeApproval: true, VerifiedSHAMapping: true, TrustedReviewerIDs: []int64{91}, RequiredApprovals: 1}, ReadReport: readReport}}, API: worker.Reader.API}
	check := github.Check{ID: 201, Name: "unit", HeadSHA: api.pr.Head.SHA, Status: "completed", Conclusion: "success", CompletedAt: time.Now().UTC()}
	check.App.ID = 9
	m := &mergeReadFixture{cloudReadAPI: api, checks: []github.Check{check}, conditions: "CLEAN"}
	host := KernelMergePolicy{CIRefMode: "head", RequiredChecks: []gate.RequiredCheck{{Name: "unit", AppID: 9, AllowedConclusions: []string{"success"}}}, ReadReport: readReport}
	// This fixture isolates external gate behavior; the integrated fixture below
	// supplies the actual kernel quality and source gates.
	g := KernelMergeGate{Publication: func(context.Context, storage.ProductTargetSnapshot) (Publication, error) {
		return Publication{Body: "quality fixture"}, nil
	}, Source: func(context.Context, storage.ProductTargetSnapshot) error { return nil }, Cloud: cloud, API: func(storage.ProductTarget) MergeReadAPI { return m }, Repositories: map[int64]KernelMergePolicy{17: host}}
	return g, round.Product, m, id, &report
}

func TestKernelMergeGateRequiresCurrentApprovalCIAndAllCapabilities(t *testing.T) {
	for _, mode := range []string{"head", "test_merge", "unknown_capability", "blank_evidence", "future_report", "wrong_report_repository", "commented", "dismissed", "changes_requested", "old_review", "wrong_actor", "unassigned_approval", "feedback", "inline_feedback", "missing_ci", "wrong_ci_app", "wrong_ci_sha", "new_pending_ci", "ci_failure", "future_ci", "duplicate_ci", "missing_ci_time", "unknown_rules", "bad_parents", "changed_head", "changed_approval", "changed_ci", "changed_comments", "changed_rules", "revoked_report", "source_hold", "quality_hold", "quality_changed", "stale_product", "invalid_check_policy", "empty_check_policy", "mixed_assigned_approval"} {
		t.Run(mode, func(t *testing.T) {
			g, saved, api, id, report := mergeGateFixture(t)
			host := g.Repositories[17]
			allowed := mode == "head" || mode == "test_merge"
			switch mode {
			case "test_merge", "bad_parents":
				host.CIRefMode = "test_merge"
				api.pr.MergeCommitSHA = strings.Repeat("c", 40)
				api.checks[0].HeadSHA = api.pr.MergeCommitSHA
				api.parents = []string{saved.Value.Candidate.BaseSHA, saved.Value.Candidate.SHA}
				if mode == "bad_parents" {
					api.parents[0] = strings.Repeat("d", 40)
				}
			case "unknown_capability":
				report.Capabilities["rules_enforced"] = contract.Capability{State: contract.Unknown}
			case "blank_evidence":
				report.Capabilities["ci_mapping"] = contract.Capability{State: contract.Verified, Evidence: " \n"}
			case "future_report":
				report.ObservedAt = time.Now().Add(time.Hour)
			case "wrong_report_repository":
				report.RepositoryID++
			case "commented", "dismissed", "changes_requested":
				api.reviews[0].State = strings.ToUpper(mode)
			case "old_review":
				api.reviews[0].SubmittedAt = time.Now().Add(-time.Hour)
			case "wrong_actor":
				api.reviews[0].User.ID = 92
			case "unassigned_approval":
				api.reviews[0].User.ID = 92
				route := g.Cloud.Repositories[17]
				route.Policy.TrustedReviewerIDs = []int64{92}
				g.Cloud.Repositories[17] = route
			case "feedback":
				api.reviews[0].Body = "missing required validation"
			case "inline_feedback":
				api.comments = []github.Comment{{ID: 301, ReviewID: 101, CommitID: saved.Value.Candidate.SHA, User: github.User{ID: 91}, Body: "defect remains"}}
			case "missing_ci":
				api.checks = nil
			case "wrong_ci_app":
				api.checks[0].App.ID = 10
			case "wrong_ci_sha":
				api.checks[0].HeadSHA = strings.Repeat("d", 40)
			case "new_pending_ci":
				next := api.checks[0]
				next.ID++
				next.Status = "in_progress"
				api.checks = append(api.checks, next)
			case "ci_failure":
				api.checks[0].Conclusion = "failure"
			case "future_ci":
				api.checks[0].CompletedAt = time.Now().Add(time.Hour)
			case "duplicate_ci":
				api.checks = append(api.checks, api.checks[0])
			case "missing_ci_time":
				api.checks[0].CompletedAt = time.Time{}
			case "unknown_rules":
				api.conditions = "UNKNOWN"
			case "changed_head":
				api.onConditions = func() { api.pr.Head.SHA = strings.Repeat("d", 40) }
			case "changed_approval":
				api.onConditions = func() { api.reviews[0].State = "DISMISSED" }
			case "changed_ci":
				api.onConditions = func() { api.checks[0].Conclusion = "failure" }
			case "changed_comments":
				api.onConditions = func() { api.comments = []github.Comment{{ID: 301, Body: "new feedback"}} }
			case "changed_rules":
				api.onConditions = func() {
					if api.conditionsReads > 1 {
						api.conditions = "BLOCKED"
					}
				}
			case "revoked_report":
				api.onConditions = func() { report.Capabilities["rules_enforced"] = contract.Capability{State: contract.Unknown} }
			case "source_hold":
				g.Source = func(context.Context, storage.ProductTargetSnapshot) error { return errors.New("source held") }
			case "quality_hold":
				g.Publication = func(context.Context, storage.ProductTargetSnapshot) (Publication, error) {
					return Publication{}, storage.ErrProductQualityHeld
				}
			case "quality_changed":
				calls := 0
				g.Publication = func(context.Context, storage.ProductTargetSnapshot) (Publication, error) {
					calls++
					return Publication{Body: strings.Repeat("x", calls)}, nil
				}
			case "stale_product":
				saved.Digest = "other"
			case "empty_check_policy":
				host.RequiredChecks = nil
			case "mixed_assigned_approval":
				next := api.reviews[0]
				next.ID++
				next.User.ID = 92
				api.reviews = append(api.reviews, next)
				route := g.Cloud.Repositories[17]
				route.Policy.TrustedReviewerIDs = []int64{91, 92}
				route.Policy.RequiredApprovals = 2
				g.Cloud.Repositories[17] = route
			case "invalid_check_policy":
				host.RequiredChecks[0].AllowedConclusions = []string{"neutral"}
			}
			g.Repositories[17] = host
			assessment, err := g.Check(context.Background(), saved, id)
			if allowed {
				if err != nil || assessment.ProductDigest != saved.Digest || len(assessment.CheckIDs) != 1 || len(assessment.ReviewIDs) != 1 || api.conditionsReads != 2 {
					t.Fatal(assessment, err)
				}
			} else if err == nil {
				t.Fatal("unsafe merge assessment", mode, assessment)
			}
			seat, e := g.Cloud.Store.OperationReservation(context.Background(), id)
			if e != nil || seat.State != "held" {
				t.Fatal("assessment released Cloud seat", seat, e)
			}
			var grants int
			if e := g.Cloud.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_merge','review_approval','wave_completion')").Scan(&grants); e != nil || grants != 0 {
				t.Fatal("assessment created merge authority", grants, e)
			}
		})
	}
}

func checkKernelMergeWithQuality(t *testing.T, e Executor, saved storage.ProductTargetSnapshot, id string, requestAPI *fakeReviewAPI, httpMode string) {
	t.Helper()
	ctx := context.Background()
	round, err := e.Store.ReadProductCloudRound(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	pr := *requestAPI.pr
	pr.Base.SHA = saved.Value.Candidate.BaseSHA
	report := contract.NewReport(saved.Value.Repository)
	report.RepositoryID, report.DefaultBranch = saved.Value.WorkItem.RepositoryID, saved.Value.BaseBranch
	for _, key := range contract.MergeRequirements {
		report.Capabilities[key] = contract.Capability{State: contract.Verified, Evidence: "fixture contract"}
	}
	readReport := func(context.Context) (contract.Report, error) { return report, nil }
	native := &cloudReadAPI{repository: Repository{ID: report.RepositoryID, Name: report.Repository, DefaultBranch: report.DefaultBranch}, pr: pr, comments: requestAPI.comments, reviews: []github.Review{{ID: 101, User: github.User{ID: 91}, State: "APPROVED", CommitID: saved.Value.Candidate.SHA, SubmittedAt: time.Now().UTC()}}, fail: "never"}
	check := github.Check{ID: 201, Name: "unit", HeadSHA: saved.Value.Candidate.SHA, Status: "completed", Conclusion: "success", CompletedAt: time.Now().UTC()}
	check.App.ID = 9
	api := &mergeReadFixture{cloudReadAPI: native, checks: []github.Check{check}, conditions: "CLEAN"}
	cloud := CloudReviewReader{Store: e.Store, API: func(storage.ProductTarget) ResourceAPI { return native }, Repositories: map[int64]CloudReviewPolicy{report.RepositoryID: {ReadReport: readReport, Policy: review.Policy{VerifiedRequest: true, VerifiedNativeApproval: true, VerifiedSHAMapping: true, TrustedReviewerIDs: []int64{91}, RequiredApprovals: 1}}}}
	// The sealer fixture predates Issue admission. Supply a version-only
	// source fixture here; actual live Issue source behavior is independently
	// covered by KernelSourceGate tests. Local quality below is production.
	source := func(ctx context.Context, p storage.ProductTargetSnapshot) error {
		w, err := e.Store.Wave(ctx, p.Value.Target.WaveID)
		if err != nil {
			return err
		}
		if w.Target() != p.Value.Target {
			return storage.ErrProductQualityHeld
		}
		return nil
	}
	g := KernelMergeGate{Publication: e.PublicationGate, Source: source, Cloud: cloud, API: func(storage.ProductTarget) MergeReadAPI { return api }, Repositories: map[int64]KernelMergePolicy{report.RepositoryID: {CIRefMode: "head", RequiredChecks: []gate.RequiredCheck{{Name: "unit", AppID: 9, AllowedConclusions: []string{"success"}}}, ReadReport: readReport}}}
	assessment, err := g.Check(ctx, saved, id)
	if err != nil || assessment.RoundDigest != round.Digest {
		t.Fatal("canonical quality not connected to merge assessment", assessment, err)
	}
	// A fresh operator hold after the successful read must invalidate the next
	// assessment despite the same previously observed approval/check results.
	if _, err := e.Store.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "merge-gate-pause", Target: saved.Value.Target, Action: "pause", Reason: "recheck operator authority"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Check(ctx, saved, id); err == nil {
		t.Fatal("cached assessment bypassed pause")
	}
	if _, err := e.Store.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "merge-gate-resume", Target: saved.Value.Target, Action: "resume", Reason: "restore fixture"}); err != nil {
		t.Fatal(err)
	}
	if httpMode == "versioned_source" || httpMode == "versioned_request" || httpMode == "versioned_work" {
		checkVersionedContinuationSource(t, e, g, saved, id, httpMode == "versioned_request", httpMode == "versioned_work")
		return
	}
	if httpMode == "partial_continuation" {
		checkPartialMergeContinuation(t, e, g, saved, id)
		return
	}
	if httpMode != "" {
		checkMergeHTTPWrapper(t, g, saved, id, httpMode)
		return
	}

	job, err := e.Store.PrepareProductMerge(ctx, id, "kernel-merge-owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Store.ClaimProductMerge(ctx, job.ID, job.Value.OwnerID, job.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Store.MarkProductMergeMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, assessment); err == nil {
		t.Fatal("pre-claim assessment reused at send fence")
	}
	fresh, err := g.Check(ctx, saved, id)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := e.Store.MarkProductMergeMayHaveSent(ctx, job.ID, job.Value.OwnerID, job.Digest, fresh)
	if err != nil || sent.State != "unknown" || sent.GateDigest == "" {
		t.Fatal("actual gate packet not journaled with send fence", sent, err)
	}

	checkMergeReconciler(t, e.Store, sent, native)

}
