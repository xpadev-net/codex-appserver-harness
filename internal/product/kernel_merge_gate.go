package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type MergeReadAPI interface {
	ResourceAPI
	ResourceConditions(context.Context, int) (string, error)
}
type KernelMergePolicy struct {
	CIRefMode      string
	RequiredChecks []gate.RequiredCheck
	ReadReport     func(context.Context) (contract.Report, error)
}

// MergeAssessment is a fresh observation, never a durable send grant. A merge
// dispatcher must repeat Check immediately before its independent send fence.
type MergeAssessment struct {
	ProductDigest               string
	OperationID                 string
	RoundDigest                 string
	HeadSHA, BaseSHA, TestedSHA string
	ReviewIDs, CheckIDs         []int64
	ObservedAt                  time.Time
}
type KernelMergeGate struct {
	Publication  PublicationGate
	Source       func(context.Context, storage.ProductTargetSnapshot) error
	Cloud        CloudReviewReader
	API          func(storage.ProductTarget) MergeReadAPI
	Repositories map[int64]KernelMergePolicy
}

func (g KernelMergeGate) Check(ctx context.Context, saved storage.ProductTargetSnapshot, requestID string) (MergeAssessment, error) {
	var empty MergeAssessment
	if g.Publication == nil || g.Source == nil || g.API == nil || g.Cloud.Store == nil {
		return empty, errors.New("canonical quality/source, Cloud journal and merge read transport required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	p := saved.Value
	host, ok := g.Repositories[p.WorkItem.RepositoryID]
	if !ok || host.ReadReport == nil || host.CIRefMode != "head" && host.CIRefMode != "test_merge" || len(host.RequiredChecks) < 1 || len(host.RequiredChecks) > 128 {
		return empty, errors.New("explicit bounded merge policy required")
	}
	// Freeze caller-owned slices and reject ambiguous or permissive check rules.
	required := slices.Clone(host.RequiredChecks)
	for i := range required {
		required[i].AllowedConclusions = slices.Clone(required[i].AllowedConclusions)
	}
	seenNames := map[string]bool{}
	for _, req := range required {
		if strings.TrimSpace(req.Name) == "" || req.AppID < 1 || seenNames[req.Name] || len(req.AllowedConclusions) != 1 || req.AllowedConclusions[0] != "success" {
			return empty, errors.New("unique required CI name/issuer and success conclusion required")
		}
		seenNames[req.Name] = true
	}
	report := func() error {
		r, err := host.ReadReport(ctx)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if g.Cloud.Store.Now != nil {
			now = g.Cloud.Store.Now().UTC()
		}
		if r.Version != 1 || r.ObservedAt.IsZero() || r.ObservedAt.After(now) || r.RepositoryID != p.WorkItem.RepositoryID || r.Repository != p.Repository || r.DefaultBranch != p.BaseBranch || !r.AutoMergeReady() {
			return errors.New("complete merge capability contract remains unverified")
		}
		for _, name := range contract.MergeRequirements {
			if strings.TrimSpace(r.Capabilities[name].Evidence) == "" {
				return errors.New("merge capability evidence missing")
			}
		}
		return nil
	}
	if err := report(); err != nil {
		return empty, err
	}
	if err := g.Source(ctx, saved); err != nil {
		return empty, err
	}
	publication, err := g.Publication(ctx, saved)
	if err != nil {
		return empty, err
	}
	cloud, err := g.Cloud.Read(ctx, requestID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(cloud.Round.Product, saved) || !cloud.Decision.Approved || cloud.Decision.Blocking || len(cloud.Decision.ReviewIDs) == 0 || len(cloud.Round.Packet.CloudReviewerIDs) == 0 {
		return empty, errors.New("current assigned Cloud round lacks formal approval")
	}
	for _, id := range cloud.Decision.ReviewIDs {
		assigned := false
		for _, v := range cloud.Reviews {
			if v.ID == id && slices.Contains(cloud.Round.Packet.CloudReviewerIDs, v.User.ID) {
				assigned = true
			}
		}
		if !assigned {
			return empty, errors.New("approval actor was not assigned to original request")
		}
	}
	transport := g.API(p)
	if transport == nil {
		return empty, errors.New("merge read transport missing")
	}
	api := &budgetResourceAPI{ResourceAPI: transport}
	tested := p.Candidate.SHA
	if host.CIRefMode == "test_merge" {
		tested = cloud.PR.MergeCommitSHA
		if len(tested) != 40 || strings.Trim(tested, "0123456789abcdef") != "" {
			return empty, errors.New("test merge SHA unavailable")
		}
		raw, err := api.ResourceGET(ctx, "commits/"+tested)
		if err != nil {
			return empty, err
		}
		var commit struct {
			SHA     string `json:"sha"`
			Parents []struct {
				SHA string `json:"sha"`
			} `json:"parents"`
		}
		if err := json.Unmarshal(raw, &commit); err != nil {
			return empty, err
		}
		if commit.SHA != tested || len(commit.Parents) != 2 || !((commit.Parents[0].SHA == p.Candidate.SHA && commit.Parents[1].SHA == p.Candidate.BaseSHA) || (commit.Parents[1].SHA == p.Candidate.SHA && commit.Parents[0].SHA == p.Candidate.BaseSHA)) {
			return empty, errors.New("test merge parent mapping changed")
		}
	}
	rawChecks, err := api.ResourcePages(ctx, "commits/"+tested+"/check-runs?per_page=100", "check_runs")
	if err != nil {
		return empty, err
	}
	checks := make([]github.Check, 0, len(rawChecks))
	seen := map[int64]bool{}
	now := time.Now().UTC()
	if g.Cloud.Store.Now != nil {
		now = g.Cloud.Store.Now().UTC()
	}
	for _, raw := range rawChecks {
		var c github.Check
		if err := json.Unmarshal(raw, &c); err != nil {
			return empty, err
		}
		if c.ID < 1 || seen[c.ID] || c.App.ID < 1 || c.StartedAt.After(now) || c.CompletedAt.After(now) {
			return empty, errors.New("invalid CI identity or future timestamp")
		}
		seen[c.ID] = true
		checks = append(checks, c)
	}
	var checkIDs []int64
	for _, req := range required {
		latest := gate.LatestCheck(checks, req, tested)
		if latest == nil || latest.Status != "completed" || latest.Conclusion != "success" || latest.CompletedAt.IsZero() || latest.CompletedAt.Before(latest.StartedAt) {
			return empty, fmt.Errorf("required CI check missing or unsuccessful: %s", req.Name)
		}
		checkIDs = append(checkIDs, latest.ID)
	}
	rawComments, err := api.ResourcePages(ctx, fmt.Sprintf("pulls/%d/comments?per_page=100", p.PRNumber), "")
	if err != nil {
		return empty, err
	}
	var comments []github.Comment
	for _, raw := range rawComments {
		var c github.Comment
		if err := json.Unmarshal(raw, &c); err != nil {
			return empty, err
		}
		comments = append(comments, c)
	}
	policy, ok := g.Cloud.Repositories[p.WorkItem.RepositoryID]
	if !ok || len(review.Findings(p.Candidate, review.Round{ID: requestID, Candidate: p.Candidate, PR: p.PRNumber, RequestedAt: cloud.Round.RequestedAt, RequestCommentID: cloud.Round.RequestCommentID, RequesterID: cloud.Round.Packet.RequesterID, RequestAttempted: cloud.Round.RequestAttempted}, policy.Policy, cloud.Reviews, comments)) > 0 {
		return empty, errors.New("review feedback remains unresolved")
	}
	conditions, err := transport.ResourceConditions(ctx, p.PRNumber)
	if err != nil {
		return empty, err
	}
	if conditions != "CLEAN" {
		return empty, errors.New("repository rules/checks/conversations not satisfied")
	}
	// Re-read formal approval and all PR/repository bookends after CI/rules reads,
	// then source/local quality and capabilities. Nothing persists merge authority.
	final, err := g.Cloud.Read(ctx, requestID)
	if err != nil {
		return empty, err
	}
	cloud.ObservedAt = final.ObservedAt
	slices.Sort(cloud.Decision.ReviewIDs)
	slices.Sort(final.Decision.ReviewIDs)
	if !reflect.DeepEqual(cloud, final) {
		return empty, errors.New("Cloud/PR facts changed during merge assessment")
	}
	finalChecks, err := api.ResourcePages(ctx, "commits/"+tested+"/check-runs?per_page=100", "check_runs")
	if err != nil {
		return empty, err
	}
	finalComments, err := api.ResourcePages(ctx, fmt.Sprintf("pulls/%d/comments?per_page=100", p.PRNumber), "")
	if err != nil {
		return empty, err
	}
	finalConditions, err := transport.ResourceConditions(ctx, p.PRNumber)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(rawChecks, finalChecks) || !reflect.DeepEqual(rawComments, finalComments) || conditions != finalConditions {
		return empty, errors.New("CI/rules/review feedback changed during merge assessment")
	}

	if err := g.Source(ctx, saved); err != nil {
		return empty, err
	}
	fresh, err := g.Publication(ctx, saved)
	if err != nil {
		return empty, err
	}
	if publication != fresh {
		return empty, storage.ErrProductQualityHeld
	}
	if err := report(); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	now = time.Now().UTC()
	if g.Cloud.Store.Now != nil {
		now = g.Cloud.Store.Now().UTC()
	}
	return MergeAssessment{ProductDigest: saved.Digest, OperationID: requestID, RoundDigest: cloud.Round.Digest, HeadSHA: p.Candidate.SHA, BaseSHA: p.Candidate.BaseSHA, TestedSHA: tested, ReviewIDs: slices.Clone(cloud.Decision.ReviewIDs), CheckIDs: checkIDs, ObservedAt: now}, nil
}
