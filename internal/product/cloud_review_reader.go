package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type CloudReviewPolicy struct {
	Policy     review.Policy
	ReadReport func(context.Context) (contract.Report, error)
}

type CloudReviewSnapshot struct {
	Round      storage.ProductCloudRound
	PR         github.PR
	Reviews    []github.Review
	Decision   review.Decision
	ObservedAt time.Time
}

// CloudReviewReader collects fresh, bounded, read-only facts. Its decision is
// not a persisted merge grant; merge must independently recheck current source,
// quality, PR, checks/rules and controls immediately before its own send fence.
type CloudReviewReader struct {
	Store        *storage.KernelStore
	Repositories map[int64]CloudReviewPolicy
	API          func(storage.ProductTarget) ResourceAPI
}

func (r CloudReviewReader) Read(ctx context.Context, operationID string) (CloudReviewSnapshot, error) {
	var snapshot CloudReviewSnapshot
	if r.Store == nil || r.API == nil {
		return snapshot, errors.New("Cloud review store and read transport required")
	}
	round, err := r.Store.ReadProductCloudRound(ctx, operationID)
	if err != nil {
		return snapshot, err
	}
	p := round.Product.Value
	host, ok := r.Repositories[p.WorkItem.RepositoryID]
	if !ok || host.ReadReport == nil {
		return snapshot, errors.New("explicit Cloud review policy/report reader required")
	}
	report, err := host.ReadReport(ctx)
	if err != nil {
		return snapshot, err
	}
	if report.Version != 1 || report.ObservedAt.IsZero() || report.RepositoryID != p.WorkItem.RepositoryID || report.Repository != p.Repository || report.DefaultBranch != p.BaseBranch {
		return snapshot, errors.New("Cloud review report identity changed")
	}
	transport := r.API(p)
	if transport == nil {
		return snapshot, errors.New("missing Cloud review read transport")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	api := &budgetResourceAPI{ResourceAPI: transport}
	raw, err := api.ResourceGET(ctx, "")
	if err != nil {
		return snapshot, err
	}
	var repo Repository
	if err := json.Unmarshal(raw, &repo); err != nil {
		return snapshot, err
	}
	if repo.ID != p.WorkItem.RepositoryID || repo.Name != p.Repository || repo.DefaultBranch != p.BaseBranch {
		return snapshot, errors.New("Cloud review repository changed")
	}
	path := fmt.Sprintf("pulls/%d", p.PRNumber)
	readPR := func() (github.PR, error) {
		var pr github.PR
		raw, err := api.ResourceGET(ctx, path)
		if err != nil {
			return pr, err
		}
		err = json.Unmarshal(raw, &pr)
		return pr, err
	}
	pr, err := readPR()
	if err != nil {
		return snapshot, err
	}
	if !matched(pr, storage.PRDispatchClaim{Product: round.Product}) || pr.State != "open" || pr.Draft || pr.Merged || pr.Base.SHA != p.Candidate.BaseSHA {
		return snapshot, errors.New("Cloud review PR/head/base changed")
	}
	comments, err := api.ResourcePages(ctx, fmt.Sprintf("issues/%d/comments?per_page=100", p.PRNumber), "")
	if err != nil {
		return snapshot, err
	}
	found := false
	for _, raw := range comments {
		var c github.Comment
		if err := json.Unmarshal(raw, &c); err != nil {
			return snapshot, err
		}
		if c.ID == round.RequestCommentID {
			if found || c.User.ID != round.Packet.RequesterID || c.Body != round.Packet.Body() {
				return snapshot, errors.New("Cloud request comment changed")
			}
			found = true
		} else if c.User.ID == round.Packet.RequesterID && c.Body == round.Packet.Body() {
			return snapshot, errors.New("ambiguous Cloud request comment")
		}
	}
	if !found {
		return snapshot, errors.New("Cloud request comment missing")
	}
	items, err := api.ResourcePages(ctx, path+"/reviews?per_page=100", "")
	if err != nil {
		return snapshot, err
	}
	reviews := make([]github.Review, 0, len(items))
	seen := map[int64]bool{}
	for _, raw := range items {
		var v github.Review
		if err := json.Unmarshal(raw, &v); err != nil {
			return snapshot, err
		}
		if v.ID < 1 || seen[v.ID] || v.User.ID < 1 {
			return snapshot, errors.New("invalid native review identity")
		}
		seen[v.ID] = true
		reviews = append(reviews, v)
	}
	final, err := readPR()
	if err != nil {
		return snapshot, err
	}
	if final != pr {
		return snapshot, errors.New("Cloud review PR changed during read")
	}
	if err := ctx.Err(); err != nil {
		return snapshot, err
	}
	raw, err = api.ResourceGET(ctx, "")
	if err != nil {
		return snapshot, err
	}
	var lastRepo Repository
	if err := json.Unmarshal(raw, &lastRepo); err != nil {
		return snapshot, err
	}
	if lastRepo.ID != repo.ID || lastRepo.Name != repo.Name || lastRepo.DefaultBranch != repo.DefaultBranch {
		return snapshot, errors.New("Cloud review repository changed during read")
	}
	report, err = host.ReadReport(ctx)
	if err != nil {
		return snapshot, err
	}
	if report.Version != 1 || report.ObservedAt.IsZero() || report.RepositoryID != p.WorkItem.RepositoryID || report.Repository != p.Repository || report.DefaultBranch != p.BaseBranch {
		return snapshot, errors.New("Cloud review report changed during read")
	}
	policy := host.Policy
	verified := func(name string) bool {
		c := report.Capabilities[name]
		return c.State == contract.Verified && strings.TrimSpace(c.Evidence) != ""
	}
	policy.VerifiedRequest = policy.VerifiedRequest && verified("cloud_request")
	policy.VerifiedNativeApproval = policy.VerifiedNativeApproval && verified("cloud_native_approval")
	policy.VerifiedSHAMapping = policy.VerifiedSHAMapping && verified("review_sha")
	if err := ctx.Err(); err != nil {
		return snapshot, err
	}
	observed := time.Now().UTC()
	if r.Store.Now != nil {
		observed = r.Store.Now().UTC()
	}
	if report.ObservedAt.After(observed) {
		return snapshot, errors.New("Cloud capability report timestamp unconfirmed")
	}
	for _, v := range reviews {
		if v.SubmittedAt.After(observed) || v.State != "PENDING" && v.SubmittedAt.IsZero() {
			return snapshot, errors.New("native review timestamp unconfirmed")
		}
	}
	return CloudReviewSnapshot{Round: round, PR: pr, Reviews: reviews, Decision: review.Evaluate(p.Candidate, review.Round{ID: round.Packet.RoundID, Candidate: p.Candidate, PR: p.PRNumber, RequestedAt: round.RequestedAt, RequestCommentID: round.RequestCommentID, RequestAttempted: round.RequestAttempted, RequesterID: round.Packet.RequesterID}, policy, reviews), ObservedAt: observed}, nil
}
