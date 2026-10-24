package product

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// CloudCompletionReader measures terminal provider responses for an original
// request. It requires the original comparison base; unlike formal merge
// assessment it permits a closed PR or newer head,
// but never adopts that head or returns an approval decision.
type CloudCompletionReader struct {
	Store  *storage.KernelStore
	Routes map[int64]CloudReviewRoute
	API    func(storage.ProductTarget) ResourceAPI
}

func (r CloudCompletionReader) now() time.Time {
	if r.Store.Now != nil {
		return r.Store.Now().UTC()
	}
	return time.Now().UTC()
}

func (r CloudCompletionReader) report(ctx context.Context, route CloudReviewRoute, p storage.ProductTarget) (contract.Report, error) {
	report, err := route.ReadReport(ctx)
	if err != nil {
		return report, err
	}
	if report.Version != 1 || report.ObservedAt.IsZero() || report.ObservedAt.After(r.now()) || report.RepositoryID != p.WorkItem.RepositoryID || report.Repository != p.Repository || report.DefaultBranch != p.BaseBranch {
		return report, storage.ErrProductCloudHeld
	}
	for _, name := range []string{"cloud_request", "review_sha"} {
		c := report.Capabilities[name]
		if c.State != contract.Verified || strings.TrimSpace(c.Evidence) == "" {
			return report, storage.ErrProductCloudHeld
		}
	}
	return report, nil
}

// Reconcile first checks a saved receipt so replay never rescans or rewrites a
// completed job. All network work finishes before the short completion Tx.
func (r CloudCompletionReader) Reconcile(ctx context.Context, id string) (storage.ProductCloudReviewCompletion, error) {
	var empty storage.ProductCloudReviewCompletion
	if r.Store == nil || r.API == nil {
		return empty, errors.New("Cloud completion store and read transport required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if saved, err := r.Store.ReadProductCloudReviewCompletion(ctx, id); err == nil {
		return saved, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return empty, err
	}
	round, err := r.Store.ReadProductCloudRound(ctx, id)
	if err != nil {
		return empty, err
	}
	if len(round.Packet.CloudReviewerIDs) == 0 {
		return empty, storage.ErrProductCloudHeld
	}
	p := round.Product.Value
	route, ok := r.Routes[p.WorkItem.RepositoryID]
	if !ok || route.ReadReport == nil {
		return empty, storage.ErrProductCloudHeld
	}
	if _, err := r.report(ctx, route, p); err != nil {
		return empty, err
	}
	transport := r.API(p)
	if transport == nil {
		return empty, errors.New("missing Cloud completion transport")
	}
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	api := &budgetResourceAPI{ResourceAPI: transport}
	readRepo := func() (Repository, error) {
		var repo Repository
		raw, err := api.ResourceGET(ctx, "")
		if err != nil {
			return repo, err
		}
		err = json.Unmarshal(raw, &repo)
		return repo, err
	}
	repo, err := readRepo()
	if err != nil {
		return empty, err
	}
	if repo.ID != p.WorkItem.RepositoryID || repo.Name != p.Repository || repo.DefaultBranch != p.BaseBranch {
		return empty, storage.ErrProductCloudHeld
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
		return empty, err
	}
	if pr.Number != p.PRNumber || pr.Head.Repo.ID != p.WorkItem.RepositoryID || pr.Base.Repo.ID != p.WorkItem.RepositoryID || pr.Head.Ref != p.WorkItem.Branch || pr.Base.Ref != p.BaseBranch || pr.Base.SHA != p.Candidate.BaseSHA || pr.State != "open" && pr.State != "closed" {
		return empty, storage.ErrProductCloudHeld
	}
	comments, err := api.ResourcePages(ctx, fmt.Sprintf("issues/%d/comments?per_page=100", p.PRNumber), "")
	if err != nil {
		return empty, err
	}
	found := false
	for _, raw := range comments {
		var c github.Comment
		if err := json.Unmarshal(raw, &c); err != nil {
			return empty, err
		}
		if c.ID == round.RequestCommentID {
			if found || c.User.ID != round.Packet.RequesterID || c.Body != round.Packet.Body() {
				return empty, storage.ErrProductCloudHeld
			}
			found = true
		} else if c.User.ID == round.Packet.RequesterID && c.Body == round.Packet.Body() {
			return empty, storage.ErrProductCloudHeld
		}
	}
	if !found {
		return empty, storage.ErrProductCloudHeld
	}
	items, err := api.ResourcePages(ctx, path+"/reviews?per_page=100", "")
	if err != nil {
		return empty, err
	}
	var facts []storage.ProductNativeReviewFact
	seen := map[int64]bool{}
	for _, raw := range items {
		var v github.Review
		if err := json.Unmarshal(raw, &v); err != nil {
			return empty, err
		}
		if v.ID < 1 || seen[v.ID] {
			return empty, storage.ErrProductCloudHeld
		}
		seen[v.ID] = true
		if !slices.Contains(round.Packet.CloudReviewerIDs, v.User.ID) || v.CommitID != p.Candidate.SHA || v.SubmittedAt.Before(round.RequestedAt) {
			continue
		}
		switch v.State {
		case "APPROVED", "COMMENTED", "CHANGES_REQUESTED", "DISMISSED":
		default:
			continue
		}
		facts = append(facts, storage.ProductNativeReviewFact{ID: v.ID, ReviewerID: v.User.ID, State: v.State, SHA: v.CommitID, SubmittedAt: v.SubmittedAt})
		if len(facts) > 128 {
			return empty, storage.ErrProductCloudHeld
		}
	}
	if len(facts) == 0 {
		return empty, storage.ErrProductCloudHeld
	}
	final, err := readPR()
	if err != nil {
		return empty, err
	}
	if final != pr {
		return empty, storage.ErrProductCloudHeld
	}
	lastRepo, err := readRepo()
	if err != nil {
		return empty, err
	}
	if lastRepo.ID != repo.ID || lastRepo.Name != repo.Name || lastRepo.DefaultBranch != repo.DefaultBranch {
		return empty, storage.ErrProductCloudHeld
	}
	baseReport, err := r.report(ctx, route, p)
	if err != nil {
		return empty, err
	}
	readAt := r.now()
	for _, v := range facts {
		if v.SubmittedAt.After(readAt) {
			return empty, storage.ErrProductCloudHeld
		}
	}
	// Issue a new, request-specific measured report only after the acknowledged
	// request comment and assigned provider/SHA response were read and bookended.
	// Never upgrade an unknown route or merely redate its old capability file.
	report := contract.NewReport(p.Repository)
	report.RepositoryID, report.DefaultBranch, report.ObservedAt = p.WorkItem.RepositoryID, p.BaseBranch, readAt
	requestEvidence := fmt.Sprintf("request:%s:%s:comment:%d", id, round.Digest, round.RequestCommentID)
	var responses []string
	for _, v := range facts {
		responses = append(responses, fmt.Sprintf("native-review:%d:provider:%d:sha:%s", v.ID, v.ReviewerID, v.SHA))
	}
	slices.Sort(responses)
	report.Capabilities["cloud_request"] = contract.Capability{State: contract.Verified, Reason: "acknowledged request comment revalidated for this completion", Evidence: baseReport.Capabilities["cloud_request"].Evidence + "\n" + requestEvidence}
	report.Capabilities["review_sha"] = contract.Capability{State: contract.Verified, Reason: "assigned provider response measured for the original request/candidate", Evidence: baseReport.Capabilities["review_sha"].Evidence + "\n" + requestEvidence + "\n" + strings.Join(responses, "\n")}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return r.Store.RecordProductCloudReviewCompleted(ctx, storage.ProductCloudCompletionValue{OperationID: id, RoundDigest: round.Digest, ProofID: id + ":provider-result", ReadAt: readAt, Report: report, Reviews: facts})
}
