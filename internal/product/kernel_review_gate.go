package product

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// CloudReviewRoute is immutable host configuration. ReadReport must reload a
// trusted capability report on each call, never consume Issue/model claims.
// A request route is not evidence of native approval or merge capability.
type CloudReviewRoute struct {
	CloudReviewerIDs []int64
	Trigger          string
	RequesterID      int64
	ReadReport       func(context.Context) (contract.Report, error)
}

// KernelReviewGate reuses new-journal publication quality and independently
// checks the bound PR and observed Cloud request route. It performs only reads.
type KernelReviewGate struct {
	Publication PublicationGate
	API         ReviewAPIFactory
	Routes      map[int64]CloudReviewRoute
}

func (g KernelReviewGate) Check(ctx context.Context, saved storage.ProductTargetSnapshot) (ReviewRequest, error) {
	var empty ReviewRequest
	if g.Publication == nil || g.API == nil {
		return empty, errors.New("canonical publication gate and read-only review transport required")
	}
	publication, err := g.Publication(ctx, saved)
	if err != nil {
		return empty, err
	}
	p := saved.Value
	if p.PRNumber < 1 {
		return empty, errors.New("bound PR required for Cloud review")
	}
	route, ok := g.Routes[p.WorkItem.RepositoryID]
	if !ok || route.ReadReport == nil || route.RequesterID < 1 || strings.TrimSpace(route.Trigger) == "" || len(route.Trigger) > 16384 || !utf8.ValidString(route.Trigger) || strings.ContainsRune(route.Trigger, '\x00') {
		return empty, errors.New("explicit bounded Cloud review request route required")
	}
	actors, err := storage.CanonicalCloudReviewerIDs(route.CloudReviewerIDs)
	if err != nil {
		return empty, err
	}
	if err := github.CheckClosingText(route.Trigger); err != nil {
		return empty, err
	}
	report, err := route.ReadReport(ctx)
	if err != nil {
		return empty, err
	}
	capability := report.Capabilities["cloud_request"]
	if report.Version != 1 || report.ObservedAt.IsZero() || report.RepositoryID != p.WorkItem.RepositoryID || report.Repository != p.Repository || report.DefaultBranch != p.BaseBranch || capability.State != contract.Verified || strings.TrimSpace(capability.Evidence) == "" {
		return empty, errors.New("Cloud review request capability is unverified for this repository/base")
	}
	api := g.API(p, func(context.Context, string, string) error { return errors.New("review gate forbids HTTP writes") })
	if api == nil {
		return empty, errors.New("missing read-only review transport")
	}
	if err := verifyReviewRepository(ctx, api, p, true); err != nil {
		return empty, err
	}
	pr, err := api.Pull(ctx, p.PRNumber)
	if err != nil {
		return empty, err
	}
	if !matched(pr, storage.PRDispatchClaim{Product: saved}) || pr.State != "open" || pr.Draft || pr.Merged || pr.Base.SHA != p.Candidate.BaseSHA {
		return empty, errors.New("review PR identity, head or comparison base changed")
	}
	// A report/remote read can overlap a local revision or quality change.
	// Revalidate the journal after those reads; the requester also repeats this
	// whole gate immediately before its separate durable HTTP write fence.
	fresh, err := g.Publication(ctx, saved)
	if err != nil {
		return empty, err
	}
	if fresh != publication {
		return empty, storage.ErrProductQualityHeld
	}
	return ReviewRequest{CloudReviewerIDs: actors, Trigger: route.Trigger, RequesterID: route.RequesterID}, nil
}
