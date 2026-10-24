package kernelcli

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/review"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Merge is an additional explicit host opt-in. Configuration is not native
// capability evidence: every parent/client gate reloads the private report.
type ProductMergeConfig struct {
	Workers      int                       `json:"workers"`
	Repositories []ProductMergeRouteConfig `json:"repositories"`
}
type ProductMergeRouteConfig struct {
	RepositoryID       int64                `json:"repository_id"`
	Method             string               `json:"method"`
	CIRefMode          string               `json:"ci_ref_mode"`
	RequiredChecks     []gate.RequiredCheck `json:"required_checks"`
	TrustedReviewerIDs []int64              `json:"trusted_reviewer_ids"`
	RequiredApprovals  int                  `json:"required_approvals"`
}

type productMergePolicies struct {
	merge   map[int64]product.KernelMergePolicy
	cloud   map[int64]product.CloudReviewPolicy
	methods map[int64]string
}

func (c *ProductMergeConfig) policies(repositories []product.ExecutionRepository, routes map[int64]product.CloudReviewRoute) (productMergePolicies, error) {
	p := productMergePolicies{merge: map[int64]product.KernelMergePolicy{}, cloud: map[int64]product.CloudReviewPolicy{}, methods: map[int64]string{}}
	if c == nil {
		return p, nil
	}
	if c.Workers < 1 || c.Workers > 8 || len(c.Repositories) < 1 || len(c.Repositories) > 128 {
		return p, errors.New("merge workers 1..8 and repositories 1..128 required")
	}
	known := map[int64]product.ExecutionRepository{}
	for _, repo := range repositories {
		known[repo.Policy.RepositoryID] = repo
	}
	for _, route := range c.Repositories {
		repo, exists := known[route.RepositoryID]
		request, requested := routes[route.RepositoryID]
		_, duplicate := p.methods[route.RepositoryID]
		if !exists || !requested || duplicate || request.ReadReport == nil || len(request.CloudReviewerIDs) == 0 || (route.Method != "merge" && route.Method != "squash" && route.Method != "rebase") || (route.CIRefMode != "head" && route.CIRefMode != "test_merge") {
			return p, errors.New("unique known merge repository, pinned Cloud route, method and CI ref mode required")
		}
		actors, err := storage.CanonicalCloudReviewerIDs(route.TrustedReviewerIDs)
		if err != nil || len(actors) == 0 || route.RequiredApprovals < 1 || route.RequiredApprovals > len(actors) {
			return p, errors.New("explicit positive trusted Cloud actors and required approval count required")
		}
		for _, id := range actors {
			if !slices.Contains(request.CloudReviewerIDs, id) {
				return p, errors.New("merge approval actor must belong to the configured Cloud request route")
			}
		}
		if len(route.RequiredChecks) < 1 || len(route.RequiredChecks) > 128 {
			return p, errors.New("merge requires 1..128 named CI checks")
		}
		checks := slices.Clone(route.RequiredChecks)
		seen := map[string]bool{}
		for i, check := range checks {
			if strings.TrimSpace(check.Name) == "" || len(check.Name) > 256 || !utf8.ValidString(check.Name) || strings.ContainsAny(check.Name, "\x00\r\n") || check.AppID < 1 || seen[check.Name] || len(check.AllowedConclusions) != 1 || check.AllowedConclusions[0] != "success" {
				return p, errors.New("unique bounded CI name, positive issuer and success-only conclusion required")
			}
			seen[check.Name] = true
			checks[i].AllowedConclusions = slices.Clone(check.AllowedConclusions)
		}
		r, err := request.ReadReport(context.Background())
		if err != nil {
			return p, err
		}
		if r.Version != 1 || r.RepositoryID != route.RepositoryID || r.Repository != repo.Name || r.DefaultBranch != repo.BaseBranch || r.ObservedAt.IsZero() || r.ObservedAt.After(time.Now().UTC()) {
			return p, errors.New("merge capability report must identify the configured repository/default branch")
		}
		// These policy flags do not make an unknown capability verified. The
		// Cloud reader checks all three observed capabilities, and the canonical
		// merge gate checks every MergeRequirement on every actual assessment.
		p.cloud[route.RepositoryID] = product.CloudReviewPolicy{Policy: review.Policy{VerifiedRequest: true, VerifiedNativeApproval: true, VerifiedSHAMapping: true, TrustedReviewerIDs: actors, RequiredApprovals: route.RequiredApprovals}, ReadReport: request.ReadReport}
		p.merge[route.RepositoryID] = product.KernelMergePolicy{CIRefMode: route.CIRefMode, RequiredChecks: checks, ReadReport: request.ReadReport}
		p.methods[route.RepositoryID] = route.Method
	}
	return p, nil
}

func productMergeDriver(e product.Executor, policies productMergePolicies, sources map[int64]product.IssueSourcePolicy, executable string) product.MergeDriver {
	source := product.KernelSourceGate{Store: e.Store, Policies: sources, API: product.GitHubResourceAPI}
	g := product.KernelMergeGate{
		Publication: e.PublicationGate, Source: source.Check,
		Cloud: product.CloudReviewReader{Store: e.Store, Repositories: policies.cloud, API: product.GitHubResourceAPI},
		API: func(p storage.ProductTarget) product.MergeReadAPI {
			return github.Client{Repository: p.Repository, RepositoryID: p.WorkItem.RepositoryID}
		},
		Repositories: policies.merge,
	}
	return product.MergeDriver{Store: e.Store, Gate: g.Check, Transport: product.MergeHTTPTransport{Store: e.Store, Executable: executable}}
}

func runProductMergePipeline(ctx context.Context, driver product.MergeDriver, methods map[int64]string, workers int, every time.Duration, wake func(), report func(error), others ...func(context.Context) error) error {
	// Acquire the private singleton authority before starting any pipeline
	// worker. The listener backlog admits the wrapper even before Serve runs.
	authority, err := product.ListenMergeHTTPAuthority(driver.Store, func(ctx context.Context, job storage.ProductMerge) (storage.ProductMergeGateObservation, error) {
		return driver.Gate(ctx, job.Value.Product, job.Value.RequestID)
	})
	if err != nil {
		return err
	}
	defer authority.Close()
	dispatch := product.MergeDispatchWorker{Driver: driver, Methods: methods, Workers: workers, Every: every, OnMutation: wake, Report: report}
	all := append(slices.Clone(others), authority.Serve, dispatch.Run)
	return runProductPipeline(ctx, all...)
}
