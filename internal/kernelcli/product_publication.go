package kernelcli

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// Publication is a second, explicit host opt-in; enabling local preparation
// alone never starts GitHub writes. Merge needs another explicit host opt-in;
// these settings do not enable model readiness.
type ProductPublicationConfig struct {
	Split          *ProductSplitDispatchConfig `json:"split,omitempty"`
	Merge          *ProductMergeConfig         `json:"merge,omitempty"`
	Workers        int                         `json:"workers"`
	ReviewRoutes   []ProductReviewRouteConfig  `json:"review_routes"`
	SourcePolicies []product.IssueSourcePolicy `json:"source_policies,omitempty"`
}

type ProductReviewRouteConfig struct {
	CloudReviewerIDs []int64 `json:"cloud_reviewer_ids,omitempty"`
	RepositoryID     int64   `json:"repository_id"`
	Trigger          string  `json:"trigger"`
	RequesterID      int64   `json:"requester_id"`
	CapabilitiesPath string  `json:"capabilities_path"`
}

func readProductCapabilities(ctx context.Context, path string) (contract.Report, error) {
	var report contract.Report
	if err := ctx.Err(); err != nil {
		return report, err
	}
	raw, err := readFile(path, 1<<20, true)
	if err != nil {
		return report, err
	}
	if err := contract.DecodeStrict(raw, &report); err != nil {
		return report, err
	}
	return report, ctx.Err()
}

func (c *ProductPublicationConfig) routes(repositories []product.ExecutionRepository) (map[int64]product.CloudReviewRoute, []string, error) {
	if c == nil {
		return nil, nil, nil
	}
	if c.Workers < 1 || c.Workers > 8 || len(c.ReviewRoutes) > 128 {
		return nil, nil, errors.New("publication workers 1..8 and review_routes at most 128 required")
	}
	known := map[int64]bool{}
	for _, repo := range repositories {
		known[repo.Policy.RepositoryID] = true
	}
	routes := map[int64]product.CloudReviewRoute{}
	var hidden []string
	for _, route := range c.ReviewRoutes {
		if _, exists := routes[route.RepositoryID]; exists || !known[route.RepositoryID] || route.RequesterID < 1 || strings.TrimSpace(route.Trigger) == "" || len(route.Trigger) > 16384 || !utf8.ValidString(route.Trigger) || strings.ContainsRune(route.Trigger, '\x00') {
			return nil, nil, errors.New("unique configured repository and bounded explicit review route required")
		}
		if err := github.CheckClosingText(route.Trigger); err != nil {
			return nil, nil, err
		}
		actors, err := storage.CanonicalCloudReviewerIDs(route.CloudReviewerIDs)
		if err != nil {
			return nil, nil, err
		}
		path := route.CapabilitiesPath
		real, err := filepath.EvalSymlinks(path)
		if err != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || real != path {
			return nil, nil, errors.New("canonical existing capability report path required")
		}
		if _, err := readProductCapabilities(context.Background(), path); err != nil {
			return nil, nil, err
		}
		routes[route.RepositoryID] = product.CloudReviewRoute{CloudReviewerIDs: actors, Trigger: route.Trigger, RequesterID: route.RequesterID, ReadReport: func(ctx context.Context) (contract.Report, error) { return readProductCapabilities(ctx, path) }}
		hidden = append(hidden, path)
	}
	return routes, hidden, nil
}

func (c *ProductPublicationConfig) sources(repositories []product.ExecutionRepository) (map[int64]product.IssueSourcePolicy, error) {
	if c == nil {
		return nil, nil
	}
	if len(c.SourcePolicies) > 128 {
		return nil, errors.New("source_policies at most 128 required")
	}
	known := map[int64]string{}
	for _, repo := range repositories {
		known[repo.Policy.RepositoryID] = repo.Name
	}
	policies := map[int64]product.IssueSourcePolicy{}
	for _, policy := range c.SourcePolicies {
		if err := policy.Validate(); err != nil {
			return nil, err
		}
		if _, exists := policies[policy.RepositoryID]; exists || known[policy.RepositoryID] != policy.Repository {
			return nil, errors.New("unique configured Issue source repository required")
		}
		policies[policy.RepositoryID] = policy
	}
	return policies, nil
}

func productPublicationDispatcher(e product.Executor, routes map[int64]product.CloudReviewRoute, sources map[int64]product.IssueSourcePolicy, workers int, every time.Duration) product.Dispatcher {
	source := product.KernelSourceGate{Store: e.Store, Policies: sources, API: product.GitHubResourceAPI}
	publication := func(ctx context.Context, saved storage.ProductTargetSnapshot) (product.Publication, error) {
		if err := source.Check(ctx, saved); err != nil {
			return product.Publication{}, err
		}
		return e.PublicationGate(ctx, saved)
	}
	branch := product.BranchPublisher{Store: e.Store, Gate: publication, API: product.GitHubAPI, Push: workspace.PublishPinned}
	gate := product.KernelReviewGate{Publication: publication, API: product.GitHubReviewAPI, Routes: routes}
	return product.Dispatcher{Store: e.Store, Publisher: product.Publisher{Store: e.Store, Gate: publication, API: product.GitHubAPI, Branch: branch.Publish}, Reviewer: product.ReviewRequester{Store: e.Store, Gate: gate.Check, API: product.GitHubReviewAPI}, Workers: workers, Every: every}
}

// Both workers keep the shared controller/store owner until physical shutdown
// completes. Failure or unexpected exit of either cancels and joins the other.
func runProductPipeline(ctx context.Context, workers ...func(context.Context) error) error {
	if len(workers) == 0 {
		return errors.New("product pipeline workers required")
	}
	running, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, len(workers))
	for _, worker := range workers {
		go func() { done <- worker(running) }()
	}
	first := <-done
	if first == nil && ctx.Err() == nil {
		first = errors.New("product pipeline worker stopped unexpectedly")
	}
	cancel()
	failures := []error{first}
	for remaining := len(workers) - 1; remaining > 0; remaining-- {
		failures = append(failures, <-done)
	}
	return errors.Join(failures...)
}

// Poll configured sources as well as checking them at send time. This allows
// a persisted read-failure/hold to recover before dispatch preflight, which
// correctly refuses to enter the publication gate while that hold is active.
func runPublicationSources(ctx context.Context, k *storage.KernelStore, sources map[int64]product.IssueSourcePolicy, every time.Duration, wake func(), report func(error)) error {
	ids := make([]int64, 0, len(sources))
	for id := range sources {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for {
		for _, id := range ids {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p := sources[id]
			o := product.IssueSourceObserver{Store: k, API: github.Client{Repository: p.Repository, RepositoryID: p.RepositoryID}, Policy: p, Attempts: 32, Every: every, OnMutation: wake, Report: report}
			_, err := o.Scan(ctx)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil && report != nil {
				report(err)
			}
			children := product.SplitChildSourceObserver{Store: k, API: o.API, Policy: p, Attempts: 32, OnMutation: wake, Report: report}
			_, err = children.Scan(ctx)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil && report != nil {
				report(err)
			}
		}
		timer := time.NewTimer(every)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
