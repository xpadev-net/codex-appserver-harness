package kernelcli

import (
	"context"
	"encoding/json"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/product"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func productFinalPipeline(e product.Executor, policies productMergePolicies, sources map[int64]product.IssueSourcePolicy, executable string) (product.FinalPlanner, product.FinalRunner) {
	gate := product.KernelFinalGate{Executor: e, Source: (product.KernelSourceGate{Store: e.Store, Policies: sources, API: product.GitHubResourceAPI}).Check, Policies: policies.merge}
	planner := product.FinalPlanner{Store: e.Store, Gate: gate.Check, API: product.GitHubResourceAPI}
	runner := product.FinalRunner{Planner: planner, Runner: e.Runner}
	runner.ResolveRemote = func(ctx context.Context, target contract.Target, id int64) (string, error) {
		host, ok := e.Repositories[id]
		if !ok {
			return "", storage.ErrProductFinalPlanHeld
		}
		api := product.GitHubResourceAPI(storage.ProductTarget{Repository: host.Name, WorkItem: domain.WorkItem{RepositoryID: id}})
		if api == nil {
			return "", storage.ErrProductFinalPlanHeld
		}
		raw, err := api.ResourceGET(github.WithResourceReadBudget(ctx, 4<<20), "")
		if err != nil {
			return "", err
		}
		var metadata product.Repository
		if len(raw) > 4<<20 || json.Unmarshal(raw, &metadata) != nil || metadata.ID != id || metadata.Name != host.Name || metadata.DefaultBranch != host.BaseBranch {
			return "", storage.ErrProductFinalPlanHeld
		}
		if err := workspace.ValidateCommitFetchRemote(metadata.CloneURL); err != nil {
			return "", err
		}
		return metadata.CloneURL, nil
	}
	runner.Runner.Executable = executable
	return planner, runner
}
