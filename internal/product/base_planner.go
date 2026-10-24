package product

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// BasePlanner pins an unused product writer's actual default HEAD and proven
// predecessor ancestry. It neither fetches nor grants workspace/model dispatch.
type BasePlanner struct {
	Executor Executor
	Source   func(context.Context, storage.ProductTargetSnapshot) error
	API      func(storage.ProductTarget) ResourceAPI
}

func (p BasePlanner) Prepare(ctx context.Context, taskID string) (storage.ProductBasePlan, error) {
	var empty storage.ProductBasePlan
	if p.Executor.Store == nil || p.Source == nil || p.API == nil {
		return empty, errors.New("explicit base-plan host, source gate and read transport required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	k := p.Executor.Store
	prep, err := k.ReadProductTaskPreparation(ctx, taskID)
	if err != nil {
		return empty, err
	}
	repo, err := p.Executor.repository(ctx, prep.Task)
	if err != nil {
		return empty, err
	}
	policy := repo.Policy
	policy.WaveID, policy.PolicyRevision = prep.Task.WaveID, prep.Task.PolicyRevision
	if _, err := k.RegisterProductVerificationPolicy(ctx, prep.Task.Target(), policy); err != nil {
		return empty, err
	}
	input, err := k.ReadProductBaseInput(ctx, taskID)
	if err != nil {
		return empty, err
	}
	product := storage.ProductTargetSnapshot{Value: storage.ProductTarget{Target: prep.Task.Target(), Repository: repo.Name, BaseBranch: repo.BaseBranch, WorkItem: domain.WorkItem{ID: prep.Registration.Value.Definition.ID, RepositoryID: repo.Policy.RepositoryID}}}
	if prep.Product != nil {
		product = *prep.Product
	}
	if err := p.Source(ctx, product); err != nil {
		return empty, err
	}
	ancestors, err := input.RequiredAncestors(repo.InitialSHA)
	if err != nil {
		return empty, err
	}
	api := p.API(product.Value)
	if api == nil {
		return empty, errors.New("base-plan repository transport missing")
	}
	requests, remaining := 0, 16<<20
	read := func(path string, out any) error {
		if requests >= 260 || remaining <= 0 {
			return errors.New("base-plan read budget exhausted")
		}
		requests++
		raw, err := api.ResourceGET(ctx, path)
		remaining -= len(raw)
		if err != nil {
			return err
		}
		if remaining < 0 || len(raw) > 4<<20 {
			return errors.New("base-plan response exceeds read budget")
		}
		return json.Unmarshal(raw, out)
	}
	metadata := func() (Repository, error) {
		var got Repository
		if err := read("", &got); err != nil {
			return got, err
		}
		if got.ID != repo.Policy.RepositoryID || got.Name != repo.Name || got.DefaultBranch != repo.BaseBranch {
			return got, storage.ErrProductBasePlanHeld
		}
		if err := workspace.ValidateCommitFetchRemote(got.CloneURL); err != nil {
			return got, err
		}
		return got, nil
	}
	head := func() (string, error) {
		var ref struct {
			Ref    string `json:"ref"`
			Object struct {
				SHA  string `json:"sha"`
				Type string `json:"type"`
			} `json:"object"`
		}
		if err := read("git/ref/heads/"+url.PathEscape(repo.BaseBranch), &ref); err != nil {
			return "", err
		}
		if ref.Ref != "refs/heads/"+repo.BaseBranch || ref.Object.Type != "commit" || len(ref.Object.SHA) != 40 || strings.Trim(ref.Object.SHA, "0123456789abcdef") != "" {
			return "", storage.ErrProductBasePlanHeld
		}
		return ref.Object.SHA, nil
	}
	first, err := metadata()
	if err != nil {
		return empty, err
	}
	sha, err := head()
	if err != nil {
		return empty, err
	}
	for _, ancestor := range ancestors {
		if ancestor == sha {
			continue
		}
		var comparison struct {
			Status     string `json:"status"`
			BaseCommit struct {
				SHA string `json:"sha"`
			} `json:"base_commit"`
			MergeBaseCommit struct {
				SHA string `json:"sha"`
			} `json:"merge_base_commit"`
		}
		if err := read("compare/"+ancestor+"..."+sha+"?per_page=1", &comparison); err != nil {
			return empty, err
		}
		if comparison.Status != "ahead" || comparison.BaseCommit.SHA != ancestor || comparison.MergeBaseCommit.SHA != ancestor {
			return empty, storage.ErrProductBasePlanHeld
		}
	}
	last, err := head()
	if err != nil {
		return empty, err
	}
	if last != sha {
		return empty, storage.ErrProductBasePlanHeld
	}
	final, err := metadata()
	if err != nil {
		return empty, err
	}
	if final != first {
		return empty, storage.ErrProductBasePlanHeld
	}
	if err := p.Source(ctx, product); err != nil {
		return empty, err
	}
	now := time.Now().UTC()
	if k.Now != nil {
		now = k.Now().UTC()
	}
	observation := storage.ProductBaseObservation{Head: storage.ProductBaseHead{RepositoryID: repo.Policy.RepositoryID, Repository: repo.Name, BaseBranch: repo.BaseBranch, InitialSHA: repo.InitialSHA, HeadSHA: sha, Remote: first.CloneURL, Ancestors: ancestors}, ObservedAt: now}
	return k.PrepareProductBasePlan(ctx, taskID, input.Digest, observation)
}
