package product

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// FinalPlanner only observes and pins verification inputs. Gate must recheck
// current source/host capability policy; this adapter never publishes, runs a
// model/runner or turns a saved merge into final acceptance.
type FinalPlanner struct {
	Store *storage.KernelStore
	Gate  func(context.Context, contract.Target) error
	API   func(storage.ProductTarget) ResourceAPI
}

func (p FinalPlanner) Prepare(ctx context.Context, target contract.Target) (storage.ProductFinalPlan, error) {
	var empty storage.ProductFinalPlan
	if p.Store == nil || p.Gate == nil || p.API == nil {
		return empty, errors.New("explicit final-plan store, source/capability gate and read transport required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	coverage, err := p.Store.ProductCoverage(ctx, target)
	if err != nil {
		return empty, err
	}
	if len(coverage.Value.Merged) == 0 || len(coverage.Value.Residual)+len(coverage.Value.Pending)+len(coverage.Value.Unavailable)+len(coverage.Value.UnmergedWorkItems) != 0 {
		return empty, storage.ErrProductFinalPlanHeld
	}
	if err := p.Gate(ctx, target); err != nil {
		return empty, err
	}
	type repository struct {
		product storage.ProductTarget
		merges  map[string]bool
	}
	repositories := map[int64]*repository{}
	for _, fact := range coverage.Value.Merged {
		receipt, err := p.Store.ReadProductMergedWorkItem(ctx, fact.MergeID)
		if err != nil {
			return empty, err
		}
		if receipt.Digest != fact.ReceiptDigest {
			return empty, storage.ErrTaskEvidence
		}
		value := receipt.Value.Product.Value
		repo := repositories[value.WorkItem.RepositoryID]
		if repo == nil {
			repo = &repository{value, map[string]bool{}}
			repositories[value.WorkItem.RepositoryID] = repo
		}
		if repo.product.Repository != value.Repository || repo.product.BaseBranch != value.BaseBranch {
			return empty, storage.ErrTaskEvidence
		}
		repo.merges[fact.MergeSHA] = true
	}
	ids := make([]int64, 0, len(repositories))
	for id := range repositories {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	requests, remaining := 0, 16<<20
	read := func(api ResourceAPI, path string, value any) error {
		if requests >= 2048 || remaining <= 0 {
			return errors.New("final-plan read budget exhausted")
		}
		requests++
		raw, err := api.ResourceGET(ctx, path)
		remaining -= len(raw)
		if err != nil {
			return err
		}
		if remaining < 0 || len(raw) > 4<<20 {
			return errors.New("final-plan response exceeds read budget")
		}
		return json.Unmarshal(raw, value)
	}
	var observations []storage.ProductFinalHeadObservation
	for _, id := range ids {
		repo := repositories[id]
		value := repo.product
		api := p.API(value)
		if api == nil {
			return empty, errors.New("final-plan repository transport missing")
		}
		readRepo := func() (Repository, error) {
			var metadata Repository
			err := read(api, "", &metadata)
			if err == nil && (metadata.ID != id || metadata.Name != value.Repository || metadata.DefaultBranch != value.BaseBranch) {
				err = storage.ErrProductFinalPlanHeld
			}
			return metadata, err
		}
		readHead := func() (string, error) {
			var ref struct {
				Ref    string `json:"ref"`
				Object struct {
					SHA  string `json:"sha"`
					Type string `json:"type"`
				} `json:"object"`
			}
			if err := read(api, "git/ref/heads/"+url.PathEscape(value.BaseBranch), &ref); err != nil {
				return "", err
			}
			if ref.Ref != "refs/heads/"+value.BaseBranch || ref.Object.Type != "commit" || len(ref.Object.SHA) != 40 || strings.Trim(ref.Object.SHA, "0123456789abcdef") != "" {
				return "", storage.ErrProductFinalPlanHeld
			}
			return ref.Object.SHA, nil
		}
		if _, err := readRepo(); err != nil {
			return empty, err
		}
		head, err := readHead()
		if err != nil {
			return empty, err
		}
		var ancestors []string
		for sha := range repo.merges {
			ancestors = append(ancestors, sha)
		}
		sort.Strings(ancestors)
		for _, sha := range ancestors {
			if sha == head {
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
			if err := read(api, "compare/"+sha+"..."+head+"?per_page=1", &comparison); err != nil {
				return empty, err
			}
			if comparison.Status != "ahead" || comparison.BaseCommit.SHA != sha || comparison.MergeBaseCommit.SHA != sha {
				return empty, storage.ErrProductFinalPlanHeld
			}
		}
		last, err := readHead()
		if err != nil || last != head {
			return empty, errors.Join(storage.ErrProductFinalPlanHeld, err)
		}
		if _, err := readRepo(); err != nil {
			return empty, err
		}
		now := time.Now().UTC()
		if p.Store.Now != nil {
			now = p.Store.Now().UTC()
		}
		observations = append(observations, storage.ProductFinalHeadObservation{RepositoryID: id, Repository: value.Repository, BaseBranch: value.BaseBranch, HeadSHA: head, AncestorMergeSHAs: ancestors, ObservedAt: now})
	}
	if err := p.Gate(ctx, target); err != nil {
		return empty, err
	}
	return p.Store.PrepareProductFinalPlan(ctx, target, coverage.Digest, observations)
}
