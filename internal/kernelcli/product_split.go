package kernelcli

import (
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/product"
)

// Split writes are a separate explicit host opt-in from PR publication and
// merge. Omitted old configuration remains disabled.
type ProductSplitDispatchConfig struct {
	Workers       int     `json:"workers"`
	RepositoryIDs []int64 `json:"repository_ids"`
}

func (c *ProductPublicationConfig) splitRepositories(repositories []product.ExecutionRepository) (map[int64]string, error) {
	if c == nil || c.Split == nil {
		return nil, nil
	}
	if c.Split.Workers < 1 || c.Split.Workers > 8 || len(c.Split.RepositoryIDs) < 1 || len(c.Split.RepositoryIDs) > 128 {
		return nil, errors.New("split workers 1..8 and explicit repositories 1..128 required")
	}
	known := map[int64]string{}
	for _, repo := range repositories {
		known[repo.Policy.RepositoryID] = repo.Name
	}
	allowed := map[int64]string{}
	for _, id := range c.Split.RepositoryIDs {
		if id < 1 || known[id] == "" || allowed[id] != "" {
			return nil, errors.New("unique configured split repository IDs required")
		}
		allowed[id] = known[id]
	}
	return allowed, nil
}
