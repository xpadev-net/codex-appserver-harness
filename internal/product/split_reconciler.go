package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type SplitResourceAPIFactory func(storage.ProductSplitCreate) ResourceAPI
type SplitCreationReconciler struct {
	Store    *storage.KernelStore
	API      SplitResourceAPIFactory
	MaxPages int
}

func GitHubSplitResourceAPI(job storage.ProductSplitCreate) ResourceAPI {
	child := job.Value.Child.Value
	return github.Client{Repository: child.Repository, RepositoryID: child.RepositoryID}
}

type splitRemoteIssue struct {
	ID          int64           `json:"id"`
	Number      int             `json:"number"`
	Title       string          `json:"title"`
	Body        string          `json:"body"`
	State       string          `json:"state"`
	PullRequest json.RawMessage `json:"pull_request,omitempty"`
}

func splitRepositoryGET(ctx context.Context, api ResourceAPI, job storage.ProductSplitCreate) error {
	raw, err := api.ResourceGET(ctx, "")
	if err != nil {
		return err
	}
	var repo Repository
	child := job.Value.Child.Value
	if json.Unmarshal(raw, &repo) != nil || repo.ID != child.RepositoryID || repo.Name != child.Repository {
		return storage.ErrProductSplitHeld
	}
	return nil
}

// Reconcile never launches a wrapper, POSTs, or infers absence from a failed or
// truncated search. A full bounded scan must find exactly one marker; a fresh
// point GET and repository bookend then precede the trusted durable binding.
func (r SplitCreationReconciler) Reconcile(ctx context.Context, id string) (storage.ProductSplitCreate, bool, error) {
	var empty storage.ProductSplitCreate
	if r.Store == nil || r.API == nil || r.MaxPages < 1 || r.MaxPages > 32 {
		return empty, false, errors.New("split journal, read API and search pages 1..32 required")
	}
	job, err := r.Store.ReadProductSplitCreate(ctx, id)
	if err != nil {
		return empty, false, err
	}
	if job.State == "observed" || job.State == "aborted" {
		return job, false, nil
	}
	if job.State != "stopped" || job.Stop == nil {
		return job, false, storage.ErrProductSplitHeld
	}
	api := r.API(job)
	if api == nil {
		return job, false, errors.New("split read transport required")
	}
	work, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	work = github.WithResourceReadBudget(work, 16<<20)
	api = &budgetResourceAPI{ResourceAPI: api}
	if err := splitRepositoryGET(work, api, job); err != nil {
		return job, false, err
	}
	var selected splitRemoteIssue
	complete := false
	for page := 1; page <= r.MaxPages; page++ {
		raw, err := api.ResourceGET(work, fmt.Sprintf("issues?state=all&per_page=100&page=%d", page))
		if err != nil {
			return job, false, err
		}
		var issues []splitRemoteIssue
		if err := json.Unmarshal(raw, &issues); err != nil {
			return job, false, err
		}
		if issues == nil || len(issues) > 100 {
			return job, false, storage.ErrProductSplitHeld
		}
		for _, issue := range issues {
			if issue.ID < 1 || issue.Number < 1 || issue.State != "open" && issue.State != "closed" {
				return job, false, storage.ErrProductSplitHeld
			}
			if !strings.Contains(issue.Body, job.Value.Child.Value.Marker) {
				continue
			}
			if len(issue.PullRequest) != 0 || selected.Number != 0 {
				return job, false, storage.ErrProductSplitHeld
			}
			selected = issue
		}
		if len(issues) < 100 {
			complete = true
			break
		}
	}
	if !complete || selected.Number < 1 {
		return job, false, storage.ErrProductSplitHeld
	}
	raw, err := api.ResourceGET(work, fmt.Sprintf("issues/%d", selected.Number))
	if err != nil {
		return job, false, err
	}
	var current splitRemoteIssue
	if err := json.Unmarshal(raw, &current); err != nil {
		return job, false, err
	}
	child := job.Value.Child.Value
	if current.ID != selected.ID || current.Number != selected.Number || len(current.PullRequest) != 0 || current.State != "open" || current.Title != child.Child.Title || current.Body != child.Body || current.Title != selected.Title || current.Body != selected.Body || current.State != selected.State {
		return job, false, storage.ErrProductSplitHeld
	}
	if err := splitRepositoryGET(work, api, job); err != nil {
		return job, false, err
	}
	if err := work.Err(); err != nil {
		return job, false, err
	}
	now := time.Now().UTC()
	if r.Store.Now != nil {
		now = r.Store.Now().UTC()
	}
	observation := storage.ProductSplitCreatedIssue{RepositoryID: child.RepositoryID, ID: current.ID, Number: current.Number, Title: current.Title, Body: current.Body, State: current.State, ObservedAt: now}
	known, err := r.Store.RecordProductSplitCreatedIssue(work, job.ID, observation)
	return known, err == nil, err
}
