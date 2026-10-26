package product

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type ResourceAPI interface {
	ResourceGET(context.Context, string) (json.RawMessage, error)
	ResourcePages(context.Context, string, string) ([]json.RawMessage, error)
}
type ResourceCollector struct {
	Store *storage.KernelStore
	API   func(storage.ProductTarget) ResourceAPI
}

type budgetResourceAPI struct {
	ResourceAPI
	mu           sync.Mutex
	calls, bytes int
}

func (a *budgetResourceAPI) available(ctx context.Context) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls < 128 && a.bytes < 16<<20 && github.ResourceReadBudgetAvailable(ctx)
}

func (a *budgetResourceAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	a.mu.Lock()
	if a.calls >= 128 || a.bytes >= 16<<20 || !github.ResourceReadBudgetAvailable(ctx) {
		a.mu.Unlock()
		return nil, errors.New("resource scan request budget exhausted")
	}
	a.calls++
	a.mu.Unlock()
	raw, err := a.ResourceAPI.ResourceGET(ctx, path)
	a.mu.Lock()
	a.bytes += len(raw)
	over := a.bytes > 16<<20
	a.mu.Unlock()
	if over {
		return nil, errors.New("resource scan byte budget exhausted")
	}
	return raw, err
}
func (a *budgetResourceAPI) ResourcePages(ctx context.Context, path, field string) ([]json.RawMessage, error) {
	// Reserve the last read for the final PR header consistency check.
	a.mu.Lock()
	if a.calls >= 127 || a.bytes >= 16<<20 || !github.ResourceReadBudgetAvailable(ctx) {
		a.mu.Unlock()
		return nil, errors.New("resource scan request budget exhausted")
	}
	a.calls++
	a.mu.Unlock()
	items, err := a.ResourceAPI.ResourcePages(ctx, path, field)
	a.mu.Lock()
	for _, raw := range items {
		a.bytes += len(raw)
	}
	over := a.bytes > 16<<20
	a.mu.Unlock()
	if over {
		return nil, errors.New("resource scan byte budget exhausted")
	}
	return items, err
}
func GitHubResourceAPI(p storage.ProductTarget) ResourceAPI {
	return github.Client{Repository: p.Repository, RepositoryID: p.WorkItem.RepositoryID}
}

func resourceEntities(items []json.RawMessage, candidate string) ([]storage.PRResourceEntity, error) {
	entities := []storage.PRResourceEntity{}
	seen := map[int64]bool{}
	for _, raw := range items {
		var h struct {
			ID       int64  `json:"id"`
			CommitID string `json:"commit_id"`
			HeadSHA  string `json:"head_sha"`
		}
		if err := json.Unmarshal(raw, &h); err != nil {
			return nil, err
		}
		if h.ID <= 0 || seen[h.ID] {
			return nil, errors.New("invalid or duplicate resource entity ID")
		}
		seen[h.ID] = true
		sha := candidate
		if h.CommitID != "" {
			sha = h.CommitID
		}
		if h.HeadSHA != "" {
			sha = h.HeadSHA
		}
		entities = append(entities, storage.PRResourceEntity{ID: strconv.FormatInt(h.ID, 10), CandidateSHA: sha, Data: raw})
	}
	return entities, nil
}
func readResourceSection(ctx context.Context, api ResourceAPI, kind, path, field, sha string) storage.PRResourceSection {
	s := storage.PRResourceSection{Kind: kind, Availability: "unavailable", Reason: "read_failed", Entities: []storage.PRResourceEntity{}}
	items, err := api.ResourcePages(ctx, path, field)
	if err != nil {
		return s
	}
	entities, err := resourceEntities(items, sha)
	if err != nil {
		s.Reason = "invalid_collection"
		return s
	}
	s.Availability, s.Reason, s.Entities = "complete", "", entities
	return s
}

// The head-wide check-runs endpoint only includes the 1000 most recent suites.
// Enumerate suites and each suite's runs instead. If the bounded scan cannot
// finish, retain the previous complete collection rather than invent deletions.
func readCheckSection(ctx context.Context, api ResourceAPI, sha string) storage.PRResourceSection {
	suites := readResourceSection(ctx, api, "check_suite", "commits/"+sha+"/check-suites?per_page=100", "check_suites", sha)
	result := storage.PRResourceSection{Kind: "check", Availability: "unavailable", Reason: "check_suites_unavailable", Entities: []storage.PRResourceEntity{}}
	if suites.Availability != "complete" {
		return result
	}
	seen := map[string]bool{}
	for _, suite := range suites.Entities {
		runs := readResourceSection(ctx, api, "check", "check-suites/"+suite.ID+"/check-runs?filter=all&per_page=100", "check_runs", sha)
		if runs.Availability != "complete" {
			result.Reason = "check_suite_runs_incomplete"
			result.Entities = nil
			return result
		}
		for _, run := range runs.Entities {
			if seen[run.ID] {
				result.Reason = "duplicate_check_run"
				result.Entities = nil
				return result
			}
			seen[run.ID] = true
			result.Entities = append(result.Entities, run)
		}
	}
	result.Availability, result.Reason = "complete", ""
	return result
}

// Collect runs outside KernelSource.Ready. Each call does fresh API reads after
// loading the CAS digest. Never retry an old snapshot with a newer digest.
// Collection reads are eventually consistent; a changing PR header is rejected.
func (c ResourceCollector) Collect(ctx context.Context, p storage.ProductTarget, id string) (storage.PRResourceUpdateReceipt, error) {
	var receipt storage.PRResourceUpdateReceipt
	if c.Store == nil || c.API == nil {
		return receipt, errors.New("resource store and explicit read API required")
	}
	var binding storage.PRResourceBinding
	if err := c.Store.Record(ctx, "product_work_item_pr", p.WorkItem.ID, &binding); err != nil {
		return receipt, err
	}
	if binding.RepositoryID != p.WorkItem.RepositoryID || binding.Number != p.PRNumber || binding.HeadBranch != p.WorkItem.Branch || binding.WaveID != p.Target.WaveID {
		return receipt, errors.New("collector product/PR binding conflict")
	}
	if err := c.Store.Record(ctx, "product_pr_scan", id, &receipt); err == nil {
		if receipt.ScanID != id || receipt.ResourceKey != binding.ResourceKey {
			return receipt, errors.New("collector scan identity conflict")
		}
		return receipt, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	expected := ""
	if state, err := c.Store.PRResourceState(ctx, binding.ResourceKey); err == nil {
		expected = state.Digest
	} else if !errors.Is(err, sql.ErrNoRows) {
		return receipt, err
	}
	transport := c.API(p)
	if transport == nil {
		return receipt, errors.New("missing resource read transport")
	}
	api := &budgetResourceAPI{ResourceAPI: transport}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	started := time.Now().UTC()
	raw, err := api.ResourceGET(ctx, "")
	if err != nil {
		return receipt, err
	}
	var repo Repository
	if err = json.Unmarshal(raw, &repo); err != nil {
		return receipt, err
	}
	if repo.ID != binding.RepositoryID {
		return receipt, errors.New("collector repository identity changed")
	}
	path := fmt.Sprintf("pulls/%d", binding.Number)
	raw, err = api.ResourceGET(ctx, path)
	if err != nil {
		return receipt, err
	}
	var pr github.PR
	if err = json.Unmarshal(raw, &pr); err != nil {
		return receipt, err
	}
	if pr.Number != binding.Number || pr.Head.Repo.ID != binding.RepositoryID || pr.Base.Repo.ID != binding.RepositoryID || pr.Head.Ref != binding.HeadBranch {
		return receipt, errors.New("collector PR identity changed")
	}
	scan := storage.PRResourceScan{ID: id, RepositoryID: binding.RepositoryID, PRNumber: binding.Number, HeadSHA: pr.Head.SHA, StartedAt: started, Sections: []storage.PRResourceSection{{Kind: "pr", Availability: "complete", Entities: []storage.PRResourceEntity{{ID: strconv.Itoa(pr.Number), CandidateSHA: pr.Head.SHA, Data: raw}}}}}
	issuePath := fmt.Sprintf("issues/%d", binding.Number)
	issue := readResourceSection(ctx, api, "issue_comment", issuePath+"/comments?per_page=100", "", "")
	inline := readResourceSection(ctx, api, "inline_comment", path+"/comments?per_page=100", "", "")
	scan.Sections = append(scan.Sections, issue, inline,
		readResourceSection(ctx, api, "review", path+"/reviews?per_page=100", "", ""),
		readCheckSection(ctx, api, pr.Head.SHA),
		readResourceSection(ctx, api, "status", "commits/"+pr.Head.SHA+"/statuses?per_page=100", "", pr.Head.SHA),
		readResourceSection(ctx, api, "history", issuePath+"/events?per_page=100", "", ""))
	reactions := readResourceSection(ctx, api, "reaction", issuePath+"/reactions?per_page=100", "", "")
	for i := range reactions.Entities {
		reactions.Entities[i].ID = "pr:" + reactions.Entities[i].ID
	}
	if issue.Availability != "complete" || inline.Availability != "complete" {
		reactions.Availability, reactions.Reason, reactions.Entities = "unavailable", "reaction_parents_unavailable", nil
	} else {
		for _, parent := range []struct {
			kind, path string
			section    storage.PRResourceSection
		}{{"issue_comment", "issues/comments", issue}, {"inline_comment", "pulls/comments", inline}} {
			for _, comment := range parent.section.Entities {
				part := readResourceSection(ctx, api, "reaction", parent.path+"/"+comment.ID+"/reactions?per_page=100", "", "")
				if part.Availability != "complete" {
					reactions.Availability, reactions.Reason = "unavailable", "reaction_collection_incomplete"
					break
				}
				for _, e := range part.Entities {
					e.ID = parent.kind + ":" + comment.ID + ":" + e.ID
					reactions.Entities = append(reactions.Entities, e)
				}
			}
			if reactions.Availability != "complete" {
				break
			}
		}
		if reactions.Availability != "complete" {
			reactions.Entities = nil
		}
	}
	scan.Sections = append(scan.Sections, reactions)
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	finalRaw, err := api.ResourceGET(ctx, path)
	if err != nil {
		return receipt, err
	}
	var final github.PR
	if err = json.Unmarshal(finalRaw, &final); err != nil {
		return receipt, err
	}
	if final != pr {
		return receipt, errors.New("PR header changed during resource scan; fetch again")
	}
	return c.Store.RecordPRResourceScan(ctx, expected, scan)
}
