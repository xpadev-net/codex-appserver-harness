package product

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type SplitChildSourceObserver struct {
	Store      *storage.KernelStore
	API        ResourceAPI
	Policy     IssueSourcePolicy
	Attempts   int
	OnMutation func()
	Report     func(error)
}
type splitChildSourceIssue struct {
	ID int64 `json:"id"`
	github.Issue
}

func readSplitChildSourceIssue(ctx context.Context, api ResourceAPI, number int) (splitChildSourceIssue, error) {
	var issue splitChildSourceIssue
	raw, err := api.ResourceGET(ctx, "issues/"+strconv.Itoa(number))
	if err != nil {
		return issue, err
	}
	if err := json.Unmarshal(raw, &issue); err != nil {
		return issue, err
	}
	if issue.ID < 1 || issue.Number != number || issue.UpdatedAt.IsZero() || len(issue.PullRequest) != 0 || issue.State != "open" && issue.State != "closed" {
		return issue, storage.ErrProductSplitHeld
	}
	sort.Slice(issue.Labels, func(i, j int) bool { return issue.Labels[i].Name < issue.Labels[j].Name })
	return issue, nil
}

func (o SplitChildSourceObserver) Validate() error {
	if o.Store == nil || o.API == nil || o.Attempts < 1 || o.Attempts > 32 {
		return storage.ErrProductSplitHeld
	}
	return o.Policy.Validate()
}

// The source-only transaction follows joined repository/Issue bookends. A
// failure records a prohibition for this child, never a global Wave hold.
func (o SplitChildSourceObserver) Observe(parent context.Context, target contract.Target, workID string) (storage.ProductSplitChildSourceUpdate, bool, error) {
	if err := o.Validate(); err != nil {
		return storage.ProductSplitChildSourceUpdate{}, false, err
	}
	read := github.WithResourceReadBudget(parent, 16<<20)
	return o.observe(parent, read, &budgetResourceAPI{ResourceAPI: o.API}, target, workID, 5*time.Second)
}

func (o SplitChildSourceObserver) observe(parent, readParent context.Context, api ResourceAPI, target contract.Target, workID string, timeout time.Duration) (storage.ProductSplitChildSourceUpdate, bool, error) {
	binding, err := o.Store.ReadProductSplitChildScope(readParent, target, workID)
	if err != nil {
		return storage.ProductSplitChildSourceUpdate{}, false, err
	}
	v := binding.Value
	if o.Policy.RepositoryID != v.Issue.RepositoryID || o.Policy.Repository != v.Child.Value.Repository {
		return storage.ProductSplitChildSourceUpdate{}, false, storage.ErrProductSplitHeld
	}
	prior, priorErr := o.Store.ReadProductSplitChildSource(readParent, target, workID)
	if priorErr != nil && priorErr != sql.ErrNoRows {
		return storage.ProductSplitChildSourceUpdate{}, false, priorErr
	}
	expected, pin := "", ""
	if priorErr == nil {
		expected = prior.ID
		pin, err = o.Store.ReadProductSplitChildSourcePolicy(readParent, target, workID)
		if err != nil {
			return storage.ProductSplitChildSourceUpdate{}, false, err
		}
	}
	facts := storage.ProductSplitChildSourceFacts{RepositoryID: v.Issue.RepositoryID, IssueID: v.Issue.ID, IssueNumber: v.Issue.Number, PolicyDigest: o.Policy.Digest(), Failure: "read_failed"}
	var readErr error
	if pin != "" && pin != o.Policy.Digest() {
		facts.Failure = "invalid_source"
		readErr = ErrIssueSourcePolicyConflict
	} else {
		readCtx, cancel := context.WithTimeout(readParent, timeout)
		readErr = sourceRepository(readCtx, api, o.Policy)
		if readErr != nil {
			facts.Failure = "repository_mismatch"
		} else {
			var issue, current splitChildSourceIssue
			issue, readErr = readSplitChildSourceIssue(readCtx, api, v.Issue.Number)
			if readErr == nil {
				current, readErr = readSplitChildSourceIssue(readCtx, api, v.Issue.Number)
			}
			if readErr == nil && !reflect.DeepEqual(issue, current) {
				readErr = storage.ErrProductSplitHeld
			}
			if readErr == nil {
				readErr = sourceRepository(readCtx, api, o.Policy)
				if readErr != nil {
					facts.Failure = "repository_mismatch"
				}
			}
			if readErr == nil && readCtx.Err() == nil {
				facts.Available, facts.Failure = true, ""
				facts.IssueID, facts.IssueNumber = issue.ID, issue.Number
				facts.IssueState, facts.UpdatedAt = issue.State, issue.UpdatedAt
				facts.RequestDigest = storage.ProductSourceRequestDigest(issue.Title + "\n\n" + issue.Body)
				for _, label := range issue.Labels {
					facts.Hold = facts.Hold || label.Name == o.Policy.HoldLabel
				}
			}
		}
		if readCtx.Err() != nil {
			readErr = errors.Join(readErr, readCtx.Err())
		}
		cancel()
	}
	if readErr != nil && facts.Available {
		facts = storage.ProductSplitChildSourceFacts{RepositoryID: v.Issue.RepositoryID, IssueID: v.Issue.ID, IssueNumber: v.Issue.Number, PolicyDigest: o.Policy.Digest(), Failure: "read_failed"}
	}
	if parent.Err() != nil {
		return storage.ProductSplitChildSourceUpdate{}, false, parent.Err()
	}
	save, stop := context.WithTimeout(parent, time.Second)
	defer stop()
	update, saveErr := o.Store.ObserveProductSplitChildSource(save, target, workID, expected, facts)
	return update, saveErr == nil, errors.Join(readErr, saveErr)
}

// Scan advances opaque rows after failures so independent children progress.
// Attempts count unique child reads; there is no model turn or external write.
func (o SplitChildSourceObserver) Scan(parent context.Context) (IssueSourceScan, error) {
	result := IssueSourceScan{Changed: []string{}, Failed: []string{}}
	if err := o.Validate(); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	api := &budgetResourceAPI{ResourceAPI: o.API}
	key := strconv.FormatInt(o.Policy.RepositoryID, 10)
	cursor := issueSourceCursor{PolicyDigest: o.Policy.Digest()}
	if err := o.Store.Record(ctx, "product_split_child_source_cursor", key, &cursor); err != nil && err != sql.ErrNoRows {
		return result, err
	}
	if cursor.PolicyDigest != o.Policy.Digest() {
		cursor = issueSourceCursor{PolicyDigest: o.Policy.Digest()}
	}
	seen := map[string]bool{}
	checkpoint := func(after string) error {
		if parent.Err() != nil {
			return parent.Err()
		}
		save, stop := context.WithTimeout(parent, time.Second)
		defer stop()
		cursor.After = after
		return o.Store.Put(save, "product_split_child_source_cursor", key, "controller", cursor)
	}
	for pages := 0; pages < 32; pages++ {
		page, err := o.Store.ProductSplitChildSourceTargets(ctx, o.Policy.RepositoryID, cursor.After, 32)
		if err != nil {
			return result, err
		}
		for _, target := range page.Targets {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			if target.Unavailable {
				result.Failed = append(result.Failed, target.Cursor)
				if o.Report != nil {
					o.Report(fmt.Errorf("split source %s: %w", target.Cursor, storage.ErrTaskEvidence))
				}
			} else if !seen[target.WorkItemID] {
				if result.Attempted >= o.Attempts || !api.available(ctx) {
					return result, ErrIssueSourceScanBudget
				}
				seen[target.WorkItemID] = true
				result.Attempted++
				update, _, err := o.observe(parent, ctx, api, target.Target, target.WorkItemID, 5*time.Second)
				if parent.Err() != nil {
					return result, parent.Err()
				}
				if update.Changed {
					result.Changed = append(result.Changed, target.WorkItemID)
					if o.OnMutation != nil {
						o.OnMutation()
					}
				}
				if err != nil {
					result.Failed = append(result.Failed, target.WorkItemID)
					if o.Report != nil {
						o.Report(fmt.Errorf("split source %s: %w", target.WorkItemID, err))
					}
				}
			}
			if err := checkpoint(target.Cursor); err != nil {
				return result, err
			}
		}
		if page.NextAfter == "" {
			return result, checkpoint("")
		}
		if err := checkpoint(page.NextAfter); err != nil {
			return result, err
		}
	}
	return result, ErrIssueSourceScanBudget
}
