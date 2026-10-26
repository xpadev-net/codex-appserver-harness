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
	"sync"
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
	updates, acknowledged, err := o.ObserveMany(parent, target, []string{workID})
	if len(updates) != 1 {
		return storage.ProductSplitChildSourceUpdate{}, acknowledged, err
	}
	return updates[0], acknowledged, err
}

// ObserveMany joins the independent GitHub reads before saving their source
// receipts in one transaction. This keeps each CheckedAt tied to its GET while
// sharing the exact Wave proof walk across an ancestor lineage.
func (o SplitChildSourceObserver) ObserveMany(parent context.Context, target contract.Target, workIDs []string) ([]storage.ProductSplitChildSourceUpdate, bool, error) {
	if err := o.Validate(); err != nil {
		return nil, false, err
	}
	if !target.Valid() || len(workIDs) == 0 || len(workIDs) > 128 {
		return nil, false, storage.ErrProductSplitHeld
	}
	seen := make(map[string]bool, len(workIDs))
	for _, id := range workIDs {
		if id == "" || seen[id] {
			return nil, false, storage.ErrProductSplitHeld
		}
		seen[id] = true
	}
	states, err := o.Store.ReadProductSplitChildSourceStates(parent, target, workIDs)
	if err != nil {
		return nil, false, err
	}
	observations, acknowledged, err := o.ObserveManyWithStates(parent, target, states)
	if !acknowledged {
		return nil, acknowledged, err
	}
	save, stop := context.WithTimeout(parent, time.Minute)
	defer stop()
	updates, saveErr := o.Store.ObserveProductSplitChildSourcesAt(save, observations)
	return updates, saveErr == nil, errors.Join(err, saveErr)
}

// ObserveManyWithStates performs joined read-only GETs against a caller's
// already authenticated source snapshot. The caller owns persistence so it can
// commit those observations atomically with a bounded split activation.
func (o SplitChildSourceObserver) ObserveManyWithStates(parent context.Context, target contract.Target, states []storage.ProductSplitChildSourceState) ([]storage.ProductSplitChildSourceObservation, bool, error) {
	if err := o.Validate(); err != nil {
		return nil, false, err
	}
	if !target.Valid() || len(states) == 0 || len(states) > 128 {
		return nil, false, storage.ErrProductSplitHeld
	}
	type result struct {
		index       int
		observation storage.ProductSplitChildSourceObservation
		err         error
		eligible    bool
	}
	seen := make(map[string]bool, len(states))
	for _, state := range states {
		id := state.Binding.Value.Child.Value.WorkItemID
		if id == "" || seen[id] || state.Binding.Value.Request.Target != target {
			return nil, false, storage.ErrProductSplitHeld
		}
		seen[id] = true
	}
	jobs := make(chan int, len(states))
	for i := range states {
		jobs <- i
	}
	close(jobs)
	results := make(chan result, len(states))
	var joined sync.WaitGroup
	for i := 0; i < min(4, len(states)); i++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for index := range jobs {
				observation, err, eligible := o.readObservation(parent, target, states[index].Binding.Value.Child.Value.WorkItemID, states[index])
				results <- result{index: index, observation: observation, err: err, eligible: eligible}
			}
		}()
	}
	joined.Wait()
	close(results)
	observations := make([]storage.ProductSplitChildSourceObservation, len(states))
	var readErr error
	eligible := true
	for item := range results {
		observations[item.index] = item.observation
		readErr = errors.Join(readErr, item.err)
		eligible = eligible && item.eligible
	}
	if parent.Err() != nil {
		return nil, false, errors.Join(readErr, parent.Err())
	}
	if !eligible {
		return observations, false, readErr
	}
	return observations, true, readErr
}

func (o SplitChildSourceObserver) readObservation(parent context.Context, target contract.Target, workID string, state storage.ProductSplitChildSourceState) (storage.ProductSplitChildSourceObservation, error, bool) {
	if err := o.Validate(); err != nil {
		return storage.ProductSplitChildSourceObservation{}, err, false
	}
	read := github.WithResourceReadBudget(parent, 16<<20)
	api := &budgetResourceAPI{ResourceAPI: o.API}
	clock := time.Now
	if o.Store.Now != nil {
		clock = o.Store.Now
	}
	readAt := clock()
	binding, prior, pin := state.Binding, state.Snapshot, state.PolicyPin
	v := binding.Value
	if o.Policy.RepositoryID != v.Issue.RepositoryID || o.Policy.Repository != v.Child.Value.Repository {
		return storage.ProductSplitChildSourceObservation{}, storage.ErrProductSplitHeld, false
	}
	expected := ""
	if prior.ID != "" {
		expected = prior.ID
	}
	facts := storage.ProductSplitChildSourceFacts{RepositoryID: v.Issue.RepositoryID, IssueID: v.Issue.ID, IssueNumber: v.Issue.Number, PolicyDigest: o.Policy.Digest(), Failure: "read_failed"}
	var readErr error
	if pin != "" && pin != o.Policy.Digest() {
		facts.Failure = "invalid_source"
		readErr = ErrIssueSourcePolicyConflict
	} else {
		readCtx, cancel := context.WithTimeout(read, 5*time.Second)
		readErr = sourceRepository(readCtx, api, o.Policy)
		if readErr != nil {
			facts.Failure = "repository_mismatch"
		} else {
			var issue, current splitChildSourceIssue
			issue, readErr = readSplitChildSourceIssue(readCtx, api, v.Issue.Number)
			if readErr == nil {
				current, readErr = readSplitChildSourceIssue(readCtx, api, v.Issue.Number)
				readAt = clock()
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
	if parent.Err() != nil {
		return storage.ProductSplitChildSourceObservation{}, parent.Err(), false
	}
	if readErr != nil && facts.Available {
		facts = storage.ProductSplitChildSourceFacts{RepositoryID: v.Issue.RepositoryID, IssueID: v.Issue.ID, IssueNumber: v.Issue.Number, PolicyDigest: o.Policy.Digest(), Failure: "read_failed"}
	}
	return storage.ProductSplitChildSourceObservation{Target: target, WorkItemID: workID, ExpectedSnapshotID: expected, Facts: facts, ReadAt: readAt}, readErr, true
}

// Scan advances opaque rows after failures so independent children progress.
// Attempts count unique child reads; there is no model turn or external write.
func (o SplitChildSourceObserver) Scan(parent context.Context) (IssueSourceScan, error) {
	result := IssueSourceScan{Changed: []string{}, Failed: []string{}}
	if err := o.Validate(); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Minute)
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
		pending := []storage.ProductSplitChildSourceTarget{}
		flush := func() error {
			if len(pending) == 0 {
				return nil
			}
			ids := make([]string, 0, len(pending))
			for _, target := range pending {
				ids = append(ids, target.WorkItemID)
			}
			scanObserver := o
			scanObserver.API = api
			updates, acknowledged, readErr := scanObserver.ObserveMany(ctx, pending[0].Target, ids)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !acknowledged || len(updates) != len(pending) {
				for _, target := range pending {
					result.Failed = append(result.Failed, target.WorkItemID)
					if o.Report != nil {
						o.Report(fmt.Errorf("split source %s: %w", target.WorkItemID, errors.Join(readErr, storage.ErrTaskEvidence)))
					}
				}
			} else {
				for i, update := range updates {
					target := pending[i]
					if update.Changed {
						result.Changed = append(result.Changed, target.WorkItemID)
						if o.OnMutation != nil {
							o.OnMutation()
						}
					}
					status := update.Snapshot.Value.Status
					if status == "unavailable" || status == "policy_changed" {
						result.Failed = append(result.Failed, target.WorkItemID)
						if o.Report != nil {
							o.Report(fmt.Errorf("split source %s: %w", target.WorkItemID, errors.Join(readErr, storage.ErrTaskEvidence)))
						}
					}
				}
			}
			after := pending[len(pending)-1].Cursor
			pending = pending[:0]
			return checkpoint(after)
		}
		for _, target := range page.Targets {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			if target.Unavailable {
				if err := flush(); err != nil {
					return result, err
				}
				result.Failed = append(result.Failed, target.Cursor)
				if o.Report != nil {
					o.Report(fmt.Errorf("split source %s: %w", target.Cursor, storage.ErrTaskEvidence))
				}
				if err := checkpoint(target.Cursor); err != nil {
					return result, err
				}
				continue
			}
			if seen[target.WorkItemID] {
				if err := flush(); err != nil {
					return result, err
				}
				if err := checkpoint(target.Cursor); err != nil {
					return result, err
				}
				continue
			}
			if result.Attempted >= o.Attempts || !api.available(ctx) {
				if err := flush(); err != nil {
					return result, err
				}
				return result, ErrIssueSourceScanBudget
			}
			seen[target.WorkItemID] = true
			result.Attempted++
			pending = append(pending, target)
			if len(pending) == 4 {
				if err := flush(); err != nil {
					return result, err
				}
			}
		}
		if err := flush(); err != nil {
			return result, err
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
