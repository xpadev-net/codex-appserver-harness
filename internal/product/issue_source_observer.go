package product

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// IssueSourceObserver reads only already admitted sources. The controller owns
// its lifecycle and API cancellation; no model turn or external write is made.
type IssueSourceObserver struct {
	Store      *storage.KernelStore
	API        ResourceAPI
	Policy     IssueSourcePolicy
	Attempts   int
	Every      time.Duration
	OnMutation func()
	Report     func(error)
}
type IssueSourceScan struct {
	Attempted int      `json:"attempted"`
	Changed   []string `json:"changed"`
	Failed    []string `json:"failed"`
}

var ErrIssueSourceScanBudget = errors.New("Issue source observation budget exhausted; resume from saved cursor")

type issueSourceCursor struct {
	PolicyDigest string `json:"policy_digest"`
	After        string `json:"after"`
}

func (p IssueSourcePolicy) Digest() string {
	raw, _ := json.Marshal(p) // Fixed struct contains only integers and strings.
	return storage.ProductSourceRequestDigest(string(raw))
}
func (o IssueSourceObserver) Validate() error {
	if o.Store == nil || o.API == nil || o.Attempts < 1 || o.Attempts > 32 {
		return errors.New("configured Issue source observer and attempts 1..32 required")
	}
	return o.Policy.Validate()
}

// observe captures the authoritative observation CAS before performing GETs.
// An unavailable or changing source is a saved prohibition, never ready data.
func (o IssueSourceObserver) observe(parent, readParent context.Context, api ResourceAPI, w domain.Wave, timeout time.Duration) (storage.ProductSourceUpdate, error) {
	number, err := strconv.Atoi(strings.TrimPrefix(w.SourceID, fmt.Sprintf("r%d-i", o.Policy.RepositoryID)))
	if err != nil || number < 1 || w.SourceID != issueSourceID(o.Policy.RepositoryID, number) {
		return storage.ProductSourceUpdate{}, errors.New("noncanonical Issue source identity")
	}
	prior, priorErr := o.Store.ReadProductSourceObservation(readParent, w.ID)
	if priorErr != nil && !errors.Is(priorErr, sql.ErrNoRows) && !errors.Is(priorErr, storage.ErrTaskEvidence) {
		return storage.ProductSourceUpdate{}, priorErr
	}
	expected := ""
	if priorErr == nil {
		expected = prior.Digest
	}
	facts := storage.ProductSourceFacts{RepositoryID: o.Policy.RepositoryID, IssueNumber: number, PolicyDigest: o.Policy.Digest(), Failure: "read_failed"}
	var readErr error
	if !errors.Is(priorErr, storage.ErrTaskEvidence) {
		readCtx, cancel := context.WithTimeout(readParent, timeout)
		readErr = sourceRepository(readCtx, api, o.Policy)
		if readErr == nil {
			var issue github.Issue
			issue, readErr = readSourceIssue(readCtx, api, number)
			if readErr == nil {
				current, err := readSourceIssue(readCtx, api, number)
				readErr = err
				if readErr == nil && !reflect.DeepEqual(issue, current) {
					readErr = errors.New("Issue changed during source observation")
				}
				if readErr == nil {
					readErr = sourceRepository(readCtx, api, o.Policy)
				}
				if readErr == nil && readCtx.Err() == nil {
					facts.Available, facts.Failure = true, ""
					facts.IssueState, facts.UpdatedAt = issue.State, issue.UpdatedAt
					facts.RequestDigest = storage.ProductSourceRequestDigest(issue.Title + "\n\n" + issue.Body)
					for _, label := range issue.Labels {
						facts.Ready = facts.Ready || label.Name == o.Policy.ReadyLabel
						facts.Hold = facts.Hold || label.Name == o.Policy.HoldLabel
					}
				}
			}
		}
		if readCtx.Err() != nil {
			readErr = errors.Join(readErr, readCtx.Err())
		}
		cancel()
	}
	// Join the read before saving. A local read deadline may persist a failure
	// using the live parent; daemon cancellation never starts detached writes.
	if parent.Err() != nil {
		return storage.ProductSourceUpdate{}, parent.Err()
	}
	saveCtx, stop := context.WithTimeout(parent, time.Second)
	defer stop()
	update, saveErr := o.Store.ObserveProductSource(saveCtx, w.Target(), expected, facts)
	return update, errors.Join(readErr, saveErr)
}

func (o IssueSourceObserver) Scan(ctx context.Context) (IssueSourceScan, error) {
	return o.scan(ctx, 45*time.Second, 5*time.Second)
}
func (o IssueSourceObserver) scan(parent context.Context, scanTimeout, readTimeout time.Duration) (IssueSourceScan, error) {
	result := IssueSourceScan{Changed: []string{}, Failed: []string{}}
	if err := o.Validate(); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(parent, scanTimeout)
	defer cancel()
	ctx = github.WithResourceReadBudget(ctx, 16<<20)
	api := &budgetResourceAPI{ResourceAPI: o.API}
	key := strconv.FormatInt(o.Policy.RepositoryID, 10)
	cursor := issueSourceCursor{PolicyDigest: o.Policy.Digest()}
	err := o.Store.Record(ctx, "product_issue_source_cursor", key, &cursor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	if cursor.PolicyDigest != o.Policy.Digest() {
		cursor = issueSourceCursor{PolicyDigest: o.Policy.Digest()}
	}
	targets, err := o.Store.ProductSourceObservationTargets(ctx, o.Policy.RepositoryID, cursor.After, o.Attempts)
	if err != nil {
		return result, err
	}
	if len(targets) == 0 {
		cursor.After = ""
		return result, o.Store.Put(ctx, "product_issue_source_cursor", key, "controller", cursor)
	}
	for _, w := range targets {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if !api.available(ctx) {
			return result, ErrIssueSourceScanBudget
		}
		result.Attempted++
		// Read deadlines are subordinate to the scan budget. On a scan timeout,
		// persist the attempted source failure/cursor with a bounded live parent.
		update, observeErr := o.observe(parent, ctx, api, w, readTimeout)
		if parent.Err() != nil {
			return result, parent.Err()
		}
		checkpoint, stop := context.WithTimeout(parent, time.Second)
		if update.Changed {
			result.Changed = append(result.Changed, w.ID)
			if o.OnMutation != nil {
				o.OnMutation()
			}
		}
		if observeErr != nil {
			result.Failed = append(result.Failed, w.ID)
			if o.Report != nil {
				o.Report(fmt.Errorf("Issue source %s: %w", w.ID, observeErr))
			}
		}
		cursor.After = w.ID
		saveErr := o.Store.Put(checkpoint, "product_issue_source_cursor", key, "controller", cursor)
		stop()
		if saveErr != nil {
			return result, saveErr
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
	}
	return result, nil
}
func (o IssueSourceObserver) Run(ctx context.Context) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if o.Every <= 0 {
		return errors.New("positive Issue source observation interval required")
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, err := o.Scan(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && o.Report != nil {
			o.Report(err)
		}
		timer := time.NewTimer(o.Every)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
