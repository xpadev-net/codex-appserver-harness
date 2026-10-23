package product

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type issuePoolAPI struct {
	*issueAdmissionAPI
	items   []json.RawMessage
	pageErr error
	started chan struct{}
	stopped chan struct{}
	release chan struct{}
}

type stallingPoolCandidate struct {
	ResourceAPI
	started chan struct{}
	stalled bool
}

type deadlinePoolMetadata struct {
	ResourceAPI
	metadataReads int
}

func (a *deadlinePoolMetadata) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if path == "" {
		a.metadataReads++
		if a.metadataReads == 2 {
			// Metadata finished at the scan deadline. A subsequent canceled
			// Issue call is not an attempted fetch and must not move cursor.
			<-ctx.Done()
			return json.RawMessage(`{"id":17,"full_name":"owner/repo"}`), nil
		}
	}
	return a.ResourceAPI.ResourceGET(ctx, path)
}

func (a *stallingPoolCandidate) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if path == "issues/42" && !a.stalled {
		a.stalled = true
		close(a.started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return a.ResourceAPI.ResourceGET(ctx, path)
}

func (a *issuePoolAPI) ResourcePages(ctx context.Context, path, field string) ([]json.RawMessage, error) {
	if path != "issues?state=open&per_page=100" || field != "" {
		return nil, errors.New("unexpected Issue pool collection")
	}
	if a.started != nil {
		close(a.started)
		<-ctx.Done()
		close(a.stopped)
		<-a.release
		return nil, ctx.Err()
	}
	return a.items, a.pageErr
}

func issuePoolFixture(t *testing.T, numbers ...int) (IssuePool, *issuePoolAPI) {
	t.Helper()
	a, original := issueAdmissionFixture(t)
	api := &issuePoolAPI{issueAdmissionAPI: original}
	for _, number := range numbers {
		issue := original.issue
		issue.Number = number
		raw, err := json.Marshal(issue)
		if err != nil {
			t.Fatal(err)
		}
		api.items = append(api.items, raw)
		api.dependencies[fmt.Sprintf("issues/%d", number)] = issue
	}
	a.API = api
	return IssuePool{Admitter: a, Attempts: 1, Every: time.Millisecond}, api
}

func setPoolItem(t *testing.T, api *issuePoolAPI, index int, change func(*github.Issue)) {
	t.Helper()
	var issue github.Issue
	if err := json.Unmarshal(api.items[index], &issue); err != nil {
		t.Fatal(err)
	}
	change(&issue)
	raw, err := json.Marshal(issue)
	if err != nil {
		t.Fatal(err)
	}
	api.items[index] = raw
	if issue.Number == 42 {
		api.issue = issue
	} else {
		api.dependencies[fmt.Sprintf("issues/%d", issue.Number)] = issue
	}
}

func TestIssuePoolPriorityBoundedAdmissionReopenAndNoRepeatWake(t *testing.T) {
	p, api := issuePoolFixture(t, 43, 42, 44)
	p.PriorityLabels = []string{"P0"}
	setPoolItem(t, api, 1, func(issue *github.Issue) {
		label := issue.Labels[0]
		label.Name = "P0"
		issue.Labels = append(issue.Labels, label)
	})
	setPoolItem(t, api, 2, func(issue *github.Issue) {
		label := issue.Labels[0]
		label.Name = "hold"
		issue.Labels = append(issue.Labels, label)
	})
	wakes := 0
	p.OnMutation = func() { wakes++ }
	first, err := p.Scan(context.Background())
	if err != nil || first.Attempted != 1 || len(first.Admitted) != 1 || len(first.Held) != 0 || wakes != 1 {
		t.Fatal(first, err, wakes)
	}
	wave, err := p.Admitter.Store.Wave(context.Background(), first.Admitted[0])
	if err != nil || wave.SourceID != "r17-i42" {
		t.Fatal("priority selection lost", wave, err)
	}
	path := p.Admitter.Store.Root + "/state.db"
	p.Admitter.Store.Close()
	p.Admitter.Store, err = storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Admitter.Store.Close()
	second, err := p.Scan(context.Background())
	if err != nil || second.Attempted != 1 || len(second.Admitted) != 1 || wakes != 2 {
		t.Fatal(second, err, wakes)
	}
	wave, err = p.Admitter.Store.Wave(context.Background(), second.Admitted[0])
	if err != nil || wave.SourceID != "r17-i43" {
		t.Fatal(wave, err)
	}
	again, err := p.Scan(context.Background())
	if err != nil || again.Attempted != 0 || len(again.Admitted) != 0 || wakes != 2 {
		t.Fatal("unchanged pool re-woke main", again, err, wakes)
	}
	var events, runs int
	if err := p.Admitter.Store.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='wave_created'").Scan(&events); err != nil || events != 2 {
		t.Fatal(events, err)
	}
	if err := p.Admitter.Store.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("pool admitted a model run", runs, err)
	}
}

func TestIssuePoolHeldItemCannotStarveNextSourceAfterReopen(t *testing.T) {
	p, api := issuePoolFixture(t, 42, 43)
	setPoolItem(t, api, 0, func(issue *github.Issue) { issue.Body = "unusable acceptance contract" })
	reported := 0
	p.Report = func(error) { reported++ }
	first, err := p.Scan(context.Background())
	if err != nil || !reflect.DeepEqual(first.Held, []int{42}) || len(first.Admitted) != 0 || reported != 1 {
		t.Fatal(first, err, reported)
	}
	path := p.Admitter.Store.Root + "/state.db"
	p.Admitter.Store.Close()
	p.Admitter.Store, err = storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Admitter.Store.Close()
	second, err := p.Scan(context.Background())
	if err != nil || len(second.Admitted) != 1 || len(second.Held) != 0 {
		t.Fatal("held head starved later Issue", second, err)
	}
	wave, err := p.Admitter.Store.Wave(context.Background(), second.Admitted[0])
	if err != nil || wave.SourceID != "r17-i43" {
		t.Fatal(wave, err)
	}
}

func TestIssuePoolRejectsIncompleteDuplicateAndForeignCollections(t *testing.T) {
	for _, mode := range []string{"unavailable", "duplicate", "invalid", "repository", "bound"} {
		t.Run(mode, func(t *testing.T) {
			p, api := issuePoolFixture(t, 42)
			switch mode {
			case "unavailable":
				api.pageErr = errors.New("collection unavailable")
			case "duplicate":
				api.items = append(api.items, api.items[0])
			case "invalid":
				api.items[0] = json.RawMessage(`null`)
			case "repository":
				api.repositoryID++
			case "bound":
				api.items = make([]json.RawMessage, 2049)
			}
			if _, err := p.Scan(context.Background()); err == nil {
				t.Fatal("invalid pool was treated as a complete empty collection")
			}
			assertNoIssueAdmission(t, p.Admitter)
		})
	}
}

func TestIssuePoolCannotExceedControllerWaveCapacity(t *testing.T) {
	p, _ := issuePoolFixture(t, 42, 43, 44, 45, 46)
	p.Attempts = 8
	wakes := 0
	p.OnMutation = func() { wakes++ }
	result, err := p.Scan(context.Background())
	if err != nil || result.Attempted != 5 || len(result.Admitted) != 4 || !reflect.DeepEqual(result.Held, []int{46}) || wakes != 4 {
		t.Fatal("pool bypassed controller capacity", result, err, wakes)
	}
	waves, err := p.Admitter.Store.Waves(context.Background())
	if err != nil || len(waves) != 4 {
		t.Fatal(waves, err)
	}
	var registrations int
	if err := p.Admitter.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&registrations); err != nil || registrations != 4 {
		t.Fatal("capacity failure leaked WorkItem registration", registrations, err)
	}
	again, err := p.Scan(context.Background())
	if err != nil || len(again.Admitted) != 0 || !reflect.DeepEqual(again.Held, []int{46}) || wakes != 4 {
		t.Fatal("full controller resumed held source", again, err, wakes)
	}
}

func nativeIssuePoolBudgetFixture(t *testing.T, count, goodNumber int) (IssuePool, string) {
	t.Helper()
	p, api := issuePoolFixture(t)
	p.Attempts = count
	items := make([]github.Issue, count)
	for i := range items {
		items[i] = api.issue
		items[i].Number = i + 1
	}
	pages, err := json.Marshal([][]github.Issue{items})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	meta, pageFile, oversized, calls := filepath.Join(dir, "meta"), filepath.Join(dir, "pages"), filepath.Join(dir, "oversized"), filepath.Join(dir, "calls")
	for path, data := range map[string][]byte{meta: []byte(`{"id":17,"full_name":"owner/repo"}`), pageFile: pages, oversized: []byte(strings.Repeat("x", github.MaxResourceReadBytes+1))} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	good := api.issue
	good.Number = goodNumber
	goodRaw, err := json.Marshal(good)
	if err != nil {
		t.Fatal(err)
	}
	goodFile := filepath.Join(dir, "good")
	if err := os.WriteFile(goodFile, goodRaw, 0600); err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf(`#!/bin/sh
case "$*" in
 'api --method GET repos/owner/repo') /bin/cat "$ISSUE_POOL_BUDGET_META" ;;
 'api --method GET --paginate --slurp repos/owner/repo/issues?state=open&per_page=100') /bin/cat "$ISSUE_POOL_BUDGET_PAGES" ;;
 'api --method GET repos/owner/repo/issues/%d') /bin/cat "$ISSUE_POOL_BUDGET_GOOD" ;;
 'api --method GET repos/owner/repo/issues/'*)
  printf 'oversized\n' >> "$ISSUE_POOL_BUDGET_CALLS"
  /bin/cat "$ISSUE_POOL_BUDGET_OVERSIZED" ;;
 *) exit 49 ;;
esac
`, goodNumber)
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("ISSUE_POOL_BUDGET_META", meta)
	t.Setenv("ISSUE_POOL_BUDGET_PAGES", pageFile)
	t.Setenv("ISSUE_POOL_BUDGET_OVERSIZED", oversized)
	t.Setenv("ISSUE_POOL_BUDGET_CALLS", calls)
	t.Setenv("ISSUE_POOL_BUDGET_GOOD", goodFile)
	p.Admitter.API = github.Client{Repository: "owner/repo", RepositoryID: 17}
	return p, calls
}

func TestIssuePoolSharesWireBudgetAcrossFailedOversizedAdmissions(t *testing.T) {
	p, calls := nativeIssuePoolBudgetFixture(t, 32, 0)
	result, err := p.Scan(context.Background())
	if !errors.Is(err, ErrIssuePoolScanBudget) || result.Attempted != 4 || len(result.Admitted) != 0 || len(result.Held) != 4 {
		t.Fatal(result, err)
	}
	readCalls, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(readCalls), "oversized\n") != 4 {
		t.Fatal("failed candidate reads reset scan wire budget", string(readCalls), err)
	}
	assertNoIssueAdmission(t, p.Admitter)
}

func TestIssuePoolWireExhaustionResumesFirstUnreadIssueAfterReopen(t *testing.T) {
	p, calls := nativeIssuePoolBudgetFixture(t, 5, 5)
	first, err := p.Scan(context.Background())
	if !errors.Is(err, ErrIssuePoolScanBudget) || first.Attempted != 4 || len(first.Admitted) != 0 || !reflect.DeepEqual(first.Held, []int{1, 2, 3, 4}) {
		t.Fatal(first, err)
	}
	path := p.Admitter.Store.Root + "/state.db"
	p.Admitter.Store.Close()
	p.Admitter.Store, err = storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Admitter.Store.Close()
	second, err := p.Scan(context.Background())
	if !errors.Is(err, ErrIssuePoolScanBudget) || len(second.Admitted) != 1 || len(second.Held) != 4 {
		t.Fatal("valid fifth Issue starved after budget exhaustion", second, err)
	}
	wave, err := p.Admitter.Store.Wave(context.Background(), second.Admitted[0])
	if err != nil || wave.SourceID != "r17-i5" {
		t.Fatal(wave, err)
	}
	readCalls, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(readCalls), "oversized\n") != 8 {
		t.Fatal(string(readCalls), err)
	}
}

func TestIssuePoolCallBudgetExhaustedBeforeIssueReadKeepsCursor(t *testing.T) {
	p, api := issuePoolFixture(t, 1, 2, 3, 4, 5)
	p.Attempts = 5
	dependencies := func(start, count int) string {
		var ids []string
		for number := start; number < start+count; number++ {
			item := api.issue
			item.Number, item.State = number, "closed"
			api.dependencies[fmt.Sprintf("issues/%d", number)] = item
			ids = append(ids, fmt.Sprint(number))
		}
		return "\nharness-depends-on: " + strings.Join(ids, ",")
	}
	full := dependencies(100, 32)
	held := dependencies(200, 15)
	item := api.dependencies["issues/214"]
	item.State = "open"
	api.dependencies["issues/214"] = item
	for i := 0; i < 3; i++ {
		setPoolItem(t, api, i, func(issue *github.Issue) { issue.Body += full })
	}
	setPoolItem(t, api, 3, func(issue *github.Issue) { issue.Body += held })
	first, err := p.Scan(context.Background())
	if !errors.Is(err, ErrIssuePoolScanBudget) || len(first.Admitted) != 3 || !reflect.DeepEqual(first.Held, []int{4}) {
		t.Fatal(first, err)
	}
	var cursor issuePoolCursor
	if err := p.Admitter.Store.Record(context.Background(), "product_issue_pool_cursor", "17", &cursor); err != nil || cursor.LastNumber != 4 {
		t.Fatal("unread fifth Issue advanced cursor", cursor, err)
	}
	second, err := p.Scan(context.Background())
	if err != nil || len(second.Admitted) != 1 {
		t.Fatal("unread fifth Issue starved", second, err)
	}
	wave, err := p.Admitter.Store.Wave(context.Background(), second.Admitted[0])
	if err != nil || wave.SourceID != "r17-i5" {
		t.Fatal(wave, err)
	}
}

func TestIssuePoolScanDeadlinePreservesAttemptedIssueProgressAfterReopen(t *testing.T) {
	p, api := issuePoolFixture(t, 42, 43)
	stall := &stallingPoolCandidate{ResourceAPI: api, started: make(chan struct{})}
	p.Admitter.API = stall
	first, err := p.scan(context.Background(), 500*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || first.Attempted != 1 || !reflect.DeepEqual(first.Held, []int{42}) {
		t.Fatal(first, err)
	}
	select {
	case <-stall.started:
	default:
		t.Fatal("scan expired before candidate fetch; deadline regression not exercised")
	}
	path := p.Admitter.Store.Root + "/state.db"
	p.Admitter.Store.Close()
	p.Admitter.Store, err = storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Admitter.Store.Close()
	second, err := p.Scan(context.Background())
	if err != nil || len(second.Admitted) != 1 {
		t.Fatal("timed-out first Issue starved healthy next Issue", second, err)
	}
	wave, err := p.Admitter.Store.Wave(context.Background(), second.Admitted[0])
	if err != nil || wave.SourceID != "r17-i43" {
		t.Fatal("deadline retry lost cursor", wave, err)
	}
}

func TestIssuePoolDaemonCancellationDoesNotDetachCursorWrite(t *testing.T) {
	p, api := issuePoolFixture(t, 42, 43)
	stall := &stallingPoolCandidate{ResourceAPI: api, started: make(chan struct{})}
	p.Admitter.API = stall
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := p.scan(ctx, time.Second)
		done <- err
	}()
	select {
	case <-stall.started:
	case <-time.After(3 * time.Second):
		t.Fatal("candidate fetch never started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("candidate fetch did not join daemon cancellation")
	}
	var cursor issuePoolCursor
	if err := p.Admitter.Store.Record(context.Background(), "product_issue_pool_cursor", "17", &cursor); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("daemon cancellation was bypassed for cursor write", cursor, err)
	}
	assertNoIssueAdmission(t, p.Admitter)
}

func TestIssuePoolDeadlineBeforeIssueFetchPreservesUnreadCursor(t *testing.T) {
	p, api := issuePoolFixture(t, 42, 43)
	p.Admitter.API = &deadlinePoolMetadata{ResourceAPI: api}
	result, err := p.scan(context.Background(), 500*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || len(result.Admitted) != 0 || len(result.Held) != 0 {
		t.Fatal(result, err)
	}
	var cursor issuePoolCursor
	if err := p.Admitter.Store.Record(context.Background(), "product_issue_pool_cursor", "17", &cursor); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("unread candidate was skipped at deadline", cursor, err)
	}
	assertNoIssueAdmission(t, p.Admitter)
}

func TestIssuePoolCancellationJoinsInFlightRead(t *testing.T) {
	p, api := issuePoolFixture(t, 42)
	api.started, api.stopped, api.release = make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	select {
	case <-api.started:
	case <-time.After(3 * time.Second):
		t.Fatal("pool read never started")
	}
	cancel()
	select {
	case <-api.stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("pool read not canceled")
	}
	select {
	case err := <-done:
		t.Fatal("pool returned before transport joined", err)
	default:
	}
	close(api.release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pool did not join canceled read")
	}
	assertNoIssueAdmission(t, p.Admitter)
}
