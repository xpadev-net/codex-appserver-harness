package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
