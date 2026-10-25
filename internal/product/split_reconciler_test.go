package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type splitGETFixtureAPI struct {
	mu      sync.Mutex
	calls   []string
	job     storage.ProductSplitCreate
	mode    string
	entered chan struct{}
	once    sync.Once
}

func (a *splitGETFixtureAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	a.mu.Lock()
	a.calls = append(a.calls, path)
	count := len(a.calls)
	a.mu.Unlock()
	child := a.job.Value.Child.Value
	selected := splitRemoteIssue{ID: 101, Number: 11, Title: child.Child.Title, Body: child.Body, State: "open"}
	switch path {
	case "":
		id := child.RepositoryID
		if a.mode == "repo_changed" && count > 1 {
			id++
		}
		return json.Marshal(Repository{ID: id, Name: child.Repository})
	case "issues?state=all&per_page=100&page=1":
		issues := []splitRemoteIssue{selected}
		switch a.mode {
		case "missing":
			issues = []splitRemoteIssue{}
		case "null":
			return json.RawMessage("null"), nil
		case "duplicate":
			other := selected
			other.ID++
			other.Number++
			issues = append(issues, other)
		case "pr":
			issues[0].PullRequest = json.RawMessage("{\"url\":\"fixture-pull\"}")
		case "closed":
			issues[0].State = "closed"
		case "full", "truncated", "multi":
			for len(issues) < 100 {
				n := 1000 + len(issues)
				issues = append(issues, splitRemoteIssue{ID: int64(n), Number: n, State: "closed"})
			}
		}
		return json.Marshal(issues)
	case "issues?state=all&per_page=100&page=2":
		if a.mode == "truncated" {
			return nil, errors.New("fixture page read failed")
		}
		return json.Marshal([]splitRemoteIssue{})
	case "issues/11":
		switch a.mode {
		case "edited":
			selected.Title = "changed outside controller"
		case "marker":
			selected.Body = strings.ReplaceAll(selected.Body, child.Marker, "")
		case "wrong_id":
			selected.ID++
		case "blocked":
			a.once.Do(func() { close(a.entered) })
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return json.Marshal(selected)
	}
	return nil, fmt.Errorf("unexpected fixture GET %q", path)
}
func (a *splitGETFixtureAPI) reads() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string{}, a.calls...)
}
func stoppedSplitGETFixture(t *testing.T) (*storage.KernelStore, storage.ProductSplitCreate, *splitGETFixtureAPI) {
	t.Helper()
	k, job, policy, _ := splitHTTPFixture(t, "direct")
	stopped, err := splitTestTransport(t, k, policy).Start(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	return k, stopped, &splitGETFixtureAPI{job: stopped}
}

func TestSplitCreationReconciliationAuthenticatesFullScanAndDoesNotResetParent(t *testing.T) {
	ctx := context.Background()
	k, job, api := stoppedSplitGETFixture(t)
	before, err := k.Wave(ctx, job.Value.Request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	r := SplitCreationReconciler{Store: k, MaxPages: 2, API: func(storage.ProductSplitCreate) ResourceAPI { return api }}
	known, changed, err := r.Reconcile(ctx, job.ID)
	if err != nil || !changed || known.State != "observed" || known.Issue == nil || known.Issue.Number != 11 {
		t.Fatal(known, changed, err)
	}
	expected := []string{"", "issues?state=all&per_page=100&page=1", "issues/11", ""}
	if !reflect.DeepEqual(api.reads(), expected) {
		t.Fatal("GET ordering", api.reads())
	}
	replay, changed, err := r.Reconcile(ctx, job.ID)
	if err != nil || changed || !reflect.DeepEqual(replay, known) || !reflect.DeepEqual(api.reads(), expected) {
		t.Fatal("known child re-read or changed", changed, err)
	}
	after, err := k.Wave(ctx, before.ID)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("GET binding retired parent or reset Wave", err)
	}
	afterBudget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil || !reflect.DeepEqual(afterBudget, budget) {
		t.Fatal("GET reset execution budget", err)
	}
	var registrations, runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_work_item_registration'").Scan(&registrations); err != nil || registrations != 1 {
		t.Fatal("GET granted child work", registrations, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("GET started a model", runs, err)
	}
}

func TestSplitCreationReconciliationUnknownAmbiguousAndEditedResultsStayHeld(t *testing.T) {
	for _, mode := range []string{"missing", "null", "duplicate", "pr", "closed", "full", "truncated", "edited", "marker", "wrong_id", "repo_changed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, api := stoppedSplitGETFixture(t)
			api.mode = mode
			pages := 2
			if mode == "full" {
				pages = 1
			}
			r := SplitCreationReconciler{Store: k, MaxPages: pages, API: func(storage.ProductSplitCreate) ResourceAPI { return api }}
			if _, changed, err := r.Reconcile(ctx, job.ID); err == nil || changed {
				t.Fatal("incomplete GET granted child", mode, changed, err)
			}
			saved, err := k.ReadProductSplitCreate(ctx, job.ID)
			if err != nil || saved.State != "stopped" || saved.Issue != nil {
				t.Fatal("hold mutated unknown outcome", saved, err)
			}
		})
	}
}
func TestSplitCreationReconciliationUsesCompletePaginationAndTracksCanceledWave(t *testing.T) {
	ctx := context.Background()
	k, job, api := stoppedSplitGETFixture(t)
	api.mode = "multi"
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "cancel-after-send", Target: job.Value.Request.Target, Action: "cancel", Reason: "retain external outcome tracking"}); err != nil {
		t.Fatal(err)
	}
	r := SplitCreationReconciler{Store: k, MaxPages: 2, API: func(storage.ProductSplitCreate) ResourceAPI { return api }}
	got, changed, err := r.Reconcile(ctx, job.ID)
	if err != nil || !changed || got.State != "observed" {
		t.Fatal("cancellation lost sent outcome", got, changed, err)
	}
	if !reflect.DeepEqual(api.reads(), []string{"", "issues?state=all&per_page=100&page=1", "issues?state=all&per_page=100&page=2", "issues/11", ""}) {
		t.Fatal(api.reads())
	}
}

func (a *splitGETFixtureAPI) ResourcePages(context.Context, string, string) ([]json.RawMessage, error) {
	return nil, errors.New("unbounded pagination must not be used by split reconciliation")
}
