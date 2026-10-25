package product

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type splitSourceFixtureAPI struct {
	issues   map[string]splitChildSourceIssue
	calls    []string
	mode     string
	affected string
	hook     func()
}

func (a *splitSourceFixtureAPI) ResourcePages(context.Context, string, string) ([]json.RawMessage, error) {
	return nil, errors.New("source observer must not list resources")
}

func (a *splitSourceFixtureAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	a.calls = append(a.calls, path)
	if a.hook != nil {
		hook := a.hook
		a.hook = nil
		hook()
	}
	if path == "" {
		id := int64(17)
		if a.mode == "repo_changed" && len(a.calls) > 1 {
			id++
		}
		return json.Marshal(Repository{ID: id, Name: "owner/repo"})
	}
	issue, ok := a.issues[path]
	if !ok {
		return nil, fmt.Errorf("unexpected GET %q", path)
	}
	if path == a.affected {
		switch a.mode {
		case "read_failed":
			return nil, errors.New("fixture read failed")
		case "cancel":
			<-ctx.Done()
			return nil, ctx.Err()
		case "edited":
			issue.Body += "changed"
		case "changing":
			if len(a.calls)%4 == 3 {
				issue.Title += "changed"
			}
		case "closed":
			issue.State = "closed"
		case "identity":
			issue.ID++
		case "pr":
			issue.PullRequest = json.RawMessage("{}")
		case "hold":
			issue.Labels = append(issue.Labels, struct {
				Name string `json:"name"`
			}{"hold"})
		}
	}
	return json.Marshal(issue)
}
func splitChildSourceFixture(t *testing.T) (SplitChildSourceObserver, *splitSourceFixtureAPI, storage.ProductSplitRegistrations, string) {
	t.Helper()
	k, jobs, apis, bin := splitWorkerRegistrationFixture(t)
	worker := splitRegistrationWorker(k, apis)
	for _, job := range jobs {
		_, err := worker.process(context.Background(), storage.SplitCreationReconciliationTarget{ID: job.ID})
		if err != nil && !errors.Is(err, storage.ErrProductSplitHeld) {
			t.Fatal(err)
		}
	}
	saved, err := k.ReadProductSplitRegistrations(context.Background(), jobs[0].Value.Request)
	if err != nil {
		t.Fatal(err)
	}
	api := &splitSourceFixtureAPI{issues: map[string]splitChildSourceIssue{}, affected: "issues/11"}
	for _, child := range saved.Value.Children {
		v := child.Value.Issue
		issue := splitChildSourceIssue{ID: v.ID}
		issue.Number = v.Number
		issue.Title = v.Title
		issue.Body = v.Body
		issue.State = "open"
		issue.UpdatedAt = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
		api.issues[fmt.Sprintf("issues/%d", v.Number)] = issue
	}
	return SplitChildSourceObserver{Store: k, API: api, Policy: IssueSourcePolicy{Repository: "owner/repo", RepositoryID: 17, ReadyLabel: "ready", HoldLabel: "hold"}, Attempts: 32}, api, saved, bin
}

func TestSplitChildSourceGateDirectParentRequiresFreshChildAndKeepsPeerIndependent(t *testing.T) {
	o, api, saved, bin := splitChildSourceFixture(t)
	ctx := context.Background()
	target := saved.Value.Request.Target
	before, err := o.Store.Wave(ctx, target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := o.Store.WaveOperationBudget(ctx, target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	first := saved.Value.Registrations[0].Value.Definition
	second := saved.Value.Registrations[1].Value.Definition
	snapshot := storage.ProductTargetSnapshot{Value: storage.ProductTarget{Target: target, WorkItem: domain.WorkItem{ID: first.ID, RepositoryID: first.RepositoryID}, Repository: first.Repository}}
	gate := KernelSourceGate{Store: o.Store, Policies: map[int64]IssueSourcePolicy{17: o.Policy}, API: func(storage.ProductTarget) ResourceAPI { return api }}
	if err := gate.Check(ctx, snapshot); err != nil {
		t.Fatal("generated child without ready label rejected", err)
	}
	if !reflect.DeepEqual(api.calls, []string{"", "issues/11", "issues/11", ""}) {
		t.Fatal(api.calls)
	}
	for _, mode := range []string{"hold", "closed", "edited", "read_failed", "changing", "identity", "pr", "repo_changed", "policy", "missing_policy"} {
		t.Run(mode, func(t *testing.T) {
			api.calls = nil
			api.mode = mode
			policy := o.Policy
			if mode == "policy" {
				policy.ReadyLabel = "changed"
			}
			gate.Policies = map[int64]IssueSourcePolicy{17: policy}
			if mode == "missing_policy" {
				gate.Policies = nil
			}
			if err := gate.Check(ctx, snapshot); err == nil {
				t.Fatal("stale ready bypassed fresh check", mode)
			}
			if (mode == "policy" || mode == "missing_policy") && len(api.calls) != 0 {
				t.Fatal("untrusted policy read external issue", api.calls)
			}
			api.mode = ""
			gate.Policies = map[int64]IssueSourcePolicy{17: o.Policy}
			peer := snapshot
			peer.Value.WorkItem = domain.WorkItem{ID: second.ID, RepositoryID: second.RepositoryID}
			if err := gate.Check(ctx, peer); err != nil {
				t.Fatal("unrelated child blocked", err)
			}
			if err := gate.Check(ctx, snapshot); err != nil {
				t.Fatal("restored source did not recover", err)
			}
		})
	}
	after, err := o.Store.Wave(ctx, target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("child prohibition changed Wave", err)
	}
	afterBudget, err := o.Store.WaveOperationBudget(ctx, target.WaveID)
	if err != nil || !reflect.DeepEqual(budget, afterBudget) {
		t.Fatal("source polling changed budget", err)
	}
	if strings.Count(splitHTTPCalls(t, bin), "POST\n") != 2 {
		t.Fatal("source polling created another issue")
	}
	var tasks int
	if err := o.Store.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks); err != nil || tasks != 0 {
		t.Fatal("source read launched model", tasks, err)
	}
}

func TestSplitChildSourceScanDeduplicatesAndResumesAfterFailureAndReopen(t *testing.T) {
	o, api, saved, _ := splitChildSourceFixture(t)
	ctx := context.Background()
	hints := 0
	o.OnMutation = func() { hints++ }
	o.Attempts = 1
	api.mode = "read_failed"
	first, err := o.Scan(ctx)
	if !errors.Is(err, ErrIssueSourceScanBudget) || first.Attempted != 1 || len(first.Failed) != 1 || hints != 1 {
		t.Fatal(first, hints, err)
	}
	path := o.Store.Root + "/state.db"
	o.Store.Close()
	o.Store, err = storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Store.Close()
	api.mode = ""
	api.calls = nil
	second, err := o.Scan(ctx)
	if err != nil || second.Attempted != 1 || len(second.Failed) != 0 || hints != 2 {
		t.Fatal(second, hints, err)
	}
	if !reflect.DeepEqual(api.calls, []string{"", "issues/12", "issues/12", ""}) {
		t.Fatal("cursor repeated failed child or duplicated binding", api.calls)
	}
	o.Attempts = 32
	api.calls = nil
	third, err := o.Scan(ctx)
	if err != nil || third.Attempted != 2 || hints != 3 || len(api.calls) != 8 {
		t.Fatal(third, hints, api.calls, err)
	}
	fourth, err := o.Scan(ctx)
	if err != nil || fourth.Attempted != 2 || len(fourth.Changed) != 0 || hints != 3 {
		t.Fatal("freshness-only renewal emitted hint", fourth, hints, err)
	}
	for _, child := range saved.Value.Children {
		if _, err := o.Store.RequireProductSplitChildSource(ctx, saved.Value.Request.Target, child.Value.Child.Value.WorkItemID, o.Policy.Digest(), time.Minute); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSplitChildSourceMissingProofCancelsWithoutGETOrDetachedWrite(t *testing.T) {
	for _, mode := range []string{"missing_scope", "missing_source", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			o, api, saved, _ := splitChildSourceFixture(t)
			target := saved.Value.Request.Target
			id := saved.Value.Registrations[0].Value.Definition.ID
			ctx := context.Background()
			if mode == "missing_source" {
				if _, _, err := o.Observe(ctx, target, id); err != nil {
					t.Fatal(err)
				}
				if _, err := o.Store.DB.Exec("DELETE FROM records WHERE kind='product_split_child_source' AND id=?", id); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "missing_scope" {
				if _, err := o.Store.DB.Exec("DELETE FROM records WHERE kind='product_split_child_scope' AND id=?", id); err != nil {
					t.Fatal(err)
				}
			}
			api.calls = nil
			if mode == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				api.mode = "cancel"
				api.hook = cancel
			}
			_, ack, err := o.Observe(ctx, target, id)
			if err == nil || ack {
				t.Fatal("lost/canceled proof acknowledged", ack, err)
			}
			if mode != "cancel" && len(api.calls) != 0 {
				t.Fatal("missing authoritative proof read GitHub", api.calls)
			}
			if mode == "cancel" {
				if _, err := o.Store.ReadProductSplitChildSource(context.Background(), target, id); !errors.Is(err, sql.ErrNoRows) {
					t.Fatal("cancellation wrote detached result", err)
				}
			}
		})
	}
}

func TestSplitChildSourceScanIsolatesCorruptionAndFailedCommitAndRejectsStaleCallback(t *testing.T) {
	o, api, saved, _ := splitChildSourceFixture(t)
	ctx := context.Background()
	target := saved.Value.Request.Target
	id := saved.Value.Registrations[0].Value.Definition.ID
	if _, err := o.Store.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_scope',?,'foreign-wave','{')", strings.Repeat("bad", 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Store.DB.Exec("CREATE TRIGGER fail_child_source BEFORE INSERT ON records WHEN NEW.kind='product_split_child_source' AND NEW.id='" + id + "' BEGIN SELECT RAISE(ABORT,'fixture source commit failed'); END"); err != nil {
		t.Fatal(err)
	}
	hints, reports := 0, 0
	o.OnMutation = func() { hints++ }
	o.Report = func(error) { reports++ }
	scan, err := o.Scan(ctx)
	if err != nil || scan.Attempted != 2 || len(scan.Failed) != 2 || hints != 1 || reports != 2 {
		t.Fatal("failed/corrupt rows blocked independent child or emitted uncommitted hint", scan, hints, reports, err)
	}
	if _, err := o.Store.ReadProductSplitChildSource(ctx, target, id); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("failed commit leaked source", err)
	}
	if _, err := o.Store.DB.Exec("DROP TRIGGER fail_child_source"); err != nil {
		t.Fatal(err)
	}
	first, ack, err := o.Observe(ctx, target, id)
	if err != nil || !ack {
		t.Fatal(first, ack, err)
	}
	var concurrent storage.ProductSplitChildSourceUpdate
	api.hook = func() {
		facts := first.Snapshot.Value.Facts
		facts.Hold = true
		var e error
		concurrent, e = o.Store.ObserveProductSplitChildSource(ctx, target, id, first.Snapshot.ID, facts)
		if e != nil {
			t.Fatal(e)
		}
	}
	stale, ack, err := o.Observe(ctx, target, id)
	if !errors.Is(err, storage.ErrProductSplitHeld) || ack || stale.Changed || stale.Snapshot.ID != "" {
		t.Fatal("delayed GET overwrote newer source", stale, ack, err)
	}
	current, err := o.Store.ReadProductSplitChildSource(ctx, target, id)
	if err != nil || current.ID != concurrent.Snapshot.ID || current.Value.Status != "hold" {
		t.Fatal("concurrent prohibition lost", current, err)
	}
}
