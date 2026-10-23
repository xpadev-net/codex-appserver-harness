package product

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func sourceObserverFixture(t *testing.T) (IssueSourceObserver, *issueAdmissionAPI, domain.Wave) {
	t.Helper()
	a, api := issueAdmissionFixture(t)
	w, err := a.Admit(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	api.reads = nil
	api.issueReads = 0
	api.metaReads = 0
	return IssueSourceObserver{Store: a.Store, API: api, Policy: a.Policy, Attempts: 1, Every: time.Millisecond}, api, w
}
func scanSource(t *testing.T, o IssueSourceObserver) IssueSourceScan {
	t.Helper()
	// A completed page resets only the pagination position, so an individual
	// source is revisited on the following scan if necessary.
	result, err := o.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempted == 0 {
		result, err = o.Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}
func TestIssueSourceObserverPersistsHoldAndRecoveryWithoutModelTurn(t *testing.T) {
	o, api, w := sourceObserverFixture(t)
	ctx := context.Background()
	var notifications int
	if err := o.Store.DB.QueryRow("SELECT count(*) FROM kernel_notifications").Scan(&notifications); err != nil {
		t.Fatal(err)
	}
	result := scanSource(t, o)
	if result.Attempted != 1 || len(result.Changed) != 0 || len(result.Failed) != 0 {
		t.Fatal(result)
	}
	if !reflect.DeepEqual(api.reads, []string{"", "issues/42", "issues/42", ""}) {
		t.Fatal(api.reads)
	}
	api.issue.Labels = append(api.issue.Labels, struct {
		Name string `json:"name"`
	}{"hold"})
	woke := 0
	o.OnMutation = func() {
		current, err := o.Store.Wave(ctx, w.ID)
		if err != nil || current.SourceHold == nil {
			t.Fatal("hint preceded commit", current, err)
		}
		woke++
	}
	result = scanSource(t, o)
	current, err := o.Store.Wave(ctx, w.ID)
	if err != nil || current.SourceHold == nil || current.SourceHold.Reason != "hold" || woke != 1 || len(result.Changed) != 1 {
		t.Fatal(result, current, woke, err)
	}
	result = scanSource(t, o)
	if len(result.Changed) != 0 || woke != 1 {
		t.Fatal("identical poll rewrote hold", result, woke)
	}
	if _, err := o.Store.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "manual", Target: w.Target(), Action: "pause", Reason: "operator pause"}); err != nil {
		t.Fatal(err)
	}
	api.issue.Labels = api.issue.Labels[:1]
	o.OnMutation = nil
	result = scanSource(t, o)
	current, err = o.Store.Wave(ctx, w.ID)
	if err != nil || current.SourceHold != nil || !current.Paused || !current.DispatchPaused() || len(result.Changed) != 1 {
		t.Fatal("ready erased manual pause", result, current, err)
	}
	var after, runs int
	if err := o.Store.DB.QueryRow("SELECT count(*) FROM kernel_notifications").Scan(&after); err != nil || after != notifications {
		t.Fatal("poll created notifications", after, notifications, err)
	}
	if err := o.Store.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 0 {
		t.Fatal("poll created model run", runs, err)
	}
}
func TestIssueSourceObserverNonreadyAndFailedReadsNeverBecomeReady(t *testing.T) {
	for _, mode := range []string{"closed", "hold", "not_ready", "edited", "read_failed", "repository", "pull_request", "changing", "policy"} {
		t.Run(mode, func(t *testing.T) {
			o, api, w := sourceObserverFixture(t)
			want := "unavailable"
			switch mode {
			case "closed":
				api.issue.State = "closed"
				want = "closed"
			case "hold":
				api.issue.Labels = append(api.issue.Labels, struct {
					Name string `json:"name"`
				}{"hold"})
				want = "hold"
			case "not_ready":
				api.issue.Labels = nil
				want = "not_ready"
			case "edited":
				api.issue.Body += "\nchanged request"
				want = "request_changed"
			case "read_failed":
				api.fail = "issues/42"
			case "repository":
				api.repositoryID = 18
			case "pull_request":
				api.issue.PullRequest = json.RawMessage(`{"url":"a PR is not an Issue"}`)
			case "changing":
				api.onFinal = func(a *issueAdmissionAPI) { a.issue.Title += " edited" }
			case "policy":
				o.Policy.ReadyLabel = "different-ready-rule"
				want = "policy_changed"
			}
			result := scanSource(t, o)
			snapshot, err := o.Store.ReadProductSourceObservation(context.Background(), w.ID)
			current, waveErr := o.Store.Wave(context.Background(), w.ID)
			if err != nil || waveErr != nil || snapshot.Value.Status != want || current.SourceHold == nil || current.SourceHold.Reason != want || !current.DispatchPaused() {
				t.Fatal(result, snapshot, current, err, waveErr)
			}
			if want == "unavailable" && (snapshot.Value.Facts.Available || len(result.Failed) != 1) {
				t.Fatal("failed GET facts reused", result, snapshot)
			}
		})
	}
}
func TestIssueSourceObserverStaleReadCannotReplaceConcurrentProhibition(t *testing.T) {
	o, api, w := sourceObserverFixture(t)
	scanSource(t, o)
	prior, err := o.Store.ReadProductSourceObservation(context.Background(), w.ID)
	if err != nil {
		t.Fatal(err)
	}
	api.onFinal = func(a *issueAdmissionAPI) {
		facts := prior.Value.Facts
		facts.Hold = true
		if _, err := o.Store.ObserveProductSource(context.Background(), w.Target(), prior.Digest, facts); err != nil {
			t.Fatal(err)
		}
	}
	result, scanErr := o.Scan(context.Background())
	if result.Attempted == 0 {
		result, scanErr = o.Scan(context.Background())
	}
	if scanErr == nil || !strings.Contains(scanErr.Error(), "CAS conflict") {
		t.Fatal("conflicting read was acknowledged", result, scanErr)
	}
	current, err := o.Store.Wave(context.Background(), w.ID)
	if err != nil || current.SourceHold == nil || current.SourceHold.Reason != "hold" || len(result.Failed) != 1 {
		t.Fatal("stale source read cleared newer hold", result, current, err)
	}
}
func TestIssueSourceObserverCursorReopensAndVisitsEverySource(t *testing.T) {
	o, api, w := sourceObserverFixture(t)
	waves := []domain.Wave{w}
	a := IssueAdmitter{Store: o.Store, API: api, Policy: o.Policy, SessionID: "main", Limits: domain.DefaultKernelLimits()}
	for _, n := range []int{43, 44, 45} {
		issue := api.issue
		issue.Number = n
		api.dependencies[fmt.Sprintf("issues/%d", n)] = issue
		other, err := a.Admit(context.Background(), n)
		if err != nil {
			t.Fatal(err)
		}
		waves = append(waves, other)
	}
	sort.Slice(waves, func(i, j int) bool { return waves[i].ID < waves[j].ID })
	for i, want := range waves {
		result := scanSource(t, o)
		if result.Attempted != 1 || len(result.Failed) != 0 {
			t.Fatal(result)
		}
		if _, err := o.Store.ReadProductSourceObservation(context.Background(), want.ID); err != nil {
			t.Fatal("cursor skipped source", i, want.ID, err)
		}
		if i == 0 {
			file := filepath.Join(o.Store.Root, "state.db")
			o.Store.Close()
			var err error
			o.Store, err = storage.OpenExistingKernel(file)
			if err != nil {
				t.Fatal(err)
			}
			defer o.Store.Close()
		}
	}
}

type stalledSourceAPI struct {
	ResourceAPI
	started, stopped, release chan struct{}
}

func (a *stalledSourceAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	if path == "issues/42" {
		close(a.started)
		<-ctx.Done()
		close(a.stopped)
		if a.release != nil {
			<-a.release
		}
		return nil, ctx.Err()
	}
	return a.ResourceAPI.ResourceGET(ctx, path)
}
func TestIssueSourceObserverReadTimeoutHoldsAndCheckpointsAttempt(t *testing.T) {
	for _, mode := range []string{"read", "scan"} {
		t.Run(mode, func(t *testing.T) {
			o, api, w := sourceObserverFixture(t)
			stalled := &stalledSourceAPI{ResourceAPI: api, started: make(chan struct{}), stopped: make(chan struct{})}
			o.API = stalled
			scanTimeout, readTimeout := time.Second, 100*time.Millisecond
			if mode == "scan" {
				scanTimeout, readTimeout = 100*time.Millisecond, time.Second
			}
			result, err := o.scan(context.Background(), scanTimeout, readTimeout)
			if mode == "read" && err != nil || mode == "scan" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(mode, result, err)
			}
			current, waveErr := o.Store.Wave(context.Background(), w.ID)
			if waveErr != nil || current.SourceHold == nil || current.SourceHold.Reason != "unavailable" {
				t.Fatal("timeout lost source prohibition", current, waveErr)
			}
			var cursor issueSourceCursor
			if err := o.Store.Record(context.Background(), "product_issue_source_cursor", "17", &cursor); err != nil || cursor.After != w.ID {
				t.Fatal("timed out attempt lost progress", cursor, err)
			}
		})
	}
}
func TestIssueSourceObserverCancellationJoinsReadWithoutDetachedMutation(t *testing.T) {
	o, api, w := sourceObserverFixture(t)
	stalled := &stalledSourceAPI{ResourceAPI: api, started: make(chan struct{}), stopped: make(chan struct{}), release: make(chan struct{})}
	o.API = stalled
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()
	select {
	case <-stalled.started:
	case <-time.After(time.Second):
		t.Fatal("source read did not start")
	}
	cancel()
	select {
	case <-stalled.stopped:
	case <-time.After(time.Second):
		t.Fatal("source read did not stop")
	}
	select {
	case err := <-done:
		t.Fatal("observer did not join reader", err)
	default:
	}
	close(stalled.release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not finish")
	}
	current, err := o.Store.Wave(context.Background(), w.ID)
	if err != nil || current.SourceHold != nil {
		t.Fatal("canceled scan wrote after shutdown", current, err)
	}
	var count int
	if err := o.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind LIKE 'product_source_observation%' OR kind='product_issue_source_cursor'").Scan(&count); err != nil || count != 0 {
		t.Fatal("detached observation after cancellation", count, err)
	}
}

type oversizedSourceAPI struct {
	ResourceAPI
	reads int
}

func (a *oversizedSourceAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	a.reads++
	if path == "issues/42" {
		return json.RawMessage(strings.Repeat("x", 17<<20)), errors.New("excessive source output")
	}
	return a.ResourceAPI.ResourceGET(ctx, path)
}
func TestIssueSourceObserverExcessiveReadHoldsWithoutAdditionalRequests(t *testing.T) {
	o, api, w := sourceObserverFixture(t)
	huge := &oversizedSourceAPI{ResourceAPI: api}
	o.API = huge
	result, err := o.Scan(context.Background())
	if err != nil || result.Attempted != 1 || len(result.Failed) != 1 || huge.reads != 2 {
		t.Fatal(result, huge.reads, err)
	}
	current, err := o.Store.Wave(context.Background(), w.ID)
	if err != nil || current.SourceHold == nil || current.SourceHold.Reason != "unavailable" {
		t.Fatal(current, err)
	}
}

func TestIssueSourceObserverPolicyMismatchFailsBeforeGETAndPreservesPinnedFacts(t *testing.T) {
	o, api, w := sourceObserverFixture(t)
	originalPolicy := o.Policy
	originalBody := api.issue.Body
	o.Policy.ReadyLabel = "different-ready-rule"
	reports := 0
	o.Report = func(err error) {
		if !errors.Is(err, ErrIssueSourcePolicyConflict) {
			t.Fatal(err)
		}
		reports++
	}
	first := scanSource(t, o)
	snapshot, err := o.Store.ReadProductSourceObservation(context.Background(), w.ID)
	if err != nil || len(first.Failed) != 1 || snapshot.Value.Status != "policy_changed" || snapshot.Value.Facts.Available || snapshot.Value.Facts.RequestDigest != "" || len(api.reads) != 0 {
		t.Fatal(first, snapshot, api.reads, err)
	}
	api.issue.Body += " changed while configured policy conflicts"
	second := scanSource(t, o)
	repeated, err := o.Store.ReadProductSourceObservation(context.Background(), w.ID)
	if err != nil || len(second.Failed) != 1 || len(second.Changed) != 0 || len(api.reads) != 0 || reports != 2 || !reflect.DeepEqual(snapshot, repeated) {
		t.Fatal("policy conflict adopted changed source facts or hid retry failure", second, repeated, api.reads, reports, err)
	}
	pinned, err := o.Store.ReadProductSourcePolicy(context.Background(), w.Target())
	if err != nil || pinned != originalPolicy.Digest() {
		t.Fatal("observer replaced authoritative policy pin", pinned, err)
	}
	o.Policy = originalPolicy
	o.Report = nil
	api.issue.Body = originalBody
	recovered := scanSource(t, o)
	current, err := o.Store.Wave(context.Background(), w.ID)
	if err != nil || len(recovered.Failed) != 0 || current.SourceHold != nil || len(api.reads) != 4 {
		t.Fatal("restored pinned policy could not recover", recovered, current, api.reads, err)
	}
}

func TestIssueSourceObserverUncommittedObservationPreservesCursorForRetry(t *testing.T) {
	o, api, w := sourceObserverFixture(t)
	ctx := context.Background()
	initial := issueSourceCursor{PolicyDigest: o.Policy.Digest()}
	if err := o.Store.Put(ctx, "product_issue_source_cursor", "17", "controller", initial); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Store.DB.Exec("CREATE TRIGGER fail_issue_source_commit BEFORE INSERT ON records WHEN NEW.kind='product_source_observation' BEGIN SELECT RAISE(FAIL,'source commit injection'); END"); err != nil {
		t.Fatal(err)
	}
	result, err := o.Scan(ctx)
	if err == nil || !strings.Contains(err.Error(), "source commit injection") || result.Attempted != 1 {
		t.Fatal(result, err)
	}
	var saved issueSourceCursor
	if err := o.Store.Record(ctx, "product_issue_source_cursor", "17", &saved); err != nil || saved != initial {
		t.Fatal("uncommitted source advanced cursor", saved, err)
	}
	var count int
	if err := o.Store.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_source_observation'").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed observation leaked", count, err)
	}
	if _, err := o.Store.DB.Exec("DROP TRIGGER fail_issue_source_commit"); err != nil {
		t.Fatal(err)
	}
	result, err = o.Scan(ctx)
	if err != nil || result.Attempted != 1 || len(api.reads) != 8 {
		t.Fatal("uncommitted source not retried", result, api.reads, err)
	}
	if _, err := o.Store.ReadProductSourceObservation(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
}

func TestIssueSourceObserverExpiredPreflightHasNoAcknowledgementOrGET(t *testing.T) {
	o, api, w := sourceObserverFixture(t)
	readCtx, cancel := context.WithCancel(context.Background())
	cancel()
	update, acknowledged, err := o.observe(context.Background(), readCtx, o.API, w, time.Second)
	if !errors.Is(err, context.Canceled) || acknowledged || update.Snapshot.ID != "" || len(api.reads) != 0 {
		t.Fatal("expired preflight acknowledged unobserved source", update, acknowledged, api.reads, err)
	}
}

func TestIssueSourceObserverCommittedInvalidStateMayCheckpointWithoutBlockingLaterSources(t *testing.T) {
	o, api, w := sourceObserverFixture(t)
	ctx := context.Background()
	if _, err := o.Store.DB.Exec("UPDATE records SET payload='broken pin' WHERE kind='product_source_policy' AND work_item_id=?", w.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		result := scanSource(t, o)
		current, err := o.Store.Wave(ctx, w.ID)
		if err != nil || len(result.Failed) != 1 || current.SourceHold == nil || current.SourceHold.Reason != "state_invalid" || len(api.reads) != 0 {
			t.Fatal(result, current, api.reads, err)
		}
		var saved issueSourceCursor
		if err := o.Store.Record(ctx, "product_issue_source_cursor", "17", &saved); err != nil || saved.After != w.ID {
			t.Fatal("durable invalid-state hold blocked scan progress", saved, err)
		}
	}
}
