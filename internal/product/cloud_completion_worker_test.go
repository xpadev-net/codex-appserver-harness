package product

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func completionWorkerFixture(t *testing.T) (CloudCompletionWorker, *cloudReadAPI, string) {
	t.Helper()
	r, requestAPI, saved, id := requestFixture(t)
	r.Gate = func(context.Context, storage.ProductTargetSnapshot) (ReviewRequest, error) {
		return ReviewRequest{Trigger: "@review please review", RequesterID: 71, CloudReviewerIDs: []int64{91}}, nil
	}
	if _, err := r.Request(context.Background(), id, "worker-owner"); err != nil {
		t.Fatal(err)
	}
	pr := *requestAPI.pr
	pr.Base.SHA = saved.Value.Candidate.BaseSHA
	api := &cloudReadAPI{repository: Repository{ID: 17, Name: saved.Value.Repository, DefaultBranch: saved.Value.BaseBranch}, pr: pr, comments: requestAPI.comments, reviews: []github.Review{{ID: 101, User: github.User{ID: 91}, State: "COMMENTED", CommitID: saved.Value.Candidate.SHA, SubmittedAt: time.Now().UTC()}}, fail: "never"}
	report := contract.NewReport(saved.Value.Repository)
	report.RepositoryID, report.DefaultBranch = 17, saved.Value.BaseBranch
	for _, key := range []string{"cloud_request", "review_sha"} {
		report.Capabilities[key] = contract.Capability{State: contract.Verified, Evidence: "fixture route"}
	}
	reader := CloudCompletionReader{Store: r.Store, API: func(storage.ProductTarget) ResourceAPI { return api }, Routes: map[int64]CloudReviewRoute{17: {ReadReport: func(context.Context) (contract.Report, error) { return report, nil }}}}
	return CloudCompletionWorker{Reader: reader, Workers: 1, Every: time.Hour}, api, id
}

func TestCloudCompletionWorkerSkipsDamagedRoundCommitsBeforeHintAndRestarts(t *testing.T) {
	w, api, id := completionWorkerFixture(t)
	k := w.Reader.Store
	// First opaque entry deliberately lacks its authenticated round. The next
	// real entry must still be processed with a single worker.
	broken := "000-damaged"
	if _, err := k.DB.Exec(`INSERT INTO kernel_dispatch_intents SELECT ?,session_id,wave_id,state,payload FROM kernel_dispatch_intents WHERE run_id=?`, broken, id); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec(`INSERT INTO kernel_tool_operations SELECT ?,session_id,thread_id,turn_id,'damaged',name,argument_digest,state,result,payload FROM kernel_tool_operations WHERE id=?`, broken, id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hints := make(chan struct{}, 1)
	failures := make(chan error, 8)
	w.Report = func(err error) { failures <- err }
	w.OnMutation = func() {
		if _, err := k.ReadProductCloudReviewCompletion(context.Background(), id); err != nil {
			failures <- err
		}
		hints <- struct{}{}
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-hints:
	case <-time.After(3 * time.Second):
		t.Fatal("damaged row starved valid request")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-failures:
	default:
		t.Fatal("corruption was not reported")
	}
	if api.metadata != 2 {
		t.Fatal("unexpected repeated reads", api.metadata)
	}
	// Reopened startup resumes from persisted held requests, never rescans the
	// successfully completed request and never creates another model run.
	var before int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&before); err != nil {
		t.Fatal(err)
	}
	// Startup validation deliberately rejects corrupted capacity metadata.
	// Remove only the synthetic scheduling rows before testing a valid reopen.
	if _, err := k.DB.Exec("DELETE FROM kernel_dispatch_intents WHERE run_id=?", broken); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_tool_operations WHERE id=?", broken); err != nil {
		t.Fatal(err)
	}
	root := k.Root
	k.Close()
	var err error
	k, err = storage.OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	w.Reader.Store = k
	w.OnMutation = func() { t.Error("replayed completion emitted hint") }
	ctx, stop := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer stop()
	if err := w.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	var after int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&after); err != nil || before != after || api.metadata != 2 {
		t.Fatal("restart rescanned or launched model", before, after, api.metadata, err)
	}
}

type joiningCompletionAPI struct {
	ResourceAPI
	started, canceled, release chan struct{}
	calls                      atomic.Int64
}

func (a *joiningCompletionAPI) ResourceGET(ctx context.Context, path string) (json.RawMessage, error) {
	a.calls.Add(1)
	close(a.started)
	<-ctx.Done()
	close(a.canceled)
	<-a.release
	return nil, ctx.Err()
}

func TestCloudCompletionWorkerJoinsCanceledTransportBeforeReturning(t *testing.T) {
	w, api, id := completionWorkerFixture(t)
	blocked := &joiningCompletionAPI{ResourceAPI: api, started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	w.Reader.API = func(storage.ProductTarget) ResourceAPI { return blocked }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-blocked.started:
	case <-time.After(3 * time.Second):
		t.Fatal("read not started")
	}
	cancel()
	select {
	case <-blocked.canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("transport not canceled")
	}
	select {
	case err := <-done:
		t.Fatal("returned before physical reader stop", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(blocked.release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read not joined")
	}
	seat, err := w.Reader.Store.OperationReservation(context.Background(), id)
	if err != nil || seat.State != "held" {
		t.Fatal("canceled read released capacity", seat, err)
	}
}

func TestCloudCompletionWorkerHeldResponseWaitsForPollWithoutHints(t *testing.T) {
	w, api, id := completionWorkerFixture(t)
	api.reviews = nil
	var hints, failures int
	w.OnMutation = func() { hints++ }
	w.Report = func(error) { failures++ }
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if hints != 0 || failures != 0 || api.metadata != 1 {
		t.Fatal("held work spun or emitted hint", hints, failures, api.metadata)
	}
	seat, err := w.Reader.Store.OperationReservation(context.Background(), id)
	if err != nil || seat.State != "held" {
		t.Fatal(seat, err)
	}
}
