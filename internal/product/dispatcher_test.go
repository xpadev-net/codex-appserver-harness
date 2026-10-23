package product

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func publicationDispatcher(p Publisher) Dispatcher {
	return Dispatcher{Store: p.Store, Publisher: p, Reviewer: ReviewRequester{Store: p.Store,
		Gate: func(context.Context, storage.ProductTargetSnapshot) (ReviewRequest, error) {
			return ReviewRequest{}, errors.New("review capability unavailable in publication fixture")
		},
		API: func(storage.ProductTarget, func(context.Context, string, string) error) ReviewAPI { return nil }}, Every: time.Hour}
}
func dispatchUntilMutation(t *testing.T, d Dispatcher) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mutated := make(chan struct{}, 8)
	d.OnMutation = func() { mutated <- struct{}{} }
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	select {
	case <-mutated:
	case <-time.After(3 * time.Second):
		t.Fatal("PR dispatch did not finish")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("dispatch worker not joined")
	}
}

func TestPRDispatcherPreparedRestartRetriesThroughSameOneUseSendFence(t *testing.T) {
	p, api, _, id := publicationFixture(t)
	good := p.Gate
	p.Gate = func(context.Context, storage.ProductTargetSnapshot) (Publication, error) {
		return Publication{}, errors.New("temporary canonical gate read failure")
	}
	d := publicationDispatcher(p)
	dispatchUntilMutation(t, d)
	var claim storage.PRDispatchClaim
	if err := p.Store.Record(context.Background(), "product_pr_dispatch", id, &claim); err != nil || claim.State != "prepared" || claim.OwnerID != dispatchOwner(id) || api.writes != 0 {
		t.Fatal(claim, api.writes, err)
	}
	path := p.Store.Root + "/state.db"
	if err := p.Store.Close(); err != nil {
		t.Fatal(err)
	}
	k, err := storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	p.Store, p.Gate = k, good
	d = publicationDispatcher(p)
	dispatchUntilMutation(t, d)
	if api.writes != 1 {
		t.Fatal("known-unsent operation did not resume exactly once", api.writes)
	}
	page, err := k.PRDispatchOperations(context.Background(), "", 16)
	if err != nil || len(page.Operations) != 0 {
		t.Fatal("completed send requeued", page, err)
	}
}

func TestPRDispatcherLostResponseNeverRequeuesUnknownAcrossRestart(t *testing.T) {
	p, api, _, id := publicationFixture(t)
	api.loseResponse = true
	d := publicationDispatcher(p)
	dispatchUntilMutation(t, d)
	if api.writes != 1 {
		t.Fatal(api.writes)
	}
	var claim storage.PRDispatchClaim
	if err := p.Store.Record(context.Background(), "product_pr_dispatch", id, &claim); err != nil || claim.State != "unknown" {
		t.Fatal(claim, err)
	}
	path := p.Store.Root + "/state.db"
	p.Store.Close()
	k, err := storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	p.Store = k
	d = publicationDispatcher(p)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if api.writes != 1 {
		t.Fatal("unknown replayed external mutation", api.writes)
	}
	if err := k.Record(context.Background(), "product_pr_dispatch", id, &claim); err != nil || claim.State != "unknown" {
		t.Fatal(claim, err)
	}
}

func TestPRDispatcherBlockedOperationDoesNotBlockAnotherResourceOrDatabase(t *testing.T) {
	limits := domain.DefaultExecutionLimits()
	limits.PRs = 2
	p, first, saved, _ := publicationFixtureLimits(t, &limits)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := saved.Value
	second.WorkItem.ID = "independent"
	second.WorkItem.Branch = "harness/independent"
	second.WorkItem.Candidate = "candidate-independent"
	second.Candidate.ID = second.WorkItem.Candidate
	if _, err := p.Store.SetProductTarget(ctx, "", second); err != nil {
		t.Fatal(err)
	}
	b, err := p.Store.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(contract.PublishPRRequest{SchemaVersion: 1, Target: second.Target, Candidate: contract.CandidateReference{WorkItemID: second.WorkItem.ID, CandidateID: second.Candidate.ID, SHA: second.Candidate.SHA}})
	reply, err := (contract.KernelGateway{Backend: p.Store}).Handle(ctx, contract.ToolInvocation{SessionID: b.ID, RunID: "model-run", ThreadID: b.ThreadID, TurnID: "turn", CallID: "independent-publish", Name: "pr_publish", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	secondAPI := &fakeAPI{repository: first.repository, head: first.head}
	started := make(chan struct{}, 1)
	good := p.Gate
	p.Gate = func(ctx context.Context, s storage.ProductTargetSnapshot) (Publication, error) {
		if s.Value.WorkItem.ID == saved.Value.WorkItem.ID {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return Publication{}, ctx.Err()
		}
		return good(ctx, s)
	}
	p.API = func(target storage.ProductTarget, before func(context.Context, string, string) error) API {
		secondAPI.before = before
		return secondAPI
	}
	d := publicationDispatcher(p)
	d.Workers = 2
	failures := make(chan error, 4)
	d.Report = func(err error) { failures <- err }
	mutated := make(chan struct{}, 4)
	d.OnMutation = func() { mutated <- struct{}{} }
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("blocked operation not started")
	}
	select {
	case <-mutated:
	case <-time.After(3 * time.Second):
		t.Fatal("independent PR stalled")
	}
	var receipt storage.PRCompletionReceipt
	if err := p.Store.Record(context.Background(), "product_pr_completion", reply.OperationID, &receipt); err != nil {
		select {
		case failure := <-failures:
			t.Fatal("independent dispatch failed", failure)
		default:
		}
		t.Fatal(err)
	}
	readCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := p.Store.StateEvents(readCtx, 0, 1); err != nil {
		t.Fatal("external wait held DB transaction", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked dispatch not joined")
	}
	if secondAPI.writes != 1 || first.writes != 0 {
		t.Fatal(first.writes, secondAPI.writes)
	}
}

func TestPRDispatcherRoutesReviewThroughGuardedRequester(t *testing.T) {
	r, api, _, id := requestFixture(t)
	p := Publisher{Store: r.Store, Gate: func(context.Context, storage.ProductTargetSnapshot) (Publication, error) {
		return Publication{}, errors.New("publication unavailable")
	}, API: func(storage.ProductTarget, func(context.Context, string, string) error) API { return nil }}
	d := publicationDispatcher(p)
	d.Reviewer = r
	dispatchUntilMutation(t, d)
	var receipt storage.PRCompletionReceipt
	if err := r.Store.Record(context.Background(), "product_pr_completion", id, &receipt); err != nil || receipt.Outcome != "review_requested" || api.requestWrites != 1 {
		t.Fatal(receipt, api.requestWrites, err)
	}
}
