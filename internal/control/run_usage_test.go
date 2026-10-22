package control

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestRuntimePersistsWholeObservedTurnUsageBeforeCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	k, b := taskFixture(t)
	p := fake(b)
	p.manualCompletion = true
	p.turnStarted = make(chan struct{})
	type result struct {
		out Outcome
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return p, nil })}).Execute(ctx, request(b))
		done <- result{out, err}
	}()
	await(t, p.turnStarted)
	for _, step := range []struct{ total, last int }{{30, 30}, {70, 40}, {70, 40}} {
		raw := json.RawMessage(fmt.Sprintf(`{"threadId":"thread","turnId":"turn","tokenUsage":{"total":{"totalTokens":%d},"last":{"totalTokens":%d}}}`, step.total, step.last))
		p.queue <- codex.Message{Method: "thread/tokenUsage/updated", Params: raw}
	}
	// The ledger must be durable while the turn is still running, so a crash or
	// a lost final response does not discard already observed usage.
	for {
		budget, err := k.WaveRunBudget(ctx, b.Target.WaveID)
		if err != nil {
			t.Fatal(err)
		}
		if budget.ObservedTokens == 70 {
			if budget.UnobservedRuns != 1 {
				t.Fatal("partial usage became final", budget)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("live usage not persisted", budget)
		case <-time.After(10 * time.Millisecond):
		}
	}
	raw, err := json.Marshal(map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn", "status": "completed", "items": []any{map[string]string{"type": "agentMessage", "text": p.resultText()}}}})
	if err != nil {
		t.Fatal(err)
	}
	p.queue <- codex.Message{Method: "turn/completed", Params: raw}
	out := await(t, done)
	if out.err != nil || !out.out.Stopped {
		t.Fatal(out)
	}
	var binding storage.RunBudgetBinding
	if err = k.Record(ctx, "run_budget", out.out.Run.ID, &binding); err != nil || binding.ObservedTokens != 70 || binding.FinalUsage || !binding.HasObservation {
		t.Fatal(binding, err)
	}
	assertCapacity(t, k, 0)
}

func TestPreparedUnusedThreadRetainsZeroBaselineOnResume(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	first := fake(b)
	owner, err := newOwner(ctx, k, factoryFunc(func(context.Context, Launch) (Process, error) { return first, nil }), b.ID, "not-dispatched", "execute", nil)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := owner.driver().Prepare(ctx, b.ID)
	if err != nil || prepared.UsageBaseline == nil || *prepared.UsageBaseline != 0 {
		t.Fatal(prepared, err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	// An actual empty thread/read on the next process confirms there are still
	// no turns; controller creation and run-reservation records prove zero.
	next := fake(b)
	next.manualCompletion = true
	next.turnStarted = make(chan struct{})
	next.threadReadReply = json.RawMessage(fmt.Sprintf(`{"thread":{"id":"thread","cwd":%q,"turns":[]}}`, b.CWD))
	type result struct {
		out Outcome
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := (Runtime{Store: k, Factory: factoryFunc(func(context.Context, Launch) (Process, error) { return next, nil })}).Execute(ctx, request(b))
		done <- result{out, err}
	}()
	await(t, next.turnStarted)
	next.queue <- codex.Message{Method: "thread/tokenUsage/updated", Params: json.RawMessage(`{"threadId":"thread","turnId":"turn","tokenUsage":{"total":{"totalTokens":70},"last":{"totalTokens":40}}}`)}
	next.queue <- codex.Message{Method: "turn/completed", Params: json.RawMessage(`{"threadId":"thread","turn":{"id":"turn","status":"completed","items":[{"type":"agentMessage","text":"{\"ok\":true}"}]}}`)}
	resultValue := await(t, done)
	if resultValue.err != nil || !resultValue.out.Stopped {
		t.Fatal(resultValue)
	}
	budget, err := k.WaveRunBudget(ctx, b.Target.WaveID)
	if err != nil || budget.ObservedTokens != 70 || budget.UnobservedRuns != 1 {
		t.Fatal(budget, err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	unused, err := k.SessionThreadUnused(ctx, b)
	if err != nil || unused {
		t.Fatal("used thread retained zero proof", unused, err)
	}
}
