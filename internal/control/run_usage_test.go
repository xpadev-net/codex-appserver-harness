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
