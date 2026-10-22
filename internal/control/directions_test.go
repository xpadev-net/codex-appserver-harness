package control

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type directionSource struct {
	*sourceFixture
	store   *storage.KernelStore
	waveID  string
	applied chan struct{}
}

func (s directionSource) Ready(ctx context.Context, after string, limit int) (WorkPage, error) {
	w, err := s.store.Wave(ctx, s.waveID)
	if err != nil {
		return WorkPage{}, err
	}
	if w.ScopeRevision > 1 {
		select {
		case s.applied <- struct{}{}:
		default:
		}
	}
	return s.sourceFixture.Ready(ctx, after, limit)
}
func (s directionSource) Complete(ctx context.Context, c Completion) error {
	if err := s.store.ArchiveSessionRun(ctx, c.SessionID, c.RunID, "trusted fixture disposed completed old-scope run"); err != nil {
		return err
	}
	return s.sourceFixture.Complete(ctx, c)
}

func TestLoopAppliesDirectionAfterCompletionBeforeNextReadyScan(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	p := fake(b)
	p.manualCompletion, p.turnStarted = true, make(chan struct{})
	var calls atomic.Int32
	f := factoryFunc(func(context.Context, Launch) (Process, error) { calls.Add(1); return p, nil })
	s := directionSource{sourceFixture: newSource(workRequest(b)), store: k, waveID: b.Target.WaveID, applied: make(chan struct{}, 1)}
	l := NewLoop(Runtime{Store: k, Factory: f}, s)
	l.ReconcileEvery = time.Hour
	loopStart(t, l)
	await(t, p.turnStarted)
	if _, err := k.RequestWaveDirection(ctx, storage.WaveDirectionRequest{Key: "direction", Target: b.Target, Instruction: "focus the report"}); err != nil {
		t.Fatal(err)
	}
	l.Wake()
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		t.Fatal("direction implicitly interrupted the old turn")
	}
	completeProcess(p)
	await(t, s.completed)
	await(t, s.applied) // No subsequent hint or hourly fallback is available.
	w, err := k.Wave(ctx, b.Target.WaveID)
	if err != nil || w.ScopeRevision != 2 || w.PendingDirectionID != "" || calls.Load() != 1 {
		t.Fatal(w, calls.Load(), err)
	}
}
