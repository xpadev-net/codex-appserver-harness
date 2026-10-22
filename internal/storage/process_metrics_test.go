package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProcessMetricsKeepUnknownAndHistoricalGenerationFacts(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 1)
	b, run, _ := reserveBudgetMain(t, k, "measured", waves[0].Target())
	initial, err := k.ProcessMetrics(ctx, b.PreparationID)
	if err != nil || initial.PID != nil || initial.PeakRSSBytes != nil || initial.StartupNanos != nil || initial.StopNanos != nil || initial.ObservedAt != nil {
		t.Fatal(initial, err)
	}
	start := initial.ReservationCreatedAt.Add(time.Second)
	ready := start.Add(2 * time.Second)
	requested := ready.Add(time.Second)
	stopped := requested.Add(3 * time.Second)
	k.Now = func() time.Time { return stopped.Add(time.Second) }
	pid, rss := int64(42), int64(8192)
	// A delayed spawn observation can follow the stop observation.
	if err := k.ObserveProcess(ctx, ProcessObservation{ReservationID: b.PreparationID, SessionID: b.ID, PID: &pid, StoppedAt: &stopped, PeakRSSBytes: &rss}); err != nil {
		t.Fatal(err)
	}
	if err := k.ObserveProcess(ctx, ProcessObservation{ReservationID: b.PreparationID, SessionID: b.ID, PID: &pid, SpawnedAt: &start, ReadyAt: &ready, StopRequestedAt: &requested}); err != nil {
		t.Fatal(err)
	}
	low := int64(1024)
	if err := k.ObserveProcess(ctx, ProcessObservation{ReservationID: b.PreparationID, SessionID: b.ID, PeakRSSBytes: &low}); err != nil {
		t.Fatal(err)
	}
	got, err := k.ProcessMetrics(ctx, b.PreparationID)
	if err != nil || got.StartupNanos == nil || *got.StartupNanos != int64(2*time.Second) || got.StopNanos == nil || *got.StopNanos != int64(3*time.Second) || got.PeakRSSBytes == nil || *got.PeakRSSBytes != rss {
		t.Fatal(got, err)
	}
	capacity, err := k.ExecutionCapacity(ctx)
	if err != nil || capacity.HeldProcessSlots != 1 {
		t.Fatal("metric released capacity", capacity, err)
	}
	finishBudgetMain(t, k, b, run)
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	k.Now = func() time.Time { return stopped.Add(2 * time.Second) }
	next := prepareWakeSession(t, k, "main")
	high := int64(16384)
	if err := k.ObserveProcess(ctx, ProcessObservation{ReservationID: b.PreparationID, SessionID: b.ID, PeakRSSBytes: &high}); err != nil {
		t.Fatal("late historical RSS rejected", err)
	}
	got, err = k.ProcessMetrics(ctx, b.PreparationID)
	if err != nil || got.PeakRSSBytes == nil || *got.PeakRSSBytes != high {
		t.Fatal(got, err)
	}
	newer, err := k.ProcessMetrics(ctx, next.PreparationID)
	if err != nil || newer.PID != nil || newer.PeakRSSBytes != nil {
		t.Fatal("late facts crossed generation", newer, err)
	}
	capacity, err = k.ExecutionCapacity(ctx)
	if err != nil || capacity.HeldProcessSlots != 1 {
		t.Fatal(capacity, err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err == nil {
		t.Fatal("historical proof released new process")
	}
}
func TestProcessMetricsRejectWrongSourcesAndConflictingFacts(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	r := operationRequest(t, w, "runner", "metrics-runner")
	reservation, err := k.ReserveOperationCapacity(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	k.Now = func() time.Time { return reservation.CreatedAt.Add(10 * time.Second) }
	start := reservation.CreatedAt.Add(time.Second)
	pid := int64(100)
	if err := k.ObserveProcess(ctx, ProcessObservation{ReservationID: r.ID, SessionID: r.SessionID, PID: &pid, SpawnedAt: &start}); err != nil {
		t.Fatal(err)
	}
	wrongPID, negative := int64(101), int64(-1)
	conflict := start.Add(time.Second)
	future := k.clock().Add(time.Second)
	past := reservation.CreatedAt.Add(-time.Second)
	for _, observation := range []ProcessObservation{
		{ReservationID: r.ID, SessionID: "foreign", PID: &pid},
		{ReservationID: r.ID, SessionID: r.SessionID, PID: &wrongPID},
		{ReservationID: r.ID, SessionID: r.SessionID, SpawnedAt: &conflict},
		{ReservationID: r.ID, SessionID: r.SessionID, PeakRSSBytes: &negative},
		{ReservationID: r.ID, SessionID: r.SessionID, ReadyAt: &future},
		{ReservationID: r.ID, SessionID: r.SessionID, StoppedAt: &past},
	} {
		if err := k.ObserveProcess(ctx, observation); err == nil {
			t.Fatal("invalid observation accepted", observation)
		}
	}
	got, err := k.ProcessMetrics(ctx, r.ID)
	if err != nil || got.PID == nil || *got.PID != pid || got.SpawnedAt == nil || !got.SpawnedAt.Equal(start) || got.ReadyAt != nil || got.StoppedAt != nil || got.PeakRSSBytes != nil {
		t.Fatal("failed update changed facts", got, err)
	}
	external := operationRequest(t, w, "pr", "external")
	if _, err := k.ReserveOperationCapacity(ctx, external); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ProcessMetrics(ctx, external.ID); err == nil {
		t.Fatal("PR treated as owned process")
	}
}
func TestProcessMetricsPagesAdvanceAcrossExternalAndUnknownHistory(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	for _, spec := range []struct{ kind, id string }{{"pr", "a-external"}, {"runner", "z-process"}} {
		if _, err := k.ReserveOperationCapacity(ctx, operationRequest(t, w, spec.kind, spec.id)); err != nil {
			t.Fatal(err)
		}
	}
	first, err := k.ProcessMetricsPage(ctx, "", 1)
	if err != nil || len(first.Processes) != 0 || first.NextAfter != "a-external" {
		t.Fatal(first, err)
	}
	second, err := k.ProcessMetricsPage(ctx, first.NextAfter, 1)
	if err != nil || len(second.Processes) != 1 || second.Processes[0].ReservationID != "z-process" || second.Processes[0].PeakRSSBytes != nil || second.NextAfter != "z-process" {
		t.Fatal(second, err)
	}
	end, err := k.ProcessMetricsPage(ctx, second.NextAfter, 1)
	if err != nil || len(end.Processes) != 0 || end.NextAfter != "" {
		t.Fatal(end, err)
	}
}
