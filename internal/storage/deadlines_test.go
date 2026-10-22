package storage

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestDeadlineHoldKeepsUnknownRunOwnershipUntilStopped(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 2)
	b, run, _ := reserveBudgetMain(t, k, "unknown-at-expiry", waves[0].Target(), waves[1].Target())
	run.State = "unknown"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	k.Now = func() time.Time { return waves[0].Deadline }
	d, err := k.ExpireWave(ctx, waves[0].ID)
	if err != nil || len(d.ActionIDs) != 1 {
		t.Fatal(d, err)
	}
	w, err := k.Wave(ctx, waves[0].ID)
	if err != nil || w.State != domain.WaveBlocked || w.Reason != "deadline_exceeded" {
		t.Fatal(w, err)
	}
	other, err := k.Wave(ctx, waves[1].ID)
	if err != nil || other.State == domain.WaveBlocked {
		t.Fatal("unrelated Wave held", other, err)
	}
	capacity, err := k.ExecutionCapacity(ctx)
	if err != nil || capacity.HeldProcessSlots != 1 {
		t.Fatal(capacity, err)
	}
	if err := k.CompleteDeadlineAction(ctx, d.ActionIDs[0]); !errors.Is(err, ErrDeadlineHeld) {
		t.Fatal("unknown process acknowledged stopped", err)
	}
	again, err := k.ExpireWave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(d, again) {
		t.Fatal(again, err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	k.Now = func() time.Time { return w.Deadline }
	pending, err := k.PendingDeadlineActions(ctx, "", 10)
	if err != nil || len(pending.Actions) != 1 || pending.Actions[0].RunID != run.ID || pending.Actions[0].PreparationID != b.PreparationID {
		t.Fatal(pending, err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := k.CompleteDeadlineAction(ctx, d.ActionIDs[0]); err != nil {
		t.Fatal(err)
	}
	if err := k.CompleteDeadlineAction(ctx, d.ActionIDs[0]); err != nil {
		t.Fatal(err)
	}
	if err := k.ArchiveSessionRun(ctx, b.ID, run.ID, "deadline elapsed"); err == nil {
		t.Fatal("deadline discarded unknown outcome")
	}
	capacity, err = k.ExecutionCapacity(ctx)
	if err != nil || capacity.HeldProcessSlots != 0 {
		t.Fatal(capacity, err)
	}
	usage, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil || usage.MainTurns != 1 || usage.UnobservedRuns != 1 {
		t.Fatal("expiry reset usage", usage, err)
	}
}
func TestDeadlineTracksSentOperationsAndRequiresVerifiedRelease(t *testing.T) {
	ctx := context.Background()
	k, w, _ := operationFixture(t)
	for _, kind := range []string{"runner", "pr", "cloud_review"} {
		r := operationRequest(t, w, kind, kind)
		if _, err := k.ReserveOperationCapacity(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	k.Now = func() time.Time { return w.Deadline }
	d, err := k.ExpireWave(ctx, w.ID)
	if err != nil || len(d.ActionIDs) != 3 {
		t.Fatal(d, err)
	}
	pending, err := k.PendingDeadlineActions(ctx, "", 10)
	if err != nil || len(pending.Actions) != 3 {
		t.Fatal(pending, err)
	}
	for _, a := range pending.Actions {
		expected := "track_external"
		if a.OperationID == "runner" {
			expected = "stop_process"
		}
		if a.Kind != expected {
			t.Fatal(a)
		}
		if err := k.CompleteDeadlineAction(ctx, a.ID); !errors.Is(err, ErrDeadlineHeld) {
			t.Fatal("unverified operation released", err)
		}
		r, err := k.OperationReservation(ctx, a.OperationID)
		if err != nil || r.State != "held" {
			t.Fatal(r, err)
		}
	}
	change := DeadlineChangeRequest{Key: "operator-extension", Target: w.Target(), ExpectedDeadline: w.Deadline, Deadline: w.Deadline.Add(time.Hour), Reason: "operator allowed additional time"}
	if err := k.ChangeWaveDeadline(ctx, change); err != nil {
		t.Fatal(err)
	}
	if err := k.ChangeWaveDeadline(ctx, change); err != nil {
		t.Fatal(err)
	}
	changed := change
	changed.Reason = "another reason"
	if err := k.ChangeWaveDeadline(ctx, changed); err == nil {
		t.Fatal("audit key changed content")
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || current.State != w.State || current.Reason != w.Reason || !current.Deadline.Equal(change.Deadline) {
		t.Fatal(current, err)
	}
	usage, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || usage.ValidationJobs != 1 {
		t.Fatal(usage, err)
	}
	for _, a := range pending.Actions {
		disposition := "completed"
		if a.OperationID == "runner" {
			disposition = "process_stopped"
		} else if a.OperationID == "pr" {
			disposition = "closed"
		}
		releaseOperation(t, k, a.OperationID, disposition)
		if err := k.CompleteDeadlineAction(ctx, a.ID); err != nil {
			t.Fatal(err)
		}
	}
}
func TestDeadlineFencesPreparedMainUntilOldGenerationStops(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 2)
	// Keep B current even when the clock passes A's deadline.
	if err := k.ChangeWaveDeadline(ctx, DeadlineChangeRequest{Key: "B-window", Target: waves[1].Target(), ExpectedDeadline: waves[1].Deadline, Deadline: waves[0].Deadline.Add(2 * time.Hour), Reason: "independent work window"}); err != nil {
		t.Fatal(err)
	}
	b := prepareWakeSession(t, k, "main")
	k.Now = func() time.Time { return waves[0].Deadline }
	d, err := k.ExpireWave(ctx, waves[0].ID)
	if err != nil || len(d.ActionIDs) != 1 {
		t.Fatal(d, err)
	}
	input := mainBudgetSnapshot(t, "B-input", "main", waves[1].Target())
	run := recoveryRun(k, b, "B-run")
	if err := k.ReserveSessionRun(ctx, b, input, run); !errors.Is(err, ErrDeadlineHeld) {
		t.Fatal("started on stopping preparation", err)
	}
	if err := k.ChangeWaveDeadline(ctx, DeadlineChangeRequest{Key: "A-extension", Target: waves[0].Target(), ExpectedDeadline: waves[0].Deadline, Deadline: waves[0].Deadline.Add(time.Hour), Reason: "audited extension"}); err != nil {
		t.Fatal(err)
	}
	if err := k.ReserveSessionRun(ctx, b, input, run); !errors.Is(err, ErrDeadlineHeld) {
		t.Fatal("extension erased stopping fence", err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, b.PreparationID); err != nil {
		t.Fatal(err)
	}
	next := prepareWakeSession(t, k, "main")
	if err := k.CompleteDeadlineAction(ctx, d.ActionIDs[0]); err != nil {
		t.Fatal("late ack confused new generation", err)
	}
	if err := k.ReserveSessionRun(ctx, next, input, recoveryRun(k, next, "B-run")); err != nil {
		t.Fatal("new generation blocked", err)
	}
}
func TestDeadlineSweepIsPagedAndRollbackKeepsHoldAtomic(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 4)
	tx, _ := k.kernelTx(ctx)
	terminal := waves[0]
	terminal.State = domain.WaveCompleted
	if err := saveWave(ctx, tx, terminal); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	k.Now = func() time.Time { return waves[3].Deadline }
	seen := map[string]bool{}
	after := ""
	for range 6 {
		page, err := k.SweepWaveDeadlines(ctx, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range page.Dispositions {
			if seen[d.Target.WaveID] {
				t.Fatal("duplicate page", d)
			}
			seen[d.Target.WaveID] = true
		}
		after = page.NextAfter
		if after == "" {
			break
		}
	}
	if len(seen) != 3 || seen[terminal.ID] {
		t.Fatal(seen)
	}
	var events int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='wave_deadline_exceeded'").Scan(&events); err != nil || events != 3 {
		t.Fatal(events, err)
	}
	// A separate store injects a failure after all stop intents were written.
	fresh, ws := budgetWaves(t, domain.DefaultKernelLimits(), 1)
	b, _, _ := reserveBudgetMain(t, fresh, "rollback-run", ws[0].Target())
	fresh.Now = func() time.Time { return ws[0].Deadline }
	if _, err := fresh.DB.Exec("CREATE TRIGGER fail_deadline_event BEFORE INSERT ON kernel_events WHEN NEW.kind='wave_deadline_exceeded' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.ExpireWave(ctx, ws[0].ID); err == nil {
		t.Fatal("injected event committed")
	}
	current, err := fresh.Wave(ctx, ws[0].ID)
	if err != nil || current.State != ws[0].State {
		t.Fatal(current, err)
	}
	var records int
	if err := fresh.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('deadline_action','deadline_disposition','deadline_preparation_stop')").Scan(&records); err != nil || records != 0 {
		t.Fatal(records, err)
	}
	var live int
	if err := fresh.DB.QueryRow("SELECT count(*) FROM kernel_sessions WHERE id=? AND process_stopped=0", b.ID).Scan(&live); err != nil || live != 1 {
		t.Fatal(live, err)
	}
}

func TestDeadlineDoesNotStopMainRunScopedOnlyToAnotherWave(t *testing.T) {
	ctx := context.Background()
	k, waves := budgetWaves(t, domain.DefaultKernelLimits(), 2)
	if err := k.ChangeWaveDeadline(ctx, DeadlineChangeRequest{Key: "B-window", Target: waves[1].Target(), ExpectedDeadline: waves[1].Deadline, Deadline: waves[0].Deadline.Add(time.Hour), Reason: "separate remaining work"}); err != nil {
		t.Fatal(err)
	}
	b, run, _ := reserveBudgetMain(t, k, "B-only", waves[1].Target())
	k.Now = func() time.Time { return waves[0].Deadline }
	d, err := k.ExpireWave(ctx, waves[0].ID)
	if err != nil || len(d.ActionIDs) != 0 {
		t.Fatal("unrelated active main run stopped", d, err)
	}
	finishBudgetMain(t, k, b, run)
	next, _, _ := reserveBudgetMain(t, k, "B-next", waves[1].Target())
	d, err = k.ExpireWave(ctx, waves[0].ID)
	if err != nil || len(d.ActionIDs) != 0 {
		t.Fatal("replay stopped newer process", d, err)
	}
	var fences int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='deadline_preparation_stop' AND id=?", next.PreparationID).Scan(&fences); err != nil || fences != 0 {
		t.Fatal(fences, err)
	}
}
