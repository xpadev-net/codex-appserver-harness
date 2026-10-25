package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestProductSourceReadQuietRefreshPreservesSemanticHistoryAndActualAge(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	now := k.clock()
	k.Now = func() time.Time { return now }
	first, err := k.ObserveProductSourceAt(ctx, w.Target(), "", facts, now)
	if err != nil {
		t.Fatal(err)
	}
	var events int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_events").Scan(&events); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	readAt := now
	now = now.Add(4 * time.Second) // The SQL save happens after the real GET.
	quiet, err := k.ObserveProductSourceAt(ctx, w.Target(), first.Snapshot.ID, facts, readAt)
	if err != nil || quiet.Changed || !reflect.DeepEqual(first.Snapshot, quiet.Snapshot) {
		t.Fatal("quiet read changed semantic history", quiet, err)
	}
	saved, err := k.ReadProductSourceRead(ctx, w.Target())
	if err != nil || !saved.Value.ReadAt.Equal(readAt) || now.Sub(saved.Value.ReadAt) != 4*time.Second {
		t.Fatal("save renewed GET time", saved, err)
	}
	var histories, afterEvents, runs int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_source_observation_history'").Scan(&histories); err != nil {
		t.Fatal(err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_events").Scan(&afterEvents); err != nil {
		t.Fatal(err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || histories != 1 || afterEvents != events || runs != 0 {
		t.Fatal("freshness created semantic events or work", histories, afterEvents, runs, err)
	}
	now = now.Add(time.Minute)
	if _, err := k.ObserveProductSource(ctx, w.Target(), "", facts); err != nil {
		t.Fatal(err)
	}
	again, err := k.ReadProductSourceRead(ctx, w.Target())
	if err != nil || !reflect.DeepEqual(saved, again) {
		t.Fatal("legacy replay renewed actual read", err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	again, err = k.ReadProductSourceRead(ctx, w.Target())
	if err != nil || !reflect.DeepEqual(saved, again) {
		t.Fatal("reopen renewed read time", err)
	}
}

func TestProductSourceReadRejectsABAStaleTimeAndPartialPublication(t *testing.T) {
	ctx := context.Background()
	k, w, facts := productSourceFixture(t)
	now := k.clock()
	k.Now = func() time.Time { return now }
	first, err := k.ObserveProductSourceAt(ctx, w.Target(), "", facts, now)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	hold := facts
	hold.Hold = true
	held, err := k.ObserveProductSourceAt(ctx, w.Target(), first.Snapshot.ID, hold, now)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	restored, err := k.ObserveProductSourceAt(ctx, w.Target(), held.Snapshot.ID, facts, now)
	if err != nil || restored.Snapshot.Digest != first.Snapshot.Digest {
		t.Fatal(err)
	}
	before, err := k.ReadProductSourceRead(ctx, w.Target())
	if err != nil {
		t.Fatal(err)
	}
	for _, readAt := range []time.Time{time.Time{}, now.Add(time.Second), now.Add(-time.Millisecond)} {
		if _, err := k.ObserveProductSourceAt(ctx, w.Target(), restored.Snapshot.ID, facts, readAt); err == nil {
			t.Fatal("invalid observation time accepted", readAt)
		}
	}
	if _, err := k.ObserveProductSourceAt(ctx, w.Target(), first.Snapshot.ID, facts, now); err == nil {
		t.Fatal("semantic ABA accepted old snapshot ID")
	}
	stale := w.Target()
	stale.PlanRevision++
	if _, err := k.ObserveProductSourceAt(ctx, stale, restored.Snapshot.ID, facts, now); err == nil {
		t.Fatal("wrong plan received freshness")
	}
	if _, err := k.DB.Exec("CREATE TRIGGER source_read_failure BEFORE UPDATE ON records WHEN NEW.kind='product_source_read_admission' BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := k.ObserveProductSourceAt(ctx, w.Target(), restored.Snapshot.ID, hold, now); err == nil {
		t.Fatal("partial receipt publication succeeded")
	}
	after, err := k.ReadProductSourceRead(ctx, w.Target())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("receipt changed after failed transaction", err)
	}
	snapshot, err := k.ReadProductSourceObservation(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(snapshot, restored.Snapshot) {
		t.Fatal("semantic source escaped failed receipt transaction", err)
	}
	wave, err := k.Wave(ctx, w.ID)
	if err != nil || wave.SourceHold != nil {
		t.Fatal("hold escaped failed receipt transaction", err)
	}
}

func TestProductSourceReadMissingCorruptOrSupersededEvidenceNeverInventsAge(t *testing.T) {
	for _, mode := range []string{"legacy_only", "missing_copy", "owner", "corrupt", "superseded"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, facts := productSourceFixture(t)
			first, err := k.ObserveProductSource(ctx, w.Target(), "", facts)
			if err != nil {
				t.Fatal(err)
			}
			if mode != "legacy_only" {
				if _, err := k.ObserveProductSourceAt(ctx, w.Target(), first.Snapshot.ID, facts, k.clock()); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "missing_copy":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_source_read_admission'")
			case "owner":
				_, err = k.DB.Exec("UPDATE records SET work_item_id='foreign' WHERE kind='product_source_read'")
			case "corrupt":
				_, err = k.DB.Exec("UPDATE records SET payload='{}' WHERE kind='product_source_read'")
			case "superseded":
				facts.Hold = true
				_, err = k.ObserveProductSource(ctx, w.Target(), first.Snapshot.Digest, facts)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductSourceRead(ctx, w.Target()); err == nil || mode == "legacy_only" && !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("missing evidence inferred current read time", err)
			}
		})
	}
}
