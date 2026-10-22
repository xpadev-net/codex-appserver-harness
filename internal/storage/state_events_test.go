package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestStateEventsResumeStableCursorWithoutNotificationMutation(t *testing.T) {
	ctx := context.Background()
	k, r := waveStoreFixture(t)
	w, err := k.CreateWave(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.AdoptPlan(ctx, w.Target(), "initial", storagePlanFixture()); err != nil {
		t.Fatal(err)
	}
	var before int
	if err = k.DB.QueryRow("SELECT count(*) FROM records").Scan(&before); err != nil {
		t.Fatal(err)
	}
	first, err := k.StateEvents(ctx, 0, 1)
	if err != nil || len(first.Events) != 1 || !first.More || first.Events[0].Kind != "wave_created" {
		t.Fatal(first, err)
	}
	second, err := k.StateEvents(ctx, first.Cursor, 1)
	if err != nil || len(second.Events) != 1 || second.More || second.Events[0].Kind != "plan_adopted" {
		t.Fatal(second, err)
	}
	again, err := k.StateEvents(ctx, first.Cursor, 1)
	if err != nil || !reflect.DeepEqual(again, second) {
		t.Fatal(again, err)
	}
	var after int
	if err = k.DB.QueryRow("SELECT count(*) FROM records").Scan(&after); err != nil || before != after {
		t.Fatal("read stream changed persistent records", before, after, err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	reopened, err := OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replay, err := reopened.StateEvents(ctx, first.Cursor, 1)
	if err != nil || !reflect.DeepEqual(replay, second) {
		t.Fatal(replay, err)
	}
}
func TestStateEventsLargePayloadRemainsInDBWithBoundedStreamReference(t *testing.T) {
	ctx := context.Background()
	k, r := waveStoreFixture(t)
	w, err := k.CreateWave(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]string{"large": strings.Repeat("x", 4*contract.MaxInline)}
	if err = k.waveEventTx(ctx, tx, w, "large_state", "", "large", payload); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	page, err := k.StateEvents(ctx, 0, contract.MaxStatePage)
	if err != nil || len(page.Events) != 2 {
		t.Fatal(page, err)
	}
	e := page.Events[1]
	raw, _ := json.Marshal(payload)
	if !e.PayloadOmitted || len(e.Payload) != 0 || e.PayloadBytes != int64(len(raw)) {
		t.Fatal(e)
	}
	var saved []byte
	if err = k.DB.QueryRow("SELECT payload FROM kernel_events WHERE id='large'").Scan(&saved); err != nil || string(saved) != string(raw) {
		t.Fatal("stream modified large source payload", err)
	}
	for _, args := range [][2]int64{{-1, 1}, {0, 0}, {0, contract.MaxStatePage + 1}, {page.Cursor + 1, 1}} {
		if _, err = k.StateEvents(ctx, args[0], int(args[1])); err == nil {
			t.Fatal("invalid cursor/limit accepted", args)
		}
	}
}
