package storage

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestProductSplitLimitsAreAdmissionSnapshotAndOldRecordsStayDisabled(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := k.WaveOperationLimits(ctx, w.ID)
	if err != nil || limits.SplitDepth != 2 || limits.SplitChildren != 8 || limits.SplitCreateRetries != 1 {
		t.Fatal("new admission lost split defaults", limits, err)
	}
	direction := directionRequest(w)
	direction.NextPolicyRevision = w.Policy.Revision + 1
	queued, err := k.RequestWaveDirection(ctx, direction)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, queued.ID); err != nil {
		t.Fatal(err)
	}
	current, err := k.WaveOperationLimits(ctx, w.ID)
	if err != nil || current != limits {
		t.Fatal("policy direction changed lifetime split limits", current, err)
	}
	// Model an actual old serialized limits record: absence means no permission.
	legacy := limits
	legacy.SplitDepth, legacy.SplitChildren, legacy.SplitCreateRetries = 0, 0, 0
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["split_depth"]; ok {
		t.Fatal("old zero encoding gained split field")
	}
	if _, ok := fields["split_children"]; ok {
		t.Fatal("old zero encoding gained child field")
	}
	if _, ok := fields["split_create_retries"]; ok {
		t.Fatal("old encoding gained retry permission")
	}
	if _, err := k.DB.Exec("UPDATE records SET payload=? WHERE kind='wave_operation_limits' AND id=?", raw, w.ID); err != nil {
		t.Fatal(err)
	}
	restored, err := k.WaveOperationLimits(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(restored, legacy) {
		t.Fatal("old limits acquired new permission", restored, err)
	}
	replay, err := k.CreateWave(ctx, request)
	if err != nil || replay.ID != w.ID {
		t.Fatal("old source retry failed", replay, err)
	}
	unchanged, err := k.WaveOperationLimits(ctx, w.ID)
	if err != nil || unchanged != legacy {
		t.Fatal("source retry overlaid split defaults", unchanged, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	reopened, err := k.WaveOperationLimits(ctx, w.ID)
	if err != nil || reopened != legacy {
		t.Fatal("restart overlaid split defaults", reopened, err)
	}
}

func TestProductSplitLimitsRejectInvalidAdmissionAtomically(t *testing.T) {
	for _, pair := range [][2]int{{-1, 8}, {2, -1}, {0, 8}, {2, 0}, {2, 1}, {maxProductWorkItems + 1, 8}, {2, maxProductWorkItems + 1}} {
		k, r := waveStoreFixture(t)
		limits := DefaultWaveOperationLimits()
		limits.SplitDepth, limits.SplitChildren = pair[0], pair[1]
		r.OperationLimits = &limits
		if _, err := k.CreateWave(context.Background(), r); err == nil {
			t.Fatal("invalid split limits admitted", pair)
		}
		var n int
		if err := k.DB.QueryRow("SELECT count(*) FROM kernel_waves").Scan(&n); err != nil || n != 0 {
			t.Fatal("invalid limits left partial Wave", pair, n, err)
		}
		k.Close()
	}
}
