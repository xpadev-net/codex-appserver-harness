package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestManifestLargePaginationReusesVerifiedBytesAndChecksLiveAuthority(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.OriginalRequest = strings.Repeat("x", 8<<20)
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	ref := inputContext(t, snapshot).Waves[0].Manifest
	b, run := reserveInputRun(t, k, "main", snapshot)
	r := contract.ContextReadRequest{SchemaVersion: 1, Target: w.Target(), Section: "manifest", ReferenceID: ref.ID, Limit: 2048}
	if _, err = k.ReadContext(ctx, b.ID, r); err != nil {
		t.Fatal(err)
	}
	before, ok := k.inputCache.get(ref)
	if !ok {
		t.Fatal("verified manifest not cached")
	}
	// Read the complete supported 8 MiB request in actual bounded slices. Timing
	// is evidence only; correctness does not depend on a machine-specific limit.
	start := time.Now()
	m := readSavedInput(t, k, b.ID, ref)
	t.Logf("manifest_bytes=%d slice_limit=%d complete_read=%s", ref.Bytes, r.Limit, time.Since(start))
	if m.Wave.OriginalRequest != request.OriginalRequest {
		t.Fatal("large input lost")
	}
	after, ok := k.inputCache.get(ref)
	if !ok || &before[0] != &after[0] || fmt.Sprintf("%x", sha256.Sum256(after)) != ref.Digest {
		t.Fatal("pages reloaded or changed verified immutable bytes")
	}
	// Cache state never authorizes a call: a committed pause must still reject
	// the exact warm reference via the real gateway/run binding.
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.Paused = true
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	call := kernelCall(t, b, run, "context_read", r)
	if _, err = (contract.KernelGateway{Backend: k}).Handle(ctx, call); err == nil {
		t.Fatal("warm cache bypassed current pause")
	}
	tx, err = k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w.ScopeRevision++
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReadContext(ctx, b.ID, r); err == nil {
		t.Fatal("warm cache bypassed current scope")
	}
}
func TestInputCacheEvictionAndMissDoNotChangeSavedInput(t *testing.T) {
	ctx := context.Background()
	k, w, _ := mailboxFixture(t)
	snapshot, err := k.BuildSessionInput(ctx, "main", []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	ref := inputContext(t, snapshot).Waves[0].Manifest
	reserveInputRun(t, k, "main", snapshot)
	before := readSavedInput(t, k, "main", ref)
	k.inputCache.clear()
	after := readSavedInput(t, k, "main", ref)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("cache miss changed judgment input")
	}
	var cache verifiedInputCache
	data := make([]byte, maxInputManifest)
	for i := 0; i < 3; i++ {
		cache.put(InputReference{ID: fmt.Sprint(i)}, data)
	}
	if cache.bytes > inputCacheBytes || len(cache.entries) != 2 {
		t.Fatal("cache exceeded its byte bound", cache.bytes, len(cache.entries))
	}
	if _, ok := cache.get(InputReference{ID: "0"}); ok {
		t.Fatal("oldest entry not evicted")
	}
	cache.clear()
	for i := 0; i < inputCacheEntries+1; i++ {
		cache.put(InputReference{ID: fmt.Sprint(i)}, []byte("small"))
	}
	if len(cache.entries) != inputCacheEntries {
		t.Fatal("cache entry bound ignored")
	}
}
func TestWaveToolScopeTracksWritesAtomicallyAndFallsBackForOldRows(t *testing.T) {
	ctx := context.Background()
	k, w, _ := mailboxFixture(t)
	w, err := k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := loadWaveToolScope(ctx, tx, w.ID)
	if err != nil || scope != toolScope(w) {
		t.Fatal(scope, err)
	}
	w.Paused = true
	if err = saveWave(ctx, tx, w); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err = k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	scope, err = loadWaveToolScope(ctx, tx, w.ID)
	if err != nil || scope.Paused {
		t.Fatal("rolled-back authority leaked", scope, err)
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='wave_tool_scope' AND id=?", w.ID); err != nil {
		t.Fatal(err)
	}
	old, err := loadWaveToolScope(ctx, tx, w.ID)
	if err != nil || old != scope {
		t.Fatal("old-row fallback changed authority", old, err)
	}
	tx.Rollback()
}
