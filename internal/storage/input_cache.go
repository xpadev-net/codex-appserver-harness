package storage

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Only immutable, fully verified bytes enter this optional cache. Every read
// still authorizes the active run/snapshot and mutable Wave scope in SQLite.
// Eviction/restart reloads and verifies the same durable content.
const inputCacheBytes = 64 << 20
const inputCacheEntries = 8

type verifiedInputCache struct {
	mu      sync.Mutex
	entries map[InputReference][]byte
	order   []InputReference
	bytes   int
}

func (c *verifiedInputCache) get(ref InputReference) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.entries[ref]
	return data, ok
}
func (c *verifiedInputCache) put(ref InputReference, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(data) > inputCacheBytes {
		return
	}
	if c.entries == nil {
		c.entries = map[InputReference][]byte{}
	}
	if _, ok := c.entries[ref]; ok {
		return
	}
	for len(c.entries) >= inputCacheEntries || c.bytes+len(data) > inputCacheBytes {
		old := c.order[0]
		c.order = c.order[1:]
		c.bytes -= len(c.entries[old])
		delete(c.entries, old)
	}
	c.entries[ref] = data
	c.order = append(c.order, ref)
	c.bytes += len(data)
}
func (c *verifiedInputCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = nil
	c.order = nil
	c.bytes = 0
}

// This small DB projection is written in the SAME transaction as every Wave
// mutation. Large original requests/criteria must not be decoded on each tool
// slice just to check mutable authority. Old rows without the projection use
// the original full record; input construction backfills it transactionally.
type waveToolScope struct {
	ID                    string           `json:"id"`
	OrchestratorSessionID string           `json:"orchestrator_session_id"`
	BoundTarget           contract.Target  `json:"target"`
	State                 domain.WaveState `json:"state"`
	Paused                bool             `json:"paused"`
	PendingDirectionID    string           `json:"pending_direction_id"`
	Deadline              time.Time        `json:"deadline"`
}

func (s waveToolScope) Target() contract.Target { return s.BoundTarget }
func (s waveToolScope) DispatchPaused() bool    { return s.Paused || s.PendingDirectionID != "" }
func toolScope(w domain.Wave) waveToolScope {
	return waveToolScope{ID: w.ID, OrchestratorSessionID: w.OrchestratorSessionID, BoundTarget: w.Target(), State: w.State, Paused: w.Paused, PendingDirectionID: w.PendingDirectionID, Deadline: w.Deadline}
}
func saveWaveToolScopeTx(ctx context.Context, tx *sql.Tx, w domain.Wave) error {
	return kernelRecordTx(ctx, tx, "wave_tool_scope", w.ID, w.ID, toolScope(w))
}
func loadWaveToolScope(ctx context.Context, tx *sql.Tx, id string) (waveToolScope, error) {
	var s waveToolScope
	err := completionRecord(ctx, tx, "wave_tool_scope", id, &s)
	if errors.Is(err, sql.ErrNoRows) {
		w, e := loadWave(ctx, tx, id)
		if e != nil {
			return s, e
		}
		s = toolScope(w)
		err = nil
	}
	if err == nil && (s.ID != id || s.BoundTarget.WaveID != id || !s.BoundTarget.Valid() || s.OrchestratorSessionID == "" || s.Deadline.IsZero()) {
		err = errors.New("invalid Wave tool authority projection")
	}
	return s, err
}

func (k *KernelStore) Close() error { k.inputCache.clear(); return k.Store.Close() }
