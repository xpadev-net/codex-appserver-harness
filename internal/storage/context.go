package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// ReadContext exposes exact persisted request/criteria/policy/applied direction
// through the same Wave ownership boundary as artifact reads, without publishing
// new artifacts or weakening artifact grants/quotas. Large data stays outside
// bounded model snapshots and is read by explicit scoped, digest-bearing slices.
func (k *KernelStore) ReadContext(ctx context.Context, sessionID string, r contract.ContextReadRequest) (contract.ContextSlice, error) {
	if !r.Valid() {
		return contract.ContextSlice{}, errors.New("invalid bounded context request")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return contract.ContextSlice{}, err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return contract.ContextSlice{}, err
	}
	return k.contextSliceTx(ctx, tx, b, r)
}
func (k *KernelStore) contextSliceTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, r contract.ContextReadRequest) (contract.ContextSlice, error) {
	var result contract.ContextSlice
	if !r.Valid() {
		return result, errors.New("invalid bounded context request")
	}
	if r.Section == "manifest" {
		scope, err := loadWaveToolScope(ctx, tx, r.Target.WaveID)
		if err != nil {
			return result, err
		}
		if scope.Target() != r.Target || b.Closed || b.Kind == "orchestrator" && scope.OrchestratorSessionID != b.ID || b.Kind == "task" && b.Target != r.Target || b.Kind != "orchestrator" && b.Kind != "task" {
			return result, errors.New("context reader is outside the current Wave contract")
		}
		data, digest, err := k.inputManifestTx(ctx, tx, b, r)
		if err != nil {
			return result, err
		}
		return contextBytes(data, r, "application/json", digest)
	}
	wave, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return result, err
	}
	if b.Closed || wave.Target() != r.Target || b.Kind == "orchestrator" && wave.OrchestratorSessionID != b.ID || b.Kind == "task" && b.Target != r.Target || b.Kind != "orchestrator" && b.Kind != "task" {
		return result, errors.New("context reader is outside the current Wave contract")
	}
	var data []byte
	media := "text/plain; charset=utf-8"
	switch r.Section {
	case "request":
		data = []byte(wave.OriginalRequest)
	case "criteria":
		data, err = json.Marshal(wave.Criteria)
		media = "application/json"
	case "policy":
		data, err = json.Marshal(wave.Policy)
		media = "application/json"
	case "direction":
		// Queued directions are not turn/steer inputs. Expose only the instruction
		// whose scope revision is already applied to this Wave.
		if wave.DirectionID != "" {
			var direction WaveDirection
			if err = completionRecord(ctx, tx, "wave_direction", wave.DirectionID, &direction); err != nil {
				return result, err
			}
			if direction.State != "applied" || direction.AppliedTarget.WaveID != wave.ID || direction.AppliedTarget.ScopeRevision != wave.ScopeRevision || direction.AppliedTarget.PolicyRevision != wave.Policy.Revision {
				return result, errors.New("applied direction binding conflict")
			}
			data = []byte(direction.Request.Instruction)
		}
	}
	if err != nil {
		return result, err
	}
	digest := sha256.Sum256(data)
	return contextBytes(data, r, media, hex.EncodeToString(digest[:]))
}
func contextBytes(data []byte, r contract.ContextReadRequest, media, digest string) (contract.ContextSlice, error) {
	var result contract.ContextSlice
	if r.Offset > int64(len(data)) || r.Offset < int64(len(data)) && !utf8.RuneStart(data[r.Offset]) {
		return result, errors.New("context offset must address a UTF-8 boundary")
	}
	end := min(int64(len(data)), r.Offset+r.Limit)
	for end > r.Offset && end < int64(len(data)) && !utf8.RuneStart(data[end]) {
		end--
	}
	if end == r.Offset && r.Offset < int64(len(data)) {
		return result, errors.New("context limit cannot fit the next UTF-8 character")
	}
	result = contract.ContextSlice{SchemaVersion: 1, Target: r.Target, Section: r.Section, Digest: digest, Bytes: int64(len(data)), Offset: r.Offset, ContentType: media, Content: string(data[r.Offset:end])}
	if end < int64(len(data)) {
		next := end
		result.NextOffset = &next
	}
	return result, nil
}
