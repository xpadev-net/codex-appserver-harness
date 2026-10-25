package storage

import (
	"context"
	"database/sql"
	"encoding/json"
)

const (
	maxSplitProofReadEntries = 16 * maxProductWorkItems
	maxSplitProofReadBytes   = 64 << 20
)

type splitProofReadContextKey struct{}
type splitProofReadKey struct{ kind, digest string }
type splitProofReadMemo struct {
	tx     *sql.Tx
	active map[splitProofReadKey]bool
	saved  map[splitProofReadKey][]byte
	bytes  int
}

// A memo belongs only to one synchronous, read-only loader call tree. Its
// private child context never escapes to the calling port. A subsequent load
// after a write, even in the same transaction, starts a new proof walk. Only
// nested loaders share results, and every hit decodes an independent copy.
func readSplitProof[T any](ctx context.Context, tx *sql.Tx, kind string, input any, load func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	memo, _ := ctx.Value(splitProofReadContextKey{}).(*splitProofReadMemo)
	if memo == nil || memo.tx != tx {
		memo = &splitProofReadMemo{tx: tx, active: map[splitProofReadKey]bool{}, saved: map[splitProofReadKey][]byte{}}
		ctx = context.WithValue(ctx, splitProofReadContextKey{}, memo)
	}
	digest, err := contentDigest(input)
	if err != nil {
		return zero, err
	}
	key := splitProofReadKey{kind: kind, digest: digest}
	if memo.active[key] || len(memo.active) >= maxSplitProofReadEntries {
		return zero, ErrProductSplitHeld
	}
	if raw, ok := memo.saved[key]; ok {
		var copy T
		if err := json.Unmarshal(raw, &copy); err != nil {
			return zero, err
		}
		return copy, nil
	}
	memo.active[key] = true
	defer delete(memo.active, key)
	value, err := load(ctx)
	if err != nil {
		return value, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	// Capacity limits only disable additional caching. They never turn a valid
	// proof into permission to omit validation or reject an otherwise valid read.
	if len(memo.saved) < maxSplitProofReadEntries {
		raw, err := json.Marshal(value)
		if err != nil {
			return zero, err
		}
		if len(raw) <= maxSplitProofReadBytes-memo.bytes {
			memo.saved[key] = raw
			memo.bytes += len(raw)
		}
	}
	return value, nil
}
