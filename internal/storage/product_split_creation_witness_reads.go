package storage

import (
	"context"
	"database/sql"
	"errors"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type splitCreationWitnessIDs struct {
	IDs      []string `json:"ids"`
	Complete bool     `json:"complete"`
}

// This is a negative metadata projection inside one private, read-only proof
// walk. A partial projection may establish a prohibition, never absence. Large
// databases fall back to the exact per-ID query without weakening any guard.
func splitChildScopeCreationKnown(ctx context.Context, q queryer, id string) (bool, error) {
	tx, txOK := q.(*sql.Tx)
	memo, memoOK := ctx.Value(splitProofReadContextKey{}).(*splitProofReadMemo)
	if txOK && memoOK && memo.tx == tx {
		witnesses, err := readSplitProof(ctx, tx, "splitCreationWitnessIDs", "all", func(read context.Context) (splitCreationWitnessIDs, error) {
			return loadSplitCreationWitnessIDs(read, tx)
		})
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, ErrProductSplitHeld) {
				return false, err
			}
			// A malformed unrelated row may make the all-ID projection fail
			// even when the exact query can settle this identity. Preserve
			// that query's result instead of widening a metadata prohibition.
			return splitChildScopeCreationKnownUncached(ctx, q, id)
		}
		for _, known := range witnesses.IDs {
			if known == id {
				return true, nil
			}
		}
		if witnesses.Complete {
			return false, nil
		}
	}
	return splitChildScopeCreationKnownUncached(ctx, q, id)
}

func loadSplitCreationWitnessIDs(ctx context.Context, tx *sql.Tx) (splitCreationWitnessIDs, error) {
	value := splitCreationWitnessIDs{IDs: []string{}, Complete: true}
	// This projection needs only the records that can physically contain a
	// creation array. Building the general split witness relation also scans and
	// expands every unrelated Wave's ancestry groups on each split preparation.
	query := `WITH split_creation_children AS MATERIALIZED (
	 SELECT DISTINCT json_quote(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.children')) AS children
	 FROM main.records WHERE kind IN ` + splitCreationGroupKinds + `
	 ), split_creation_arrays AS MATERIALIZED (
	 SELECT json_quote(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.creations')) AS creations
	 FROM main.records WHERE kind IN ` + splitCreationLeafKinds + `
	 UNION SELECT json_quote(json_extract(CASE WHEN json_valid(b.value) THEN b.value ELSE '{}' END,'$.value.creations'))
	 FROM split_creation_children r,json_each(r.children) b
	 UNION SELECT json_quote(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.creations'))
	 FROM main.records WHERE kind='product_split_child_scope_source'
	 UNION SELECT coalesce(json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.value.split_scope.creations'),json_extract(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.split_scope.creations'),'null')
	 FROM main.records WHERE kind IN ` + splitCreationRegistrationKinds + `
	 ), split_creation_ids AS NOT MATERIALIZED (
 SELECT json_extract(CASE WHEN json_valid(c.value) THEN c.value ELSE '{}' END,'$.id') AS creation_id
 FROM split_creation_arrays r,json_each(r.creations) c
 ) SELECT DISTINCT CASE WHEN length(cast(creation_id AS BLOB))<=? THEN creation_id ELSE NULL END
	 FROM split_creation_ids WHERE typeof(creation_id)='text' LIMIT ?`
	rows, err := tx.QueryContext(ctx, query, contract.MaxInline, maxSplitProofReadEntries+1)
	if err != nil {
		return value, err
	}
	defer rows.Close()
	for rows.Next() {
		var id sql.NullString
		if err := rows.Scan(&id); err != nil {
			return value, err
		}
		if !id.Valid || !utf8.ValidString(id.String) {
			value.Complete = false
			continue
		}
		if len(value.IDs) == maxSplitProofReadEntries {
			value.Complete = false
			break
		}
		value.IDs = append(value.IDs, id.String)
	}
	return value, rows.Err()
}
