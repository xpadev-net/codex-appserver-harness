package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ProductSourceObservationTargets pages runnable or held GitHub product sources
// without loading every Wave or entering the scheduler's ready evaluation.
func (k *KernelStore) ProductSourceObservationTargets(ctx context.Context, repositoryID int64, after string, limit int) ([]domain.Wave, error) {
	if repositoryID < 1 || limit < 1 || limit > 32 || len(after) > 4096 || !utf8.ValidString(after) || strings.ContainsAny(after, "\x00\r\n") {
		return nil, errors.New("bounded product source page required")
	}
	rows, err := k.DB.QueryContext(ctx, `SELECT id,source_id,payload FROM kernel_waves WHERE source='github' AND source_id LIKE ? AND id>? AND json_extract(payload,'$.policy.stage')='P' AND json_extract(payload,'$.policy.product')=1 AND json_extract(payload,'$.state') NOT IN ('completed','canceled') ORDER BY id LIMIT ?`, fmt.Sprintf("r%d-i%%", repositoryID), after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Wave{}
	for rows.Next() {
		var id, sourceID string
		var raw []byte
		var w domain.Wave
		if err := rows.Scan(&id, &sourceID, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &w); err != nil {
			return nil, err
		}
		if err := w.Validate(); err != nil {
			return nil, err
		}
		if w.ID != id || w.SourceID != sourceID || w.Source != "github" {
			return nil, fmt.Errorf("%w: source Wave identity conflict", ErrTaskEvidence)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}
