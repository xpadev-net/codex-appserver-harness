package storage

import (
	"context"
	"database/sql"
	"sort"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Dependencies come from the authenticated split intent, independently of a
// model's Task DAG. A completed Task or closed Issue is no merged-change proof.
// This read grants no execution, source freshness or quality permission.
func productSplitDependencyIDsTx(ctx context.Context, tx *sql.Tx, w domain.Wave, workIDs []string) ([]string, error) {
	required := map[string]bool{}
	for _, workID := range workIDs {
		registration, err := loadProductWorkItemRegistration(ctx, tx, workID)
		if err != nil {
			return nil, err
		}
		binding, err := productSplitChildScopeTx(ctx, tx, w, registration)
		if err != nil {
			return nil, err
		}
		if binding == nil {
			continue
		}
		for _, id := range binding.Value.Child.Value.DependsOnWorkItemIDs {
			if id == workID || id == "" {
				return nil, ErrProductSplitHeld
			}
			required[id] = true
		}
	}
	ids := make([]string, 0, len(required))
	for id := range required {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}
