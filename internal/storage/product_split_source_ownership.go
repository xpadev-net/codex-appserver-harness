package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrProductSplitChildSourceHeld = errors.New("split child Issue belongs to its existing Wave, not a new root")

// This is a prohibition, not a child execution grant. Independent resource and
// observed journal witnesses retain numeric ownership after body/marker edits.
// Non-Issue direct identities continue to use their ordinary source contract.
func rejectProductSplitChildRoot(ctx context.Context, q queryer, identity string) error {
	if !strings.HasPrefix(identity, "r") {
		return nil
	}
	repoPart, issuePart, ok := strings.Cut(strings.TrimPrefix(identity, "r"), "-i")
	if !ok {
		return nil
	}
	repositoryID, repoErr := strconv.ParseInt(repoPart, 10, 64)
	number, numberErr := strconv.Atoi(issuePart)
	if repoErr != nil || numberErr != nil || repositoryID < 1 || number < 1 || identity != fmt.Sprintf("r%d-i%d", repositoryID, number) {
		return nil
	}
	known, err := knownProductSplitChildIssue(ctx, q, repositoryID, number)
	if err != nil {
		return err
	}
	if known {
		return ErrProductSplitChildSourceHeld
	}
	return nil
}

// IsKnownProductSplitChildIssue classifies managed source data; it grants no
// ownership, completion or child execution authority.
func (k *KernelStore) IsKnownProductSplitChildIssue(ctx context.Context, repositoryID int64, number int) (bool, error) {
	if repositoryID < 1 || number < 1 {
		return false, errors.New("positive repository and Issue identity required")
	}
	return knownProductSplitChildIssue(ctx, k.DB, repositoryID, number)
}
func knownProductSplitChildIssue(ctx context.Context, q queryer, repositoryID int64, number int) (bool, error) {
	key := fmt.Sprintf("github:%d:issue:%d", repositoryID, number)
	var known bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_created_resource' AND id=?)
 OR EXISTS(SELECT 1 FROM records WHERE kind IN ('product_split_create','product_split_create_event') AND json_extract(payload,'$.value.issue.repository_id')=? AND json_extract(payload,'$.value.issue.number')=?)`, key, repositoryID, number).Scan(&known)
	if err != nil || known {
		return known, err
	}
	return splitChildScopeIssueKnown(ctx, q, key, repositoryID, number)
}
