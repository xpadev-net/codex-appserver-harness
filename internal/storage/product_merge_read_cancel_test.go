package storage

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"testing"
)

type mergeReadBoundaryProbe struct {
	queryer
	seen   map[string]bool
	at     string
	cancel context.CancelFunc
	hit    bool
}

func (p *mergeReadBoundaryProbe) QueryRowContext(ctx context.Context, sqlText string, args ...any) *sql.Row {
	if strings.Contains(sqlText, "FROM records") && len(args) > 0 {
		if kind, ok := args[0].(string); ok && strings.HasPrefix(kind, "product_") {
			if p.seen != nil {
				p.seen[kind] = true
			}
			if kind == p.at {
				p.hit = true
				p.cancel()
			}
		}
	}
	return p.queryer.QueryRowContext(ctx, sqlText, args...)
}
func TestProductMergeReadPreservesCancellationAtEveryEvidenceRead(t *testing.T) {
	k, request := mergeJournalFixture(t)
	job, err := k.PrepareProductMerge(context.Background(), request, "cancel-read-owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	probe := &mergeReadBoundaryProbe{queryer: k.DB, seen: map[string]bool{}}
	if _, found, err := lookupProductMerge(context.Background(), probe, job.ID); err != nil || !found {
		t.Fatal("valid baseline merge unavailable", found, err)
	}
	kinds := make([]string, 0, len(probe.seen))
	for kind := range probe.seen {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	if len(kinds) < 5 {
		t.Fatal("did not exercise saved merge dependencies", kinds)
	}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := &mergeReadBoundaryProbe{queryer: k.DB, at: kind, cancel: cancel}
			_, _, err := lookupProductMerge(ctx, reader, job.ID)
			if !reader.hit || !errors.Is(err, context.Canceled) {
				t.Fatal("cancelled read was mistaken for missing or corrupt evidence", reader.hit, err)
			}
		})
	}
	// Read failures do not rewrite the admitted job or create a new effect.
	current, err := k.ReadProductMerge(context.Background(), job.ID)
	if err != nil || current.State != "prepared" || current.ClaimedAt != job.ClaimedAt {
		t.Fatal("read changed physical ownership", current, err)
	}
}
