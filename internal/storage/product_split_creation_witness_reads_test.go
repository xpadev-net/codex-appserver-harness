package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func TestSplitCreationWitnessProjectionMatchesExactChecksAndExpiresBeforeWrites(t *testing.T) {
	ctx := context.Background()
	k, _, scopes := splitRegistrationsFixture(t)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ids := []string{"not-an-existing-creation"}
	for _, child := range scopes.Value.Children {
		for _, ref := range child.Value.Creations {
			ids = append(ids, ref.ID)
		}
	}
	_, err = readSplitProof(ctx, tx, "test-creation-projection", "all", func(read context.Context) (bool, error) {
		for _, id := range ids {
			want, err := splitChildScopeCreationKnownUncached(read, tx, id)
			if err != nil {
				return false, err
			}
			got, err := splitChildScopeCreationKnown(read, tx, id)
			if err != nil || got != want {
				t.Fatal("projection changed the witness predicate", got, want, err)
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A new independent witness in the same transaction must be visible after
	// the private read scope ended, including when all prior evidence was absent.
	id := "not-an-existing-creation"
	raw, _ := json.Marshal(map[string]any{"value": map[string]any{"creations": []map[string]string{{"id": id}}}})
	if _, err := tx.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_scope','new-negative-witness','fixture',?)", raw); err != nil {
		t.Fatal(err)
	}
	if known, err := splitChildScopeCreationKnown(ctx, tx, id); err != nil || !known {
		t.Fatal("cached absence hid a later witness", known, err)
	}
}

func TestSplitCreationWitnessProjectionOverflowFallsBackToExactLookup(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	creations := make([]map[string]string, maxSplitProofReadEntries+2)
	for i := range creations {
		creations[i] = map[string]string{"id": fmt.Sprintf("bounded-witness-%04d", i)}
	}
	raw, _ := json.Marshal(map[string]any{"value": map[string]any{"creations": creations}})
	if _, err := tx.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_scope','overflow-witness','fixture',?)", raw); err != nil {
		t.Fatal(err)
	}
	_, err = readSplitProof(ctx, tx, "test-creation-overflow", "all", func(read context.Context) (bool, error) {
		projection, err := readSplitProof(read, tx, "splitCreationWitnessIDs", "all", func(inner context.Context) (splitCreationWitnessIDs, error) {
			return loadSplitCreationWitnessIDs(inner, tx)
		})
		if err != nil {
			return false, err
		}
		if projection.Complete || len(projection.IDs) > maxSplitProofReadEntries {
			t.Fatal("overflow was treated as complete", len(projection.IDs))
		}
		set := map[string]bool{}
		for _, id := range projection.IDs {
			set[id] = true
		}
		for _, creation := range creations {
			if !set[creation["id"]] {
				if known, err := splitChildScopeCreationKnown(read, tx, creation["id"]); err != nil || !known {
					t.Fatal("partial projection granted absence", known, err)
				}
				return true, nil
			}
		}
		t.Fatal("fixture lacked a witness beyond the projection bound")
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSplitCreationWitnessProjectionOversizeIdentityFallsBack(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	id := strings.Repeat("x", contract.MaxInline+1)
	raw, _ := json.Marshal(map[string]any{"value": map[string]any{"creations": []map[string]string{{"id": id}}}})
	if _, err := tx.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_scope','oversize-identity','fixture',?)", raw); err != nil {
		t.Fatal(err)
	}
	_, err = readSplitProof(ctx, tx, "test-creation-oversize", "all", func(read context.Context) (bool, error) {
		known, err := splitChildScopeCreationKnown(read, tx, id)
		if err != nil || !known {
			t.Fatal("identity byte bound granted absence", known, err)
		}
		return known, err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSplitCreationWitnessProjectionErrorPreservesExactLookup(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_scope','intact','fixture',?)", []byte(`{"value":{"creations":[{"id":"known-creation"}]}}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_work_item_registration','damaged-other-wave','other-wave',?)", []byte(`{"value":{"split_scope":{"creations":"not-json"}}}`)); err != nil {
		t.Fatal(err)
	}
	if known, err := splitChildScopeCreationKnownUncached(ctx, tx, "known-creation"); err != nil || !known {
		t.Fatal("exact fixture query did not settle identity", known, err)
	}
	_, err = readSplitProof(ctx, tx, "test-creation-projection-error", "all", func(read context.Context) (bool, error) {
		if _, err := loadSplitCreationWitnessIDs(read, tx); err == nil {
			t.Fatal("fixture did not fail the aggregate projection")
		}
		known, err := splitChildScopeCreationKnown(read, tx, "known-creation")
		if err != nil || !known {
			t.Fatal("projection widened an unrelated metadata failure", known, err)
		}
		return known, err
	})
	if err != nil {
		t.Fatal(err)
	}
}
