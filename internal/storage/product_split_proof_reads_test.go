package storage

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestSplitProofReadsNeverSurviveWritesOrTransactions(t *testing.T) {
	ctx := context.Background()
	k, r, scopes := splitRegistrationsFixture(t)
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	other := cloneNestedScopeStore(t, k)
	childID := scopes.Value.Children[0].Value.Child.Value.WorkItemID
	if _, err := other.DB.Exec("DELETE FROM records WHERE kind='product_split_child_scope' AND id=?", childID); err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Each original caller context starts a new proof walk after the previous
	// read returns. A write in this very transaction must therefore be visible.
	if _, err := loadProductSplitRegistrations(ctx, tx, r); err != nil {
		t.Fatal(err)
	}
	var owner string
	var raw []byte
	if err := tx.QueryRow("SELECT work_item_id,payload FROM records WHERE kind='product_split_child_scope' AND id=?", childID).Scan(&owner, &raw); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("DELETE FROM records WHERE kind='product_split_child_scope' AND id=?", childID); err != nil {
		t.Fatal(err)
	}
	if _, err := loadProductSplitRegistrations(ctx, tx, r); err == nil {
		t.Fatal("prior read hid a missing scope in the same transaction")
	}
	if _, err := tx.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES('product_split_child_scope',?,?,?)", childID, owner, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := loadProductSplitRegistrations(ctx, tx, r); err != nil {
		t.Fatal("exact restore was not revalidated", err)
	}
	otherTx, err := other.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer otherTx.Rollback()
	_, err = readSplitProof(ctx, tx, "test-transaction-boundary", r, func(read context.Context) (ProductSplitRegistrations, error) {
		first, err := loadProductSplitRegistrations(read, tx, r)
		if err != nil {
			return first, err
		}
		if _, err := loadProductSplitRegistrations(read, otherTx, r); err == nil {
			t.Fatal("memo crossed into a different transaction")
		}
		return first, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSplitProofReadCopiesAndInputsRemainIndependent(t *testing.T) {
	ctx := context.Background()
	k, r, _ := splitRegistrationsFixture(t)
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = readSplitProof(ctx, tx, "test-copy-boundary", r, func(read context.Context) (ProductSplitRegistrations, error) {
		first, err := loadProductSplitRegistrations(read, tx, r)
		if err != nil {
			return first, err
		}
		original := first.Value.Children[0].Value.Issue.Number
		first.Value.Children[0].Value.Issue.Number++
		first.Value.Registrations[0].Value.Definition.Criteria[0] = "tampered"
		second, err := loadProductSplitRegistrations(read, tx, r)
		if err != nil {
			return second, err
		}
		if second.Value.Children[0].Value.Issue.Number != original || second.Value.Registrations[0].Value.Definition.Criteria[0] == "tampered" {
			t.Fatal("caller mutation reached a cached proof")
		}
		second.Value.Children[0].Value.Issue.Number++
		third, err := loadProductSplitRegistrations(read, tx, r)
		if err != nil {
			return third, err
		}
		if third.Value.Children[0].Value.Issue.Number != original {
			t.Fatal("cache hits shared mutable storage")
		}
		changed := r
		changed.ProposalDigest = strings.Repeat("f", 64)
		if _, err := loadProductSplitRegistrations(read, tx, changed); err == nil {
			t.Fatal("changed request reused a proof for different inputs")
		}
		return third, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSplitProofReadCycleAndCancellationAreHeldWithoutPoisoningNextRead(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	type proof struct {
		Value string `json:"value"`
	}
	_, err = readSplitProof(ctx, tx, "test-cycle", "same", func(read context.Context) (proof, error) {
		return readSplitProof(read, tx, "test-cycle", "same", func(context.Context) (proof, error) { t.Fatal("cycle callback ran"); return proof{}, nil })
	})
	if !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("cyclic graph not held", err)
	}
	got, err := readSplitProof(ctx, tx, "test-cycle", "same", func(context.Context) (proof, error) { return proof{Value: "valid"}, nil })
	if err != nil || got.Value != "valid" {
		t.Fatal("failed graph poisoned a subsequent call", got, err)
	}
	partial, err := readSplitProof(ctx, tx, "test-error", "same", func(context.Context) (proof, error) {
		return proof{Value: "partial"}, ErrTaskEvidence
	})
	if !errors.Is(err, ErrTaskEvidence) || partial.Value != "partial" {
		t.Fatal("loader error result changed", partial, err)
	}
	valid, err := readSplitProof(ctx, tx, "test-error", "same", func(context.Context) (proof, error) { return proof{Value: "valid"}, nil })
	if err != nil || valid.Value != "valid" {
		t.Fatal("error result was reused", valid, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	_, err = readSplitProof(cancelled, tx, "test-cancel", "same", func(read context.Context) (proof, error) { cancel(); return proof{Value: "valid"}, nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read returned a proof", err)
	}
}

func TestSplitProofReadCapacityStillRunsTheLoader(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	type proof struct {
		Value string `json:"value"`
	}
	for _, limit := range []string{"entries", "bytes"} {
		t.Run(limit, func(t *testing.T) {
			memo := &splitProofReadMemo{tx: tx, active: map[splitProofReadKey]bool{}, saved: map[splitProofReadKey][]byte{}}
			if limit == "entries" {
				for i := 0; i < maxSplitProofReadEntries; i++ {
					memo.saved[splitProofReadKey{kind: "occupied", digest: strconv.Itoa(i)}] = nil
				}
			} else {
				memo.bytes = maxSplitProofReadBytes
			}
			read := context.WithValue(ctx, splitProofReadContextKey{}, memo)
			entries, bytes, calls := len(memo.saved), memo.bytes, 0
			for i := 0; i < 2; i++ {
				got, err := readSplitProof(read, tx, "capacity", "same", func(context.Context) (proof, error) { calls++; return proof{Value: "valid"}, nil })
				if err != nil || got.Value != "valid" {
					t.Fatal("capacity rejected a valid proof", got, err)
				}
			}
			if calls != 2 || len(memo.saved) != entries || memo.bytes != bytes {
				t.Fatal("capacity failed to bypass additional caching", calls, len(memo.saved), memo.bytes)
			}
		})
	}
}

func TestSplitProofReadActiveWalkBoundaries(t *testing.T) {
	ctx := context.Background()
	k, _ := waveStoreFixture(t)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	type proof struct {
		Value string `json:"value"`
	}
	memo := &splitProofReadMemo{tx: tx, active: map[splitProofReadKey]bool{}, saved: map[splitProofReadKey][]byte{}}
	for i := 0; i < maxSplitProofReadEntries; i++ {
		memo.active[splitProofReadKey{kind: "active", digest: strconv.Itoa(i)}] = true
	}
	read := context.WithValue(ctx, splitProofReadContextKey{}, memo)
	_, err = readSplitProof(read, tx, "limit", "same", func(context.Context) (proof, error) {
		t.Fatal("excessive walk executed its loader")
		return proof{}, nil
	})
	if !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("active limit ignored", err)
	}
	delete(memo.active, splitProofReadKey{kind: "active", digest: "0"})
	got, err := readSplitProof(read, tx, "limit", "same", func(context.Context) (proof, error) { return proof{Value: "valid"}, nil })
	if err != nil || got.Value != "valid" || len(memo.active) != maxSplitProofReadEntries-1 {
		t.Fatal("last permitted walk failed or leaked active state", got, err)
	}
}

func TestSplitProofLatestCreationNeverHidesADeletedRetryHead(t *testing.T) {
	ctx := context.Background()
	k, _, _, initial := splitCreationFixture(t)
	if _, err := k.AbortUnactivatedProductSplitCreate(ctx, initial.ID, initial.Value.OwnerID, initial.Digest, "memo-latest-unsent"); err != nil {
		t.Fatal(err)
	}
	retry, err := k.ReserveProductSplitCreateUnsentRetry(ctx, initial.ID, "memo-latest-retry")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	childID := initial.Value.Child.ID
	_, err = readSplitProof(ctx, tx, "test-latest-creation", childID, func(read context.Context) (ProductSplitCreate, error) {
		first, err := loadLatestProductSplitCreate(read, tx, childID)
		if err != nil {
			return first, err
		}
		second, err := loadLatestProductSplitCreate(read, tx, childID)
		if err != nil || first.ID != retry.ID || second.ID != retry.ID || second.Value.Attempt != 1 {
			t.Fatal("latest retry proof changed", first.ID, second.ID, err)
		}
		return second, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("DELETE FROM records WHERE kind='product_split_create' AND id=?", retry.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLatestProductSplitCreate(ctx, tx, childID); !readinessHoldError(err) {
		t.Fatal("previous read concealed a deleted latest retry", err)
	}
}
