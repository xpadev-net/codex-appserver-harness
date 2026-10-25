package storage

import (
	"context"
	"errors"
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
