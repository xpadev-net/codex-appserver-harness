package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Contract-boundary fixture: merge authentication is independently covered by
// the original real transport/receipt fixture. Stage a scope-bearing historical
// registration for the post-authentication contract check; this does not claim
// execution or a successful end-to-end merge of a split child.
func TestProductContinuationParentContractRejectsScopedHistoryWithoutChangingIt(t *testing.T) {
	ctx := context.Background()
	k, request, original := productContinuationFixture(t)
	w, err := k.Wave(ctx, request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	if err := continuationParentContractTx(ctx, k.DB, request, w, original); err != nil {
		t.Fatal("authenticated root contract rejected", err)
	}
	p := original.Value.Product.Value
	var registration ProductWorkItemRegistrationSnapshot
	id := structureID("product-work-", p.WorkItem.ID, original.Value.RegistrationDigest)
	if err := k.Record(ctx, "product_work_item_registration_version", id, &registration); err != nil {
		t.Fatal(err)
	}
	registration.Value.SplitScope = &ProductSplitChildScopeReference{Request: ProductSplitChildPlanRequest{Target: p.Target, ParentWorkItemID: "original-split-parent", ProposalID: "original-split-proposal", ProposalDigest: strings.Repeat("a", 64)}, BindingID: "original-local-scope", BindingDigest: strings.Repeat("b", 64)}
	registration.Digest, err = contentDigest(registration.Value)
	if err != nil {
		t.Fatal(err)
	}
	scopedVersion := structureID("product-work-", p.WorkItem.ID, registration.Digest)
	if err := k.Put(ctx, "product_work_item_registration_version", scopedVersion, w.ID, registration); err != nil {
		t.Fatal(err)
	}
	parent := original
	parent.Value.RegistrationDigest = registration.Digest
	parent.Digest, err = contentDigest(parent.Value)
	if err != nil {
		t.Fatal(err)
	}
	request.ParentReceiptDigest = parent.Digest
	var recordsBefore int
	if err := k.DB.QueryRow("SELECT count(*) FROM records").Scan(&recordsBefore); err != nil {
		t.Fatal(err)
	}
	if err := continuationParentContractTx(ctx, k.DB, request, w, parent); !errors.Is(err, ErrProductContinuationHeld) {
		t.Fatal("scope-bearing parent allowed an unscoped successor", err)
	}
	fixed := k.clock()
	path := k.Root + "/state.db"
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.Now = func() time.Time { return fixed }
	k = reopened
	if err := continuationParentContractTx(ctx, k.DB, request, w, parent); !errors.Is(err, ErrProductContinuationHeld) {
		t.Fatal("reopen erased scope restriction", err)
	}
	var recordsAfter int
	if err := k.DB.QueryRow("SELECT count(*) FROM records").Scan(&recordsAfter); err != nil || recordsBefore != recordsAfter {
		t.Fatal("held contract changed durable state", recordsBefore, recordsAfter, err)
	}
	var preserved ProductWorkItemRegistrationSnapshot
	if err := k.Record(ctx, "product_work_item_registration_version", scopedVersion, &preserved); err != nil || !reflect.DeepEqual(preserved, registration) {
		t.Fatal("historical scope was erased", err)
	}
	if read, err := k.ReadProductMergedWorkItem(ctx, original.Value.MergeID); err != nil || !reflect.DeepEqual(read, original) {
		t.Fatal("scope hold rewrote original merge evidence", err)
	}
}
