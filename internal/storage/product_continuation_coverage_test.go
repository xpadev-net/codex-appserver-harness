package storage

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductContinuationCoverageRetiresOnlyAuthenticatedOldContributor(t *testing.T) {
	ctx := context.Background()
	k, request, parent := productContinuationFixture(t)
	before, err := k.ProductCoverage(ctx, request.Target)
	if err != nil || len(before.Value.Historical) != 1 || len(before.Value.Unavailable) == 0 || len(before.Value.Retired) != 0 {
		t.Fatal("old scope disappeared without successor", before, err)
	}
	node, err := k.RegisterProductContinuation(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := k.ProductCoverage(ctx, request.Target)
	if err != nil || len(ledger.Value.Retired) != 1 || len(ledger.Value.Historical) != 1 || len(ledger.Value.Merged) != 0 || len(ledger.Value.Pending)+len(ledger.Value.Unavailable) != 0 || !reflect.DeepEqual(ledger.Value.UnmergedWorkItems, []string{request.Definition.ID}) || len(ledger.Value.Residual) != len(w.Criteria) {
		t.Fatal("old merge counted as current quality or old work stayed active", ledger, err)
	}
	retired := ledger.Value.Retired[0]
	if retired.WorkItemID != node.Value.ParentWorkItemID || retired.RegistrationDigest != node.Value.ParentRegistrationDigest || retired.CurrentWorkItemID != request.Definition.ID || retired.ContinuationID != node.ID || retired.ContinuationDigest != node.Digest {
		t.Fatal("retirement lost its original binding", retired, node)
	}
	fact := ledger.Value.Historical[0]
	if fact.MergeID != request.ParentMergeID || fact.Target != node.Value.ParentTarget || fact.ReceiptDigest != parent.Digest || fact.MergeSHA != parent.Value.MergeSHA {
		t.Fatal("historical fact relabeled", fact)
	}
	manifest := registrationManifest(t, k, w.OrchestratorSessionID, request.Target)
	if manifest.ProductMergeCoverage == nil || !reflect.DeepEqual(*manifest.ProductMergeCoverage, ledger) {
		t.Fatal("parent lacks exact retirement ledger", manifest.ProductMergeCoverage)
	}
	current, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(current, w) || current.State == domain.WaveCompleted {
		t.Fatal("coverage changed Wave", current, err)
	}
	afterBudget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(afterBudget, budget) {
		t.Fatal("coverage reset budget", afterBudget, err)
	}
	projection, err := k.ReadProductTarget(ctx, parent.Value.Product.Value.Target, retired.WorkItemID)
	if err != nil || !reflect.DeepEqual(projection, parent.Value.Projection.After) {
		t.Fatal("coverage rewrote old PR", projection, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	reopened, err := k.ProductCoverage(ctx, request.Target)
	if err != nil || !reflect.DeepEqual(reopened, ledger) {
		t.Fatal("retirement changed after reopen", reopened, err)
	}
}

func TestProductContinuationCoverageLostProofNeverSilentlyRetiresWork(t *testing.T) {
	ctx := context.Background()
	k, request, _ := productContinuationFixture(t)
	node, err := k.RegisterProductContinuation(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	intact, err := k.ProductCoverage(ctx, request.Target)
	if err != nil {
		t.Fatal(err)
	}
	for _, loss := range []struct{ kind, id string }{
		{"product_continuation", request.Definition.ID},
		{"product_continuation_history", node.ID},
		{"product_continuation_admission", request.Definition.ID},
		{"product_continuation_request", productContinuationRequestID(request)},
		{"product_work_item_registration", request.Definition.ID},
		{"product_work_item_registration", node.Value.ParentWorkItemID},
		{"product_work_item_registration_version", structureID("product-work-", request.Definition.ID, node.Value.Registration.Digest)},
		{"product_work_item_registration_version", structureID("product-work-", node.Value.ParentWorkItemID, node.Value.ParentRegistrationDigest)},
		{"product_merged_work_item", request.ParentMergeID},
		{"product_merged_work_item_history", request.ParentMergeID},
		{"product_target", productTargetID(node.Value.ParentTarget, node.Value.ParentWorkItemID)},
	} {
		t.Run(loss.kind+loss.id, func(t *testing.T) {
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(ctx, "DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			ledger, err := k.productCoverageTx(ctx, tx, w)
			if err != nil || len(ledger.Value.Retired) != 0 || len(ledger.Value.Unavailable) == 0 || !slices.Contains(ledger.Value.UnmergedWorkItems, node.Value.ParentWorkItemID) || len(ledger.Value.Residual) != len(w.Criteria) {
				t.Fatal("missing proof hid an old obligation", ledger, err)
			}
		})
	}
	restored, err := k.ProductCoverage(ctx, request.Target)
	if err != nil || !reflect.DeepEqual(restored, intact) {
		t.Fatal("proof inspection changed ledger", restored, err)
	}
}

func TestProductContinuationCoverageCannotRetireRefreshedCurrentRegistration(t *testing.T) {
	ctx := context.Background()
	k, request, parent := productContinuationFixture(t)
	node, err := k.RegisterProductContinuation(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	p := parent.Value.Product.Value
	var registration ProductWorkItemRegistrationSnapshot
	if err := k.Record(ctx, "product_work_item_registration_version", structureID("product-work-", p.WorkItem.ID, node.Value.ParentRegistrationDigest), &registration); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductWorkItem(ctx, request.Target, registration.Digest, registration.Value.Definition); err != nil {
		t.Fatal(err)
	}
	ledger, err := k.ProductCoverage(ctx, request.Target)
	if err != nil || len(ledger.Value.Retired) != 0 || !slices.Contains(ledger.Value.UnmergedWorkItems, p.WorkItem.ID) || !slices.Contains(ledger.Value.UnmergedWorkItems, request.Definition.ID) || len(ledger.Value.Residual) == 0 {
		t.Fatal("lineage erased a new current source obligation", ledger, err)
	}
}
