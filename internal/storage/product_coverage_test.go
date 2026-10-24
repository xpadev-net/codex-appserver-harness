package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func knownCoverageFixture(t *testing.T) (*KernelStore, ProductMerge) {
	t.Helper()
	ctx := context.Background()
	k, job, proof := registeredHTTPStopFixture(t, "merged")
	if _, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof); err != nil {
		t.Fatal(err)
	}
	return k, job
}

func TestProductCoverageAuthenticatesFactsAndKeepsPendingAndImmutableParentInput(t *testing.T) {
	ctx := context.Background()
	k, job := knownCoverageFixture(t)
	target := job.Value.Product.Value.Target
	before, err := k.ProductCoverage(ctx, target)
	if err != nil || len(before.Value.Merged) != 0 || len(before.Value.Pending) != 1 || len(before.Value.Residual) == 0 {
		t.Fatal("unrecorded merge became coverage", before, err)
	}
	oldInput, err := k.BuildSessionInput(ctx, "main", []contract.Target{target})
	if err != nil {
		t.Fatal(err)
	}
	oldRef := inputContext(t, oldInput).Waves[0].Manifest
	var oldManifest savedInputManifest
	if err := k.Record(ctx, "session_input_manifest", oldRef.ID, &oldManifest); err != nil {
		t.Fatal(err)
	}
	oldBytes := append([]byte{}, oldManifest.Data...)
	receipt, _, err := k.RecordProductMergedWorkItem(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	current, err := k.ProductCoverage(ctx, target)
	if err != nil || len(current.Value.Merged) != 1 || len(current.Value.Residual) != 0 || len(current.Value.Pending) != 0 || len(current.Value.Unavailable) != 0 || current.Digest == before.Digest {
		t.Fatal(current, err)
	}
	fact := current.Value.Merged[0]
	if fact.Target != target || fact.MergeID != job.ID || fact.ReceiptDigest != receipt.Digest || fact.CandidateSHA != job.Value.Product.Value.Candidate.SHA || fact.MergeSHA != receipt.Value.MergeSHA || !reflect.DeepEqual(fact.Criteria, receipt.Value.Criteria) {
		t.Fatal("original merge identity relabeled", fact, receipt)
	}
	manifest := registrationManifest(t, k, "main", target)
	if manifest.ProductMergeCoverage == nil || !reflect.DeepEqual(*manifest.ProductMergeCoverage, current) || manifest.UnavailableProductCoverage != "" {
		t.Fatal("parent manifest lacks exact saved coverage", manifest.ProductMergeCoverage, manifest.UnavailableProductCoverage)
	}
	newInput, err := k.BuildSessionInput(ctx, "main", []contract.Target{target})
	if err != nil || strings.Contains(string(newInput.Input), fact.MergeSHA) || len(newInput.Input) > contract.MaxSessionInput {
		t.Fatal("coverage leaked into inline wake instead of scoped manifest", err)
	}
	newRef := inputContext(t, newInput).Waves[0].Manifest
	if newRef.Digest == oldRef.Digest {
		t.Fatal("new fact did not change the manifest reference")
	}
	if err := k.Record(ctx, "session_input_manifest", oldRef.ID, &oldManifest); err != nil || string(oldManifest.Data) != string(oldBytes) {
		t.Fatal("old input bytes changed", err)
	}
	var old sessionManifest
	if err := json.Unmarshal(oldManifest.Data, &old); err != nil || old.ProductMergeCoverage == nil || old.ProductMergeCoverage.Digest != before.Digest {
		t.Fatal("old snapshot was relabeled with new facts", old.ProductMergeCoverage, err)
	}
	w, err := k.Wave(ctx, target.WaveID)
	if err != nil || w.State == domain.WaveCompleted {
		t.Fatal("coverage became acceptance", w, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	reopened, err := k.ProductCoverage(ctx, target)
	if err != nil || !reflect.DeepEqual(reopened, current) {
		t.Fatal("coverage changed after reopen", reopened, err)
	}
}

func TestProductCoverageRetainsOriginalPlanAndNeverCoversAnotherScopePolicyOrSource(t *testing.T) {
	ctx := context.Background()
	k, job := knownCoverageFixture(t)
	if _, _, err := k.RecordProductMergedWorkItem(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, job.Value.Product.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"plan", "scope", "policy", "source"} {
		t.Run(mode, func(t *testing.T) {
			// This is the pure ledger classification boundary; public reads
			// obtain this versioned contract from loadWave, never caller facts.
			current := w
			switch mode {
			case "plan":
				current.PlanRevision++
			case "scope":
				current.ScopeRevision++
			case "policy":
				current.Policy.Revision++
			case "source":
				current.SourceID = "different-source"
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			ledger, err := k.productCoverageTx(ctx, tx, current)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "plan" {
				if len(ledger.Value.Merged) != 1 || len(ledger.Value.Residual) != 0 || ledger.Value.Target != current.Target() || ledger.Value.Merged[0].Target != w.Target() {
					t.Fatal("planning rewrote historical merge identity", ledger)
				}
			} else if len(ledger.Value.Merged) != 0 || len(ledger.Value.Historical) != 1 || len(ledger.Value.Residual) != len(w.Criteria) {
				t.Fatal("old merge covered another contract", ledger)
			}
		})
	}
	wrong := w.Target()
	wrong.PlanRevision++
	if _, err := k.ProductCoverage(ctx, wrong); !errors.Is(err, ErrProductCoverageHeld) {
		t.Fatal("public read accepted caller-selected future target", err)
	}
}

func TestProductCoverageKeepsUnknownCorruptAndMissingHeadsUnavailable(t *testing.T) {
	for _, mode := range []string{"unknown", "merge_head", "receipt_head", "receipt_history", "corrupt_first"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			kind := "merged"
			if mode == "unknown" {
				kind = "outcome_unknown"
			}
			k, job, proof := registeredHTTPStopFixture(t, kind)
			if _, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof); err != nil {
				t.Fatal(err)
			}
			if mode != "unknown" {
				if _, _, err := k.RecordProductMergedWorkItem(ctx, job.ID); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "merge_head", "receipt_head", "receipt_history":
				kind := map[string]string{"merge_head": "product_merge", "receipt_head": "product_merged_work_item", "receipt_history": "product_merged_work_item_history"}[mode]
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, job.ID); err != nil {
					t.Fatal(err)
				}
			case "corrupt_first":
				if err := k.Put(ctx, "product_merge", "000-broken", job.Value.Product.Value.Target.WaveID, map[string]string{"corrupt": "head"}); err != nil {
					t.Fatal(err)
				}
			}
			ledger, err := k.ProductCoverage(ctx, job.Value.Product.Value.Target)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "unknown" {
				if len(ledger.Value.Pending) != 1 || ledger.Value.Pending[0].Reason != "stopped_unknown" || len(ledger.Value.Merged) != 0 || len(ledger.Value.Residual) == 0 {
					t.Fatal("unknown result became coverage", ledger)
				}
			} else if len(ledger.Value.Unavailable) != 1 || (mode == "corrupt_first" && len(ledger.Value.Merged) != 1) || (mode != "corrupt_first" && len(ledger.Value.Merged) != 0) {
				t.Fatal("lost proof silently disappeared or hid healthy fact", ledger)
			}
		})
	}
}

func TestProductCoverageIsParentOnlyAndBoundedWithoutAuthorizingReadiness(t *testing.T) {
	ctx := context.Background()
	k, w, definition := productRegistrationFixture(t, "direct")
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	for i := range plan.Phases {
		for j := range plan.Phases[i].Tasks {
			plan.Phases[i].Tasks[j].WorkItemIDs = []string{definition.ID}
		}
	}
	plan.Phases[len(plan.Phases)-1].Tasks[0].Criteria = slices.Clone(definition.Criteria)
	admitted, err := k.AdoptPlan(ctx, w.Target(), "coverage-plan", plan)
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	parent := registrationManifest(t, k, "main", w.Target())
	if parent.ProductMergeCoverage == nil || len(parent.ProductMergeCoverage.Value.Residual) != len(w.Criteria) {
		t.Fatal("parent lacks residual criteria", parent.ProductMergeCoverage)
	}
	for _, id := range admitted.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		input := registrationManifest(t, k, task.SessionID, w.Target())
		if input.ProductMergeCoverage != nil || input.UnavailableProductCoverage != "" {
			t.Fatal("Task received parent-wide merge coverage")
		}
	}
	ready, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
	if err != nil || len(ready.Tasks) != 0 || len(ready.Held) == 0 || ready.Held[0].Reason != "product_readiness_unavailable" {
		t.Fatal("metadata authorized product model dispatch", ready, err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxProductCoverageMerges+1; i++ {
		if err := kernelRecordTx(ctx, tx, "product_merge", fmt.Sprintf("bounded-%04d", i), w.ID, map[string]string{"opaque": "untrusted discovery"}); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ProductCoverage(ctx, w.Target()); !errors.Is(err, ErrProductCoverageHeld) {
		t.Fatal("oversized metadata silently truncated", err)
	}
	bounded := registrationManifest(t, k, "main", w.Target())
	if bounded.ProductMergeCoverage != nil || bounded.UnavailableProductCoverage != "coverage_metadata_exceeds_bound" {
		t.Fatal("parent did not receive explicit unavailable state", bounded.ProductMergeCoverage, bounded.UnavailableProductCoverage)
	}
}

func TestProductCoverageDoesNotHideNewCandidateBeforeAnotherMergeIntentExists(t *testing.T) {
	ctx := context.Background()
	k, job := knownCoverageFixture(t)
	if _, _, err := k.RecordProductMergedWorkItem(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	p := job.Value.Product.Value
	before, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	next := before.Value
	next.Candidate.ID, next.WorkItem.Candidate = "new-local-fix", "new-local-fix"
	next.WorkItem.Phase = domain.Verifying
	next.WorkItem.Version++
	if _, err := k.SetProductTarget(ctx, before.Digest, next); err != nil {
		t.Fatal(err)
	}
	ledger, err := k.ProductCoverage(ctx, p.Target)
	if err != nil || len(ledger.Value.Merged) != 0 || len(ledger.Value.Historical) != 1 || len(ledger.Value.Residual) == 0 || !reflect.DeepEqual(ledger.Value.UnmergedWorkItems, []string{p.WorkItem.ID}) {
		t.Fatal("old merge hid new local work without a new merge intent", ledger, err)
	}
}
