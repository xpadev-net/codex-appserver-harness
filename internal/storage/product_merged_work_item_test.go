package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func TestProductMergedWorkItemDerivesKnownNativeFactsAndPreservesHistory(t *testing.T) {
	ctx := context.Background()
	k, job, proof := registeredHTTPStopFixture(t, "merged")
	if _, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof); err != nil {
		t.Fatal(err)
	}
	saved, changed, err := k.RecordProductMergedWorkItem(ctx, job.ID)
	if err != nil || !changed || saved.Value.MergeSHA != proof.Response.MergeSHA || saved.Value.Projection == nil || len(saved.Value.Criteria) == 0 {
		t.Fatal(saved, changed, err)
	}
	p := job.Value.Product.Value
	current, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
	if err != nil || current.Value.WorkItem.Phase != domain.Merged || current.Value.WorkItem.Version != p.WorkItem.Version+1 || current.Value.Candidate != p.Candidate || current.Value.WorkItem.MergeObservedAt != proof.FinishedAt {
		t.Fatal("wrong merged projection", current, err)
	}
	w, err := k.Wave(ctx, p.Target.WaveID)
	if err != nil || w.State == domain.WaveCompleted {
		t.Fatal("merge receipt completed Wave", w, err)
	}
	page, err := k.MergeDispatchTargets(ctx, "", 128)
	if err != nil || len(page.Targets) != 0 {
		t.Fatal("adopted merge requeued", page, err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	replay, changed, err := k.RecordProductMergedWorkItem(ctx, job.ID)
	if err != nil || changed || !reflect.DeepEqual(replay, saved) {
		t.Fatal("receipt replay changed evidence", replay, changed, err)
	}
	read, err := k.ReadProductMergedWorkItem(ctx, job.ID)
	if err != nil || !reflect.DeepEqual(read, saved) {
		t.Fatal(read, err)
	}
}

func TestProductMergedWorkItemOldFactsNeverOverwriteNewCandidateOrCanceledWave(t *testing.T) {
	for _, mode := range []string{"new_candidate", "cancel", "paused_controls", "reconciled"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			kind := "merged"
			if mode == "reconciled" {
				kind = "outcome_unknown"
			}
			k, job, proof := registeredHTTPStopFixture(t, kind)
			if _, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof); err != nil {
				t.Fatal(err)
			}
			p := job.Value.Product.Value
			if mode == "reconciled" {
				positive := mergeReconciliationFixture(k, job)
				positive.StopProofID = proof.ProofID
				if _, err := k.ConfirmProductMergeObserved(ctx, job.ID, job.Value.OwnerID, job.Digest, positive); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "new_candidate":
				p.Candidate.ID, p.WorkItem.Candidate = "after-merge", "after-merge"
				p.WorkItem.Version++
				if _, err := k.SetProductTarget(ctx, job.Value.Product.Digest, p); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-before-projection", Target: p.Target, Action: "cancel", Reason: "old merge fact remains known"}); err != nil {
					t.Fatal(err)
				}
			case "paused_controls":
				p.WorkItem.Paused, p.WorkItem.MergeHold = true, true
				if _, err := k.SetProductTarget(ctx, job.Value.Product.Digest, p); err != nil {
					t.Fatal(err)
				}
			}
			before, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
			if err != nil {
				t.Fatal(err)
			}
			saved, changed, err := k.RecordProductMergedWorkItem(ctx, job.ID)
			if err != nil || !changed || saved.Value.Product.Digest != job.Value.Product.Digest {
				t.Fatal(saved, changed, err)
			}
			after, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "new_candidate" || mode == "cancel" {
				if saved.Value.Projection != nil || !reflect.DeepEqual(before, after) {
					t.Fatal("old fact overwrote current work", saved, before, after)
				}
			} else if saved.Value.Projection == nil || after.Value.WorkItem.Phase != domain.Merged || (mode == "paused_controls" && (!after.Value.WorkItem.Paused || !after.Value.WorkItem.MergeHold)) {
				t.Fatal("current candidate fact lost controls", saved, after)
			}
		})
	}
}

func TestProductMergedWorkItemMissingEvidenceAndRollbackNeverRegenerate(t *testing.T) {
	for _, mode := range []string{"rollback", "head", "history", "admission", "http_stop", "registration", "prior_observation", "projection"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, proof := registeredHTTPStopFixture(t, "merged")
			if _, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof); err != nil {
				t.Fatal(err)
			}
			if mode == "rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_merge_adoption BEFORE INSERT ON records WHEN NEW.kind='product_merged_work_item_history' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
					t.Fatal(err)
				}
				if _, changed, err := k.RecordProductMergedWorkItem(ctx, job.ID); err == nil || changed {
					t.Fatal("failed receipt committed", changed, err)
				}
				current, err := k.ReadProductTarget(ctx, job.Value.Product.Value.Target, job.Value.Product.Value.WorkItem.ID)
				if err != nil || !reflect.DeepEqual(current, job.Value.Product) {
					t.Fatal("projection survived rolled-back receipt", current, err)
				}
				return
			}
			if mode == "prior_observation" {
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_target_observation' AND id=?", job.Value.Product.Digest); err != nil {
					t.Fatal(err)
				}
				if _, changed, err := k.RecordProductMergedWorkItem(ctx, job.ID); !errors.Is(err, ErrTaskEvidence) || changed {
					t.Fatal("missing original observation recreated", changed, err)
				}
				return
			}
			saved, _, err := k.RecordProductMergedWorkItem(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			kind, id := "product_merged_work_item", job.ID
			switch mode {
			case "history":
				kind = "product_merged_work_item_history"
			case "admission":
				kind = "product_merged_work_item_admission"
			case "http_stop":
				kind = "product_merge_http_stop"
			case "registration":
				kind, id = "product_work_item_registration_version", structureID("product-work-", job.Value.Product.Value.WorkItem.ID, saved.Value.RegistrationDigest)
			case "projection":
				kind, id = "product_target_observation", saved.Value.Projection.After.Digest
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, id); err != nil {
				t.Fatal(err)
			}
			if _, _, err := k.RecordProductMergedWorkItem(ctx, job.ID); err == nil || errors.Is(err, ErrProductMergeHeld) {
				t.Fatal("lost admitted evidence regenerated or treated as ordinary hold", err)
			}
		})
	}
}

func TestProductMergedWorkItemRequiresRegisteredKnownMergeAndSavedPhysicalStop(t *testing.T) {
	for _, mode := range []string{"outcome_unknown", "rejected", "client_not_started", "no_stop", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var k *KernelStore
			var job ProductMerge
			var proof ProductMergeStop
			if mode == "legacy" {
				k, job, proof = mergeResultFixture(t, true)
				proof.Disposition = "response_received"
				p := job.Value.Product.Value
				proof.Response = &ProductMergeResponse{RepositoryID: p.WorkItem.RepositoryID, PRNumber: p.PRNumber, ExpectedHeadSHA: p.Candidate.SHA, Method: job.Value.Method, Merged: true, MergeSHA: strings.Repeat("e", 40)}
				if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, proof); err != nil {
					t.Fatal(err)
				}
			} else {
				kind := mode
				if mode == "no_stop" {
					kind = "merged"
				}
				k, job, proof = registeredHTTPStopFixture(t, kind)
				if mode != "no_stop" {
					if _, err := k.FinishProductMergeHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, job.HTTP.SpecDigest, proof); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, changed, err := k.RecordProductMergedWorkItem(ctx, job.ID); !errors.Is(err, ErrProductMergeHeld) || changed {
				t.Fatal("unproven result became merged WorkItem", changed, err)
			}
			current, err := k.ReadProductTarget(ctx, job.Value.Product.Value.Target, job.Value.Product.Value.WorkItem.ID)
			if err != nil || !reflect.DeepEqual(current, job.Value.Product) {
				t.Fatal("refused result changed current work", current, err)
			}
		})
	}
}
