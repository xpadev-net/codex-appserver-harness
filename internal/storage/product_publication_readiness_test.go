package storage

import (
	"context"
	"reflect"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func publicationQualityFixture(t *testing.T, machine, reviewApplied bool) (*KernelStore, domain.Wave) {
	t.Helper()
	ctx := context.Background()
	k, w, plan, runtime, review, run := productQualityFixture(t)
	if reviewApplied {
		if _, err := k.ApplyTaskResult(ctx, review.SessionID, run.ID); err != nil {
			t.Fatal(err)
		}
	}
	if machine {
		completeProductRunnerFixture(t, k, plan, runtime, "unit", "passed")
	}
	return k, w
}

func TestProductPublicationPreparationTransitionsOnlyWithCurrentQuality(t *testing.T) {
	ctx := context.Background()
	k, w := publicationQualityFixture(t, true, true)
	before, err := k.ReadProductTarget(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	budget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.PrepareProductPublication(ctx, w.Target(), "work")
	if err != nil || saved.Value.Product.Value.WorkItem.Phase != domain.Reviewing || saved.Value.Product.Value.WorkItem.Version != before.Value.WorkItem.Version+1 || saved.Value.Product.Value.Candidate != before.Value.Candidate || saved.Value.Product.Value.PRNumber != 0 {
		t.Fatal(saved, err)
	}
	read, err := k.ReadProductPublicationReadiness(ctx, w.Target(), "work")
	if err != nil || !reflect.DeepEqual(read, saved) {
		t.Fatal(read, err)
	}
	again, err := k.PrepareProductPublication(ctx, w.Target(), "work")
	if err != nil || !reflect.DeepEqual(again, saved) {
		t.Fatal("replay changed phase/receipt", again, err)
	}
	afterBudget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(afterBudget, budget) {
		t.Fatal("quality transition spent execution budget", afterBudget, err)
	}
	var events, operations int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='product_publication_ready'").Scan(&events); err != nil || events != 1 {
		t.Fatal(events, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('product_pr_intent','product_pr_dispatch','product_pr_completion')").Scan(&operations); err != nil || operations != 0 {
		t.Fatal("quality fabricated an external operation", operations, err)
	}
}

func TestProductPublicationPreparationMissingQualityAndRollbackChangeNothing(t *testing.T) {
	for _, mode := range []string{"missing_machine", "missing_review", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w := publicationQualityFixture(t, mode != "missing_machine", mode != "missing_review")
			before, err := k.ReadProductTarget(ctx, w.Target(), "work")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_quality_ready BEFORE INSERT ON records WHEN NEW.kind='product_publication_readiness' BEGIN SELECT RAISE(FAIL,'quality rollback'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.PrepareProductPublication(ctx, w.Target(), "work"); err == nil {
				t.Fatal("incomplete/failed quality transition succeeded")
			}
			after, err := k.ReadProductTarget(ctx, w.Target(), "work")
			if err != nil || !reflect.DeepEqual(after, before) {
				t.Fatal("failed quality transition changed target", after, err)
			}
			var history int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_publication_readiness_history'").Scan(&history); err != nil || history != 0 {
				t.Fatal("failed quality transition saved history", history, err)
			}
		})
	}
}

func TestProductPublicationPreparationLostReceiptAndControlsCannotGrant(t *testing.T) {
	for _, mode := range []string{"head", "history", "pause", "merge_hold"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w := publicationQualityFixture(t, true, true)
			saved, err := k.PrepareProductPublication(ctx, w.Target(), "work")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "head" || mode == "history" {
				kind, id := "product_publication_readiness", productTargetID(w.Target(), "work")
				if mode == "history" {
					kind, id = "product_publication_readiness_history", saved.ID
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, id); err != nil {
					t.Fatal(err)
				}
			} else {
				product := saved.Value.Product
				if mode == "pause" {
					product.Value.WorkItem.Paused = true
				} else {
					product.Value.WorkItem.MergeHold = true
				}
				if _, err := k.SetProductTarget(ctx, product.Digest, product.Value); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.ReadProductPublicationReadiness(ctx, w.Target(), "work"); err == nil {
				t.Fatal("lost/stale receipt granted readiness")
			}
			if _, err := k.PrepareProductPublication(ctx, w.Target(), "work"); err == nil {
				t.Fatal("lost/held receipt regenerated readiness")
			}
		})
	}
}
