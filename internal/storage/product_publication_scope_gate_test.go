package storage

import (
	"context"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// Exercise only the residual metadata gate; this synthetic readiness is never
// used as a quality, publication or merge grant.
func TestPublicationScopeGateRejectsMissingOrRelabeledResidual(t *testing.T) {
	for _, mode := range []string{"valid", "missing_pointer", "changed_summary", "head", "history", "registration", "membership", "same_count_foreign_criterion", "duplicate_criterion"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, w, outside := publicationScopeFixture(t, true)
			scope, err := k.PrepareProductPublicationScope(ctx, w.Target(), "work")
			if err != nil {
				t.Fatal(err)
			}
			ready := ProductPublicationReadiness{Value: ProductPublicationReadinessValue{Scope: &scope, Product: ProductTargetSnapshot{Value: ProductTarget{Target: w.Target(), WorkItem: domain.WorkItem{ID: "work"}}}}}
			switch mode {
			case "missing_pointer":
				ready.Value.Scope = nil
			case "changed_summary":
				altered := scope
				altered.Value.OutsideCriteria = []string{}
				ready.Value.Scope = &altered
			case "head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_publication_scope'")
			case "history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_publication_scope_history'")
			case "registration":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration' AND id=?", outside.Value.Definition.ID)
			case "membership":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='wave_work_item' AND id=?", outside.Value.Definition.ID)
			case "same_count_foreign_criterion", "duplicate_criterion":
				// Author a self-consistent but invalid source registration to test
				// the reader's set validation independently of registration admission.
				r := scope.Value.Included
				r.Value.Definition.Criteria = []string{"AC", "foreign"}
				if mode == "duplicate_criterion" {
					r.Value.Definition.Criteria = []string{"AC", "AC"}
				}
				r.Digest, err = contentDigest(r.Value)
				if err == nil {
					err = k.Put(ctx, "product_work_item_registration", "work", w.ID, r)
				}
				if err == nil {
					err = k.Put(ctx, "product_work_item_registration_version", structureID("product-work-", "work", r.Digest), w.ID, r)
				}
				ready.Value.Scope = nil
			}
			if err != nil {
				t.Fatal(err)
			}
			tx, err := k.kernelTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = k.checkProductPublicationScopeTx(ctx, tx, ready)
			tx.Rollback()
			if (err == nil) != (mode == "valid") {
				t.Fatal("residual gate result", mode, err)
			}
		})
	}
}
