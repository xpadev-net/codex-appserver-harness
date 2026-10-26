package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// A nested split creates a bounded sibling set in one transaction. Validate
// every absent creation and the shared immutable ancestry before the first
// lease/journal write, so each sibling does not repeat the full tree walk.
func (k *KernelStore) prepareNestedSplitCreationsTx(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest, plan ProductSplitChildPlan, allocation ProductSplitAllocation) (map[string]ProductSplitCreate, error) {
	created := make(map[string]ProductSplitCreate, len(plan.Children))
	if plan.Set.Value.Request != r || allocation.Value.Request != r || allocation.Value.PlanID != plan.Set.ID || allocation.Value.PlanDigest != plan.Set.Digest || allocation.Value.ParentHeadID != plan.Set.Value.ParentHeadID || len(plan.Children) < 2 || len(plan.Children) > maxProductWorkItems || len(allocation.Value.WorkItemIDs) != len(plan.Children) {
		return created, ErrProductSplitHeld
	}
	for i, child := range plan.Children {
		if child.ID == "" || allocation.Value.WorkItemIDs[i] != child.Value.WorkItemID {
			return created, ErrProductSplitHeld
		}
	}

	read := context.WithValue(ctx, splitProofReadContextKey{}, newSplitProofReadMemo(tx))
	ancestry, err := loadSplitCreationAncestry(read, tx, r)
	if err != nil {
		return created, fmt.Errorf("nested creation ancestry: %w", err)
	}
	for _, child := range plan.Children {
		_, err := loadSplitCreate(read, tx, splitCreateID(child.ID))
		if err == nil {
			return created, fmt.Errorf("sibling creation already exists: %w", ErrProductSplitHeld)
		}
		if err != sql.ErrNoRows {
			return created, fmt.Errorf("sibling creation absence: %w", err)
		}
	}
	if err := k.nestedSplitCreationSourcePreflightTx(read, tx, r, plan, allocation, ancestry); err != nil {
		return created, fmt.Errorf("final nested source preflight: %w", err)
	}

	// The proof memo ends above before any sibling writes. The transaction
	// remains atomic, and each resource lease still has its own unique claim.
	for _, child := range plan.Children {
		owner := splitOwnerID(r, child)
		job := ProductSplitCreate{ID: splitCreateID(child.ID), Value: ProductSplitCreateValue{Ancestry: ancestry, Request: r, ParentHeadID: plan.Set.Value.ParentHeadID, AllocationID: allocation.ID, AllocationDigest: allocation.Digest, Child: child, OwnerID: owner, CreatedAt: k.clock()}, State: "prepared"}
		job.Digest, err = contentDigest(job.Value)
		if err != nil {
			return created, err
		}
		job.Lease, err = claimResourceTx(ctx, tx, fmt.Sprintf("product-split-mutation:%d:%s", child.Value.RepositoryID, child.ID), job.ID, r.Target.WaveID)
		if err != nil {
			return created, err
		}
		if err := kernelRecordTx(ctx, tx, "product_split_create_history", job.ID, r.ProposalID, job.Value); err != nil {
			return created, err
		}
		if err := kernelRecordTx(ctx, tx, "product_split_create_admission", job.ID, r.Target.WaveID, splitCreateAdmission{job.Value.Ancestry, r.ProposalID, job.Value.ParentHeadID, child.ID, job.Digest}); err != nil {
			return created, err
		}
		job, err = saveSplitCreateTx(ctx, tx, job, nil)
		if err != nil {
			return created, err
		}
		created[child.ID] = job
	}
	return created, nil
}

// Proposal, plan, and allocation were constructed and authenticated by the
// same transaction immediately before this read phase. Recheck the live Wave,
// current canonical parent binding, pinned policy, and every ancestor GET time
// without repeating the full immutable proposal/creation proof walk.
func (k *KernelStore) nestedSplitCreationSourcePreflightTx(ctx context.Context, tx *sql.Tx, r ProductSplitChildPlanRequest, plan ProductSplitChildPlan, allocation ProductSplitAllocation, ancestry *ProductNestedSplitAllocation) error {
	w, err := loadWave(ctx, tx, r.Target.WaveID)
	if err != nil {
		return fmt.Errorf("load nested source wave: %w", err)
	}
	if w.Target() != r.Target || w.DispatchPaused() || w.State == domain.WaveCompleted || w.State == domain.WaveCanceled || w.State == domain.WaveBlocked || !k.clock().Before(w.Deadline) {
		return fmt.Errorf("nested source wave is not live: %w", ErrProductSplitHeld)
	}
	if plan.Set.Value.Request != r {
		return fmt.Errorf("nested source plan request changed: %w", ErrProductSplitHeld)
	}
	if allocation.Value.Request != r {
		return fmt.Errorf("nested source allocation request changed: %w", ErrProductSplitHeld)
	}
	if allocation.Value.PlanID != plan.Set.ID || allocation.Value.PlanDigest != plan.Set.Digest || allocation.Value.ParentHeadID != plan.Set.Value.ParentHeadID {
		return fmt.Errorf("nested source allocation plan changed: %w", ErrProductSplitHeld)
	}
	if ancestry == nil {
		return fmt.Errorf("nested source ancestry missing: %w", ErrProductSplitHeld)
	}
	if ancestry.Value.ProductSplitRegistrationsValue.Request.Target != r.Target || ancestry.Value.ParentWorkItemID != r.ParentWorkItemID {
		return fmt.Errorf("nested source ancestry scope changed: %w", ErrProductSplitHeld)
	}
	proposal := ancestry.Value.Proposal
	if proposal.Value.Request.Target != r.Target || proposal.Value.Request.ParentWorkItemID != r.ParentWorkItemID || proposal.ID != r.ProposalID || proposal.Digest != r.ProposalDigest {
		return fmt.Errorf("nested source proposal changed: %w", ErrProductSplitHeld)
	}
	binding, err := splitChildSourceBindingTx(ctx, tx, r.Target, r.ParentWorkItemID)
	if err != nil {
		return fmt.Errorf("load nested source binding: %w", splitEvidenceErr(err))
	}
	if binding.ID != ancestry.Value.BindingID || binding.Digest != ancestry.Value.BindingDigest || binding.Value.Child.Value.WorkItemID != r.ParentWorkItemID {
		return fmt.Errorf("nested source binding changed: %w", ErrProductSplitHeld)
	}
	_, err = k.splitSourceLineageReadyTx(ctx, tx, binding, ancestry.Value.SourcePolicyDigest, time.Minute)
	if err != nil {
		return fmt.Errorf("nested source lineage is not ready: %w", err)
	}
	return nil
}

func splitOwnerID(r ProductSplitChildPlanRequest, child ProductSplitChildIntent) string {
	if strings.TrimSpace(child.ID) == "" || r.ProposalID == "" {
		return ""
	}
	return structureID("split-controller-", r.ProposalID, child.ID)
}
