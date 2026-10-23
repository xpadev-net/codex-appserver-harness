package storage

import (
	"context"
	"errors"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type PRObservationTarget struct {
	Resource PRResourceBinding `json:"resource"`
	Product  ProductTarget     `json:"product"`
}
type PRObservationTargetPage struct {
	Targets   []PRObservationTarget `json:"targets"`
	NextAfter string                `json:"next_after"`
}

// Read targets keep observing admitted resources after pause/cancel or a new
// plan. They do not reauthorize publication, candidate adoption, or approval.
func (k *KernelStore) PRObservationTargets(ctx context.Context, after string, limit int) (PRObservationTargetPage, error) {
	page := PRObservationTargetPage{Targets: []PRObservationTarget{}}
	if limit < 1 || limit > 128 {
		return page, errors.New("bounded PR observation page required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return page, err
	}
	defer tx.Rollback()
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM records WHERE kind='product_pr_resource' AND id>? ORDER BY id LIMIT ?", after, limit+1)
	if err != nil {
		return page, err
	}
	for _, id := range ids {
		if len(page.Targets) == limit {
			page.NextAfter = page.Targets[len(page.Targets)-1].Resource.ResourceKey
			break
		}
		var binding PRResourceBinding
		if err = completionRecord(ctx, tx, "product_pr_resource", id, &binding); err != nil {
			return page, err
		}
		var p ProductTargetSnapshot
		if err = completionRecord(ctx, tx, "product_target", productTargetID(contract.Target{WaveID: binding.WaveID}, binding.WorkItemID), &p); err != nil {
			return page, err
		}
		digest, err := contentDigest(p.Value)
		if err != nil {
			return page, err
		}
		if err = p.Value.validate(); err != nil {
			return page, err
		}
		if binding.ResourceKey != id || p.Digest != digest || p.Value.Target.WaveID != binding.WaveID || p.Value.WorkItem.ID != binding.WorkItemID || p.Value.WorkItem.RepositoryID != binding.RepositoryID {
			return page, errors.New("PR observation source identity conflict")
		}
		// Header observation is pinned to the admitted PR, even if a later
		// canonical projection changed the branch or has not attached its number.
		p.Value.PRNumber = binding.Number
		p.Value.WorkItem.Branch = binding.HeadBranch
		page.Targets = append(page.Targets, PRObservationTarget{Resource: binding, Product: p.Value})
	}
	return page, tx.Commit()
}
