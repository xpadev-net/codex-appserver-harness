package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"sort"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrProductCoverageHeld = errors.New("product merge coverage requires complete bounded current metadata")

const maxProductCoverageMerges = 1024

type ProductCoverageFact struct {
	MergeID            string          `json:"merge_id"`
	ReceiptDigest      string          `json:"receipt_digest"`
	Target             contract.Target `json:"target"`
	WorkItemID         string          `json:"work_item_id"`
	CandidateID        string          `json:"candidate_id"`
	CandidateSHA       string          `json:"candidate_sha"`
	MergeSHA           string          `json:"merge_sha"`
	RegistrationDigest string          `json:"registration_digest"`
	Criteria           []string        `json:"criteria"`
}
type ProductCoveragePending struct {
	MergeID    string `json:"merge_id,omitempty"`
	WorkItemID string `json:"work_item_id,omitempty"`
	Reason     string `json:"reason"`
}
type ProductCoverageValue struct {
	Target            contract.Target          `json:"target"`
	Source            string                   `json:"source"`
	SourceID          string                   `json:"source_id"`
	Criteria          []domain.WaveCriterion   `json:"criteria"`
	Merged            []ProductCoverageFact    `json:"merged"`
	Historical        []ProductCoverageFact    `json:"historical"`
	Residual          []string                 `json:"residual"`
	Pending           []ProductCoveragePending `json:"pending"`
	Unavailable       []ProductCoveragePending `json:"unavailable"`
	UnmergedWorkItems []string                 `json:"unmerged_work_items"`
}

// Coverage describes known merges, not parent criterion acceptance. Every fact
// retains its original plan target; only the same scope/policy/source can cover
// current criterion IDs. New planning does not rewrite the original evidence.
type ProductCoverageLedger struct {
	Value  ProductCoverageValue `json:"value"`
	Digest string               `json:"digest"`
}

func (k *KernelStore) ProductCoverage(ctx context.Context, target contract.Target) (ProductCoverageLedger, error) {
	var ledger ProductCoverageLedger
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ledger, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return ledger, err
	}
	if w.Target() != target {
		return ledger, ErrProductCoverageHeld
	}
	return k.productCoverageTx(ctx, tx, w)
}

func (k *KernelStore) productCoverageTx(ctx context.Context, tx *sql.Tx, w domain.Wave) (ProductCoverageLedger, error) {
	var ledger ProductCoverageLedger
	if w.Policy.Stage != "P" || !w.Policy.Product {
		return ledger, ErrProductCoverageHeld
	}
	ledger.Value = ProductCoverageValue{Target: w.Target(), Source: w.Source, SourceID: w.SourceID, Criteria: slices.Clone(w.Criteria), Merged: []ProductCoverageFact{}, Historical: []ProductCoverageFact{}, Residual: []string{}, Pending: []ProductCoveragePending{}, Unavailable: []ProductCoveragePending{}, UnmergedWorkItems: []string{}}
	sort.Slice(ledger.Value.Criteria, func(i, j int) bool { return ledger.Value.Criteria[i].ID < ledger.Value.Criteria[j].ID })
	workIDs, err := inputIDsTx(ctx, tx, "SELECT DISTINCT id FROM records WHERE kind IN ('product_work_item_registration','wave_work_item') AND work_item_id=? ORDER BY id LIMIT ?", w.ID, maxProductWorkItems+1)
	if err != nil {
		return ledger, err
	}
	if len(workIDs) > maxProductWorkItems {
		return ledger, ErrProductCoverageHeld
	}
	registrations := map[string]string{}
	workCriteria := map[string][]string{}
	mergedWork := map[string]bool{}
	unknownRegistration := false
	for _, id := range workIDs {
		r, err := loadProductWorkItemRegistration(ctx, tx, id)
		if ctx.Err() != nil {
			return ledger, ctx.Err()
		}
		if err != nil || !productRegistrationScope(r, w) {
			ledger.Value.Unavailable = append(ledger.Value.Unavailable, ProductCoveragePending{WorkItemID: id, Reason: "source_registration_unavailable"})
			unknownRegistration = true
			continue
		}
		registrations[id] = r.Digest
		workCriteria[id] = slices.Clone(r.Value.Definition.Criteria)
	}
	// Include independent history/admission records so a missing merge head or
	// receipt head remains visible rather than silently disappearing from input.
	ids, err := inputIDsTx(ctx, tx, `SELECT DISTINCT id FROM records WHERE work_item_id=? AND kind IN
 ('product_merge','product_merge_history','product_merge_admission','product_merge_claim','product_merge_send',
 'product_merged_work_item','product_merged_work_item_history','product_merged_work_item_admission')
 ORDER BY id LIMIT ?`, w.ID, maxProductCoverageMerges+1)
	if err != nil {
		return ledger, err
	}
	if len(ids) > maxProductCoverageMerges {
		return ledger, ErrProductCoverageHeld
	}
	known := map[string]bool{}
	covered := map[string]bool{}
	for _, c := range w.Criteria {
		known[c.ID] = true
	}
	for _, id := range ids {
		job, err := k.readProductMergeTx(ctx, tx, id)
		if ctx.Err() != nil {
			return ledger, ctx.Err()
		}
		if err != nil || job.Value.Product.Value.Target.WaveID != w.ID {
			ledger.Value.Unavailable = append(ledger.Value.Unavailable, ProductCoveragePending{MergeID: id, Reason: "merge_evidence_unavailable"})
			continue
		}
		switch job.State {
		case "prepared", "launching", "unknown", "stopped_unknown":
			ledger.Value.Pending = append(ledger.Value.Pending, ProductCoveragePending{MergeID: id, Reason: job.State})
			continue
		case "merged":
			// Continue below. Known failed/unsent results cannot cover criteria.
		default:
			continue
		}
		receipt, err := readMergedWorkItem(ctx, tx, job, k.clock())
		if err == sql.ErrNoRows {
			ledger.Value.Pending = append(ledger.Value.Pending, ProductCoveragePending{MergeID: id, Reason: "merged_receipt_not_recorded"})
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ledger, ctx.Err()
			}
			ledger.Value.Unavailable = append(ledger.Value.Unavailable, ProductCoveragePending{MergeID: id, Reason: "merged_receipt_unavailable"})
			continue
		}
		v := receipt.Value
		p := v.Product.Value
		fact := ProductCoverageFact{MergeID: id, ReceiptDigest: receipt.Digest, Target: p.Target, WorkItemID: p.WorkItem.ID, CandidateID: p.Candidate.ID, CandidateSHA: p.Candidate.SHA, MergeSHA: v.MergeSHA, RegistrationDigest: v.RegistrationDigest, Criteria: slices.Clone(v.Criteria)}
		var registration ProductWorkItemRegistrationSnapshot
		if err := taskEvidenceRecord(ctx, tx, "product_work_item_registration_version", structureID("product-work-", p.WorkItem.ID, v.RegistrationDigest), &registration); err != nil {
			if ctx.Err() != nil {
				return ledger, ctx.Err()
			}
			ledger.Value.Unavailable = append(ledger.Value.Unavailable, ProductCoveragePending{MergeID: id, WorkItemID: p.WorkItem.ID, Reason: "registration_version_unavailable"})
			continue
		}
		if p.Target.ScopeRevision != w.ScopeRevision || p.Target.PolicyRevision != w.Policy.Revision || registration.Value.Source != w.Source || registration.Value.SourceID != w.SourceID {
			ledger.Value.Historical = append(ledger.Value.Historical, fact)
			continue
		}
		if registrations[p.WorkItem.ID] != v.RegistrationDigest {
			ledger.Value.Unavailable = append(ledger.Value.Unavailable, ProductCoveragePending{MergeID: id, WorkItemID: p.WorkItem.ID, Reason: "current_registration_unavailable"})
			continue
		}
		var projection ProductTargetSnapshot
		if err := taskEvidenceRecord(ctx, tx, "product_target", productTargetID(p.Target, p.WorkItem.ID), &projection); err != nil {
			if ctx.Err() != nil {
				return ledger, ctx.Err()
			}
			ledger.Value.Unavailable = append(ledger.Value.Unavailable, ProductCoveragePending{MergeID: id, WorkItemID: p.WorkItem.ID, Reason: "current_projection_unavailable"})
			continue
		}
		projectionDigest, err := contentDigest(projection.Value)
		if err != nil || projectionDigest != projection.Digest || projection.Value.validate() != nil || projection.Value.WorkItem.ID != p.WorkItem.ID || projection.Value.WorkItem.RepositoryID != p.WorkItem.RepositoryID || projection.Value.Repository != p.Repository || projection.Value.Target.WaveID != w.ID {
			ledger.Value.Unavailable = append(ledger.Value.Unavailable, ProductCoveragePending{MergeID: id, WorkItemID: p.WorkItem.ID, Reason: "current_projection_unavailable"})
			continue
		}
		current := projection.Value
		if current.Target.ScopeRevision != w.ScopeRevision || current.Target.PolicyRevision != w.Policy.Revision || current.Candidate != p.Candidate || current.PRNumber != p.PRNumber || current.Repository != p.Repository || current.BaseBranch != p.BaseBranch || current.WorkItem.Branch != p.WorkItem.Branch || current.WorkItem.Phase != domain.Merged {
			ledger.Value.Historical = append(ledger.Value.Historical, fact)
			continue
		}
		valid := true
		for _, criterion := range fact.Criteria {
			valid = valid && known[criterion]
		}
		if !valid {
			ledger.Value.Unavailable = append(ledger.Value.Unavailable, ProductCoveragePending{MergeID: id, Reason: "criterion_scope_mismatch"})
			continue
		}
		for _, criterion := range fact.Criteria {
			covered[criterion] = true
		}
		ledger.Value.Merged = append(ledger.Value.Merged, fact)
		mergedWork[p.WorkItem.ID] = true
	}
	blockedCriteria := map[string]bool{}
	for _, id := range workIDs {
		if !mergedWork[id] {
			ledger.Value.UnmergedWorkItems = append(ledger.Value.UnmergedWorkItems, id)
			for _, criterion := range workCriteria[id] {
				blockedCriteria[criterion] = true
			}
		}
	}
	for _, c := range ledger.Value.Criteria {
		// Every registered contributor must have its own current merged fact.
		// Unknown registration content may cover any criterion; without its
		// authenticated mapping, conservatively retain the entire residual.
		if unknownRegistration || blockedCriteria[c.ID] || !covered[c.ID] {
			ledger.Value.Residual = append(ledger.Value.Residual, c.ID)
		}
	}
	if err := ctx.Err(); err != nil {
		return ledger, err
	}
	raw, err := json.Marshal(ledger.Value)
	if err != nil {
		return ledger, err
	}
	// This is a context-reference bound, not permission to truncate history or
	// silently accept incomplete coverage. Oversized metadata stays unavailable.
	if len(raw) > 1<<20 {
		return ledger, ErrProductCoverageHeld
	}
	ledger.Digest, err = contentDigest(ledger.Value)
	return ledger, err
}
