package storage

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// ProductMergeGateObservation is produced only by the trusted host gate, never
// accepted from a model tool. Native review/check IDs reference the actual API
// reads; this packet is an audit binding, not an approval fabricated by storage.
type ProductMergeGateObservation struct {
	ProductDigest string          `json:"product_digest"`
	OperationID   string          `json:"operation_id"`
	RoundDigest   string          `json:"round_digest"`
	HeadSHA       string          `json:"head_sha"`
	BaseSHA       string          `json:"base_sha"`
	TestedSHA     string          `json:"tested_sha"`
	ReviewIDs     []int64         `json:"review_ids"`
	CheckIDs      []int64         `json:"check_ids"`
	ObservedAt    time.Time       `json:"observed_at"`
	Report        contract.Report `json:"report"`
}

func canonicalMergeGateObservation(v ProductMergeGateObservation) ProductMergeGateObservation {
	v.ObservedAt = v.ObservedAt.Round(0).UTC()
	v.Report.ObservedAt = v.Report.ObservedAt.Round(0).UTC()
	v.Report.Capabilities = maps.Clone(v.Report.Capabilities)
	v.ReviewIDs = slices.Clone(v.ReviewIDs)
	slices.Sort(v.ReviewIDs)
	v.CheckIDs = slices.Clone(v.CheckIDs)
	slices.Sort(v.CheckIDs)
	return v
}
func validateMergeGateObservation(job ProductMerge, v ProductMergeGateObservation) error {
	p := job.Value.Product.Value
	r := v.Report
	if v.ProductDigest != job.Value.Product.Digest || v.OperationID != job.Value.RequestID || v.RoundDigest != job.Value.RoundDigest || v.HeadSHA != p.Candidate.SHA || v.BaseSHA != p.Candidate.BaseSHA || !fixedSHA(v.TestedSHA) || v.ObservedAt.IsZero() || v.ObservedAt.Before(job.ClaimedAt) || job.ClaimedAt.IsZero() || r.Version != 1 || r.RepositoryID != p.WorkItem.RepositoryID || r.Repository != p.Repository || r.DefaultBranch != p.BaseBranch || r.ObservedAt.IsZero() || r.ObservedAt.After(v.ObservedAt) || len(r.Capabilities) > 128 || !r.AutoMergeReady() {
		return ErrProductMergeHeld
	}
	for _, name := range contract.MergeRequirements {
		if strings.TrimSpace(r.Capabilities[name].Evidence) == "" {
			return ErrProductMergeHeld
		}
	}
	for _, ids := range [][]int64{v.ReviewIDs, v.CheckIDs} {
		if len(ids) < 1 || len(ids) > 128 {
			return ErrProductMergeHeld
		}
		for i, id := range ids {
			if id < 1 || i > 0 && ids[i-1] >= id {
				return ErrProductMergeHeld
			}
		}
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return ErrProductMergeHeld
	}
	return nil
}
