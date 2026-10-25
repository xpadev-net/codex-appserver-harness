package storage

import (
	"context"
	"database/sql"
	"reflect"
	"time"
)

type ProductSplitCreateGate struct {
	AncestorSourcesVersion int                                   `json:"ancestor_sources_version,omitempty"`
	AncestorSources        []ProductSplitAncestorSourceReference `json:"ancestor_sources,omitempty"`
	Ancestry               *ProductNestedSplitAllocation         `json:"ancestry,omitempty"`
	ParentSourceID         string                                `json:"parent_source_id,omitempty"`
	ParentSourceDigest     string                                `json:"parent_source_digest,omitempty"`
	CreationID             string                                `json:"creation_id"`
	CreationDigest         string                                `json:"creation_digest"`
	Request                ProductSplitChildPlanRequest          `json:"request"`
	ParentHeadID           string                                `json:"parent_head_id"`
	SourceID               string                                `json:"source_id"`
	SourceDigest           string                                `json:"source_digest"`
	ObservedAt             time.Time                             `json:"observed_at"`
}
type splitCreateGateRecord struct {
	Value  ProductSplitCreateGate `json:"value"`
	Digest string                 `json:"digest"`
}

func loadSplitCreateGate(ctx context.Context, tx *sql.Tx, job ProductSplitCreate) (splitCreateGateRecord, error) {
	var head, history splitCreateGateRecord
	if err := taskEvidenceRecord(ctx, tx, "product_split_create_gate", job.ID, &head); err != nil {
		return head, splitEvidenceErr(err)
	}
	digest, err := contentDigest(head.Value)
	if err != nil {
		return head, err
	}
	if digest != head.Digest || head.Value.CreationID != job.ID || head.Value.CreationDigest != job.Digest || head.Value.Request != job.Value.Request || head.Value.ParentHeadID != job.Value.ParentHeadID || head.Value.ObservedAt.Before(job.Value.CreatedAt) {
		return head, ErrProductSplitHeld
	}
	if !reflect.DeepEqual(head.Value.Ancestry, job.Value.Ancestry) || job.Value.Ancestry == nil && (head.Value.ParentSourceID != "" || head.Value.ParentSourceDigest != "") || job.Value.Ancestry != nil && (head.Value.ParentSourceID == "" || !validSourceDigest(head.Value.ParentSourceDigest)) {
		return head, ErrProductSplitHeld
	}
	if err := taskEvidenceRecord(ctx, tx, "product_split_create_gate_history", head.Digest, &history); err != nil {
		return head, splitEvidenceErr(err)
	}
	if !reflect.DeepEqual(head, history) {
		return head, ErrProductSplitHeld
	}
	if err := validateNestedSplitCreationGateSource(ctx, tx, job, head.Value); err != nil {
		return head, err
	}
	return head, nil
}

// Trusted host only, after a fresh source GET (or the saved direct request).
// The source observation is content-deduplicated; its original ObservedAt is
// not the time of the latest GET. This separate bounded gate records that read.
// It is not a model tool and never grants quality or parent retirement.
func (k *KernelStore) ObserveProductSplitCreateGate(ctx context.Context, id, owner, digest, sourceID, sourceDigest string) (ProductSplitCreateGate, error) {
	var empty ProductSplitCreateGate
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	job, err := loadSplitCreate(ctx, tx, id)
	if err != nil {
		return empty, err
	}
	if job.Value.OwnerID != owner || job.Digest != digest || job.State != "prepared" {
		return empty, ErrProductSplitHeld
	}
	if err := k.splitCreatePreflightTx(ctx, tx, job.Value.Request); err != nil {
		return empty, err
	}
	w, err := loadWave(ctx, tx, job.Value.Request.Target.WaveID)
	if err != nil {
		return empty, err
	}
	if w.Source == "direct" {
		if sourceID != "" || sourceDigest != "" {
			return empty, ErrProductSplitHeld
		}
	} else {
		snapshot, err := loadProductSourceSnapshot(ctx, tx, w.ID)
		if err != nil {
			return empty, splitEvidenceErr(err)
		}
		if sourceID != snapshot.ID || sourceDigest != snapshot.Digest {
			return empty, ErrProductSplitHeld
		}
	}
	// A lost authoritative head must be restored from trusted evidence offline.
	var current splitCreateGateRecord
	err = taskEvidenceRecord(ctx, tx, "product_split_create_gate", job.ID, &current)
	if err == nil {
		if _, err := loadSplitCreateGate(ctx, tx, job); err != nil {
			return empty, err
		}
	} else if err == sql.ErrNoRows {
		var known bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_create_gate_history' AND work_item_id=?)", job.ID).Scan(&known); err != nil {
			return empty, err
		}
		if known {
			return empty, ErrProductSplitHeld
		}
	} else {
		return empty, splitEvidenceErr(err)
	}
	value := ProductSplitCreateGate{Ancestry: job.Value.Ancestry, CreationID: job.ID, CreationDigest: job.Digest, Request: job.Value.Request, ParentHeadID: job.Value.ParentHeadID, SourceID: sourceID, SourceDigest: sourceDigest, ObservedAt: k.clock()}
	if job.Value.Ancestry != nil {
		source, ancestors, err := k.nestedSplitCreationSourceTx(ctx, tx, job)
		if err != nil {
			return empty, err
		}
		value.ParentSourceID, value.ParentSourceDigest = source.ID, source.Digest
		value.AncestorSources = ancestors
		if len(ancestors) != 0 {
			value.AncestorSourcesVersion = 1
		}
	}
	checksum, err := contentDigest(value)
	if err != nil {
		return empty, err
	}
	record := splitCreateGateRecord{value, checksum}
	if err := kernelRecordTx(ctx, tx, "product_split_create_gate", job.ID, w.ID, record); err != nil {
		return empty, err
	}
	if err := kernelRecordTx(ctx, tx, "product_split_create_gate_history", checksum, job.ID, record); err != nil {
		return empty, err
	}
	return value, tx.Commit()
}
func (k *KernelStore) splitCreateSendGateTx(ctx context.Context, tx *sql.Tx, job ProductSplitCreate) (string, error) {
	gate, err := loadSplitCreateGate(ctx, tx, job)
	if err != nil {
		return "", err
	}
	now := k.clock()
	if gate.Value.ObservedAt.After(now) || now.Sub(gate.Value.ObservedAt) > 5*time.Second {
		return "", ErrProductSplitHeld
	}
	w, err := loadWave(ctx, tx, job.Value.Request.Target.WaveID)
	if err != nil {
		return "", err
	}
	if w.Source == "direct" {
		if gate.Value.SourceID != "" || gate.Value.SourceDigest != "" {
			return "", ErrProductSplitHeld
		}
	} else {
		snapshot, err := loadProductSourceSnapshot(ctx, tx, w.ID)
		if err != nil {
			return "", splitEvidenceErr(err)
		}
		if snapshot.ID != gate.Value.SourceID || snapshot.Digest != gate.Value.SourceDigest {
			return "", ErrProductSplitHeld
		}
	}
	if job.Value.Ancestry != nil {
		source, ancestors, err := k.nestedSplitCreationSourceTx(ctx, tx, job)
		if err != nil {
			return "", err
		}
		if source.ID != gate.Value.ParentSourceID || source.Digest != gate.Value.ParentSourceDigest || !reflect.DeepEqual(ancestors, gate.Value.AncestorSources) {
			return "", ErrProductSplitHeld
		}
	}
	return gate.Digest, nil
}

func validateSplitCreatePinnedGate(ctx context.Context, tx *sql.Tx, job ProductSplitCreate) error {
	if job.HTTP == nil {
		return nil
	}
	gate, err := loadSplitCreateGate(ctx, tx, job)
	if err != nil {
		return err
	}
	if gate.Digest != job.HTTP.GateDigest || job.HTTP.ActivatedAt.Before(gate.Value.ObservedAt) || job.HTTP.ActivatedAt.Sub(gate.Value.ObservedAt) > 5*time.Second {
		return ErrProductSplitHeld
	}
	return nil
}
