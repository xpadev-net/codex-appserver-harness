package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type ProductSourceReadValue struct {
	Target         contract.Target `json:"target"`
	SourceID       string          `json:"source_id"`
	SnapshotID     string          `json:"snapshot_id"`
	SnapshotDigest string          `json:"snapshot_digest"`
	PolicyDigest   string          `json:"policy_digest"`
	ReadAt         time.Time       `json:"read_at"`
}

// This replaceable freshness header does not change semantic source history.
// It grants no quality approval, dispatch, or extension of a source's age.
type ProductSourceRead struct {
	ID     string                 `json:"id"`
	Value  ProductSourceReadValue `json:"value"`
	Digest string                 `json:"digest"`
}

type productSourceReadRequest struct {
	ExpectedID string
	At         time.Time
}

func loadProductSourceRead(ctx context.Context, tx *sql.Tx, waveID string) (ProductSourceRead, error) {
	var saved ProductSourceRead
	var first []byte
	missing := 0
	for _, kind := range []string{"product_source_read", "product_source_read_admission"} {
		var owner string
		var raw []byte
		err := tx.QueryRowContext(ctx, "SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", kind, waveID).Scan(&owner, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			missing++
			continue
		}
		if err != nil {
			return saved, err
		}
		var value ProductSourceRead
		if owner != waveID || contract.DecodeStrict(raw, &value) != nil {
			return saved, ErrTaskEvidence
		}
		if first != nil && !reflect.DeepEqual(saved, value) {
			return saved, ErrTaskEvidence
		}
		first, saved = raw, value
	}
	if missing == 2 {
		return saved, sql.ErrNoRows
	}
	if missing != 0 {
		return saved, ErrTaskEvidence
	}
	v := saved.Value
	digest, err := contentDigest(v)
	if err != nil {
		return saved, err
	}
	if saved.Digest != digest || saved.ID != structureID("product-source-read-", waveID, digest) || !v.Target.Valid() || v.Target.WaveID != waveID || v.SourceID == "" || v.SnapshotID == "" || !validSourceDigest(v.SnapshotDigest) || !validSourceDigest(v.PolicyDigest) || v.ReadAt.IsZero() {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

func preflightProductSourceRead(ctx context.Context, tx *sql.Tx, w domain.Wave, prior ProductSourceSnapshot, priorErr error, read productSourceReadRequest) error {
	if priorErr == nil && prior.ID != read.ExpectedID || errors.Is(priorErr, sql.ErrNoRows) && read.ExpectedID != "" {
		return errors.New("source observation CAS conflict")
	}
	if read.At.Before(w.CreatedAt) || priorErr == nil && read.At.Before(prior.ObservedAt) {
		return errors.New("completed source observation time changed")
	}
	previous, err := loadProductSourceRead(ctx, tx, w.ID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && read.At.Before(previous.Value.ReadAt) {
		return errors.New("source read time moved backwards")
	}
	return nil
}

func saveProductSourceRead(ctx context.Context, tx *sql.Tx, w domain.Wave, snapshot ProductSourceSnapshot, at time.Time) error {
	v := ProductSourceReadValue{Target: w.Target(), SourceID: w.SourceID, SnapshotID: snapshot.ID, SnapshotDigest: snapshot.Digest, PolicyDigest: snapshot.Value.Facts.PolicyDigest, ReadAt: at}
	digest, err := contentDigest(v)
	if err != nil {
		return err
	}
	saved := ProductSourceRead{ID: structureID("product-source-read-", w.ID, digest), Value: v, Digest: digest}
	for _, kind := range []string{"product_source_read", "product_source_read_admission"} {
		if err := kernelRecordTx(ctx, tx, kind, w.ID, w.ID, saved); err != nil {
			return err
		}
	}
	return nil
}

func readProductSourceReadTx(ctx context.Context, tx *sql.Tx, w domain.Wave) (ProductSourceRead, error) {
	saved, err := loadProductSourceRead(ctx, tx, w.ID)
	if err != nil {
		return saved, err
	}
	snapshot, err := loadProductSourceSnapshot(ctx, tx, w.ID)
	if err != nil {
		return saved, err
	}
	var history ProductSourceSnapshot
	if err := taskEvidenceRecord(ctx, tx, "product_source_observation_history", snapshot.ID, &history); err != nil {
		return saved, err
	}
	policy, err := productSourcePolicyTx(ctx, tx, w)
	if err != nil {
		return saved, err
	}
	v := saved.Value
	if !reflect.DeepEqual(snapshot, history) || v.Target != w.Target() || v.SourceID != w.SourceID || v.SnapshotID != snapshot.ID || v.SnapshotDigest != snapshot.Digest || v.PolicyDigest != snapshot.Value.Facts.PolicyDigest || v.PolicyDigest != policy || snapshot.Value.ScopeRevision != w.ScopeRevision || snapshot.Value.PolicyRevision != w.Policy.Revision || snapshot.Value.SourceID != w.SourceID || v.ReadAt.Before(snapshot.ObservedAt) || v.ReadAt.Before(w.CreatedAt) {
		return saved, ErrTaskEvidence
	}
	return saved, nil
}

// ReadProductSourceRead authenticates metadata only. Callers deciding readiness
// must additionally check current source status, controls and age after their
// complete proof walk. A failed read can have a header but is never ready.
func (k *KernelStore) ReadProductSourceRead(ctx context.Context, target contract.Target) (ProductSourceRead, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return ProductSourceRead{}, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return ProductSourceRead{}, err
	}
	if w.Target() != target || w.Source != "github" || w.Policy.Stage != "P" || !w.Policy.Product {
		return ProductSourceRead{}, ErrTaskEvidence
	}
	return readProductSourceReadTx(ctx, tx, w)
}
