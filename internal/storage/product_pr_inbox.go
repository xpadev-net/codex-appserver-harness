package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// PRInbox is a trusted controller port implementing github.Inbox. Only attach
// it behind the signature-verifying Webhook handler, never a model tool. It
// captures before PR binding and does not interpret bodies as authority/state.
type PRInbox struct {
	Store        *KernelStore
	RepositoryID int64
}
type PRRawDelivery struct {
	ID           string    `json:"id"`
	Event        string    `json:"event"`
	RepositoryID int64     `json:"repository_id"`
	Sequence     int64     `json:"sequence"`
	Digest       string    `json:"digest"`
	Payload      []byte    `json:"payload"`
	ReceivedAt   time.Time `json:"received_at"`
}
type PRIngressCoverage struct {
	RepositoryID     int64     `json:"repository_id"`
	FirstPersistedAt time.Time `json:"first_persisted_at"`
	FirstSequence    int64     `json:"first_sequence"`
	HistoryBefore    string    `json:"history_before"` // unavailable, never implied empty
	Coverage         string    `json:"coverage"`       // captured_deliveries_only; webhook loss needs API reconciliation
}

func rawDeliveryDigest(id, event string, repo int64, payload []byte) (string, error) {
	return contentDigest(struct {
		ID, Event    string
		RepositoryID int64
		Payload      []byte
	}{id, event, repo, payload})
}
func (in PRInbox) Receive(ctx context.Context, id, event string, payload []byte) error {
	if in.Store == nil || in.RepositoryID <= 0 || !validPRProofID(id) || strings.TrimSpace(event) == "" || len(event) > 128 || !utf8.ValidString(event) || strings.ContainsAny(event, "\x00\r\n") || len(payload) > 1<<20 || !utf8.Valid(payload) || !json.Valid(payload) {
		return errors.New("bounded authenticated PR delivery required")
	}
	var header struct {
		Repository struct {
			ID int64 `json:"id"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(payload, &header); err != nil {
		return err
	}
	if header.Repository.ID != in.RepositoryID {
		return errors.New("webhook repository differs from configured ingress")
	}
	// Retain and hash exact signed bytes. Base64 storage prevents JSON re-encoding
	// from silently changing the authenticated payload during persistence.
	digest, err := rawDeliveryDigest(id, event, in.RepositoryID, payload)
	if err != nil {
		return err
	}
	k := in.Store
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var old PRRawDelivery
	err = completionRecord(ctx, tx, "product_pr_raw_delivery", id, &old)
	if err == nil {
		oldDigest, e := rawDeliveryDigest(old.ID, old.Event, old.RepositoryID, old.Payload)
		if e != nil {
			return e
		}
		if old.ID != id || old.Event != event || old.RepositoryID != in.RepositoryID || old.Digest != digest || old.Digest != oldDigest || old.Sequence <= 0 || old.ReceivedAt.IsZero() {
			return errors.New("webhook delivery ID replay conflict")
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var sequence int64
	err = completionRecord(ctx, tx, "product_pr_ingress_cursor", "sequence", &sequence)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if sequence < 0 || sequence == math.MaxInt64 {
		return errors.New("webhook sequence exhausted or corrupt")
	}
	sequence++
	delivery := PRRawDelivery{ID: id, Event: event, RepositoryID: in.RepositoryID, Sequence: sequence, Digest: digest, Payload: append([]byte(nil), payload...), ReceivedAt: k.clock()}
	if err = kernelRecordTx(ctx, tx, "product_pr_raw_delivery", id, "", delivery); err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "product_pr_raw_sequence", prRawSequenceKey(in.RepositoryID, sequence), "", map[string]string{"delivery_id": id}); err != nil {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "product_pr_ingress_cursor", "sequence", "", sequence); err != nil {
		return err
	}
	key := structureID("pr-ingress-", "repository", strconv.FormatInt(in.RepositoryID, 10))
	var coverage PRIngressCoverage
	err = completionRecord(ctx, tx, "product_pr_ingress_coverage", key, &coverage)
	if errors.Is(err, sql.ErrNoRows) {
		coverage = PRIngressCoverage{RepositoryID: in.RepositoryID, FirstPersistedAt: delivery.ReceivedAt, FirstSequence: sequence, HistoryBefore: "unavailable", Coverage: "captured_deliveries_only"}
		if err = kernelRecordTx(ctx, tx, "product_pr_ingress_coverage", key, "", coverage); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if coverage.RepositoryID != in.RepositoryID {
		return errors.New("webhook coverage identity conflict")
	}
	return tx.Commit()
}

type PRRawDeliveryPage struct {
	Deliveries   []PRRawDelivery `json:"deliveries"`
	NextAfter    int64           `json:"next_after"`
	LastSequence int64           `json:"last_sequence"`
}

func prRawSequenceKey(repo, sequence int64) string { return fmt.Sprintf("%020d:%020d", repo, sequence) }

// PRRawDeliveries is a bounded controller read, not a worker history grant.
// The counter is explicit and survives VACUUM/restart independently of rowid.
func (k *KernelStore) PRRawDeliveries(ctx context.Context, repo, after int64, limit int) (PRRawDeliveryPage, error) {
	page := PRRawDeliveryPage{Deliveries: []PRRawDelivery{}, LastSequence: after}
	if repo <= 0 || after < 0 || limit < 1 || limit > 128 {
		return page, errors.New("bounded repository delivery cursor required")
	}
	rows, err := k.DB.QueryContext(ctx, `SELECT idx.id,d.id,d.payload FROM records idx LEFT JOIN records d ON d.kind='product_pr_raw_delivery' AND d.id=json_extract(idx.payload,'$.delivery_id') WHERE idx.kind='product_pr_raw_sequence' AND idx.id>? AND idx.id<=? ORDER BY idx.id LIMIT ?`, prRawSequenceKey(repo, after), prRawSequenceKey(repo, math.MaxInt64), limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	last := after
	total := 0
	for rows.Next() {
		var indexID, id string
		var raw []byte
		if err = rows.Scan(&indexID, &id, &raw); err != nil {
			return page, err
		}
		var delivery PRRawDelivery
		if err = json.Unmarshal(raw, &delivery); err != nil {
			return page, err
		}
		digest, e := rawDeliveryDigest(delivery.ID, delivery.Event, delivery.RepositoryID, delivery.Payload)
		if e != nil {
			return page, e
		}
		if delivery.ID != id || indexID != prRawSequenceKey(repo, delivery.Sequence) || delivery.RepositoryID != repo || delivery.Sequence <= last || delivery.Digest != digest || delivery.ReceivedAt.IsZero() {
			return page, errors.New("raw delivery identity conflict")
		}
		if len(page.Deliveries) == limit || (len(page.Deliveries) > 0 && total+len(raw) > 2<<20) {
			page.NextAfter = last
			return page, nil
		}
		page.Deliveries = append(page.Deliveries, delivery)
		last = delivery.Sequence
		page.LastSequence = last
		total += len(raw)
	}
	return page, rows.Err()
}

func (k *KernelStore) PRIngressCoverage(ctx context.Context, repo int64) (PRIngressCoverage, error) {
	var coverage PRIngressCoverage
	if repo <= 0 {
		return coverage, errors.New("repository identity required")
	}
	err := k.Record(ctx, "product_pr_ingress_coverage", structureID("pr-ingress-", "repository", strconv.FormatInt(repo, 10)), &coverage)
	if err == nil && (coverage.RepositoryID != repo || coverage.FirstPersistedAt.IsZero() || coverage.FirstSequence <= 0 || coverage.HistoryBefore != "unavailable" || coverage.Coverage != "captured_deliveries_only") {
		err = errors.New("PR ingress coverage identity conflict")
	}
	return coverage, err
}
