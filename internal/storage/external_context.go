package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// Read only the immutable observation actually presented to this active run.
// Knowing a delivery/scan ID or receiving another run's provenance is no grant.
// Bodies remain untrusted data and cannot change scope, policy or approval.
func (k *KernelStore) externalContextTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, r contract.ContextReadRequest) ([]byte, string, error) {
	var runID string
	if err := tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&runID); err != nil {
		return nil, "", err
	}
	if runID == "" {
		return nil, "", errors.New("external context requires an active run input")
	}
	sources, err := k.messageInputSourcesTx(ctx, tx, b, runID, r.Target)
	if err != nil {
		return nil, "", err
	}
	for _, source := range sources {
		if source.NotificationID != r.ReferenceID || source.SessionID != b.ID || source.RunID != runID || source.Target != r.Target {
			continue
		}
		var data []byte
		switch source.ObservedVia {
		case "webhook":
			var delivery PRRawDelivery
			if err := completionRecord(ctx, tx, "product_pr_raw_delivery", source.DeliveryID, &delivery); err != nil {
				return nil, "", err
			}
			data = delivery.Payload // Exact signed bytes; the provenance helper verified the delivery digest.
		case "resource_scan":
			var version PRResourceState
			if err := completionRecord(ctx, tx, "product_pr_state_version", structureID("pr-state-", source.ResourceKey, source.Digest), &version); err != nil {
				return nil, "", err
			}
			// Match resourceFactDigest's canonical bytes, excluding mutable scan
			// timestamps and the current state pointer. Later scans cannot relabel it.
			data, err = resourceFactBytes(version.HeadSHA, version.Sections)
			if err != nil {
				return nil, "", err
			}
		default:
			return nil, "", errors.New("unsupported external context source")
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(data))
		if source.ObservedVia == "resource_scan" && digest != source.Digest {
			return nil, "", errors.New("external snapshot bytes conflict")
		}
		return data, digest, nil
	}
	return nil, "", errors.New("external reference was not granted in this run")
}
