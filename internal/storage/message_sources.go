package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// MessageSource is derived by the controller from the sender's actual run input.
// A model cannot supply these facts, sender IDs, or privileges. Repeated relays
// and replies keep the original observation rather than claiming new authority.
type MessageSource struct {
	ResourceKey    string          `json:"resource_key"`
	Target         contract.Target `json:"target"`
	SessionID      string          `json:"session_id"`
	RunID          string          `json:"run_id"`
	NotificationID string          `json:"notification_id"`
	EventID        string          `json:"event_id"`
	ObservedVia    string          `json:"observed_via"`
	Digest         string          `json:"digest"`
	DeliveryID     string          `json:"delivery_id,omitempty"`
	ScanID         string          `json:"scan_id,omitempty"`
	HeadSHA        string          `json:"head_sha"`
	SubjectSHA     string          `json:"subject_sha"`
	ActorID        int64           `json:"actor_id,omitempty"`
}

func mergeMessageSources(groups ...[]MessageSource) ([]MessageSource, error) {
	byDigest := map[string]MessageSource{}
	for _, sources := range groups {
		for _, s := range sources {
			digest, err := contentDigest(s)
			if err != nil {
				return nil, err
			}
			byDigest[digest] = s
			if len(byDigest) > 64 {
				return nil, errors.New("external message provenance exceeds bounded source budget")
			}
		}
	}
	keys := make([]string, 0, len(byDigest))
	for key := range byDigest {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var sources []MessageSource
	for _, key := range keys {
		sources = append(sources, byDigest[key])
	}
	return sources, nil
}

func (k *KernelStore) messageInputSourcesTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, runID string, target contract.Target) ([]MessageSource, error) {
	return k.runInputSourcesTx(ctx, tx, b, runID, target, true)
}

// Direct sources are proven by an external notification in this exact input.
// Relayed provenance is descriptive metadata and never grants body access.
func (k *KernelStore) directInputSourcesTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, runID string, target contract.Target) ([]MessageSource, error) {
	return k.runInputSourcesTx(ctx, tx, b, runID, target, false)
}

func (k *KernelStore) runInputSourcesTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, runID string, target contract.Target, includeRelayed bool) ([]MessageSource, error) {
	if runID == "" {
		return nil, nil
	} // Trusted operator messages have no model input.
	var digest string
	var size int
	var input []byte
	if err := tx.QueryRowContext(ctx, `SELECT s.digest,length(s.data),substr(s.data,1,?) FROM kernel_run_bindings r JOIN kernel_snapshots s ON s.id=r.snapshot_id WHERE r.run_id=? AND r.session_id=?`, contract.MaxSessionInput+1, runID, b.ID).Scan(&digest, &size, &input); err != nil {
		return nil, err
	}
	if size > contract.MaxSessionInput || size != len(input) || digest != fmt.Sprintf("%x", sha256.Sum256(input)) {
		return nil, errors.New("message source input snapshot integrity conflict")
	}
	var envelope struct {
		SessionID     string         `json:"session_id"`
		Notifications []Notification `json:"notifications"`
	}
	if err := json.Unmarshal(input, &envelope); err != nil {
		return nil, err
	}
	if len(envelope.Notifications) == 0 {
		return nil, nil
	} // Legacy/controller inputs have no wake sources.
	if envelope.SessionID != b.ID || len(envelope.Notifications) > 128 {
		return nil, errors.New("external message input exceeds notification bound")
	}
	var sources []MessageSource
	for _, saved := range envelope.Notifications {
		if saved.Kind != "external_update" && saved.Kind != "message" {
			continue
		}
		id := saved.ID
		n, err := notificationTx(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if n.SessionID != b.ID || n.RunID != runID || n.State != "bound_to_run" || n.Kind != saved.Kind || n.Target != saved.Target || n.SourceTarget != saved.SourceTarget || n.EventID != saved.EventID || n.SourceEventID != saved.SourceEventID || n.MessageID != saved.MessageID || n.Detail != saved.Detail {
			return nil, errors.New("message source notification owner conflict")
		}
		if b.Kind == "task" && n.Target != target {
			return nil, errors.New("Task message source is outside its input scope")
		}
		if n.Kind == "message" {
			if !includeRelayed {
				continue
			}
			prior, err := loadMessage(ctx, tx, n.MessageID)
			if err != nil {
				return nil, err
			}
			if prior.RecipientSessionID != b.ID || prior.Target != n.Target {
				return nil, errors.New("message source conversation conflict")
			}
			sources, err = mergeMessageSources(sources, prior.Sources)
			if err != nil {
				return nil, err
			}
			continue
		}
		var kind string
		var payload []byte
		if err = tx.QueryRowContext(ctx, "SELECT kind,payload FROM kernel_events WHERE id=? AND wave_id=?", n.SourceEventID, n.Target.WaveID).Scan(&kind, &payload); err != nil {
			return nil, err
		}
		var intent notificationIntent
		if err = completionRecord(ctx, tx, "notification_intent", n.SourceEventID, &intent); err != nil {
			return nil, err
		}
		if intent.Target != n.SourceTarget || intent.EventID != n.SourceEventID || intent.Kind != kind {
			return nil, errors.New("message source event revision conflict")
		}
		s := MessageSource{Target: n.SourceTarget, SessionID: b.ID, RunID: runID, NotificationID: id, EventID: n.SourceEventID, ObservedVia: n.ObservedVia}
		switch kind {
		case "product_webhook_observed":
			var o PRWebhookObservation
			if err = json.Unmarshal(payload, &o); err != nil {
				return nil, err
			}
			if o.SubscriptionID != n.SubscriptionID || n.ObservedVia != "webhook" {
				return nil, errors.New("message webhook source conflict")
			}
			var presented PRWebhookObservation
			if err = json.Unmarshal([]byte(saved.Detail), &presented); err != nil {
				return nil, err
			}
			if presented != o || n.SourceEventID != structureID("pr-webhook-event-", o.SubscriptionID, o.DeliveryID) {
				return nil, errors.New("message webhook differs from presented notification")
			}
			var delivery PRRawDelivery
			if err = completionRecord(ctx, tx, "product_pr_raw_delivery", o.DeliveryID, &delivery); err != nil {
				return nil, err
			}
			digest, err := rawDeliveryDigest(delivery.ID, delivery.Event, delivery.RepositoryID, delivery.Payload)
			if err != nil {
				return nil, err
			}
			if digest != o.Digest || digest != delivery.Digest || delivery.Sequence != o.Sequence {
				return nil, errors.New("message webhook digest conflict")
			}
			var binding PRResourceBinding
			if err = completionRecord(ctx, tx, "product_pr_resource", o.ResourceKey, &binding); err != nil {
				return nil, err
			}
			if binding.ResourceKey != o.ResourceKey || binding.WaveID != n.Target.WaveID || binding.RepositoryID != delivery.RepositoryID {
				return nil, errors.New("message webhook resource conflict")
			}
			expected, match := webhookPRObservation(delivery, binding.Number)
			expected.SubscriptionID, expected.ResourceKey = o.SubscriptionID, o.ResourceKey
			if !match || expected != o {
				return nil, errors.New("message webhook provenance differs from signed bytes")
			}
			s.ResourceKey, s.Digest, s.DeliveryID, s.HeadSHA, s.SubjectSHA, s.ActorID = o.ResourceKey, o.Digest, o.DeliveryID, o.HeadSHA, o.SubjectSHA, o.ActorID
		case "product_resource_updated":
			var ref struct {
				ScanID string `json:"scan_id"`
			}
			if err = json.Unmarshal(payload, &ref); err != nil {
				return nil, err
			}
			var receipt PRResourceUpdateReceipt
			if err = completionRecord(ctx, tx, "product_pr_scan", ref.ScanID, &receipt); err != nil {
				return nil, err
			}
			var presented struct {
				ScanID      string `json:"scan_id"`
				ResourceKey string `json:"resource_key"`
				Digest      string `json:"snapshot_digest"`
				Previous    string `json:"previous_digest"`
			}
			if err = json.Unmarshal([]byte(saved.Detail), &presented); err != nil {
				return nil, err
			}
			if receipt.ScanID != ref.ScanID || presented.ScanID != ref.ScanID || presented.ResourceKey != receipt.ResourceKey || presented.Digest != receipt.Digest || presented.Previous != receipt.PreviousDigest || n.SourceEventID != structureID("pr-resource-event-", receipt.ResourceKey, receipt.ScanID) {
				return nil, errors.New("message scan differs from presented notification")
			}
			var version PRResourceState
			if err = completionRecord(ctx, tx, "product_pr_state_version", structureID("pr-state-", receipt.ResourceKey, receipt.Digest), &version); err != nil {
				return nil, err
			}
			digest, err := resourceFactDigest(version.HeadSHA, version.Sections)
			if err != nil {
				return nil, err
			}
			if digest != receipt.Digest || version.Resource.WaveID != n.Target.WaveID || version.Resource.ResourceKey != receipt.ResourceKey || n.ObservedVia != "resource_scan" {
				return nil, errors.New("message snapshot source conflict")
			}
			s.ResourceKey, s.Digest, s.ScanID, s.HeadSHA = receipt.ResourceKey, receipt.Digest, receipt.ScanID, version.HeadSHA
		default:
			return nil, errors.New("unsupported external message source")
		}
		sources, err = mergeMessageSources(sources, []MessageSource{s})
		if err != nil {
			return nil, err
		}
	}
	return sources, nil
}
