package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

var ErrMessageTransportLimit = errors.New("message and provenance exceed the single-message wake budget")

// Validate the actual single-message envelope, including receiver context. If
// provenance makes an accepted message undeliverable, shrinking a wake batch
// cannot recover it. Roll back the message instead of leaving a poison input.
func (k *KernelStore) checkMessageTransportTx(ctx context.Context, tx *sql.Tx, m Message) error {
	if len(m.Sources) == 0 {
		return nil
	} // Preserve legacy message admission.
	id := structureID("notification-", mailboxSubscriptionID(m.RecipientSessionID, m.Target, m.TaskID), m.ID)
	n, err := notificationTx(ctx, tx, id)
	if err != nil {
		return err
	}
	if n.State == "archived" {
		return nil
	} // History only; no turn is authorized.
	b, err := sessionBindingTx(ctx, tx, m.RecipientSessionID)
	if err != nil {
		return err
	}
	state, _, err := k.sessionContextTx(ctx, tx, b, []contract.Target{m.Target})
	if err != nil {
		return err
	}
	combined, err := encodeWakeJSON(struct {
		State        SessionContext  `json:"state"`
		Supplemental json.RawMessage `json:"supplemental"`
	}{State: state})
	if err != nil {
		return err
	}
	input := wakeInput{SchemaVersion: 1, SessionID: b.ID, Targets: []contract.Target{m.Target}, Notifications: []Notification{n}, Messages: []WakeMessage{wakeMessage(m)}, Context: combined}
	raw, err := encodeWakeJSON(input)
	if err != nil {
		return err
	}
	if err = contract.ValidateSessionInput(raw); err != nil {
		return errors.Join(ErrMessageTransportLimit, err)
	}
	return nil
}
