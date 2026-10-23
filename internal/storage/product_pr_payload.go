package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"unicode/utf8"
)

// Publication text is controller-derived, never a PR tool argument. Keep the
// exact packet for response-loss reconciliation across config/scope changes.
type PRPublicationPayload struct {
	OperationID   string `json:"operation_id"`
	ProductDigest string `json:"product_digest"`
	Title         string `json:"title"`
	Body          string `json:"body"`
}

func (k *KernelStore) SavePRPublicationPayload(ctx context.Context, id, owner, title, body string) (PRPublicationPayload, error) {
	var packet PRPublicationPayload
	if strings.TrimSpace(title) == "" || utf8.RuneCountInString(title) > 256 || strings.ContainsAny(title, "\x00\r\n") || !utf8.ValidString(title) || len(body) > 64<<10 || !utf8.ValidString(body) {
		return packet, errors.New("bounded publication text required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return packet, err
	}
	defer tx.Rollback()
	intent, state, err := prIntentTx(ctx, tx, id)
	if err != nil {
		return packet, err
	}
	if state != "accepted" || intent.Source.Name != "pr_publish" {
		return packet, ErrPRDispatchHeld
	}
	var claim PRDispatchClaim
	if err = completionRecord(ctx, tx, "product_pr_dispatch", id, &claim); err != nil {
		return packet, err
	}
	if claim.State != "prepared" || claim.OwnerID != owner {
		return packet, ErrPRDispatchHeld
	}
	if _, _, err = k.preflightPRTx(ctx, tx, intent); err != nil {
		return packet, err
	}
	if err = validatePRClaimTx(ctx, tx, intent, claim); err != nil {
		return packet, err
	}
	packet = PRPublicationPayload{OperationID: id, ProductDigest: claim.Product.Digest, Title: title, Body: body}
	var old PRPublicationPayload
	err = completionRecord(ctx, tx, "product_pr_publication_payload", id, &old)
	if err == nil && !reflect.DeepEqual(old, packet) {
		return packet, errors.New("publication payload is immutable")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return packet, err
	}
	if err = kernelRecordTx(ctx, tx, "product_pr_publication_payload", id, intent.Source.SessionID, packet); err != nil {
		return packet, err
	}
	return packet, tx.Commit()
}
