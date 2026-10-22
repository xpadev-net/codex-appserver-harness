package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

var ErrSessionAccountChanged = errors.New("logical session authentication account changed")

type SessionAuthentication struct {
	SessionID string    `json:"session_id"`
	AccountID string    `json:"account_id"`
	BoundAt   time.Time `json:"bound_at"`
}

// BindSessionAuthentication binds identity, never credentials, to the logical
// session across native process/thread generations. Only its current managed
// launch can initialize the binding; subsequent launches must match it.
func (k *KernelStore) BindSessionAuthentication(ctx context.Context, expected contract.SessionBinding, account string) error {
	if strings.TrimSpace(account) == "" || len(account) > 1024 || !utf8.ValidString(account) {
		return errors.New("bounded authentication account required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	binding, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(binding, expected) || binding.PreparationID == "" {
		return errors.New("authentication session preparation changed")
	}
	var owner string
	if err = completionRecord(ctx, tx, "managed_preparation", binding.PreparationID, &owner); err != nil {
		return err
	}
	managed, err := managedProcessTx(ctx, tx, owner)
	if err != nil {
		return err
	}
	if managed.PreparationID != binding.PreparationID || managed.Request.Binding.ID != binding.ID || managed.State != "launching" {
		return errors.New("authentication requires the current managed launch")
	}
	var prior SessionAuthentication
	err = completionRecord(ctx, tx, "session_authentication", binding.ID, &prior)
	if err == nil {
		if prior.SessionID != binding.ID || prior.AccountID != account || prior.BoundAt.IsZero() {
			return ErrSessionAccountChanged
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	record := SessionAuthentication{SessionID: binding.ID, AccountID: account, BoundAt: k.clock()}
	if err = kernelRecordTx(ctx, tx, "session_authentication", binding.ID, binding.ID, record); err != nil {
		return err
	}
	return tx.Commit()
}
