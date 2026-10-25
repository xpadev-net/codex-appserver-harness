package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type SessionToolProfile struct {
	SessionID  string    `json:"session_id"`
	Generation int       `json:"generation"`
	Profile    string    `json:"profile"`
	CreatedAt  time.Time `json:"created_at"`
}

// PinSessionTools records the host-selected thread tool configuration. It is
// not a grant: every call still passes the common kernel gateway. Old threads
// without a profile can only resume with the original kernel tool set.
func (k *KernelStore) PinSessionTools(ctx context.Context, expected contract.SessionBinding, profile string) error {
	if profile != "kernel_v1" && profile != "product_v1" && profile != "product_v2" && profile != "product_v3" && profile != "product_v4" && profile != "product_v5" {
		return errors.New("known session tool profile required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b, expected) {
		return errors.New("session changed before tool profile selection")
	}
	if err = sessionDispatchableTx(ctx, tx, b); err != nil {
		return err
	}
	id := structureID("session-tools-", b.ID, fmt.Sprint(b.Generation))
	var saved SessionToolProfile
	err = completionRecord(ctx, tx, "session_tool_profile", id, &saved)
	if err == nil {
		if saved.SessionID != b.ID || saved.Generation != b.Generation || saved.Profile != profile || saved.CreatedAt.IsZero() {
			return errors.New("thread tool profile differs; reconcile and rotate explicitly before changing tools")
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if b.ThreadID != "" && profile != "kernel_v1" {
		return errors.New("existing thread has no proven product tool profile; reconcile and rotate explicitly")
	}
	saved = SessionToolProfile{SessionID: b.ID, Generation: b.Generation, Profile: profile, CreatedAt: k.clock()}
	if err = kernelRecordTx(ctx, tx, "session_tool_profile", id, b.ID, saved); err != nil {
		return err
	}
	return tx.Commit()
}
