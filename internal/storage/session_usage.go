package storage

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

// SessionThreadUnused proves an empty resumed history belongs to a thread
// created here which has never reserved a run. Empty history alone is not proof
// of zero usage: an imported or truncated thread remains unknown.
func (k *KernelStore) SessionThreadUnused(ctx context.Context, expected contract.SessionBinding) (bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, expected.ID)
	if err != nil {
		return false, err
	}
	if !reflect.DeepEqual(b, expected) || b.ThreadID == "" {
		return false, errors.New("unused thread binding changed")
	}
	var original contract.SessionBinding
	err = completionRecord(ctx, tx, "session_new_thread", b.ThreadID, &original)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if original.ID != b.ID || original.ThreadID != b.ThreadID || original.Generation != b.Generation || original.CWD != b.CWD || original.Model != b.Model || original.Sandbox != b.Sandbox {
		return false, errors.New("new thread proof binding conflict")
	}
	var used bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM kernel_run_bindings WHERE session_id=? AND thread_id=?)", b.ID, b.ThreadID).Scan(&used); err != nil {
		return false, err
	}
	return !used, nil
}
