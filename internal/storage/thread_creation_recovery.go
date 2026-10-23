package storage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type ThreadCreationResolutionRequest struct {
	Key      string                  `json:"key"`
	Action   string                  `json:"action"`
	Expected contract.SessionBinding `json:"expected"`
	Snapshot *contract.InputSnapshot `json:"snapshot,omitempty"`
	Reason   string                  `json:"reason"`
}
type ThreadCreationResolution struct {
	ID            string                  `json:"id"`
	RequestDigest string                  `json:"request_digest"`
	Action        string                  `json:"action"`
	Before        contract.SessionBinding `json:"before"`
	After         contract.SessionBinding `json:"after"`
	SnapshotID    string                  `json:"snapshot_id,omitempty"`
	Reason        string                  `json:"reason"`
	CreatedAt     time.Time               `json:"created_at"`
}

// ResolveThreadCreation is an audited operator disposition, never a model tool.
// Receipt, generation/retirement and old creation evidence share one transaction.
func (k *KernelStore) ResolveThreadCreation(ctx context.Context, r ThreadCreationResolutionRequest) (ThreadCreationResolution, error) {
	var out ThreadCreationResolution
	if strings.TrimSpace(r.Key) == "" || len(r.Key) > 256 || !utf8.ValidString(r.Key) || strings.TrimSpace(r.Reason) == "" || len(r.Reason) > contract.MaxInline || !utf8.ValidString(r.Reason) {
		return out, errors.New("bounded thread recovery key and reason required")
	}
	if r.Action != "recover_main" && r.Action != "retire_task" || r.Action == "recover_main" && r.Snapshot == nil || r.Action == "retire_task" && r.Snapshot != nil {
		return out, errors.New("explicit main recovery snapshot or Task retirement required")
	}
	digest, err := contentDigest(r)
	if err != nil {
		return out, err
	}
	id := structureID("thread-creation-resolution-", r.Expected.ID, r.Key)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	err = completionRecord(ctx, tx, "thread_creation_resolution", id, &out)
	if err == nil {
		if out.ID != id || out.RequestDigest != digest {
			return out, errors.New("thread recovery replay conflict")
		}
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	var unresolved int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='managed_process' AND json_extract(payload,'$.request.binding.id')=? AND json_extract(payload,'$.state')<>'stopped'", r.Expected.ID).Scan(&unresolved); err != nil {
		return out, err
	}
	if unresolved != 0 {
		return out, ErrManagedProcessHeld
	}
	switch r.Action {
	case "recover_main":
		if _, err = k.rotationSnapshotScopeTx(ctx, tx, r.Expected, *r.Snapshot, false); err != nil {
			return out, err
		}
		if err = k.recoverUnboundMainThreadTx(ctx, tx, r.Expected, *r.Snapshot, r.Reason); err != nil {
			return out, err
		}
	case "retire_task":
		if err = k.retireUnboundTaskSessionTx(ctx, tx, r.Expected.ID, &r.Expected, r.Reason); err != nil {
			return out, err
		}
	}
	next, err := sessionBindingTx(ctx, tx, r.Expected.ID)
	if err != nil {
		return out, err
	}
	out = ThreadCreationResolution{ID: id, RequestDigest: digest, Action: r.Action, Before: r.Expected, After: next, Reason: r.Reason, CreatedAt: k.clock()}
	if r.Snapshot != nil {
		out.SnapshotID = r.Snapshot.ID
	}
	if err = kernelRecordTx(ctx, tx, "thread_creation_resolution", id, r.Expected.ID, out); err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return ThreadCreationResolution{}, err
	}
	return out, nil
}
