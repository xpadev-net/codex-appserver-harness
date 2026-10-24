package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type UnavailableFinalTarget struct {
	WaveID string `json:"wave_id"`
	Reason string `json:"reason"`
}
type ProductFinalExecutionPage struct {
	Unavailable []UnavailableFinalTarget `json:"unavailable"`
	Targets     []contract.Target        `json:"targets"`
	NextAfter   string                   `json:"next_after"`
}

func (k *KernelStore) ProductFinalExecutionTargets(ctx context.Context, after string, limit int) (ProductFinalExecutionPage, error) {
	out := ProductFinalExecutionPage{Targets: []contract.Target{}}
	if limit < 1 || limit > 128 {
		return out, errors.New("bounded final execution page required")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM kernel_waves WHERE id>? AND CASE WHEN json_valid(payload) THEN (json_type(payload,'$.policy.stage') IS NULL OR json_type(payload,'$.state') IS NULL OR (json_extract(payload,'$.policy.stage')='P' AND json_extract(payload,'$.state') IN ('running','waiting'))) ELSE 1 END ORDER BY id LIMIT ?", after, limit+1)
	if err != nil {
		return out, err
	}
	if len(ids) > limit {
		ids = ids[:limit]
		out.NextAfter = ids[len(ids)-1]
	}
	for _, id := range ids {
		w, err := loadWave(ctx, tx, id)
		if err != nil {
			var syntax *json.SyntaxError
			var shape *json.UnmarshalTypeError
			if errors.Is(err, sql.ErrNoRows) || errors.As(err, &syntax) || errors.As(err, &shape) {
				out.Unavailable = append(out.Unavailable, UnavailableFinalTarget{WaveID: id, Reason: "wave_evidence_unavailable"})
				continue
			}
			return out, err
		}
		if err := w.Validate(); err != nil {
			out.Unavailable = append(out.Unavailable, UnavailableFinalTarget{WaveID: id, Reason: "wave_evidence_invalid"})
			continue
		}
		if w.Policy.Stage != "P" || !w.Policy.Product {
			continue
		}
		if w.DispatchPaused() || !k.clock().Before(w.Deadline) {
			continue
		}
		out.Targets = append(out.Targets, w.Target())
	}
	return out, nil
}

// NotifyProductFinalVerification queues one parent judgment input from actual
// current stopped results. It neither completes the Wave nor calls a model.
func (k *KernelStore) NotifyProductFinalVerification(ctx context.Context, target contract.Target) (bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	w, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return false, err
	}
	if w.Target() != target || w.DispatchPaused() {
		return false, ErrProductFinalPlanHeld
	}
	evidence, err := k.productFinalVerificationEvidenceTx(ctx, tx, w)
	if err != nil {
		return false, err
	}
	if !evidence.Value.Current || !evidence.Value.Complete {
		return false, nil
	}
	ids, err := inputIDsTx(ctx, tx, "SELECT t.id FROM kernel_tasks t JOIN kernel_plan_bindings p ON p.task_id=t.id WHERE p.wave_id=? AND p.revision=? AND t.state='completed' ORDER BY t.id LIMIT 257", w.ID, w.PlanRevision)
	if err != nil {
		return false, err
	}
	if len(ids) > 256 {
		return false, ErrProductFinalPlanHeld
	}
	for _, id := range ids {
		task, err := loadTask(ctx, tx, id)
		if err != nil {
			return false, err
		}
		if task.Spec.Kind != "review" || len(task.Spec.WorkItemIDs) != 1 {
			continue
		}
		changed, err := k.saveParentAttentionTx(ctx, tx, w, task, "product_final_verification", evidence.Digest)
		if err != nil {
			return false, err
		}
		return changed, tx.Commit()
	}
	return false, ErrProductFinalPlanHeld
}
