package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func prAwaitKey(watch PRSubscriptionIntent) string {
	if watch.OperationID != "" {
		return watch.OperationID
	}
	return watch.ID
}

// An assignment is a read subscription, not a fabricated model/tool operation.
// Both assignment and operation subscriptions retain their real source owner.
func prWatchWorkItemTx(ctx context.Context, tx *sql.Tx, watch PRSubscriptionIntent) (string, error) {
	switch watch.SourceKind {
	case "", "operation":
		intent, _, err := prIntentTx(ctx, tx, watch.OperationID)
		if err != nil {
			return "", err
		}
		if intent.Source.SessionID != watch.SessionID || intent.Source.Target != watch.Target || intent.SubscriptionID != watch.ID || intent.TaskID != watch.TaskID || (watch.WorkItemID != "" && watch.WorkItemID != intent.Candidate.WorkItemID) {
			return "", errors.New("PR operation subscriber source conflict")
		}
		return intent.Candidate.WorkItemID, nil
	case "assignment":
		if watch.OperationID != "" || watch.WorkItemID == "" || watch.ID != structureID("product-assignment-sub-", watch.TaskID, watch.WorkItemID) {
			return "", errors.New("invalid assignment subscriber identity")
		}
		task, err := loadTask(ctx, tx, watch.TaskID)
		if err != nil {
			return "", err
		}
		w, err := loadWave(ctx, tx, watch.Target.WaveID)
		if err != nil {
			return "", err
		}
		if task.SessionID != watch.SessionID || task.Target() != watch.Target || task.ParentSessionID != w.OrchestratorSessionID || !slices.Contains(task.Spec.WorkItemIDs, watch.WorkItemID) {
			return "", errors.New("PR subscriber is outside Task assignment")
		}
		b, err := sessionBindingTx(ctx, tx, watch.SessionID)
		if err != nil {
			return "", err
		}
		if b.Kind != "task" || b.TaskID != task.ID || b.Target != task.Target() || b.ParentSessionID != task.ParentSessionID {
			return "", errors.New("assignment session binding conflict")
		}
		return watch.WorkItemID, nil
	default:
		return "", errors.New("unknown PR subscriber source")
	}
}

func prWatchResourceTx(ctx context.Context, tx *sql.Tx, watch PRSubscriptionIntent) (PRResourceBinding, error) {
	var binding PRResourceBinding
	work, err := prWatchWorkItemTx(ctx, tx, watch)
	if err != nil {
		return binding, err
	}
	if err = completionRecord(ctx, tx, "product_pr_resource", watch.ResourceKey, &binding); err != nil {
		return binding, err
	}
	if binding.ResourceKey != watch.ResourceKey || binding.WaveID != watch.Target.WaveID || binding.WorkItemID != work || binding.RepositoryID != watch.RepositoryID || binding.Number != watch.PRNumber {
		return binding, errors.New("PR subscriber resource ownership conflict")
	}
	return binding, nil
}

func (k *KernelStore) ensureTaskPRSubscriptionsTx(ctx context.Context, tx *sql.Tx, w domain.Wave, task domain.Task) error {
	if w.Policy.Stage != "P" || !w.Policy.Product {
		return nil
	}
	for _, work := range task.Spec.WorkItemIDs {
		id := structureID("product-assignment-sub-", task.ID, work)
		var watch PRSubscriptionIntent
		err := completionRecord(ctx, tx, "product_pr_subscription_intent", id, &watch)
		if err == nil {
			if _, err = prWatchWorkItemTx(ctx, tx, watch); err != nil {
				return err
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			watch = PRSubscriptionIntent{ID: id, SourceKind: "assignment", WorkItemID: work, SessionID: task.SessionID, TaskID: task.ID, Target: task.Target(), State: "pending", CreatedAt: k.clock()}
			if err = kernelRecordTx(ctx, tx, "product_pr_subscription_intent", id, task.SessionID, watch); err != nil {
				return err
			}
		} else {
			return err
		}
		var p ProductTargetSnapshot
		err = completionRecord(ctx, tx, "product_target", productTargetID(w.Target(), work), &p)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			if p.Value.Target.WaveID != w.ID || p.Value.WorkItem.ID != work {
				return errors.New("assigned product belongs to another Wave")
			}
			if watch.RepositoryID != 0 && watch.RepositoryID != p.Value.WorkItem.RepositoryID {
				return errors.New("assignment repository changed")
			}
			watch.RepositoryID = p.Value.WorkItem.RepositoryID
		}
		if err = savePRReadSubscriptionTx(ctx, tx, watch); err != nil {
			return err
		}
		var binding PRResourceBinding
		err = completionRecord(ctx, tx, "product_work_item_pr", work, &binding)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if binding.WaveID != w.ID || binding.WorkItemID != work || (watch.RepositoryID != 0 && binding.RepositoryID != watch.RepositoryID) {
			return errors.New("assignment PR ownership conflict")
		}
		watch.RepositoryID = binding.RepositoryID
		if err = bindPRReceiveSubscriptionTx(ctx, tx, &watch, binding); err != nil {
			return err
		}
	}
	return nil
}

func savePRReadSubscriptionTx(ctx context.Context, tx *sql.Tx, watch PRSubscriptionIntent) error {
	resource := "kernel:product-pending:" + watch.WorkItemID
	if watch.ResourceKey != "" {
		resource = "external:" + watch.ResourceKey
	}
	raw, err := json.Marshal(watch)
	if err != nil {
		return err
	}
	var oldSession, oldWave string
	var oldScope, oldPlan int
	err = tx.QueryRowContext(ctx, "SELECT session_id,wave_id,scope_revision,plan_revision FROM kernel_subscriptions WHERE id=?", watch.ID).Scan(&oldSession, &oldWave, &oldScope, &oldPlan)
	if err == nil && (oldSession != watch.SessionID || oldWave != watch.Target.WaveID || oldScope != watch.Target.ScopeRevision || oldPlan != watch.Target.PlanRevision) {
		return errors.New("PR read subscription identity conflict")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = kernelRecordTx(ctx, tx, "product_pr_subscription_intent", watch.ID, watch.SessionID, watch); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO kernel_subscriptions(id,session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload) VALUES(?,?,?,?,?,?,0,?) ON CONFLICT(id) DO UPDATE SET resource=excluded.resource,payload=excluded.payload`, watch.ID, watch.SessionID, watch.Target.WaveID, watch.Target.ScopeRevision, watch.Target.PlanRevision, resource, raw)
	return err
}

func (k *KernelStore) refreshAssignedPRSubscriptionsTx(ctx context.Context, tx *sql.Tx, w domain.Wave) error {
	ids, err := inputIDsTx(ctx, tx, "SELECT id FROM kernel_tasks WHERE wave_id=? ORDER BY id", w.ID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		task, err := loadTask(ctx, tx, id)
		if err != nil {
			return err
		}
		if err = k.ensureTaskPRSubscriptionsTx(ctx, tx, w, task); err != nil {
			return err
		}
	}
	return nil
}
