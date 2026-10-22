package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// ParentAttention is a controller observation, never a model-supplied message or
// permission to retry/complete work. The complete source Task is immutable here;
// current Task state and scope are checked again before dispatching its wake.
type ParentAttention struct {
	ID         string          `json:"id"`
	SessionID  string          `json:"session_id"`
	Target     contract.Target `json:"target"`
	Task       domain.Task     `json:"task"`
	TaskDigest string          `json:"task_digest"`
	RunID      string          `json:"run_id"`
	RunState   string          `json:"run_state"`
	Reason     string          `json:"reason"`
	Detail     string          `json:"detail"`
	CreatedAt  time.Time       `json:"created_at"`
}
type attentionSubscription struct {
	ID          string          `json:"id"`
	SessionID   string          `json:"session_id"`
	Target      contract.Target `json:"target"`
	AttentionID string          `json:"attention_id"`
}

func parentAttentionID(a ParentAttention) string {
	digest, _ := contentDigest(struct {
		Target                                      contract.Target
		TaskDigest, RunID, RunState, Reason, Detail string
	}{a.Target, a.TaskDigest, a.RunID, a.RunState, a.Reason, a.Detail})
	return "attention-" + digest
}
func parentAttentionTx(ctx context.Context, q queryer, id string) (ParentAttention, error) {
	var a ParentAttention
	if err := completionRecord(ctx, q, "parent_attention", id, &a); err != nil {
		return a, err
	}
	digest, err := contentDigest(a.Task)
	if err != nil || a.ID != id || parentAttentionID(a) != id || !a.Target.Valid() || a.TaskDigest != digest || a.SessionID != a.Task.ParentSessionID || a.Target.WaveID != a.Task.WaveID || a.Target.ScopeRevision != a.Task.ScopeRevision || a.Target.PolicyRevision != a.Task.PolicyRevision || a.CreatedAt.IsZero() {
		return a, errors.New("parent attention source conflict")
	}
	return a, nil
}
func (k *KernelStore) saveParentAttentionTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, task domain.Task, reason, detail string) (bool, error) {
	if wave.DispatchPaused() || wave.State == domain.WaveCompleted || wave.State == domain.WaveCanceled || !k.clock().Before(wave.Deadline) || task.Paused && !task.State.Terminal() {
		return false, nil
	}
	if err := currentObservedTaskTx(ctx, tx, wave, task); err != nil {
		return false, err
	}
	digest, err := contentDigest(task)
	if err != nil {
		return false, err
	}
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", task.SessionID).Scan(&active); err != nil {
		return false, err
	}
	// The reason code is bounded controller vocabulary. Detail is explanatory
	// data, with the full blocker/result retained in the manifest/source record.
	if len(detail) > 1024 {
		detail = validUTF8Prefix(detail, 1024)
	}
	a := ParentAttention{SessionID: wave.OrchestratorSessionID, Target: wave.Target(), Task: task, TaskDigest: digest, RunID: active, Reason: reason, Detail: detail, CreatedAt: k.clock()}
	if active != "" {
		if err = tx.QueryRowContext(ctx, "SELECT state FROM kernel_run_bindings WHERE run_id=? AND session_id=?", active, task.SessionID).Scan(&a.RunState); err != nil {
			return false, err
		}
	}
	a.ID = parentAttentionID(a)
	var previous ParentAttention
	err = completionRecord(ctx, tx, "parent_attention", a.ID, &previous)
	if err == nil {
		_, err = parentAttentionTx(ctx, tx, a.ID)
		return false, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err = kernelRecordTx(ctx, tx, "parent_attention", a.ID, wave.ID, a); err != nil {
		return false, err
	}
	sub := attentionSubscription{ID: structureID("attention-subscription-", a.SessionID, a.ID), SessionID: a.SessionID, Target: a.Target, AttentionID: a.ID}
	raw, err := json.Marshal(sub)
	if err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_subscriptions(id,session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload) VALUES(?,?,?,?,?,?,0,?)", sub.ID, sub.SessionID, wave.ID, wave.ScopeRevision, wave.PlanRevision, "kernel:attention:"+a.ID, raw); err != nil {
		return false, err
	}
	if err = k.waveEventTx(ctx, tx, wave, "parent_attention", task.ID, a.ID, a); err != nil {
		return false, err
	}
	return true, nil
}
func validUTF8Prefix(s string, limit int) string {
	for limit < len(s) && limit > 0 && s[limit]&0xc0 == 0x80 {
		limit--
	}
	return s[:limit]
}
func (k *KernelStore) saveTemplateAttention(ctx context.Context, request TemplateExpansionRequest, detail string) (bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	wave, err := loadWave(ctx, tx, request.Target.WaveID)
	if err != nil {
		return false, err
	}
	if wave.Target() != request.Target {
		return false, nil
	}
	task, err := loadTask(ctx, tx, request.ReviewTaskID)
	if err != nil {
		return false, err
	}
	result, err := k.completedTaskResultTx(ctx, tx, wave, task)
	if err != nil {
		return false, err
	}
	if result.Review == nil || result.Review.Verdict != "findings" {
		return false, nil
	}
	changed, err := k.saveParentAttentionTx(ctx, tx, wave, task, "template_requires_main", strings.TrimPrefix(detail, ErrTemplateNeedsMain.Error()+": "))
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return changed, nil
}
func (k *KernelStore) fanoutParentAttentionTx(ctx context.Context, tx *sql.Tx, intent notificationIntent) error {
	if intent.Kind != "parent_attention" {
		return nil
	}
	a, err := parentAttentionTx(ctx, tx, intent.EventID)
	if err != nil {
		return err
	}
	if a.Target != intent.Target || a.Task.ID != intent.TaskID {
		return errors.New("parent attention event conflict")
	}
	subID := structureID("attention-subscription-", a.SessionID, a.ID)
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT payload FROM kernel_subscriptions WHERE id=? AND session_id=? AND resource=?", subID, a.SessionID, "kernel:attention:"+a.ID).Scan(&raw); err != nil {
		return err
	}
	var sub attentionSubscription
	if err = json.Unmarshal(raw, &sub); err != nil {
		return err
	}
	if sub.ID != subID || sub.SessionID != a.SessionID || sub.Target != a.Target || sub.AttentionID != a.ID {
		return errors.New("parent attention subscription conflict")
	}
	n := Notification{ID: structureID("notification-", sub.ID, a.ID), SubscriptionID: sub.ID, SessionID: a.SessionID, Target: a.Target, SourceTarget: a.Task.Target(), EventID: a.ID, SourceEventID: a.ID, EventBoundary: intent.Sequence, ObservedVia: "event", Kind: "parent_attention", TaskID: a.Task.ID, TaskState: a.Task.State, ResultID: a.Task.ResultID, AttentionID: a.ID, Reason: a.Reason, Detail: a.Detail, State: "pending", CreatedAt: k.clock()}
	raw, err = json.Marshal(n)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,payload) VALUES(?,?,?,?,'pending',?) ON CONFLICT(subscription_id,event_id) DO NOTHING", n.ID, n.SubscriptionID, n.EventID, n.SessionID, raw); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_subscriptions SET cursor=max(cursor,?) WHERE id=?", intent.Sequence, subID)
	return err
}
func (k *KernelStore) checkParentAttentionWakeTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, n Notification) error {
	return k.checkParentAttentionSourceTx(ctx, tx, b, n, false)
}

// Ignore a temporary Task pause only when deciding whether to retire an
// unstarted reservation. Pause holds dispatch without invalidating the source.
func (k *KernelStore) checkParentAttentionSourceTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, n Notification, ignorePause bool) error {
	a, err := parentAttentionTx(ctx, tx, n.AttentionID)
	if err != nil {
		return err
	}
	subID := structureID("attention-subscription-", a.SessionID, a.ID)
	if b.Kind != "orchestrator" || a.SessionID != b.ID || n.ID != structureID("notification-", subID, a.ID) || n.SubscriptionID != subID || n.SessionID != a.SessionID || n.Target != a.Target || n.SourceTarget != a.Task.Target() || n.TaskID != a.Task.ID || n.TaskState != a.Task.State || n.ResultID != a.Task.ResultID || n.EventID != a.ID || n.SourceEventID != a.ID || n.Reason != a.Reason || n.Detail != a.Detail {
		return errors.New("parent attention wake conflict")
	}
	task, err := loadTask(ctx, tx, a.Task.ID)
	if err != nil {
		return err
	}
	paused := task.Paused && !task.State.Terminal()
	task.Paused = a.Task.Paused
	digest, err := contentDigest(task)
	if err != nil {
		return err
	}
	if digest != a.TaskDigest {
		return ErrWakeHeld
	}
	var active string
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", task.SessionID).Scan(&active); err != nil {
		return err
	}
	if active != a.RunID {
		return ErrWakeHeld
	}
	if active != "" {
		var state string
		if err = tx.QueryRowContext(ctx, "SELECT state FROM kernel_run_bindings WHERE run_id=? AND session_id=?", active, task.SessionID).Scan(&state); err != nil {
			return err
		}
		if state != a.RunState {
			return ErrWakeHeld
		}
	}
	if a.Reason == "task_evidence_unavailable" {
		wave, err := loadWave(ctx, tx, a.Target.WaveID)
		if err != nil {
			return err
		}
		_, err = k.completedTaskResultTx(ctx, tx, wave, task)
		if err == nil {
			return ErrWakeHeld
		}
		if !readinessHoldError(err) {
			return err
		}
	}
	if paused && !ignorePause {
		return ErrWakeHeld
	}
	return nil
}

func (k *KernelStore) savePhaseAttentionTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, tasks []domain.Task) (bool, error) {
	changed := false
	for _, task := range tasks {
		reason, detail := "", ""
		switch task.State {
		case contract.TaskBlocked:
			reason, detail = "task_blocked", task.Reason
		case contract.TaskFailed:
			reason, detail = "task_failed", task.Reason
		case contract.TaskCanceled:
			reason, detail = "task_canceled", task.Reason
		case contract.TaskCompleted:
			result, err := k.completedTaskResultTx(ctx, tx, wave, task)
			if err != nil {
				if !readinessHoldError(err) {
					return false, err
				}
				reason, detail = "task_evidence_unavailable", err.Error()
			} else if task.Spec.Kind == "review" {
				if result.Review == nil || result.Review.Verdict == "inconclusive" {
					reason = "review_inconclusive"
				}
			}
		default:
			var state string
			err := tx.QueryRowContext(ctx, "SELECT r.state FROM kernel_sessions s JOIN kernel_run_bindings r ON r.run_id=s.active_run_id WHERE s.id=?", task.SessionID).Scan(&state)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return false, err
			}
			if state == "unknown" || state == "failed" || state == "interrupted" || state == "blocked" {
				reason, detail = "task_run_unresolved", state
			}
		}
		if reason == "" {
			continue
		}
		saved, err := k.saveParentAttentionTx(ctx, tx, wave, task, reason, detail)
		if err != nil {
			return false, err
		}
		changed = changed || saved
	}
	return changed, nil
}
