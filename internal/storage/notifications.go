package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// SubscriptionRequest is a controller API. A model cannot choose SessionID.
// These K conditions observe facts; they do not authorize a turn or acceptance.
type SubscriptionRequest struct {
	SessionID string          `json:"session_id"`
	Target    contract.Target `json:"target"`
	Key       string          `json:"key"`
	Kind      string          `json:"kind"` // task_terminal or wave_terminal
	TaskID    string          `json:"task_id"`
}
type Subscription struct {
	ID        string              `json:"id"`
	Request   SubscriptionRequest `json:"request"`
	Target    contract.Target     `json:"target"`
	Cursor    int64               `json:"cursor"`
	CreatedAt time.Time           `json:"created_at"`
}

// Notification references immutable facts. Artifact IDs in an observation never
// grant read access; artifact_read still checks the receiving Task's own grants.
// Delivery ack, run binding, and decision application are separate facts.
type Notification struct {
	Sealing              *ProductSealingSummary     `json:"product_sealing,omitempty"`
	Preparation          *ProductPreparationSummary `json:"product_preparation,omitempty"`
	ID                   string                     `json:"id"`
	SubscriptionID       string                     `json:"subscription_id"`
	SessionID            string                     `json:"session_id"`
	Target               contract.Target            `json:"target"`
	SourceTarget         contract.Target            `json:"source_target"`
	EventID              string                     `json:"event_id"`
	SourceEventID        string                     `json:"source_event_id"`
	EventBoundary        int64                      `json:"event_boundary"`
	ObservedVia          string                     `json:"observed_via"`
	DeliverySequence     int64                      `json:"delivery_sequence"`
	Kind                 string                     `json:"kind"`
	TaskID               string                     `json:"task_id"`
	MessageID            string                     `json:"message_id,omitempty"`
	SenderSessionID      string                     `json:"sender_session_id,omitempty"`
	OperationID          string                     `json:"operation_id,omitempty"`
	ArtifactID           string                     `json:"artifact_id,omitempty"`
	OperationState       string                     `json:"operation_state,omitempty"`
	AttentionID          string                     `json:"attention_id,omitempty"`
	Reason               string                     `json:"reason,omitempty"`
	Detail               string                     `json:"detail,omitempty"`
	TaskState            contract.TaskState         `json:"task_state"`
	WaveState            domain.WaveState           `json:"wave_state"`
	ResultID             string                     `json:"result_id"`
	InputDigest          string                     `json:"input_digest"`
	State                string                     `json:"state"`
	RunID                string                     `json:"run_id"`
	DeliveryAcknowledged bool                       `json:"delivery_acknowledged"`
	CreatedAt            time.Time                  `json:"created_at"`
}
type notificationIntent struct {
	EventID  string          `json:"event_id"`
	Sequence int64           `json:"sequence"`
	Target   contract.Target `json:"target"`
	TaskID   string          `json:"task_id"`
	Kind     string          `json:"kind"`
}

func subscriptionResource(r SubscriptionRequest) string {
	if r.Kind == "task_terminal" {
		return "kernel:task-terminal:" + r.TaskID
	}
	return "kernel:wave-terminal:" + r.Target.WaveID
}
func subscriptionTx(ctx context.Context, q queryer, id string) (Subscription, error) {
	var s Subscription
	var session, wave, resource string
	var scope, plan int
	var cursor int64
	var raw []byte
	err := q.QueryRowContext(ctx, "SELECT session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload FROM kernel_subscriptions WHERE id=?", id).Scan(&session, &wave, &scope, &plan, &resource, &cursor, &raw)
	if err != nil {
		return s, err
	}
	if err = json.Unmarshal(raw, &s); err != nil {
		return s, err
	}
	if s.ID != id || s.Request.SessionID != session || s.Target != s.Request.Target || s.Target.WaveID != wave || s.Target.ScopeRevision != scope || s.Target.PlanRevision != plan || resource != subscriptionResource(s.Request) {
		return s, errors.New("inconsistent subscription identity")
	}
	s.Cursor = cursor
	return s, nil
}

func (k *KernelStore) Subscribe(ctx context.Context, request SubscriptionRequest) (Subscription, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return Subscription{}, err
	}
	defer tx.Rollback()
	sub, err := k.subscribeRequestTx(ctx, tx, request)
	if err != nil {
		return sub, err
	}
	return sub, tx.Commit()
}
func (k *KernelStore) subscribeRequestTx(ctx context.Context, tx *sql.Tx, request SubscriptionRequest) (Subscription, error) {
	var s Subscription
	if !request.Target.Valid() || request.SessionID == "" || strings.TrimSpace(request.Key) == "" || len(request.Key) > 256 || (request.Kind != "task_terminal" && request.Kind != "wave_terminal") || (request.Kind == "task_terminal" && request.TaskID == "") || (request.Kind == "wave_terminal" && request.TaskID != "") {
		return s, errors.New("invalid subscription request")
	}
	id := structureID("subscription-", request.SessionID, request.Key)
	s, err := subscriptionTx(ctx, tx, id)
	if err == nil {
		if s.Request != request {
			return s, errors.New("subscription key reused with different arguments")
		}
		return s, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return s, err
	}
	b, err := sessionBindingTx(ctx, tx, request.SessionID)
	if err != nil {
		return s, err
	}
	wave, err := loadWave(ctx, tx, request.Target.WaveID)
	if err != nil {
		return s, err
	}
	if b.Closed || wave.Target() != request.Target {
		return s, errors.New("closed subscriber or stale target")
	}
	if b.Kind == "orchestrator" {
		if wave.OrchestratorSessionID != b.ID {
			return s, errors.New("subscriber does not own Wave")
		}
	} else if b.Kind == "task" {
		if b.Target != request.Target || request.Kind != "task_terminal" {
			return s, errors.New("Task cannot observe another Wave or manage Wave subscriptions")
		}
	} else {
		return s, errors.New("unknown subscriber kind")
	}
	if request.Kind == "task_terminal" {
		task, err := loadTask(ctx, tx, request.TaskID)
		if err != nil {
			return s, err
		}
		if task.WaveID != wave.ID || task.ParentSessionID != wave.OrchestratorSessionID {
			return s, errors.New("unrelated observed Task")
		}
		if err = currentObservedTaskTx(ctx, tx, wave, task); err != nil {
			return s, err
		}
		if b.Kind == "task" && task.ID != b.TaskID {
			subscriber, err := loadTask(ctx, tx, b.TaskID)
			if err != nil {
				return s, err
			}
			granted := false
			for _, dep := range subscriber.Spec.DependsOn {
				granted = granted || dep.TaskID == task.LocalID
			}
			if !granted {
				return s, errors.New("Task cannot observe an undeclared sibling")
			}
		}
	}
	var sequence int64
	if err = tx.QueryRowContext(ctx, "SELECT coalesce(max(sequence),0) FROM kernel_events").Scan(&sequence); err != nil {
		return s, err
	}
	s = Subscription{ID: id, Request: request, Target: request.Target, Cursor: sequence, CreatedAt: k.clock()}
	raw, err := json.Marshal(s)
	if err != nil {
		return s, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_subscriptions(id,session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload) VALUES(?,?,?,?,?,?,?,?)", s.ID, b.ID, wave.ID, wave.ScopeRevision, wave.PlanRevision, subscriptionResource(request), sequence, raw); err != nil {
		return s, err
	}
	// The current terminal snapshot and event boundary are read in the same
	// transaction as registration. Neither order of completion can lose a fact.
	if err = k.observeSubscriptionTx(ctx, tx, s, "", sequence); err != nil {
		return s, err
	}
	return s, nil
}

func currentObservedTaskTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, task domain.Task) error {
	if task.ScopeRevision != wave.ScopeRevision || task.PolicyRevision != wave.Policy.Revision {
		return fmt.Errorf("%w: observed Task is outside current scope/policy", ErrTaskBinding)
	}
	var result, digest string
	if err := tx.QueryRowContext(ctx, "SELECT source_result_id,input_digest FROM kernel_plan_bindings WHERE wave_id=? AND revision=? AND task_id=?", wave.ID, wave.PlanRevision, task.ID).Scan(&result, &digest); err != nil {
		return err
	}
	if task.PlanRevision != wave.PlanRevision && (task.State != contract.TaskCompleted || task.ResultID == "" || result != task.ResultID || digest != task.InputDigest) {
		return fmt.Errorf("%w: observed Task is not explicitly carried into this plan", ErrTaskBinding)
	}
	return nil
}

func (k *KernelStore) observeSubscriptionTx(ctx context.Context, tx *sql.Tx, sub Subscription, sourceEvent string, sequence int64) error {
	wave, err := loadWave(ctx, tx, sub.Target.WaveID)
	if err != nil {
		return err
	}
	if wave.Target() != sub.Target {
		return nil // History remains available; never reinterpret an old condition.
	}
	n := Notification{SubscriptionID: sub.ID, SessionID: sub.Request.SessionID, Target: sub.Target, SourceTarget: sub.Target, EventBoundary: sequence, ObservedVia: "state_snapshot", Kind: sub.Request.Kind, State: "pending", CreatedAt: k.clock()}
	switch sub.Request.Kind {
	case "task_terminal":
		task, err := loadTask(ctx, tx, sub.Request.TaskID)
		if err != nil {
			return err
		}
		if err = currentObservedTaskTx(ctx, tx, wave, task); err != nil {
			return err
		}
		if !task.State.Terminal() {
			return nil
		}
		n.TaskID, n.TaskState, n.ResultID, n.InputDigest = task.ID, task.State, task.ResultID, task.InputDigest
		n.SourceTarget = task.Target()
		n.EventID = task.ID + ":terminal"
	case "wave_terminal":
		if wave.State != domain.WaveCompleted && wave.State != domain.WaveCanceled {
			return nil
		}
		n.WaveState = wave.State
		n.EventID = wave.ID + ":terminal"
	default:
		return errors.New("unsupported subscription condition")
	}
	if sourceEvent != "" {
		matches, err := notificationSourceMatchesTx(ctx, tx, n, sourceEvent)
		if err != nil {
			return err
		}
		if matches {
			n.SourceEventID, n.ObservedVia = sourceEvent, "event"
		}
	}
	if n.ObservedVia == "state_snapshot" {
		// A current-state observation can be newer than a replayed intent.
		if err = tx.QueryRowContext(ctx, "SELECT coalesce(max(sequence),0) FROM kernel_events").Scan(&n.EventBoundary); err != nil {
			return err
		}
	}
	n.ID = structureID("notification-", sub.ID, n.EventID)
	raw, err := json.Marshal(n)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,payload) VALUES(?,?,?,?,'pending',?) ON CONFLICT(subscription_id,event_id) DO NOTHING", n.ID, sub.ID, n.EventID, n.SessionID, raw)
	return err
}

func notificationSourceMatchesTx(ctx context.Context, tx *sql.Tx, n Notification, eventID string) (bool, error) {
	var wave, task, kind string
	var scope, plan int
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT wave_id,task_id,kind,scope_revision,plan_revision,payload FROM kernel_events WHERE id=?", eventID).Scan(&wave, &task, &kind, &scope, &plan, &raw)
	if err != nil {
		return false, err
	}
	if wave != n.SourceTarget.WaveID || task != n.TaskID || scope != n.SourceTarget.ScopeRevision || plan != n.SourceTarget.PlanRevision {
		return false, nil
	}
	switch kind {
	case "task_result_applied", "task_canceled":
		var applied ResultApplication
		if err = json.Unmarshal(raw, &applied); err != nil {
			return false, err
		}
		return applied.State.Terminal() && applied.State == n.TaskState && applied.TaskID == n.TaskID && applied.Target == n.SourceTarget && applied.ResultID == n.ResultID, nil
	case "task_failed":
		return n.TaskState == contract.TaskFailed, nil
	case "wave_completed":
		return n.WaveState == domain.WaveCompleted, nil
	case "wave_canceled":
		return n.WaveState == domain.WaveCanceled, nil
	default:
		return false, nil
	}
}

// fanoutNotificationTx is called with each state transition and its saved
// intent. The separate receipt permits bounded recovery of pre-existing intents.
func (k *KernelStore) fanoutNotificationTx(ctx context.Context, tx *sql.Tx, intent notificationIntent) error {
	rows, err := tx.QueryContext(ctx, "SELECT id FROM kernel_subscriptions WHERE wave_id=? AND scope_revision=? AND plan_revision=? AND (resource LIKE 'kernel:task-terminal:%' OR resource LIKE 'kernel:wave-terminal:%') ORDER BY id", intent.Target.WaveID, intent.Target.ScopeRevision, intent.Target.PlanRevision)
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		sub, err := subscriptionTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if sub.Target != intent.Target {
			continue
		}
		// Recovery may visit an unrelated or older event after completion. In
		// that case this is a terminal-state snapshot, not a claim that the
		// visited event caused the observed result.
		sourceEvent := ""
		if sub.Request.Kind == "task_terminal" && intent.TaskID == sub.Request.TaskID && (intent.Kind == "task_result_applied" || intent.Kind == "task_failed" || intent.Kind == "task_canceled") || sub.Request.Kind == "wave_terminal" && (intent.Kind == "wave_completed" || intent.Kind == "wave_canceled") {
			sourceEvent = intent.EventID
		}
		if err = k.observeSubscriptionTx(ctx, tx, sub, sourceEvent, intent.Sequence); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_subscriptions SET cursor=max(cursor,?) WHERE id=?", intent.Sequence, id); err != nil {
			return err
		}
	}
	if err = k.fanoutMessageNotificationTx(ctx, tx, intent); err != nil {
		return err
	}
	if err = k.fanoutKernelOperationTx(ctx, tx, intent); err != nil {
		return err
	}
	if err = k.fanoutPROperationTx(ctx, tx, intent); err != nil {
		return err
	}
	if err = k.fanoutPRResourceUpdateTx(ctx, tx, intent); err != nil {
		return err
	}
	if err = k.fanoutPRWebhookTx(ctx, tx, intent); err != nil {
		return err
	}
	if err = k.fanoutParentAttentionTx(ctx, tx, intent); err != nil {
		return err
	}
	return kernelRecordTx(ctx, tx, "notification_fanout", intent.EventID, intent.Target.WaveID, intent)
}

func (k *KernelStore) ReconcileNotifications(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, errors.New("notification reconciliation limit must be 1..1000")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT i.payload FROM records i LEFT JOIN records f ON f.kind='notification_fanout' AND f.id=i.id WHERE i.kind='notification_intent' AND f.id IS NULL ORDER BY i.id LIMIT ?", limit)
	if err != nil {
		return 0, err
	}
	intents := []notificationIntent{}
	for rows.Next() {
		var raw []byte
		var intent notificationIntent
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &intent)
		}
		if err != nil {
			rows.Close()
			return 0, err
		}
		intents = append(intents, intent)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, intent := range intents {
		if err = k.fanoutNotificationTx(ctx, tx, intent); err != nil {
			return 0, err
		}
	}
	return len(intents), tx.Commit()
}

// Notifications is a bounded history page, including acknowledged but not yet
// applied observations. It is not a list of runnable work.
func (k *KernelStore) Notifications(ctx context.Context, sessionID string, after int64, limit int) ([]Notification, error) {
	if after < 0 || limit < 1 || limit > 1000 {
		return nil, errors.New("invalid notification page")
	}
	if _, err := sessionBindingTx(ctx, k.DB, sessionID); err != nil {
		return nil, err
	}
	rows, err := k.DB.QueryContext(ctx, "SELECT n.rowid,n.id,n.state,n.run_id,n.payload,EXISTS(SELECT 1 FROM records d WHERE d.kind='notification_delivery' AND d.id=n.id) FROM kernel_notifications n WHERE n.session_id=? AND n.rowid>? ORDER BY n.rowid LIMIT ?", sessionID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Notification{}
	for rows.Next() {
		var n Notification
		var id, state, run string
		var sequence int64
		var ack bool
		var raw []byte
		if err = rows.Scan(&sequence, &id, &state, &run, &raw, &ack); err == nil {
			err = json.Unmarshal(raw, &n)
		}
		if err != nil {
			return nil, err
		}
		if n.ID != id || n.SessionID != sessionID {
			return nil, errors.New("inconsistent notification identity")
		}
		n.State, n.RunID, n.DeliverySequence, n.DeliveryAcknowledged = state, run, sequence, ack
		out = append(out, n)
	}
	return out, rows.Err()
}

func (k *KernelStore) AcknowledgeNotification(ctx context.Context, sessionID, notificationID string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var owner string
	if err = tx.QueryRowContext(ctx, "SELECT session_id FROM kernel_notifications WHERE id=?", notificationID).Scan(&owner); err != nil {
		return err
	}
	if owner != sessionID {
		return errors.New("cannot acknowledge another session's notification")
	}
	raw, err := json.Marshal(map[string]any{"session_id": sessionID, "delivered_at": k.clock()})
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records(kind,id,work_item_id,payload) VALUES('notification_delivery',?,?,?) ON CONFLICT(kind,id) DO NOTHING", notificationID, sessionID, raw); err != nil {
		return err
	}
	return tx.Commit()
}
