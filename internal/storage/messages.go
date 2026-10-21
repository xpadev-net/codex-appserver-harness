package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

// MessageRequest is a controller port. The tool backend must select the sender
// from its authenticated process, never from model arguments. Body text grants
// no artifact access, Wave-management authority, or scope/policy changes.
type MessageRequest struct {
	SenderSessionID string          `json:"sender_session_id"`
	SourceRunID     string          `json:"source_run_id"`
	Target          contract.Target `json:"target"`
	Key             string          `json:"key"`
	Recipient       string          `json:"recipient"` // opaque Task ID, or parent
	Body            string          `json:"body"`
	ReplyTo         string          `json:"reply_to"`
}
type Message struct {
	ID                 string          `json:"id"`
	Request            MessageRequest  `json:"request"`
	SenderSessionID    string          `json:"sender_session_id"`
	RecipientSessionID string          `json:"recipient_session_id"`
	TaskID             string          `json:"task_id"` // the direct parent/child pair
	Target             contract.Target `json:"target"`
	CreatedAt          time.Time       `json:"created_at"`
}
type MailboxEntry struct {
	Message              Message `json:"message"`
	State                string  `json:"state"`
	RunID                string  `json:"run_id"`
	DeliverySequence     int64   `json:"delivery_sequence"`
	DeliveryAcknowledged bool    `json:"delivery_acknowledged"`
	HasReply             bool    `json:"has_reply"`
}
type mailboxSubscription struct {
	ID        string          `json:"id"`
	SessionID string          `json:"session_id"`
	TaskID    string          `json:"task_id"`
	Target    contract.Target `json:"target"`
	CreatedAt time.Time       `json:"created_at"`
}

func mailboxSubscriptionID(session string, target contract.Target, task string) string {
	digest, _ := contentDigest(target)
	return structureID("mailbox-subscription-", session, task+":"+digest)
}
func (k *KernelStore) ensureMailboxSubscriptionTx(ctx context.Context, tx *sql.Tx, session string, target contract.Target, task string) (mailboxSubscription, bool, error) {
	sub := mailboxSubscription{ID: mailboxSubscriptionID(session, target, task), SessionID: session, TaskID: task, Target: target, CreatedAt: k.clock()}
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT payload FROM kernel_subscriptions WHERE id=?", sub.ID).Scan(&raw)
	if err == nil {
		var old mailboxSubscription
		if err = json.Unmarshal(raw, &old); err != nil {
			return sub, false, err
		}
		if old.ID != sub.ID || old.SessionID != session || old.TaskID != task || old.Target != target {
			return sub, false, errors.New("inconsistent mailbox subscription")
		}
		return old, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return sub, false, err
	}
	raw, err = json.Marshal(sub)
	if err != nil {
		return sub, false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO kernel_subscriptions(id,session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload) VALUES(?,?,?,?,?,?,0,?)", sub.ID, session, target.WaveID, target.ScopeRevision, target.PlanRevision, "kernel:mailbox:"+task, raw)
	return sub, true, err
}

// ensureTaskMailboxesTx runs after a plan's bindings and Wave revision are saved,
// before plan_adopted commits. Closed, explicitly carried Tasks get historical
// receive subscriptions too; receiving a message cannot reopen their session.
func (k *KernelStore) ensureTaskMailboxesTx(ctx context.Context, tx *sql.Tx, wave domain.Wave, task domain.Task) ([]string, error) {
	if err := currentObservedTaskTx(ctx, tx, wave, task); err != nil {
		return nil, err
	}
	if task.ParentSessionID != wave.OrchestratorSessionID {
		return nil, errors.New("mailbox Task has an unrelated parent")
	}
	ids := []string{}
	for _, session := range []string{task.ParentSessionID, task.SessionID} {
		sub, created, err := k.ensureMailboxSubscriptionTx(ctx, tx, session, wave.Target(), task.ID)
		if err != nil {
			return nil, err
		}
		ids = append(ids, sub.ID)
		if created {
			if err = k.backfillMailboxTx(ctx, tx, sub); err != nil {
				return nil, err
			}
		}
	}
	return ids, nil
}
func (k *KernelStore) EnsureTaskMailboxes(ctx context.Context, target contract.Target, taskID string) ([]string, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	wave, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return nil, err
	}
	if !target.Valid() || wave.Target() != target {
		return nil, errors.New("stale mailbox registration")
	}
	task, err := loadTask(ctx, tx, taskID)
	if err != nil {
		return nil, err
	}
	ids, err := k.ensureTaskMailboxesTx(ctx, tx, wave, task)
	if err != nil {
		return nil, err
	}
	return ids, tx.Commit()
}

func loadMessage(ctx context.Context, q queryer, id string) (Message, error) {
	var m Message
	var sender, recipient, wave string
	var raw []byte
	err := q.QueryRowContext(ctx, "SELECT session_id,recipient_id,wave_id,payload FROM kernel_messages WHERE id=?", id).Scan(&sender, &recipient, &wave, &raw)
	if err != nil {
		return m, err
	}
	if err = json.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	if m.ID != id || m.SenderSessionID != sender || m.RecipientSessionID != recipient || m.Target.WaveID != wave || m.Request.SenderSessionID != sender || m.Request.Target != m.Target {
		return m, errors.New("inconsistent message identity")
	}
	return m, nil
}
func (k *KernelStore) Message(ctx context.Context, sessionID, id string) (Message, error) {
	m, err := loadMessage(ctx, k.DB, id)
	if err != nil {
		return m, err
	}
	if sessionID != m.SenderSessionID && sessionID != m.RecipientSessionID {
		return Message{}, errors.New("message is outside this session's conversation")
	}
	return m, nil
}
func (k *KernelStore) SendMessage(ctx context.Context, request MessageRequest) (Message, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return Message{}, err
	}
	defer tx.Rollback()
	m, err := k.sendMessageTx(ctx, tx, request)
	if err != nil {
		return m, err
	}
	return m, tx.Commit()
}
func (k *KernelStore) sendMessageTx(ctx context.Context, tx *sql.Tx, request MessageRequest) (Message, error) {
	var m Message
	if !request.Target.Valid() || request.SenderSessionID == "" || request.Recipient == "" || strings.TrimSpace(request.Key) == "" || len(request.Key) > 256 || strings.TrimSpace(request.Body) == "" || len(request.Body) > contract.MaxInline || !utf8.ValidString(request.Body) {
		return m, errors.New("invalid message request")
	}
	callKey := structureID("message-call-", request.SenderSessionID, request.Key)
	var id string
	err := tx.QueryRowContext(ctx, "SELECT id FROM kernel_messages WHERE call_key=?", callKey).Scan(&id)
	if err == nil {
		m, err = loadMessage(ctx, tx, id)
		if err == nil && m.Request != request {
			err = errors.New("message key reused with different arguments")
		}
		return m, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return m, err
	}
	b, err := sessionBindingTx(ctx, tx, request.SenderSessionID)
	if err != nil {
		return m, err
	}
	wave, err := loadWave(ctx, tx, request.Target.WaveID)
	if err != nil {
		return m, err
	}
	if b.Closed || wave.Target() != request.Target {
		return m, errors.New("closed sender or stale message target")
	}
	var task domain.Task
	switch b.Kind {
	case "orchestrator":
		if wave.OrchestratorSessionID != b.ID || request.Recipient == "parent" {
			return m, errors.New("parent can address only its direct Task")
		}
		task, err = loadTask(ctx, tx, request.Recipient)
		m.RecipientSessionID = task.SessionID
	case "task":
		if b.Target != request.Target || request.Recipient != "parent" {
			return m, errors.New("Task can address only its parent in the current Wave")
		}
		task, err = loadTask(ctx, tx, b.TaskID)
		m.RecipientSessionID = b.ParentSessionID
	default:
		return m, errors.New("unknown message sender kind")
	}
	if err != nil {
		return m, err
	}
	if task.ParentSessionID != wave.OrchestratorSessionID || task.WaveID != wave.ID {
		return m, errors.New("unrelated message recipient")
	}
	if err = currentObservedTaskTx(ctx, tx, wave, task); err != nil {
		return m, err
	}
	if request.SourceRunID != "" {
		var active, owner, thread, state string
		var stopped, applied int
		if err = tx.QueryRowContext(ctx, "SELECT active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&active); err != nil {
			return m, err
		}
		if err = tx.QueryRowContext(ctx, "SELECT session_id,thread_id,state,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", request.SourceRunID).Scan(&owner, &thread, &state, &stopped, &applied); err != nil {
			return m, err
		}
		if active != request.SourceRunID || owner != b.ID || thread != b.ThreadID || stopped != 0 || applied != 0 || (state != "starting" && state != "running" && state != "inProgress") {
			return m, errors.New("message source run is not active in this session")
		}
	}
	if request.ReplyTo != "" {
		prior, err := loadMessage(ctx, tx, request.ReplyTo)
		if err != nil {
			return m, err
		}
		if prior.Target != request.Target || prior.TaskID != task.ID || prior.RecipientSessionID != b.ID || prior.SenderSessionID != m.RecipientSessionID {
			return m, errors.New("reply must reference the opposite direction of this exact conversation")
		}
	}
	if _, err = k.ensureTaskMailboxesTx(ctx, tx, wave, task); err != nil {
		return m, err
	}
	m.ID, err = opaqueID("message-")
	if err != nil {
		return m, err
	}
	m.Request, m.SenderSessionID, m.TaskID, m.Target, m.CreatedAt = request, b.ID, task.ID, request.Target, k.clock()
	raw, err := json.Marshal(m)
	if err != nil {
		return m, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_messages(id,session_id,recipient_id,wave_id,call_key,payload) VALUES(?,?,?,?,?,?)", m.ID, b.ID, m.RecipientSessionID, wave.ID, callKey, raw); err != nil {
		return m, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_mailbox(message_id,session_id,state) VALUES(?,?,'pending')", m.ID, m.RecipientSessionID); err != nil {
		return m, err
	}
	if request.ReplyTo != "" {
		if err = kernelRecordTx(ctx, tx, "message_reply", m.ID, request.ReplyTo, map[string]string{"reply_to": request.ReplyTo, "message_id": m.ID}); err != nil {
			return m, err
		}
	}
	if err = k.waveEventTx(ctx, tx, wave, "message_received", task.ID, m.ID+":received", map[string]string{"message_id": m.ID}); err != nil {
		return m, err
	}
	return m, nil
}

func (k *KernelStore) backfillMailboxTx(ctx context.Context, tx *sql.Tx, sub mailboxSubscription) error {
	// Read in bounded pages before writing, so no result cursor holds the
	// connection while notifications are inserted. Existing messages are facts.
	var after int64
	for {
		rows, err := tx.QueryContext(ctx, "SELECT rowid,id FROM kernel_messages WHERE recipient_id=? AND wave_id=? AND rowid>? ORDER BY rowid LIMIT 128", sub.SessionID, sub.Target.WaveID, after)
		if err != nil {
			return err
		}
		ids := []string{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&after, &id); err != nil {
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
			m, err := loadMessage(ctx, tx, id)
			if err != nil {
				return err
			}
			if m.Target == sub.Target && m.TaskID == sub.TaskID {
				if err = k.messageNotificationTx(ctx, tx, sub, m, "", 0); err != nil {
					return err
				}
			}
		}
		if len(ids) < 128 {
			return nil
		}
	}
}
func (k *KernelStore) messageNotificationTx(ctx context.Context, tx *sql.Tx, sub mailboxSubscription, m Message, eventID string, sequence int64) error {
	n := Notification{ID: structureID("notification-", sub.ID, m.ID), SubscriptionID: sub.ID, SessionID: m.RecipientSessionID, Target: m.Target, SourceTarget: m.Target, EventID: m.ID, SourceEventID: eventID, EventBoundary: sequence, ObservedVia: "event", Kind: "message", TaskID: m.TaskID, MessageID: m.ID, SenderSessionID: m.SenderSessionID, State: "pending", CreatedAt: k.clock()}
	if eventID == "" {
		n.ObservedVia = "state_snapshot"
		if err := tx.QueryRowContext(ctx, "SELECT coalesce(max(sequence),0) FROM kernel_events").Scan(&n.EventBoundary); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(n)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,payload) VALUES(?,?,?,?,'pending',?) ON CONFLICT(subscription_id,event_id) DO NOTHING", n.ID, sub.ID, n.EventID, n.SessionID, raw); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_subscriptions SET cursor=max(cursor,?) WHERE id=?", n.EventBoundary, sub.ID)
	return err
}
func (k *KernelStore) fanoutMessageNotificationTx(ctx context.Context, tx *sql.Tx, intent notificationIntent) error {
	if intent.Kind != "message_received" {
		return nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT payload FROM kernel_events WHERE id=? AND kind='message_received'", intent.EventID).Scan(&raw); err != nil {
		return err
	}
	var event struct {
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return err
	}
	m, err := loadMessage(ctx, tx, event.MessageID)
	if err != nil {
		return err
	}
	if m.Target != intent.Target || m.TaskID != intent.TaskID || intent.EventID != m.ID+":received" {
		return errors.New("message event binding conflict")
	}
	// Recreate a missing historical receive subscription without rebasing it
	// onto the current Wave. The original validated message remains the source.
	sub, _, err := k.ensureMailboxSubscriptionTx(ctx, tx, m.RecipientSessionID, m.Target, m.TaskID)
	if err != nil {
		return err
	}
	return k.messageNotificationTx(ctx, tx, sub, m, intent.EventID, intent.Sequence)
}

func (k *KernelStore) Mailbox(ctx context.Context, sessionID string, after int64, limit int) ([]MailboxEntry, error) {
	if after < 0 || limit < 1 || limit > 1000 {
		return nil, errors.New("invalid mailbox page")
	}
	if _, err := sessionBindingTx(ctx, k.DB, sessionID); err != nil {
		return nil, err
	}
	rows, err := k.DB.QueryContext(ctx, "SELECT m.rowid,m.id,m.payload,b.state,b.run_id,EXISTS(SELECT 1 FROM records d WHERE d.kind='message_delivery' AND d.id=m.id),EXISTS(SELECT 1 FROM records r WHERE r.kind='message_reply' AND r.work_item_id=m.id) FROM kernel_messages m JOIN kernel_mailbox b ON b.message_id=m.id AND b.session_id=m.recipient_id WHERE m.recipient_id=? AND m.rowid>? ORDER BY m.rowid LIMIT ?", sessionID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MailboxEntry{}
	for rows.Next() {
		var e MailboxEntry
		var id string
		var raw []byte
		if err = rows.Scan(&e.DeliverySequence, &id, &raw, &e.State, &e.RunID, &e.DeliveryAcknowledged, &e.HasReply); err == nil {
			err = json.Unmarshal(raw, &e.Message)
		}
		if err != nil {
			return nil, err
		}
		if e.Message.ID != id || e.Message.RecipientSessionID != sessionID {
			return nil, errors.New("inconsistent mailbox identity")
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
func (k *KernelStore) AcknowledgeMessage(ctx context.Context, sessionID, messageID string) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var owner string
	if err = tx.QueryRowContext(ctx, "SELECT recipient_id FROM kernel_messages WHERE id=?", messageID).Scan(&owner); err != nil {
		return err
	}
	if owner != sessionID {
		return errors.New("cannot acknowledge another session's message")
	}
	raw, err := json.Marshal(map[string]any{"session_id": sessionID, "delivered_at": k.clock()})
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO records(kind,id,work_item_id,payload) VALUES('message_delivery',?,?,?) ON CONFLICT(kind,id) DO NOTHING", messageID, sessionID, raw); err != nil {
		return err
	}
	return tx.Commit()
}
