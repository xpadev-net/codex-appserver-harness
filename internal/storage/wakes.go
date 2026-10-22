package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var ErrWakeHeld = errors.New("wake is held by session state or current policy")
var ErrWakeInputLimit = errors.New("wake input exceeds the bounded snapshot representation")
var ErrWakeScopeLimit = errors.New("wake exceeds the main Wave batch limit")
var ErrWakeBusy = errors.New("session already has an unfinished wake")

// Context is constructed by controller code for these exact versioned inputs.
// It must not contain credentials. A model cannot select the session or IDs.
type WakeRequest struct {
	SessionID             string          `json:"session_id"`
	Key                   string          `json:"key"`
	NotificationIDs       []string        `json:"notification_ids"`
	Context               json.RawMessage `json:"context"`
	IncludeSessionContext bool            `json:"include_session_context,omitempty"`
}
type WakeIntent struct {
	ID              string            `json:"id"`
	SessionID       string            `json:"session_id"`
	RunID           string            `json:"run_id"`
	RequestDigest   string            `json:"request_digest"`
	SnapshotID      string            `json:"snapshot_id"`
	SnapshotDigest  string            `json:"snapshot_digest"`
	NotificationIDs []string          `json:"notification_ids"`
	Targets         []contract.Target `json:"targets"`
	State           string            `json:"state"`
	CreatedAt       time.Time         `json:"created_at"`
}
type WakeReservation struct {
	Intent   WakeIntent
	Snapshot contract.InputSnapshot
}
type wakeInput struct {
	SchemaVersion int               `json:"schema_version"`
	SessionID     string            `json:"session_id"`
	Targets       []contract.Target `json:"targets"`
	Notifications []Notification    `json:"notifications"`
	Messages      []WakeMessage     `json:"messages"`
	Context       json.RawMessage   `json:"context"`
}

// WakeMessage preserves the complete authorized body. Encoding overhead must
// not make one valid inline message impossible to deliver within a snapshot.
type WakeMessage struct {
	ID                 string          `json:"id"`
	SenderSessionID    string          `json:"sender_session_id"`
	RecipientSessionID string          `json:"recipient_session_id"`
	TaskID             string          `json:"task_id"`
	Target             contract.Target `json:"target"`
	SourceRunID        string          `json:"source_run_id"`
	ReplyTo            string          `json:"reply_to"`
	Body               string          `json:"body"`
	BodyEncoding       string          `json:"body_encoding"`
	EncodedBodyChunks  []string        `json:"encoded_body_chunks"`
	BodyDigest         string          `json:"body_digest"`
}

func encodeWakeJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(buffer.Bytes()), nil
}
func wakeMessage(m Message) WakeMessage {
	w := WakeMessage{ID: m.ID, SenderSessionID: m.SenderSessionID, RecipientSessionID: m.RecipientSessionID, TaskID: m.TaskID, Target: m.Target, SourceRunID: m.Request.SourceRunID, ReplyTo: m.Request.ReplyTo, Body: m.Request.Body, BodyEncoding: "utf-8", EncodedBodyChunks: []string{}, BodyDigest: fmt.Sprintf("%x", sha256.Sum256([]byte(m.Request.Body)))}
	bodyJSON, _ := encodeWakeJSON(w.Body)
	if len(bodyJSON) > contract.MaxInline+1024 {
		// Quotes, controls and JS separator characters can still expand in
		// JSON even with HTML escaping disabled. Chunk the reversible encoding
		// so every inline value also respects its individual byte limit.
		w.Body, w.BodyEncoding = "", "base64"
		encoded := base64.StdEncoding.EncodeToString([]byte(m.Request.Body))
		for len(encoded) > 0 {
			length := min(len(encoded), contract.MaxInline)
			w.EncodedBodyChunks = append(w.EncodedBodyChunks, encoded[:length])
			encoded = encoded[length:]
		}
	}
	return w
}

func notificationTx(ctx context.Context, q queryer, id string) (Notification, error) {
	var n Notification
	var session, sub, event, state, run string
	var raw []byte
	var sequence int64
	var acknowledged bool
	err := q.QueryRowContext(ctx, "SELECT n.rowid,n.session_id,n.subscription_id,n.event_id,n.state,n.run_id,n.payload,EXISTS(SELECT 1 FROM records d WHERE d.kind='notification_delivery' AND d.id=n.id) FROM kernel_notifications n WHERE n.id=?", id).Scan(&sequence, &session, &sub, &event, &state, &run, &raw, &acknowledged)
	if err != nil {
		return n, err
	}
	if err = json.Unmarshal(raw, &n); err != nil {
		return n, err
	}
	if n.ID != id || n.SessionID != session || n.SubscriptionID != sub || n.EventID != event || !n.Target.Valid() {
		return n, errors.New("inconsistent wake notification")
	}
	n.State, n.RunID = state, run
	n.DeliverySequence, n.DeliveryAcknowledged = sequence, acknowledged
	return n, nil
}
func wakeTx(ctx context.Context, q queryer, id string) (WakeReservation, error) {
	var w WakeReservation
	var session, run, state string
	var raw []byte
	err := q.QueryRowContext(ctx, "SELECT session_id,run_id,state,payload FROM kernel_wake_intents WHERE id=?", id).Scan(&session, &run, &state, &raw)
	if err != nil {
		return w, err
	}
	if err = json.Unmarshal(raw, &w.Intent); err != nil {
		return w, err
	}
	if w.Intent.ID != id || w.Intent.SessionID != session || w.Intent.RunID != run {
		return w, errors.New("inconsistent wake identity")
	}
	w.Intent.State = state
	w.Snapshot.ID = w.Intent.SnapshotID
	if err = q.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", w.Snapshot.ID).Scan(&w.Snapshot.Digest, &w.Snapshot.Input); err != nil {
		return w, err
	}
	if w.Snapshot.Digest != w.Intent.SnapshotDigest || w.Snapshot.Digest != fmt.Sprintf("%x", sha256.Sum256(w.Snapshot.Input)) {
		return w, errors.New("wake snapshot digest conflict")
	}
	return w, nil
}
func (k *KernelStore) Wake(ctx context.Context, id string) (WakeReservation, error) {
	return wakeTx(ctx, k.DB, id)
}
func obsoleteWakeNotification(b contract.SessionBinding, wave domain.Wave, n Notification, now time.Time) bool {
	if b.Closed || wave.Target() != n.Target || !now.Before(wave.Deadline) {
		return true
	}
	parent := b.Kind == "orchestrator" && wave.OrchestratorSessionID == b.ID
	if !parent && (b.Kind != "task" || b.Target != n.Target || b.ParentSessionID != wave.OrchestratorSessionID) {
		return true
	}
	terminal := wave.State == domain.WaveCanceled || wave.State == domain.WaveCompleted
	if n.Kind == "wave_terminal" {
		return !parent || !terminal || n.WaveState != wave.State || n.TaskID != ""
	}
	return terminal
}
func (k *KernelStore) wakeNotificationTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, n Notification) error {
	wave, err := loadWave(ctx, tx, n.Target.WaveID)
	if err != nil {
		return err
	}
	if obsoleteWakeNotification(b, wave, n, k.clock()) || wave.DispatchPaused() {
		return ErrWakeHeld
	}
	held, err := wakeBudgetHeldTx(ctx, tx, b, wave)
	if err != nil {
		return err
	}
	if held {
		return ErrWakeHeld
	}
	if b.Kind == "orchestrator" && wave.OrchestratorSessionID == b.ID {
		if n.Kind == "parent_attention" {
			return k.checkParentAttentionWakeTx(ctx, tx, b, n)
		}
		return nil
	}
	if b.Kind != "task" || b.Target != n.Target {
		return ErrWakeHeld
	}
	task, err := loadTask(ctx, tx, b.TaskID)
	if err != nil {
		return err
	}
	if task.Paused || task.State.Terminal() {
		return ErrWakeHeld
	}
	return nil
}
func (k *KernelStore) checkWakeTargetsTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, intent WakeIntent) error {
	for _, id := range intent.NotificationIDs {
		n, err := notificationTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if n.SessionID != b.ID {
			return errors.New("wake input belongs to another session")
		}
		if err := k.wakeNotificationTx(ctx, tx, b, n); err != nil {
			return err
		}
	}
	return nil
}

// PendingWakeNotifications selects eligible input, not runnable work. The later
// scheduler still owns slots/budgets/process creation. Acked inputs are included.
func (k *KernelStore) PendingWakeNotifications(ctx context.Context, sessionID string, limit int) ([]Notification, error) {
	if limit < 1 || limit > 1000 {
		return nil, errors.New("invalid wake notification limit")
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	b, err := sessionBindingTx(ctx, tx, sessionID)
	if err != nil {
		return nil, err
	}
	if b.Closed {
		return []Notification{}, nil
	}
	out := []Notification{}
	type gateKey struct {
		Target      contract.Target
		Terminal    bool
		State       domain.WaveState
		AttentionID string
	}
	gateCache := map[gateKey]error{}
	var after int64
	for {
		rows, err := tx.QueryContext(ctx, "SELECT rowid,id FROM kernel_notifications WHERE session_id=? AND state='pending' AND run_id='' AND rowid>? ORDER BY rowid LIMIT 128", sessionID, after)
		if err != nil {
			return nil, err
		}
		ids := []string{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&after, &id); err != nil {
				rows.Close()
				return nil, err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			n, err := notificationTx(ctx, tx, id)
			if err != nil {
				return nil, err
			}
			key := gateKey{Target: n.Target, Terminal: n.Kind == "wave_terminal", State: n.WaveState}
			if n.Kind == "parent_attention" {
				key.AttentionID = n.ID
			}
			gate, known := gateCache[key]
			if !known {
				gate = k.wakeNotificationTx(ctx, tx, b, n)
				gateCache[key] = gate
			}
			if errors.Is(gate, ErrWakeHeld) {
				continue
			} else if gate != nil {
				return nil, gate
			}
			out = append(out, n)
			if len(out) == limit {
				return out, nil
			}
		}
		if len(ids) < 128 {
			return out, nil
		}
	}
}

func (k *KernelStore) ReserveWake(ctx context.Context, request WakeRequest) (WakeReservation, error) {
	var reservation WakeReservation
	if request.SessionID == "" || strings.TrimSpace(request.Key) == "" || len(request.Key) > 256 || len(request.NotificationIDs) == 0 || len(request.NotificationIDs) > 1000 {
		return reservation, errors.New("wake identity and bounded notification set required")
	}
	request.NotificationIDs = append([]string{}, request.NotificationIDs...)
	sort.Strings(request.NotificationIDs)
	for i, id := range request.NotificationIDs {
		if id == "" || i > 0 && request.NotificationIDs[i-1] == id {
			return reservation, errors.New("wake notification set must be unique")
		}
	}
	if len(request.Context) == 0 {
		request.Context = json.RawMessage(`{}`)
	}
	if err := contract.ValidateSessionInput(request.Context); err != nil {
		return reservation, err
	}
	digest, err := contentDigest(request)
	if err != nil {
		return reservation, err
	}
	id := structureID("wake-", request.SessionID, request.Key)
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return reservation, err
	}
	defer tx.Rollback()
	reservation, err = wakeTx(ctx, tx, id)
	if err == nil {
		if reservation.Intent.RequestDigest != digest {
			return reservation, errors.New("wake key reused with changed input")
		}
		b, err := sessionBindingTx(ctx, tx, request.SessionID)
		if err != nil {
			return reservation, err
		}
		return reservation, k.checkWakeTargetsTx(ctx, tx, b, reservation.Intent)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return reservation, err
	}
	var activeWake string
	err = tx.QueryRowContext(ctx, "SELECT id FROM kernel_wake_intents WHERE session_id=? AND state IN ('reserved','bound') ORDER BY id LIMIT 1", request.SessionID).Scan(&activeWake)
	if err == nil {
		reservation, err = wakeTx(ctx, tx, activeWake)
		if err != nil {
			return reservation, err
		}
		return reservation, ErrWakeBusy
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return reservation, err
	}
	b, err := sessionBindingTx(ctx, tx, request.SessionID)
	if err != nil {
		return reservation, err
	}
	var active string
	var stopped, creating int
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &stopped, &creating); err != nil {
		return reservation, err
	}
	if active != "" || stopped != 1 || creating != 0 {
		return reservation, ErrWakeHeld
	}
	intent := WakeIntent{ID: id, SessionID: b.ID, RequestDigest: digest, NotificationIDs: request.NotificationIDs, Targets: []contract.Target{}, State: "reserved", CreatedAt: k.clock()}
	input := wakeInput{SchemaVersion: 1, SessionID: b.ID, Notifications: []Notification{}, Messages: []WakeMessage{}, Context: request.Context}
	seenTargets := map[string]bool{}
	seenMessages := map[string]bool{}
	for _, notificationID := range request.NotificationIDs {
		n, err := notificationTx(ctx, tx, notificationID)
		if err != nil {
			return reservation, err
		}
		if n.SessionID != b.ID || n.State != "pending" || n.RunID != "" {
			return reservation, errors.New("wake input belongs to another session or run")
		}
		if err = k.wakeNotificationTx(ctx, tx, b, n); err != nil {
			return reservation, err
		}
		if !seenTargets[n.Target.WaveID] {
			intent.Targets = append(intent.Targets, n.Target)
			if b.Kind == "orchestrator" && len(intent.Targets) > contract.MaxMainWaves {
				return reservation, ErrWakeScopeLimit
			}
			seenTargets[n.Target.WaveID] = true
		}
		input.Notifications = append(input.Notifications, n)
		if n.MessageID != "" && !seenMessages[n.MessageID] {
			m, err := loadMessage(ctx, tx, n.MessageID)
			if err != nil {
				return reservation, err
			}
			if m.RecipientSessionID != b.ID || m.Target != n.Target {
				return reservation, errors.New("wake message binding conflict")
			}
			var state, run string
			if err = tx.QueryRowContext(ctx, "SELECT state,run_id FROM kernel_mailbox WHERE message_id=? AND session_id=?", m.ID, b.ID).Scan(&state, &run); err != nil {
				return reservation, err
			}
			if state != "pending" || run != "" {
				return reservation, errors.New("wake message is already bound or processed")
			}
			input.Messages = append(input.Messages, wakeMessage(m))
			seenMessages[m.ID] = true
		}
	}
	sort.Slice(intent.Targets, func(i, j int) bool { return intent.Targets[i].WaveID < intent.Targets[j].WaveID })
	input.Targets = intent.Targets
	intent.SnapshotID = structureID("wake-snapshot-", b.ID, id)
	if request.IncludeSessionContext {
		state, refs, contextErr := k.sessionContextTx(ctx, tx, b, intent.Targets)
		if contextErr != nil {
			return reservation, contextErr
		}
		combined, contextErr := encodeWakeJSON(struct {
			State        SessionContext  `json:"state"`
			Supplemental json.RawMessage `json:"supplemental"`
		}{state, request.Context})
		if contextErr != nil {
			return reservation, contextErr
		}
		input.Context = combined
		if contextErr = saveInputGrantsTx(ctx, tx, b.ID, intent.SnapshotID, refs); contextErr != nil {
			return reservation, contextErr
		}
	}
	rawInput, err := encodeWakeJSON(input)
	if err != nil {
		return reservation, err
	}
	if err = contract.ValidateSessionInput(rawInput); err != nil {
		return reservation, errors.Join(ErrWakeInputLimit, err) // Leave every selected input pending for a smaller batch.
	}
	intent.RunID, err = opaqueID("run-")
	if err != nil {
		return reservation, err
	}
	intent.SnapshotDigest = fmt.Sprintf("%x", sha256.Sum256(rawInput))
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_snapshots(id,digest,data) VALUES(?,?,?)", intent.SnapshotID, intent.SnapshotDigest, rawInput); err != nil {
		return reservation, err
	}
	raw, err := json.Marshal(intent)
	if err != nil {
		return reservation, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_wake_intents(id,session_id,run_id,state,payload) VALUES(?,?,?,'reserved',?)", id, b.ID, intent.RunID, raw); err != nil {
		return reservation, err
	}
	for _, n := range input.Notifications {
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_notifications SET state='bound_to_run',run_id=? WHERE id=? AND state='pending' AND run_id=''", intent.RunID, n.ID); err != nil {
			return reservation, err
		}
	}
	for _, m := range input.Messages {
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_mailbox SET state='bound_to_run',run_id=? WHERE message_id=? AND session_id=? AND state='pending' AND run_id=''", intent.RunID, m.ID, b.ID); err != nil {
			return reservation, err
		}
	}
	reservation = WakeReservation{Intent: intent, Snapshot: contract.InputSnapshot{ID: intent.SnapshotID, Digest: intent.SnapshotDigest, Input: rawInput}}
	return reservation, tx.Commit()
}

func (k *KernelStore) bindWakeRunTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, snapshot contract.InputSnapshot, run contract.SessionRun) error {
	var id string
	err := tx.QueryRowContext(ctx, "SELECT id FROM kernel_wake_intents WHERE run_id=?", run.ID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, "SELECT id FROM kernel_wake_intents WHERE session_id=? AND state IN ('reserved','bound') LIMIT 1", b.ID).Scan(&id)
	}
	if errors.Is(err, sql.ErrNoRows) {
		if b.Kind == "task" && b.TaskState == contract.TaskWaiting {
			return errors.New("waiting Task requires a reserved wake")
		}
		return nil // Initial/non-notification turns do not require a wake.
	}
	if err != nil {
		return err
	}
	w, err := wakeTx(ctx, tx, id)
	if err != nil {
		return err
	}
	if w.Intent.SessionID != b.ID || w.Intent.State != "reserved" || w.Intent.RunID != run.ID || snapshot.ID != w.Snapshot.ID || snapshot.Digest != w.Snapshot.Digest || string(snapshot.Input) != string(w.Snapshot.Input) {
		return errors.New("run must use the reserved wake and immutable snapshot")
	}
	if err = k.checkWakeTargetsTx(ctx, tx, b, w.Intent); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_wake_intents SET state='bound' WHERE id=?", id)
	return err
}

// finishWakeTx is called by the result owner in its application transaction.
// A physical completion or delivery ack alone never reaches this function.
func (k *KernelStore) finishWakeTx(ctx context.Context, tx *sql.Tx, sessionID, runID, outcome string) error {
	var id string
	err := tx.QueryRowContext(ctx, "SELECT id FROM kernel_wake_intents WHERE session_id=? AND run_id=?", sessionID, runID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	w, err := wakeTx(ctx, tx, id)
	if err != nil {
		return err
	}
	if w.Intent.State != "bound" {
		return errors.New("wake outcome requires its bound run")
	}
	next, runBinding := outcome, runID
	if outcome == "failed" {
		next, runBinding = "pending", "" // A proven stopped failed attempt may retry.
	}
	seenMessages := map[string]bool{}
	for _, notificationID := range w.Intent.NotificationIDs {
		n, err := notificationTx(ctx, tx, notificationID)
		if err != nil {
			return err
		}
		if n.State != "bound_to_run" || n.RunID != runID || n.SessionID != sessionID {
			return errors.New("wake outcome input binding conflict")
		}
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_notifications SET state=?,run_id=? WHERE id=?", next, runBinding, n.ID); err != nil {
			return err
		}
		if n.MessageID != "" && !seenMessages[n.MessageID] {
			seenMessages[n.MessageID] = true
			result, err := tx.ExecContext(ctx, "UPDATE kernel_mailbox SET state=?,run_id=? WHERE message_id=? AND session_id=? AND state='bound_to_run' AND run_id=?", next, runBinding, n.MessageID, sessionID, runID)
			if err != nil {
				return err
			}
			count, err := result.RowsAffected()
			if err != nil || count != 1 {
				return errors.New("wake outcome mailbox binding conflict")
			}
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_wake_intents SET state=? WHERE id=?", outcome, id)
	return err
}

// ReconcileUnstartedWake retires an obsolete intent only after proving that no
// run was reserved and no prepared process remains. Other still-current Waves
// in a shared main batch return to pending. Exhausted budgets also retire an
// unstarted batch, retaining its inputs as pending; pause alone never retires it.
func (k *KernelStore) ReconcileUnstartedWake(ctx context.Context, id string) (WakeReservation, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return WakeReservation{}, err
	}
	defer tx.Rollback()
	w, err := wakeTx(ctx, tx, id)
	if err != nil {
		return w, err
	}
	if w.Intent.State == "superseded" {
		return w, nil
	}
	if w.Intent.State != "reserved" {
		return w, ErrWakeHeld
	}
	var active string
	var stopped, creating, runs int
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped,creating FROM kernel_sessions WHERE id=?", w.Intent.SessionID).Scan(&active, &stopped, &creating); err != nil {
		return w, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM kernel_run_bindings WHERE run_id=?", w.Intent.RunID).Scan(&runs); err != nil {
		return w, err
	}
	if active != "" || stopped != 1 || creating != 0 || runs != 0 {
		return w, ErrWakeHeld
	}
	b, err := sessionBindingTx(ctx, tx, w.Intent.SessionID)
	if err != nil {
		return w, err
	}
	obsolete := map[string]bool{}
	hasObsolete := false
	budgetHeld := map[string]bool{}
	for _, notificationID := range w.Intent.NotificationIDs {
		n, err := notificationTx(ctx, tx, notificationID)
		if err != nil {
			return w, err
		}
		wave, err := loadWave(ctx, tx, n.Target.WaveID)
		if err != nil {
			return w, err
		}
		old := obsoleteWakeNotification(b, wave, n, k.clock())
		obsolete[n.ID], hasObsolete = old, hasObsolete || old
		held, err := wakeBudgetHeldTx(ctx, tx, b, wave)
		if err != nil {
			return w, err
		}
		budgetHeld[n.ID] = held
		hasObsolete = hasObsolete || held
	}
	if !hasObsolete {
		return w, ErrWakeHeld
	}
	seenMessages := map[string]bool{}
	for _, notificationID := range w.Intent.NotificationIDs {
		n, err := notificationTx(ctx, tx, notificationID)
		if err != nil {
			return w, err
		}
		if n.SessionID != b.ID || n.State != "bound_to_run" || n.RunID != w.Intent.RunID {
			return w, errors.New("unstarted wake input binding conflict")
		}
		next, nextRun := "pending", ""
		if obsolete[n.ID] {
			next, nextRun = "archived", w.Intent.RunID
		}
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_notifications SET state=?,run_id=? WHERE id=?", next, nextRun, n.ID); err != nil {
			return w, err
		}
		if n.MessageID != "" && !seenMessages[n.MessageID] {
			seenMessages[n.MessageID] = true
			result, err := tx.ExecContext(ctx, "UPDATE kernel_mailbox SET state=?,run_id=? WHERE message_id=? AND session_id=? AND state='bound_to_run' AND run_id=?", next, nextRun, n.MessageID, b.ID, w.Intent.RunID)
			if err != nil {
				return w, err
			}
			if count, err := result.RowsAffected(); err != nil || count != 1 {
				return w, errors.New("unstarted wake mailbox binding conflict")
			}
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_wake_intents SET state='superseded' WHERE id=?", id); err != nil {
		return w, err
	}
	if err = kernelRecordTx(ctx, tx, "wake_disposition", id, b.ID, map[string]any{"reason": "unstarted input superseded, terminal or budget held", "obsolete_notifications": obsolete, "budget_held_notifications": budgetHeld, "created_at": k.clock()}); err != nil {
		return w, err
	}
	w.Intent.State = "superseded"
	return w, tx.Commit()
}
