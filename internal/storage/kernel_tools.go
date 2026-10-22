package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

var _ contract.KernelToolBackend = (*KernelStore)(nil)
var ErrKernelToolUnavailable = errors.New("kernel tool operation is not connected")

type kernelToolOperation struct {
	ID             string          `json:"id"`
	SessionID      string          `json:"session_id"`
	RunID          string          `json:"run_id"`
	ThreadID       string          `json:"thread_id"`
	TurnID         string          `json:"turn_id"`
	CallID         string          `json:"call_id"`
	Name           string          `json:"name"`
	ArgumentDigest string          `json:"argument_digest"`
	Target         contract.Target `json:"target"`
	SnapshotID     string          `json:"snapshot_id"`
	CreatedAt      time.Time       `json:"created_at"`
}

func kernelToolTarget(command any) (string, contract.Target) {
	switch a := command.(type) {
	case *contract.CreateWaveRequest:
		return "wave_create", contract.Target{}
	case *contract.PlanWaveRequest:
		return "wave_plan", a.Target
	case *contract.StateRequest:
		return "state_get", a.Target
	case *contract.InstructRequest:
		return "task_instruct", a.Target
	case *contract.WaitRequest:
		return "session_wait", a.Target
	case *contract.ReadArtifactRequest:
		return "artifact_read", a.Target
	case *contract.PublishArtifactRequest:
		return "artifact_publish", a.Target
	case *contract.SendMessageRequest:
		return "send_message", a.Target
	}
	return "", contract.Target{}
}

// kernelToolAuthorityTx binds a server-observed early turn only to the already
// reserved, active run whose known history baseline excludes that turn. This
// permits a tool call before turn/start replies without accepting an old turn.
func (k *KernelStore) kernelToolAuthorityTx(ctx context.Context, tx *sql.Tx, call contract.ToolInvocation, target contract.Target) (contract.SessionAuthority, contract.SessionBinding, string, error) {
	var a contract.SessionAuthority
	b, err := sessionBindingTx(ctx, tx, call.SessionID)
	if err != nil {
		return a, b, "", err
	}
	var active, runSession, runThread, state, snapshotID string
	var stopped, applied int
	if err = tx.QueryRowContext(ctx, "SELECT active_run_id,process_stopped FROM kernel_sessions WHERE id=?", b.ID).Scan(&active, &stopped); err != nil {
		return a, b, "", err
	}
	if call.RunID == "" || active != call.RunID || stopped != 0 || call.ThreadID != b.ThreadID {
		return a, b, "", errors.New("tool source is not the active prepared session/run")
	}
	if err = tx.QueryRowContext(ctx, "SELECT session_id,thread_id,state,snapshot_id,process_stopped,applied FROM kernel_run_bindings WHERE run_id=?", call.RunID).Scan(&runSession, &runThread, &state, &snapshotID, &stopped, &applied); err != nil {
		return a, b, "", err
	}
	if runSession != b.ID || runThread != b.ThreadID || stopped != 0 || applied != 0 || (state != "starting" && state != "running" && state != "inProgress") {
		return a, b, "", errors.New("tool source run is stopped or inactive")
	}
	var run contract.SessionRun
	if err = completionRecord(ctx, tx, "agent_run", call.RunID, &run); err != nil {
		return a, b, "", err
	}
	if run.ID != call.RunID || run.ThreadID != b.ThreadID || !run.BaselineKnown || run.State != state || call.TurnID == "" || slices.Contains(run.BaselineTurnIDs, call.TurnID) || run.TurnID != "" && run.TurnID != call.TurnID {
		return a, b, "", errors.New("tool source turn is outside the reserved run")
	}
	if run.TurnID == "" {
		run.TurnID, run.State = call.TurnID, "running"
		if err = kernelRecordTx(ctx, tx, "agent_run", run.ID, b.ID, run); err != nil {
			return a, b, "", err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE kernel_run_bindings SET state='running' WHERE run_id=?", run.ID); err != nil {
			return a, b, "", err
		}
	}
	a = contract.SessionAuthority{SessionID: b.ID, ThreadID: b.ThreadID, ActiveTurnID: run.TurnID, Kind: b.Kind, TaskID: b.TaskID, ParentSessionID: b.ParentSessionID, TaskState: b.TaskState, Closed: b.Closed}
	if b.Kind == "task" {
		task, err := loadTask(ctx, tx, b.TaskID)
		if err != nil {
			return a, b, "", err
		}
		wave, err := loadWave(ctx, tx, task.WaveID)
		if err != nil {
			return a, b, "", err
		}
		if wave.Target() != b.Target || task.ParentSessionID != wave.OrchestratorSessionID {
			return a, b, "", errors.New("Task tool source has been superseded")
		}
		a.Target, a.Paused = wave.Target(), wave.DispatchPaused() || task.Paused
		a.Canceled = wave.State == domain.WaveCanceled
		a.Closed = a.Closed || wave.State == domain.WaveCompleted
		if !k.clock().Before(wave.Deadline) {
			return a, b, "", errors.New("tool source Wave deadline exceeded")
		}
	} else if b.Kind == "orchestrator" {
		var raw []byte
		var digest string
		if err = tx.QueryRowContext(ctx, "SELECT digest,data FROM kernel_snapshots WHERE id=?", snapshotID).Scan(&digest, &raw); err != nil {
			return a, b, "", err
		}
		if digest != fmt.Sprintf("%x", sha256.Sum256(raw)) {
			return a, b, "", errors.New("tool source snapshot digest conflict")
		}
		var scope struct {
			SchemaVersion int               `json:"schema_version"`
			SessionID     string            `json:"session_id"`
			Targets       []contract.Target `json:"targets"`
		}
		if err = json.Unmarshal(raw, &scope); err != nil || scope.SchemaVersion != 1 || scope.SessionID != b.ID {
			return a, b, "", errors.New("main tool requires a controller-scoped snapshot")
		}
		if call.Name != "wave_create" {
			wave, err := loadWave(ctx, tx, target.WaveID)
			if err != nil {
				return a, b, "", err
			}
			if wave.OrchestratorSessionID != b.ID || !slices.Contains(scope.Targets, wave.Target()) {
				return a, b, "", errors.New("main tool target is outside this run's Wave scope")
			}
			a.Target, a.Paused = wave.Target(), wave.DispatchPaused()
			if !k.clock().Before(wave.Deadline) {
				return a, b, "", errors.New("tool target Wave deadline exceeded")
			}
		}
	} else {
		return a, b, "", errors.New("unknown tool source session kind")
	}
	return a, b, snapshotID, nil
}

// Apply owns authorization, exact call replay and database effects in one
// transaction. Filesystem publication and Wave command adapters are connected
// separately; advertising a schema never makes an unavailable action succeed.
func (k *KernelStore) Apply(ctx context.Context, call contract.ToolInvocation, command any, guard func(contract.SessionAuthority) error) (contract.ToolReply, error) {
	var reply contract.ToolReply
	name, target := kernelToolTarget(command)
	if guard == nil || name == "" || name != call.Name || call.Namespace != "" || call.SessionID == "" || call.RunID == "" || call.ThreadID == "" || call.TurnID == "" || call.CallID == "" || len(call.Arguments) > contract.MaxSessionInput {
		return reply, errors.New("invalid kernel tool invocation")
	}
	if publication, ok := command.(*contract.PublishArtifactRequest); ok {
		return k.applyKernelPublication(ctx, call, publication, guard)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return reply, err
	}
	defer tx.Rollback()
	a, b, snapshotID, err := k.kernelToolAuthorityTx(ctx, tx, call, target)
	if err != nil {
		return reply, err
	}
	if err = guard(a); err != nil {
		return reply, err
	}
	digest, err := contentDigest(command)
	if err != nil {
		return reply, err
	}
	var oldRaw, oldReply []byte
	var oldName, oldDigest, oldThread, state string
	err = tx.QueryRowContext(ctx, "SELECT name,argument_digest,thread_id,state,result,payload FROM kernel_tool_operations WHERE session_id=? AND turn_id=? AND call_id=?", b.ID, call.TurnID, call.CallID).Scan(&oldName, &oldDigest, &oldThread, &state, &oldReply, &oldRaw)
	if err == nil {
		var prior kernelToolOperation
		if err = json.Unmarshal(oldRaw, &prior); err != nil {
			return reply, err
		}
		if oldName != call.Name || oldDigest != digest || oldThread != b.ThreadID || prior.RunID != call.RunID || prior.SnapshotID != snapshotID || state != "completed" {
			return reply, errors.New("tool call replay identity conflict")
		}
		if err = json.Unmarshal(oldReply, &reply); err != nil {
			return reply, err
		}
		return reply, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return reply, err
	}
	op := kernelToolOperation{ID: structureID("kernel-operation-", b.ID, call.ThreadID+":"+call.TurnID+":"+call.CallID), SessionID: b.ID, RunID: call.RunID, ThreadID: b.ThreadID, TurnID: call.TurnID, CallID: call.CallID, Name: call.Name, ArgumentDigest: digest, Target: target, SnapshotID: snapshotID, CreatedAt: k.clock()}
	reply = contract.ToolReply{SchemaVersion: 1, State: "completed", OperationID: op.ID, SubscriptionIDs: []string{}}
	switch r := command.(type) {
	case *contract.StateRequest:
		content, err := k.kernelStateTx(ctx, tx, b, *r)
		if err != nil {
			return reply, err
		}
		reply.ResourceID, reply.Content = target.WaveID, content
	case *contract.SendMessageRequest:
		m, err := k.sendMessageTx(ctx, tx, MessageRequest{SenderSessionID: b.ID, SourceRunID: call.RunID, Target: target, Key: op.ID, Recipient: r.Recipient, Body: r.Body, ReplyTo: r.ReplyTo})
		if err != nil {
			return reply, err
		}
		reply.ResourceID = m.ID
		reply.SubscriptionIDs = []string{mailboxSubscriptionID(b.ID, target, m.TaskID)}
		raw, err := json.Marshal(map[string]any{"message_id": m.ID, "recipient_session_id": m.RecipientSessionID, "target": m.Target})
		if err != nil {
			return reply, err
		}
		reply.Content = string(raw)
	case *contract.InstructRequest:
		if b.Kind != "orchestrator" {
			return reply, errors.New("instruction target must be a direct child")
		}
		m, err := k.sendMessageTx(ctx, tx, MessageRequest{SenderSessionID: b.ID, SourceRunID: call.RunID, Target: target, Key: op.ID, Recipient: r.TaskID, Body: r.Body})
		if err != nil {
			return reply, err
		}
		reply.ResourceID = m.ID
		reply.SubscriptionIDs = []string{mailboxSubscriptionID(b.ID, target, m.TaskID)}
	case *contract.WaitRequest:
		for _, id := range r.SubscriptionIDs {
			var owner, wave string
			var scope, plan int
			var raw []byte
			if err = tx.QueryRowContext(ctx, "SELECT session_id,wave_id,scope_revision,plan_revision,payload FROM kernel_subscriptions WHERE id=?", id).Scan(&owner, &wave, &scope, &plan, &raw); err != nil {
				return reply, err
			}
			var sub struct {
				Target contract.Target `json:"target"`
			}
			if err = json.Unmarshal(raw, &sub); err != nil {
				return reply, err
			}
			if owner != b.ID || wave != target.WaveID || scope != target.ScopeRevision || plan != target.PlanRevision || sub.Target != target {
				return reply, errors.New("wait subscription is outside this session and target")
			}
		}
		if err = kernelRecordTx(ctx, tx, "kernel_session_wait", op.ID, b.ID, map[string]any{"run_id": call.RunID, "target": target, "subscription_ids": r.SubscriptionIDs}); err != nil {
			return reply, err
		}
		reply.ResourceID, reply.SubscriptionIDs = b.ID, append([]string{}, r.SubscriptionIDs...)
	case *contract.ReadArtifactRequest:
		content, err := k.kernelReadArtifactTx(ctx, tx, b, target, *r)
		if err != nil {
			return reply, err
		}
		reply.ResourceID, reply.Content = r.ArtifactID, content
	default:
		return reply, ErrKernelToolUnavailable
	}
	if len(reply.Content) > contract.MaxInline {
		return reply, errors.New("kernel reply exceeds inline limit")
	}
	raw, err := json.Marshal(op)
	if err != nil {
		return reply, err
	}
	result, err := json.Marshal(reply)
	if err != nil {
		return reply, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_tool_operations(id,session_id,thread_id,turn_id,call_id,name,argument_digest,state,result,payload) VALUES(?,?,?,?,?,?,?,'completed',?,?)", op.ID, b.ID, b.ThreadID, call.TurnID, call.CallID, call.Name, digest, result, raw); err != nil {
		return reply, err
	}
	return reply, tx.Commit()
}

func (k *KernelStore) kernelStateTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, request contract.StateRequest) (string, error) {
	target := request.Target
	page := contract.StatePage{Limit: 16}
	if request.Page != nil {
		page = *request.Page
	}
	if page.Limit < 1 || page.Limit > 64 || request.Page != nil && page.Collection != "tasks" && page.Collection != "subscriptions" {
		return "", errors.New("invalid state page limit")
	}
	tasksEnabled, subscriptionsEnabled := request.Page == nil || page.Collection == "tasks", request.Page == nil || page.Collection == "subscriptions"
	tasksAfter, subscriptionsAfter := "", ""
	if page.Collection == "tasks" {
		tasksAfter = page.After
	}
	if page.Collection == "subscriptions" {
		subscriptionsAfter = page.After
	}
	wave, err := loadWave(ctx, tx, target.WaveID)
	if err != nil {
		return "", err
	}
	type taskState struct {
		ID    string             `json:"id"`
		State contract.TaskState `json:"state"`
	}
	view := struct {
		Target                 contract.Target  `json:"target"`
		WaveState              domain.WaveState `json:"wave_state"`
		Paused                 bool             `json:"paused"`
		SessionID              string           `json:"session_id"`
		TaskID                 string           `json:"task_id"`
		Tasks                  []taskState      `json:"tasks"`
		SubscriptionIDs        []string         `json:"subscription_ids"`
		PendingMessages        int              `json:"pending_messages"`
		PendingNotifications   int              `json:"pending_notifications"`
		NextTasksAfter         string           `json:"next_tasks_after"`
		NextSubscriptionsAfter string           `json:"next_subscriptions_after"`
	}{Target: target, WaveState: wave.State, Paused: wave.DispatchPaused(), SessionID: b.ID, TaskID: b.TaskID, Tasks: []taskState{}, SubscriptionIDs: []string{}}
	allowed := map[string]bool{}
	if b.Kind == "task" {
		task, err := loadTask(ctx, tx, b.TaskID)
		if err != nil {
			return "", err
		}
		allowed[b.TaskID] = true
		for _, dep := range task.Spec.DependsOn {
			allowed[structureID("task-", wave.ID, dep.TaskID)] = true
		}
	}
	rows, err := tx.QueryContext(ctx, "SELECT t.id,t.state FROM kernel_tasks t JOIN kernel_plan_bindings p ON p.task_id=t.id WHERE p.wave_id=? AND p.revision=? AND ?=1 AND t.id>? ORDER BY t.id", wave.ID, wave.PlanRevision, tasksEnabled, tasksAfter)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var task taskState
		if err = rows.Scan(&task.ID, &task.State); err != nil {
			rows.Close()
			return "", err
		}
		if b.Kind == "orchestrator" || allowed[task.ID] {
			view.Tasks = append(view.Tasks, task)
			if len(view.Tasks) > page.Limit {
				break
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	rows, err = tx.QueryContext(ctx, "SELECT id,payload FROM kernel_subscriptions WHERE session_id=? AND wave_id=? AND scope_revision=? AND plan_revision=? AND ?=1 AND id>? ORDER BY id", b.ID, wave.ID, target.ScopeRevision, target.PlanRevision, subscriptionsEnabled, subscriptionsAfter)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return "", err
		}
		var sub struct {
			Target contract.Target `json:"target"`
		}
		if err = json.Unmarshal(raw, &sub); err != nil {
			rows.Close()
			return "", err
		}
		if sub.Target == target {
			view.SubscriptionIDs = append(view.SubscriptionIDs, id)
			if len(view.SubscriptionIDs) > page.Limit {
				break
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	if len(view.Tasks) > page.Limit {
		view.Tasks = view.Tasks[:page.Limit]
		view.NextTasksAfter = view.Tasks[len(view.Tasks)-1].ID
	}
	if len(view.SubscriptionIDs) > page.Limit {
		view.SubscriptionIDs = view.SubscriptionIDs[:page.Limit]
		view.NextSubscriptionsAfter = view.SubscriptionIDs[len(view.SubscriptionIDs)-1]
	}
	for _, item := range []struct {
		query string
		count *int
	}{
		{"SELECT m.payload FROM kernel_mailbox b JOIN kernel_messages m ON m.id=b.message_id AND m.recipient_id=b.session_id WHERE b.session_id=? AND m.wave_id=? AND b.state='pending'", &view.PendingMessages},
		{"SELECT n.payload FROM kernel_notifications n JOIN kernel_subscriptions s ON s.id=n.subscription_id WHERE n.session_id=? AND s.wave_id=? AND n.state='pending'", &view.PendingNotifications},
	} {
		rows, err = tx.QueryContext(ctx, item.query, b.ID, wave.ID)
		if err != nil {
			return "", err
		}
		for rows.Next() {
			var raw []byte
			if err = rows.Scan(&raw); err != nil {
				rows.Close()
				return "", err
			}
			var scope struct {
				Target contract.Target `json:"target"`
			}
			if err = json.Unmarshal(raw, &scope); err != nil {
				rows.Close()
				return "", err
			}
			if scope.Target == target {
				*item.count++
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return "", err
		}
	}
	raw, err := json.Marshal(view)
	return string(raw), err
}
func (k *KernelStore) kernelReadArtifactTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, target contract.Target, request contract.ReadArtifactRequest) (string, error) {
	if request.Offset < 0 || request.Limit < 1 || request.Limit > contract.MaxInline {
		return "", errors.New("invalid Artifact chunk range")
	}
	_, wave, err := artifactAuthorityTx(ctx, tx, b.ID, target)
	if err != nil {
		return "", err
	}
	a, err := loadArtifact(ctx, tx, request.ArtifactID)
	if err != nil {
		return "", err
	}
	if err = artifactReadableTx(ctx, tx, b, wave, a); err != nil {
		return "", err
	}
	data, err := k.readArtifactObject(a)
	if err != nil {
		return "", err
	}
	if request.Offset > int64(len(data)) {
		return "", errors.New("Artifact offset exceeds its size")
	}
	length := min(request.Limit, 4096, len(data)-int(request.Offset))
	chunk := data[int(request.Offset) : int(request.Offset)+length]
	raw, err := json.Marshal(map[string]any{"artifact_id": a.ID, "digest": a.Digest, "offset": request.Offset, "bytes": length, "next_offset": request.Offset + int64(length), "eof": request.Offset+int64(length) == int64(len(data)), "encoding": "base64", "data": base64.StdEncoding.EncodeToString(chunk)})
	return string(raw), err
}
