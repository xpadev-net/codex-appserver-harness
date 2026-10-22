package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type kernelPublication struct {
	Operation      kernelToolOperation `json:"operation"`
	ArtifactID     string              `json:"artifact_id"`
	Import         ArtifactImport      `json:"import"`
	Content        string              `json:"content"`
	SubscriptionID string              `json:"subscription_id"`
}
type kernelOperationSubscription struct {
	ID          string          `json:"id"`
	SessionID   string          `json:"session_id"`
	Target      contract.Target `json:"target"`
	OperationID string          `json:"operation_id"`
}

func kernelOperationSubscriptionID(sessionID, operationID string) string {
	return structureID("operation-subscription-", sessionID, operationID)
}
func (k *KernelStore) ensureOperationSubscriptionTx(ctx context.Context, tx *sql.Tx, op kernelToolOperation) (kernelOperationSubscription, error) {
	sub := kernelOperationSubscription{ID: kernelOperationSubscriptionID(op.SessionID, op.ID), SessionID: op.SessionID, Target: op.Target, OperationID: op.ID}
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT payload FROM kernel_subscriptions WHERE id=?", sub.ID).Scan(&raw)
	if err == nil {
		var old kernelOperationSubscription
		if err = json.Unmarshal(raw, &old); err != nil {
			return sub, err
		}
		if old != sub {
			return sub, errors.New("operation subscription binding conflict")
		}
		return sub, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return sub, err
	}
	raw, err = json.Marshal(sub)
	if err != nil {
		return sub, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO kernel_subscriptions(id,session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload) VALUES(?,?,?,?,?,?,0,?)", sub.ID, sub.SessionID, sub.Target.WaveID, sub.Target.ScopeRevision, sub.Target.PlanRevision, "kernel:operation:"+op.ID, raw)
	return sub, err
}
func (k *KernelStore) applyKernelPublication(ctx context.Context, call contract.ToolInvocation, request *contract.PublishArtifactRequest, guard func(contract.SessionAuthority) error) (contract.ToolReply, error) {
	var reply contract.ToolReply
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return reply, err
	}
	defer tx.Rollback()
	authority, binding, snapshotID, err := k.kernelToolAuthorityTx(ctx, tx, call, request.Target)
	if err != nil {
		return reply, err
	}
	if err = guard(authority); err != nil {
		return reply, err
	}
	digest, err := contentDigest(request)
	if err != nil {
		return reply, err
	}
	var pub kernelPublication
	var oldName, oldDigest, oldThread, state string
	var raw, result []byte
	err = tx.QueryRowContext(ctx, "SELECT name,argument_digest,thread_id,state,result,payload FROM kernel_tool_operations WHERE session_id=? AND turn_id=? AND call_id=?", binding.ID, call.TurnID, call.CallID).Scan(&oldName, &oldDigest, &oldThread, &state, &result, &raw)
	if err == nil {
		var op kernelToolOperation
		if err = json.Unmarshal(raw, &op); err != nil {
			return reply, err
		}
		if oldName != call.Name || oldDigest != digest || oldThread != binding.ThreadID || op.RunID != call.RunID || op.SnapshotID != snapshotID {
			return reply, errors.New("publication call replay identity conflict")
		}
		if state == "completed" {
			if err = json.Unmarshal(result, &reply); err != nil {
				return reply, err
			}
			return reply, tx.Commit()
		}
		if state != "accepted" && state != "unknown" {
			return reply, errors.New("publication has an unsupported disposition")
		}
		if err = completionRecord(ctx, tx, "kernel_publication", op.ID, &pub); err != nil {
			return reply, err
		}
		if pub.Operation != op {
			return reply, errors.New("publication intent identity conflict")
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		op := kernelToolOperation{ID: structureID("kernel-operation-", binding.ID, call.ThreadID+":"+call.TurnID+":"+call.CallID), SessionID: binding.ID, RunID: call.RunID, ThreadID: binding.ThreadID, TurnID: call.TurnID, CallID: call.CallID, Name: call.Name, ArgumentDigest: digest, Target: request.Target, SnapshotID: snapshotID, CreatedAt: k.clock()}
		origin := domain.ArtifactOrigin{Kind: "task", Reference: binding.TaskID}
		if binding.Kind == "orchestrator" {
			origin = domain.ArtifactOrigin{Kind: "session", Reference: binding.ID}
		}
		artifactRequest := ArtifactImport{SessionID: binding.ID, Target: request.Target, Key: op.ID, Kind: request.Kind, ContentType: request.ContentType, Origin: origin, InputArtifactIDs: append([]string{}, request.InputArtifactIDs...)}
		artifact, err := k.reserveArtifactTx(ctx, tx, artifactRequest, []byte(request.Content))
		if err != nil {
			return reply, err
		}
		sub, err := k.ensureOperationSubscriptionTx(ctx, tx, op)
		if err != nil {
			return reply, err
		}
		pub = kernelPublication{Operation: op, ArtifactID: artifact.ID, Import: artifactRequest, Content: request.Content, SubscriptionID: sub.ID}
		reply = contract.ToolReply{SchemaVersion: 1, State: "accepted", OperationID: op.ID, SubscriptionIDs: []string{sub.ID}, ResourceID: artifact.ID}
		raw, err = json.Marshal(op)
		if err != nil {
			return reply, err
		}
		result, err = json.Marshal(reply)
		if err != nil {
			return reply, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_tool_operations(id,session_id,thread_id,turn_id,call_id,name,argument_digest,state,result,payload) VALUES(?,?,?,?,?,?,?,'accepted',?,?)", op.ID, binding.ID, binding.ThreadID, call.TurnID, call.CallID, call.Name, digest, result, raw); err != nil {
			return reply, err
		}
		if err = kernelRecordTx(ctx, tx, "kernel_publication", op.ID, binding.ID, pub); err != nil {
			return reply, err
		}
		if err = kernelRecordTx(ctx, tx, "artifact_kernel_operation", artifact.ID, artifact.WaveID, op.ID); err != nil {
			return reply, err
		}
	} else {
		return reply, err
	}
	if err = tx.Commit(); err != nil {
		return reply, err
	}
	// No filesystem bytes exist before the source-authorized operation, manifest
	// reservation, input grants and capacity have committed. Every retry uses
	// these same bytes/identity. Already admitted facts keep their original target.
	_, writeErr := k.ImportArtifact(ctx, pub.Import, []byte(pub.Content))
	return k.kernelPublicationReply(ctx, pub, writeErr)
}
func (k *KernelStore) kernelPublicationReply(ctx context.Context, pub kernelPublication, writeErr error) (contract.ToolReply, error) {
	var reply contract.ToolReply
	var state string
	var raw []byte
	if err := k.DB.QueryRowContext(ctx, "SELECT state,result FROM kernel_tool_operations WHERE id=?", pub.Operation.ID).Scan(&state, &raw); err != nil {
		return reply, err
	}
	if state == "completed" {
		if err := json.Unmarshal(raw, &reply); err != nil {
			return reply, err
		}
		return reply, nil
	}
	if writeErr != nil {
		if err := k.awaitKernelPublication(context.WithoutCancel(ctx), pub); err != nil {
			return reply, err
		}
		// Errors may contain private host paths. The model gets only an immutable
		// reconciliation ID and owned receive subscription, never those paths.
		return contract.ToolReply{SchemaVersion: 1, State: "unknown", OperationID: pub.Operation.ID, SubscriptionIDs: []string{pub.SubscriptionID}, ResourceID: pub.ArtifactID, Content: "Publication is pending controller reconciliation."}, nil
	}
	return reply, errors.New("committed publication lacks its operation result")
}

// Called with the immutable manifest transition. A publication is an admitted
// intent, so recovery can finish its old-scope facts after a process stops; it
// does not acquire authority for current-scope work or acceptance.
func (k *KernelStore) finishKernelPublicationTx(ctx context.Context, tx *sql.Tx, artifact domain.Artifact) error {
	var id string
	err := completionRecord(ctx, tx, "artifact_kernel_operation", artifact.ID, &id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var pub kernelPublication
	if err = completionRecord(ctx, tx, "kernel_publication", id, &pub); err != nil {
		return err
	}
	if artifact.ID != pub.ArtifactID || artifact.Target() != pub.Operation.Target || artifact.SessionID != pub.Operation.SessionID || artifact.State != "committed" {
		return errors.New("publication manifest/operation binding conflict")
	}
	var state string
	if err = tx.QueryRowContext(ctx, "SELECT state FROM kernel_tool_operations WHERE id=?", id).Scan(&state); err != nil {
		return err
	}
	if state == "completed" {
		return nil
	}
	if state != "accepted" && state != "unknown" {
		return errors.New("publication operation cannot be completed")
	}
	reply := contract.ToolReply{SchemaVersion: 1, State: "completed", OperationID: id, SubscriptionIDs: []string{pub.SubscriptionID}, ResourceID: artifact.ID}
	raw, err := json.Marshal(map[string]any{"artifact_id": artifact.ID, "digest": artifact.Digest, "bytes": artifact.Bytes, "target": artifact.Target()})
	if err != nil {
		return err
	}
	reply.Content = string(raw)
	raw, err = json.Marshal(reply)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_tool_operations SET state='completed',result=? WHERE id=?", raw, id); err != nil {
		return err
	}
	// Recovery needs plaintext only while the object is unfinished. Once the
	// immutable bytes and result commit, retain provenance without a second
	// durable content copy that could outlive artifact retention.
	pub.Content = ""
	if err = kernelRecordTx(ctx, tx, "kernel_publication", id, pub.Operation.SessionID, pub); err != nil {
		return err
	}
	wave, err := loadWave(ctx, tx, artifact.WaveID)
	if err != nil {
		return err
	}
	wave.ScopeRevision, wave.PlanRevision, wave.Policy.Revision = artifact.ScopeRevision, artifact.PlanRevision, artifact.PolicyRevision
	return k.waveEventTx(ctx, tx, wave, "kernel_operation_completed", artifact.TaskID, id+":completed", map[string]string{"operation_id": id, "artifact_id": artifact.ID})
}
func (k *KernelStore) fanoutKernelOperationTx(ctx context.Context, tx *sql.Tx, intent notificationIntent) error {
	if intent.Kind != "kernel_operation_completed" {
		return nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT payload FROM kernel_events WHERE id=? AND kind='kernel_operation_completed'", intent.EventID).Scan(&raw); err != nil {
		return err
	}
	var event struct {
		OperationID string `json:"operation_id"`
		ArtifactID  string `json:"artifact_id"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return err
	}
	var pub kernelPublication
	if err := completionRecord(ctx, tx, "kernel_publication", event.OperationID, &pub); err != nil {
		return err
	}
	if pub.ArtifactID != event.ArtifactID || pub.Operation.Target != intent.Target || intent.EventID != pub.Operation.ID+":completed" {
		return errors.New("operation notification binding conflict")
	}
	var state string
	if err := tx.QueryRowContext(ctx, "SELECT state FROM kernel_tool_operations WHERE id=?", pub.Operation.ID).Scan(&state); err != nil {
		return err
	}
	if state != "completed" {
		return errors.New("operation notification precedes completion")
	}
	sub, err := k.ensureOperationSubscriptionTx(ctx, tx, pub.Operation)
	if err != nil {
		return err
	}
	notificationState, notificationRun, err := k.publicationNotificationStateTx(ctx, tx, pub)
	if err != nil {
		return err
	}
	n := Notification{ID: structureID("notification-", sub.ID, intent.EventID), SubscriptionID: sub.ID, SessionID: sub.SessionID, Target: intent.Target, SourceTarget: intent.Target, EventID: intent.EventID, SourceEventID: intent.EventID, EventBoundary: intent.Sequence, ObservedVia: "event", Kind: "operation", TaskID: intent.TaskID, OperationID: pub.Operation.ID, ArtifactID: pub.ArtifactID, OperationState: "completed", State: notificationState, RunID: notificationRun, CreatedAt: k.clock()}
	raw, err = json.Marshal(n)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,run_id,payload) VALUES(?,?,?,?,?,?,?) ON CONFLICT(subscription_id,event_id) DO NOTHING", n.ID, sub.ID, intent.EventID, sub.SessionID, n.State, n.RunID, raw); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_subscriptions SET cursor=max(cursor,?) WHERE id=?", intent.Sequence, sub.ID)
	return err
}

// ReconcileKernelPublications repairs bytes from the admitted controller record,
// then commits manifest/result/notification once. It does not need a model turn.
type KernelPublicationReconciliation struct {
	Completed []string `json:"completed"`
	Pending   []string `json:"pending"`
}

func (k *KernelStore) ReconcileKernelPublications(ctx context.Context, limit int) (KernelPublicationReconciliation, error) {
	report := KernelPublicationReconciliation{Completed: []string{}, Pending: []string{}}
	if limit < 1 || limit > 1000 {
		return report, errors.New("invalid publication reconciliation limit")
	}
	rows, err := k.DB.QueryContext(ctx, "SELECT p.payload FROM records p JOIN kernel_tool_operations o ON o.id=p.id WHERE p.kind='kernel_publication' AND o.state IN ('accepted','unknown') ORDER BY p.id LIMIT ?", limit)
	if err != nil {
		return report, err
	}
	publications := []kernelPublication{}
	for rows.Next() {
		var raw []byte
		var pub kernelPublication
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &pub)
		}
		if err != nil {
			rows.Close()
			return report, err
		}
		publications = append(publications, pub)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return report, err
	}
	for _, pub := range publications {
		if _, err = k.ImportArtifact(ctx, pub.Import, []byte(pub.Content)); err != nil {
			report.Pending = append(report.Pending, pub.Operation.ID)
			continue
		}
		reply, err := k.kernelPublicationReply(ctx, pub, nil)
		if err != nil {
			return report, err
		}
		if reply.State != "completed" {
			return report, fmt.Errorf("publication %s is not committed", pub.Operation.ID)
		}
		report.Completed = append(report.Completed, pub.Operation.ID)
	}
	return report, nil
}

// Direct successful tool results belong to the current run's decision. They
// must not become a second wake merely because a completion event was saved.
func (k *KernelStore) publicationNotificationStateTx(ctx context.Context, tx *sql.Tx, pub kernelPublication) (string, string, error) {
	var awaited bool
	err := completionRecord(ctx, tx, "kernel_publication_awaited", pub.Operation.ID, &awaited)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", "", err
	}
	if awaited {
		return "pending", "", nil
	}
	var state string
	var applied int
	if err = tx.QueryRowContext(ctx, "SELECT state,applied FROM kernel_run_bindings WHERE run_id=? AND session_id=?", pub.Operation.RunID, pub.Operation.SessionID).Scan(&state, &applied); err != nil {
		return "", "", err
	}
	if applied == 0 {
		return "bound_to_tool", pub.Operation.RunID, nil
	}
	if state == "failed" || state == "interrupted" {
		return "pending", "", nil
	}
	var result ResultApplication
	err = completionRecord(ctx, tx, "task_result_application", pub.Operation.RunID, &result)
	if err == nil && !result.Archived && result.ResultID != "" {
		return "applied", pub.Operation.RunID, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", "", err
	}
	return "archived", pub.Operation.RunID, nil
}
func (k *KernelStore) awaitKernelPublication(ctx context.Context, pub kernelPublication) error {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = kernelRecordTx(ctx, tx, "kernel_publication_awaited", pub.Operation.ID, pub.Operation.SessionID, true); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE kernel_notifications SET state='pending',run_id='' WHERE subscription_id=? AND state='bound_to_tool' AND run_id=?", pub.SubscriptionID, pub.Operation.RunID); err != nil {
		return err
	}
	return tx.Commit()
}
func (k *KernelStore) finishDirectOperationInputsTx(ctx context.Context, tx *sql.Tx, sessionID, runID, outcome string) error {
	next, nextRun := outcome, runID
	if outcome == "failed" {
		next, nextRun = "pending", ""
	}
	_, err := tx.ExecContext(ctx, "UPDATE kernel_notifications SET state=?,run_id=? WHERE session_id=? AND run_id=? AND state='bound_to_tool'", next, nextRun, sessionID, runID)
	return err
}
