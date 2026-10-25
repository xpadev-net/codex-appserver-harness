package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

type ProductSplitCompletionChild struct {
	WorkItemID    string `json:"work_item_id"`
	BindingID     string `json:"binding_id"`
	BindingDigest string `json:"binding_digest"`
	ScopeDigest   string `json:"scope_digest"`
	RepositoryID  int64  `json:"repository_id"`
	IssueNumber   int    `json:"issue_number"`
	Depth         int    `json:"depth"`
}

// Registration is metadata completion, not executable readiness or quality.
type ProductSplitCompletionSummary struct {
	Stage               string                        `json:"stage"`
	ParentWorkItemID    string                        `json:"parent_work_item_id"`
	RegistrationsID     string                        `json:"registrations_id"`
	RegistrationsDigest string                        `json:"registrations_digest"`
	Children            []ProductSplitCompletionChild `json:"children"`
}
type productSplitCompletionValue struct {
	ProductSplitChildScopesValue
	AncestorSet    *ProductSplitAncestorWitnesses `json:"ancestor_set,omitempty"`
	Source         kernelToolOperation            `json:"source"`
	SubscriptionID string                         `json:"subscription_id"`
	Summary        ProductSplitCompletionSummary  `json:"summary"`
}

func productSplitCompletionAncestors(registered ProductSplitRegistrations) *ProductSplitAncestorWitnesses {
	if len(registered.Value.Registrations) == 0 || registered.Value.Registrations[0].Value.SplitScope == nil {
		return nil
	}
	return registered.Value.Registrations[0].Value.SplitScope.AncestorSet
}

type productSplitCompletion struct {
	Value  productSplitCompletionValue `json:"value"`
	Digest string                      `json:"digest"`
}

func productSplitCompletionSummary(registered ProductSplitRegistrations) ProductSplitCompletionSummary {
	summary := ProductSplitCompletionSummary{Stage: "registered", ParentWorkItemID: registered.Value.Request.ParentWorkItemID, RegistrationsID: registered.ID, RegistrationsDigest: registered.Digest, Children: []ProductSplitCompletionChild{}}
	for _, binding := range registered.Value.Children {
		v := binding.Value
		summary.Children = append(summary.Children, ProductSplitCompletionChild{WorkItemID: v.Child.Value.WorkItemID, BindingID: binding.ID, BindingDigest: binding.Digest, ScopeDigest: v.ScopeDigest, RepositoryID: v.Issue.RepositoryID, IssueNumber: v.Issue.Number, Depth: v.Child.Value.Depth})
	}
	return summary
}

func (k *KernelStore) queueProductSplitCompletionTx(ctx context.Context, tx *sql.Tx, registered ProductSplitRegistrations) error {
	adoption, model, err := productSplitNotificationSourceTx(ctx, tx, registered)
	if err != nil || !model {
		return err
	}
	op := adoption.Source
	w, err := loadWave(ctx, tx, op.Target.WaveID)
	if err != nil {
		return err
	}
	if w.Target() != op.Target || w.OrchestratorSessionID != op.SessionID {
		return ErrProductSplitHeld
	}
	subID := adoption.SubscriptionID
	if subID == "" {
		// Older accepted calls had no completion watch. Their original reply is
		// immutable; the controller may subscribe that authenticated source here.
		sub, err := k.ensureOperationSubscriptionTx(ctx, tx, op)
		if err != nil {
			return err
		}
		subID = sub.ID
	}
	if err := checkProductSplitSubscriptionTx(ctx, tx, op, subID); err != nil {
		return err
	}
	var known bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM records WHERE kind='product_split_completion' AND id=?) OR EXISTS(SELECT 1 FROM kernel_events WHERE id=?)", op.ID, op.ID+":split-registered").Scan(&known); err != nil {
		return err
	}
	if known {
		return ErrProductSplitHeld
	}
	value := productSplitCompletionValue{ProductSplitChildScopesValue: registered.Value.ProductSplitChildScopesValue, AncestorSet: productSplitCompletionAncestors(registered), Source: op, SubscriptionID: subID, Summary: productSplitCompletionSummary(registered)}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(raw) > 16<<20 {
		return ErrProductSplitHeld
	}
	digest, err := contentDigest(value)
	if err != nil {
		return err
	}
	if err := kernelRecordTx(ctx, tx, "product_split_completion", op.ID, w.ID, productSplitCompletion{Value: value, Digest: digest}); err != nil {
		return err
	}
	return k.waveEventTx(ctx, tx, w, "product_split_registered", "", op.ID+":split-registered", map[string]string{"operation_id": op.ID, "completion_digest": digest})
}

func loadProductSplitCompletionTx(ctx context.Context, tx *sql.Tx, id string) (productSplitCompletion, error) {
	var saved productSplitCompletion
	if err := taskEvidenceRecord(ctx, tx, "product_split_completion", id, &saved); err != nil {
		return saved, splitEvidenceErr(err)
	}
	digest, err := contentDigest(saved.Value)
	if err != nil {
		return saved, err
	}
	if digest != saved.Digest || saved.Value.Source.ID != id {
		return saved, ErrProductSplitHeld
	}
	registered, err := loadProductSplitRegistrations(ctx, tx, saved.Value.Request)
	if err != nil {
		return saved, splitEvidenceErr(err)
	}
	adoption, model, err := productSplitNotificationSourceTx(ctx, tx, registered)
	if err != nil {
		return saved, err
	}
	if !model || adoption.Source != saved.Value.Source || !reflect.DeepEqual(productSplitCompletionAncestors(registered), saved.Value.AncestorSet) || !reflect.DeepEqual(productSplitCompletionSummary(registered), saved.Value.Summary) || !reflect.DeepEqual(registered.Value.ProductSplitChildScopesValue, saved.Value.ProductSplitChildScopesValue) {
		return saved, ErrProductSplitHeld
	}
	if adoption.SubscriptionID != "" && adoption.SubscriptionID != saved.Value.SubscriptionID {
		return saved, ErrProductSplitHeld
	}
	if err := checkProductSplitSubscriptionTx(ctx, tx, saved.Value.Source, saved.Value.SubscriptionID); err != nil {
		return saved, err
	}
	return saved, nil
}

func (k *KernelStore) fanoutProductSplitCompletionTx(ctx context.Context, tx *sql.Tx, intent notificationIntent) error {
	if intent.Kind != "product_split_registered" {
		return nil
	}
	var raw []byte
	var sequence int64
	if err := tx.QueryRowContext(ctx, "SELECT sequence,payload FROM kernel_events WHERE id=? AND kind='product_split_registered'", intent.EventID).Scan(&sequence, &raw); err != nil {
		return err
	}
	var event struct {
		OperationID      string `json:"operation_id"`
		CompletionDigest string `json:"completion_digest"`
	}
	if json.Unmarshal(raw, &event) != nil {
		return errors.Join(ErrProductSplitHeld, ErrTaskEvidence)
	}
	saved, err := loadProductSplitCompletionTx(ctx, tx, event.OperationID)
	if err != nil {
		return err
	}
	v := saved.Value
	if saved.Digest != event.CompletionDigest || v.Source.Target != intent.Target || sequence != intent.Sequence || intent.EventID != v.Source.ID+":split-registered" || intent.TaskID != "" {
		return ErrProductSplitHeld
	}
	n := Notification{ID: structureID("notification-", v.SubscriptionID, intent.EventID), SubscriptionID: v.SubscriptionID, SessionID: v.Source.SessionID, Target: intent.Target, SourceTarget: intent.Target, EventID: intent.EventID, SourceEventID: intent.EventID, EventBoundary: sequence, ObservedVia: "event", Kind: "split_registered", OperationID: v.Source.ID, OperationState: "completed", State: "pending", CreatedAt: k.clock(), Split: &v.Summary}
	raw, err = json.Marshal(n)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,run_id,payload) VALUES(?,?,?,?,?,'',?) ON CONFLICT(subscription_id,event_id) DO NOTHING", n.ID, n.SubscriptionID, n.EventID, n.SessionID, n.State, raw); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_subscriptions SET cursor=max(cursor,?) WHERE id=?", sequence, n.SubscriptionID)
	return err
}

func checkProductSplitCompletionWakeTx(ctx context.Context, tx *sql.Tx, b contract.SessionBinding, n Notification) error {
	saved, err := loadProductSplitCompletionTx(ctx, tx, n.OperationID)
	if err != nil {
		if readinessHoldError(err) {
			return ErrWakeHeld
		}
		return err
	}
	v := saved.Value
	if b.Kind != "orchestrator" || b.ID != v.Source.SessionID || n.SessionID != b.ID || n.Kind != "split_registered" || n.SourceTarget != v.Source.Target || n.Target != v.Source.Target || n.SubscriptionID != v.SubscriptionID || n.EventID != v.Source.ID+":split-registered" || n.SourceEventID != n.EventID || n.ObservedVia != "event" || n.OperationState != "completed" || n.TaskID != "" || n.Split == nil || !reflect.DeepEqual(*n.Split, v.Summary) {
		return ErrWakeHeld
	}
	var event notificationIntent
	if err := taskEvidenceRecord(ctx, tx, "notification_intent", n.EventID, &event); err != nil {
		if readinessHoldError(err) {
			return ErrWakeHeld
		}
		return err
	}
	if event.EventID != n.EventID || event.Target != n.Target || event.Sequence != n.EventBoundary || event.Kind != "product_split_registered" || event.TaskID != "" {
		return ErrWakeHeld
	}
	var kind string
	var sequence int64
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT kind,sequence,payload FROM kernel_events WHERE id=?", n.EventID).Scan(&kind, &sequence, &raw); err != nil {
		if readinessHoldError(err) {
			return ErrWakeHeld
		}
		return err
	}
	var source struct {
		OperationID      string `json:"operation_id"`
		CompletionDigest string `json:"completion_digest"`
	}
	if json.Unmarshal(raw, &source) != nil || kind != event.Kind || sequence != n.EventBoundary || source.OperationID != v.Source.ID || source.CompletionDigest != saved.Digest {
		return ErrWakeHeld
	}
	return nil
}
