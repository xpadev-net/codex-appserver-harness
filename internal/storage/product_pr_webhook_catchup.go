package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

type PRWebhookObservation struct {
	SubscriptionID string `json:"subscription_id"`
	ResourceKey    string `json:"resource_key"`
	DeliveryID     string `json:"delivery_id"`
	Sequence       int64  `json:"sequence"`
	Digest         string `json:"digest"`
	Kind           string `json:"kind"`
	Action         string `json:"action"`
	ActorID        int64  `json:"actor_id"`
	HeadSHA        string `json:"head_sha"`
	SubjectSHA     string `json:"subject_sha"`
}

func webhookPRObservation(d PRRawDelivery, number int) (PRWebhookObservation, bool) {
	var h struct {
		Action string `json:"action"`
		Review struct {
			CommitID string `json:"commit_id"`
		} `json:"review"`
		Comment struct {
			CommitID string `json:"commit_id"`
		} `json:"comment"`
		Sender struct {
			ID int64 `json:"id"`
		} `json:"sender"`
		PullRequest struct {
			Number int `json:"number"`
			Head   struct {
				SHA string `json:"sha"`
			} `json:"head"`
		} `json:"pull_request"`
		Issue struct {
			Number      int             `json:"number"`
			PullRequest json.RawMessage `json:"pull_request"`
		} `json:"issue"`
		CheckRun struct {
			HeadSHA      string `json:"head_sha"`
			PullRequests []struct {
				Number int `json:"number"`
			} `json:"pull_requests"`
		} `json:"check_run"`
		CheckSuite struct {
			HeadSHA      string `json:"head_sha"`
			PullRequests []struct {
				Number int `json:"number"`
			} `json:"pull_requests"`
		} `json:"check_suite"`
		WorkflowRun struct {
			HeadSHA      string `json:"head_sha"`
			PullRequests []struct {
				Number int `json:"number"`
			} `json:"pull_requests"`
		} `json:"workflow_run"`
	}
	if json.Unmarshal(d.Payload, &h) != nil {
		return PRWebhookObservation{}, false
	}
	match, sha := false, ""
	switch d.Event {
	case "issue_comment":
		match = h.Issue.Number == number && len(h.Issue.PullRequest) > 0 && string(h.Issue.PullRequest) != "null"
	case "pull_request", "pull_request_review", "pull_request_review_comment":
		match = h.PullRequest.Number == number
		sha = h.PullRequest.Head.SHA
	case "check_run":
		for _, p := range h.CheckRun.PullRequests {
			match = match || p.Number == number
		}
		sha = h.CheckRun.HeadSHA
	case "check_suite":
		for _, p := range h.CheckSuite.PullRequests {
			match = match || p.Number == number
		}
		sha = h.CheckSuite.HeadSHA
	case "workflow_run":
		for _, p := range h.WorkflowRun.PullRequests {
			match = match || p.Number == number
		}
		sha = h.WorkflowRun.HeadSHA
	default:
		return PRWebhookObservation{}, false
	}
	if !match {
		return PRWebhookObservation{}, false
	}
	if !fixedSHA(sha) {
		sha = ""
	}
	subject := sha
	if d.Event == "pull_request_review" {
		subject = h.Review.CommitID
	} else if d.Event == "pull_request_review_comment" {
		subject = h.Comment.CommitID
	}
	if !fixedSHA(subject) {
		subject = ""
	}
	action := h.Action
	switch action {
	case "created", "edited", "deleted", "submitted", "dismissed", "closed", "reopened", "synchronize", "opened", "completed", "requested", "rerequested", "in_progress", "queued", "labeled", "unlabeled", "ready_for_review", "converted_to_draft":
	default:
		action = "other"
	}
	return PRWebhookObservation{DeliveryID: d.ID, Sequence: d.Sequence, Digest: d.Digest, Kind: d.Event, Action: action, ActorID: h.Sender.ID, HeadSHA: sha, SubjectSHA: subject}, true
}

type PRWebhookReconcileResult struct {
	Advanced []string `json:"advanced"`
	More     bool     `json:"more"`
}

func (k *KernelStore) ReconcilePRWebhookNotifications(ctx context.Context, limit int) (PRWebhookReconcileResult, error) {
	completed := PRWebhookReconcileResult{Advanced: []string{}}
	if limit < 1 || limit > 128 {
		return completed, errors.New("bounded webhook watch batch required")
	}
	ids, more, err := k.nextPRBoundWatchBatch(ctx, limit)
	if err != nil {
		return completed, err
	}
	completed.More = more
	var failures []error
	for _, id := range ids {
		count, err := k.catchUpPRWebhookWatch(ctx, id)
		if err != nil {
			failures = append(failures, err)
		} else if count > 0 {
			completed.Advanced = append(completed.Advanced, id)
		}
	}
	// A backlog on an earlier watch must survive the final idle watch page.
	// Stop only after a complete pass without progress; a failing watch alone
	// does not mark the pass active and cannot force an endless recheck loop.
	continued, err := k.finishPRWebhookWatchPass(ctx, len(completed.Advanced) > 0, more)
	if err != nil {
		failures = append(failures, err)
	} else {
		completed.More = continued
	}
	return completed, errors.Join(failures...)
}

func (k *KernelStore) finishPRWebhookWatchPass(ctx context.Context, progress, morePages bool) (bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var previous bool
	err = completionRecord(ctx, tx, "kernel_reconcile_cursor", "pr_webhook_pass_active", &previous)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	dirty := previous || progress
	if morePages && dirty {
		if err = kernelRecordTx(ctx, tx, "kernel_reconcile_cursor", "pr_webhook_pass_active", "controller", true); err != nil {
			return false, err
		}
	} else if !morePages && err == nil {
		if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='kernel_reconcile_cursor' AND id='pr_webhook_pass_active'"); err != nil {
			return false, err
		}
	}
	return morePages || dirty, tx.Commit()
}
func (k *KernelStore) nextPRBoundWatchBatch(ctx context.Context, limit int) ([]string, bool, error) {
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	var after string
	err = completionRecord(ctx, tx, "kernel_reconcile_cursor", "pr_webhook_watches", &after)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	read := func(cursor string) ([]string, error) {
		return inputIDsTx(ctx, tx, "SELECT id FROM records WHERE kind='product_pr_subscription_intent' AND json_extract(payload,'$.state')='bound' AND id>? ORDER BY id LIMIT ?", cursor, limit+1)
	}
	ids, err := read(after)
	if err != nil {
		return nil, false, err
	}
	if len(ids) == 0 && after != "" {
		ids, err = read("")
		if err != nil {
			return nil, false, err
		}
	}
	more := len(ids) > limit
	if more {
		ids = ids[:limit]
	}
	next := ""
	if len(ids) > 0 {
		next = ids[len(ids)-1]
	}
	if next != after {
		if err = kernelRecordTx(ctx, tx, "kernel_reconcile_cursor", "pr_webhook_watches", "controller", next); err != nil {
			return nil, false, err
		}
	}
	return ids, more, tx.Commit()
}
func (k *KernelStore) catchUpPRWebhookWatch(ctx context.Context, id string) (int, error) {
	var watch PRSubscriptionIntent
	if err := k.Record(ctx, "product_pr_subscription_intent", id, &watch); err != nil {
		return 0, err
	}
	if watch.ID != id || watch.State != "bound" || watch.ResourceKey == "" || watch.PRNumber < 1 || watch.CreatedAt.IsZero() {
		return 0, errors.New("bound webhook subscriber required")
	}
	var after int64
	err := k.Record(ctx, "product_pr_webhook_cursor", id, &after)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	page, err := k.PRRawDeliveries(ctx, watch.RepositoryID, after, 16)
	if err != nil {
		return 0, err
	}
	if len(page.Deliveries) == 0 {
		return 0, nil
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var fresh PRSubscriptionIntent
	if err = completionRecord(ctx, tx, "product_pr_subscription_intent", id, &fresh); err != nil {
		return 0, err
	}
	if fresh != watch {
		return 0, errors.New("webhook watch changed; retry from its cursor")
	}
	var current int64
	err = completionRecord(ctx, tx, "product_pr_webhook_cursor", id, &current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if current != after {
		return 0, errors.New("webhook cursor changed; retry")
	}
	if _, err = prWatchResourceTx(ctx, tx, watch); err != nil {
		return 0, err
	}
	w, err := loadWave(ctx, tx, watch.Target.WaveID)
	if err != nil {
		return 0, err
	}
	w.ScopeRevision, w.PlanRevision, w.Policy.Revision = watch.Target.ScopeRevision, watch.Target.PlanRevision, watch.Target.PolicyRevision
	count := 0
	for _, delivery := range page.Deliveries {
		if delivery.ReceivedAt.Before(watch.CreatedAt) {
			continue
		}
		o, match := webhookPRObservation(delivery, watch.PRNumber)
		if !match {
			continue
		}
		o.SubscriptionID, o.ResourceKey = watch.ID, watch.ResourceKey
		eventID := structureID("pr-webhook-event-", watch.ID, delivery.ID)
		if err = k.waveEventTx(ctx, tx, w, "product_webhook_observed", watch.TaskID, eventID, o); err != nil {
			return 0, err
		}
		count++
	}
	if err = kernelRecordTx(ctx, tx, "product_pr_webhook_cursor", id, watch.SessionID, page.LastSequence); err != nil {
		return 0, err
	}
	if count == 0 && page.NextAfter != 0 {
		count = 1
	}
	return count, tx.Commit()
}

func (k *KernelStore) fanoutPRWebhookTx(ctx context.Context, tx *sql.Tx, event notificationIntent) error {
	if event.Kind != "product_webhook_observed" {
		return nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT payload FROM kernel_events WHERE id=? AND kind='product_webhook_observed'", event.EventID).Scan(&raw); err != nil {
		return err
	}
	var o PRWebhookObservation
	if err := json.Unmarshal(raw, &o); err != nil {
		return err
	}
	var watch PRSubscriptionIntent
	if err := completionRecord(ctx, tx, "product_pr_subscription_intent", o.SubscriptionID, &watch); err != nil {
		return err
	}
	if watch.Target != event.Target || watch.ResourceKey != o.ResourceKey || event.EventID != structureID("pr-webhook-event-", watch.ID, o.DeliveryID) {
		return errors.New("webhook notification origin conflict")
	}
	b, err := sessionBindingTx(ctx, tx, watch.SessionID)
	if err != nil {
		return err
	}
	w, err := loadWave(ctx, tx, watch.Target.WaveID)
	if err != nil {
		return err
	}
	state := "deferred"
	if b.Closed || w.Target() != watch.Target || w.State == domain.WaveCanceled || w.State == domain.WaveCompleted || (b.Kind == "task" && b.TaskState.Terminal()) {
		state = "archived"
	} else {
		var awaited prOperationAwait
		err = completionRecord(ctx, tx, "product_pr_await", prAwaitKey(watch), &awaited)
		if err == nil {
			if awaited.SessionID != watch.SessionID || awaited.SubscriptionID != watch.ID || awaited.Target != watch.Target {
				return errors.New("webhook wait identity conflict")
			}
			state = "pending"
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	detail, err := json.Marshal(o)
	if err != nil {
		return err
	}
	n := Notification{ID: structureID("notification-", watch.ID, event.EventID), SubscriptionID: watch.ID, SessionID: watch.SessionID, Target: watch.Target, SourceTarget: event.Target, EventID: event.EventID, SourceEventID: event.EventID, EventBoundary: event.Sequence, ObservedVia: "webhook", Kind: "external_update", TaskID: watch.TaskID, OperationID: watch.OperationID, Detail: string(detail), State: state, CreatedAt: k.clock()}
	raw, err = json.Marshal(n)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,run_id,payload) VALUES(?,?,?,?,?,'',?) ON CONFLICT(subscription_id,event_id) DO NOTHING", n.ID, watch.ID, event.EventID, n.SessionID, n.State, raw); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE kernel_subscriptions SET cursor=max(cursor,?) WHERE id=?", event.Sequence, watch.ID)
	return err
}
