package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
)

func splitNoticeFixture(t *testing.T, retry ...bool) (*KernelStore, domain.Wave, contract.ToolInvocation, contract.ToolReply, ProductSplitChildPlanRequest) {
	t.Helper()
	ctx := context.Background()
	k, w, call, _ := productSplitToolFixture(t, "product_v3")
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || len(reply.SubscriptionIDs) != 1 {
		t.Fatal("prepared split has no completion watch", reply, err)
	}
	var adoption ProductSplitToolAdoption
	if err := k.Record(ctx, "product_split_tool_adoption", reply.OperationID, &adoption); err != nil {
		t.Fatal(err)
	}
	r := ProductSplitChildPlanRequest{Target: w.Target(), ParentWorkItemID: "split-root", ProposalID: adoption.Preparation.Value.ProposalID, ProposalDigest: adoption.Preparation.Value.ProposalDigest}
	for i, ref := range adoption.Preparation.Value.Creations {
		job, err := k.ReadProductSplitCreate(ctx, ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && len(retry) > 0 && retry[0] {
			if _, err := k.AbortUnactivatedProductSplitCreate(ctx, job.ID, job.Value.OwnerID, job.Digest, "notice-fixture-unsent"); err != nil {
				t.Fatal(err)
			}
			job, err = k.ReserveProductSplitCreateUnsentRetry(ctx, job.ID, "notice-fixture-retry")
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Remove(filepath.Join(k.Root, "split-http-started.json")); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		identity := startSplitCreationOwner(t, k, job)
		stop, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := process.StopGroup(stop, identity); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		if _, err := k.FinishProductSplitCreateHTTP(ctx, job.ID, job.Value.OwnerID, job.Digest, strings.Repeat("a", 64), "notice-fixture-stop"); err != nil {
			t.Fatal(err)
		}
		if _, err := k.RecordProductSplitCreatedIssue(ctx, job.ID, ProductSplitCreatedIssue{RepositoryID: 17, ID: int64(200 + i), Number: 20 + i, Title: job.Value.Child.Value.Child.Title, Body: job.Value.Child.Value.Body, State: "open", ObservedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := k.BindProductSplitChildScopes(ctx, r); err != nil {
		t.Fatal(err)
	}
	return k, w, call, reply, r
}

func finishSplitMainWait(t *testing.T, k *KernelStore, call contract.ToolInvocation, reply contract.ToolReply, target contract.Target) {
	t.Helper()
	ctx := context.Background()
	var run contract.SessionRun
	if err := k.Record(ctx, "agent_run", call.RunID, &run); err != nil {
		t.Fatal(err)
	}
	result := contract.MainResult{SchemaVersion: 1, SessionID: call.SessionID, Decisions: []contract.MainDecision{{Target: target, Action: "wait", Summary: "await whole split registration", SubscriptionIDs: append([]string{}, reply.SubscriptionIDs...), Remaining: []string{}, Blockers: []string{}}}}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	run.State, run.Final = "completed", string(raw)
	if err := k.Put(ctx, "agent_run", run.ID, call.SessionID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, call.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyMainResult(ctx, call.SessionID, run.ID); err != nil {
		t.Fatal(err)
	}
}

func splitNotices(t *testing.T, k *KernelStore, session string) []Notification {
	t.Helper()
	notices, err := k.Notifications(context.Background(), session, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	out := []Notification{}
	for _, n := range notices {
		if n.Kind == "split_registered" {
			out = append(out, n)
		}
	}
	return out
}

func TestProductSplitCompletionIsAtomicDurableAndBindsOneWake(t *testing.T) {
	ctx := context.Background()
	k, w, call, reply, r := splitNoticeFixture(t)
	if notices := splitNotices(t, k, call.SessionID); len(notices) != 0 {
		t.Fatal("prepared/observed metadata completed registration", notices)
	}
	finishSplitMainWait(t, k, call, reply, w.Target())
	before, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	notices := splitNotices(t, k, call.SessionID)
	if len(notices) != 1 || notices[0].Split == nil || !reflect.DeepEqual(*notices[0].Split, productSplitCompletionSummary(saved)) || notices[0].SubscriptionID != reply.SubscriptionIDs[0] || notices[0].OperationID != reply.OperationID {
		t.Fatal("unbound/unsafe completion", notices)
	}
	n := notices[0]
	raw, err := json.Marshal(n)
	if err != nil || strings.Contains(string(raw), k.Root) || strings.Contains(string(raw), "harness-split-child:") || strings.Contains(string(raw), "parent title") {
		t.Fatal("private proof in completion", string(raw), err)
	}
	if err := k.AcknowledgeNotification(ctx, call.SessionID, n.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReconcileNotifications(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if len(splitNotices(t, k, call.SessionID)) != 1 {
		t.Fatal("replay duplicated completion")
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	pending, err := k.PendingWakeNotifications(ctx, call.SessionID, 100)
	if err != nil || len(pending) != 1 || pending[0].ID != n.ID || !pending[0].DeliveryAcknowledged {
		t.Fatal("ACK/reopen consumed judgment input", pending, err)
	}
	request := WakeRequest{SessionID: call.SessionID, Key: "registered-split-wake", NotificationIDs: []string{n.ID}, IncludeSessionContext: true}
	first, err := k.ReserveWake(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	again, err := k.ReserveWake(ctx, request)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("wake replay diverged", again, err)
	}
	after, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("completion charged model/execution budget", before, after, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&count); err != nil || count != 1 {
		t.Fatal("notification launched extra model run", count, err)
	}
}

func TestProductSplitCompletionFailureRollsBackWholeRegistration(t *testing.T) {
	ctx := context.Background()
	k, _, call, _, r := splitNoticeFixture(t)
	if _, err := k.DB.Exec("CREATE TRIGGER fail_split_notice BEFORE INSERT ON kernel_notifications WHEN json_extract(NEW.payload,'$.kind')='split_registered' BEGIN SELECT RAISE(ABORT,'late notification failure'); END"); err != nil {
		t.Fatal(err)
	}
	if saved, err := k.RegisterProductSplitChildren(ctx, r); err == nil || saved.ID != "" {
		t.Fatal("notice failure returned success", saved, err)
	}
	assertNoRegisteredSplitChildren(t, k)
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_split_completion'").Scan(&n); err != nil || n != 0 {
		t.Fatal("partial completion receipt", n, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_events WHERE kind='product_split_registered'").Scan(&n); err != nil || n != 0 {
		t.Fatal("partial completion event", n, err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_split_notice"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal("notice rollback not retryable", err)
	}
	if len(splitNotices(t, k, call.SessionID)) != 1 {
		t.Fatal("no unique notice after retry")
	}
}

func TestProductSplitCompletionSourceEvidenceLossHoldsWithoutConsumingNotice(t *testing.T) {
	ctx := context.Background()
	k, w, call, reply, r := splitNoticeFixture(t)
	finishSplitMainWait(t, k, call, reply, w.Target())
	saved, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	n := splitNotices(t, k, call.SessionID)[0]
	for _, loss := range []struct{ kind, id string }{
		{"product_split_completion", reply.OperationID}, {"product_split_tool_adoption", reply.OperationID},
		{"product_split_registrations", r.ProposalID}, {"product_split_registrations_history", saved.ID},
		{"notification_intent", n.EventID},
	} {
		t.Run(loss.kind, func(t *testing.T) {
			var owner string
			var raw []byte
			if err := k.DB.QueryRow("SELECT work_item_id,payload FROM records WHERE kind=? AND id=?", loss.kind, loss.id).Scan(&owner, &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", loss.kind, loss.id); err != nil {
				t.Fatal(err)
			}
			pending, err := k.PendingWakeNotifications(ctx, call.SessionID, 100)
			if err != nil || len(pending) != 0 {
				t.Fatal("missing source proof authorized wake/fatal scan", pending, err)
			}
			if notices := splitNotices(t, k, call.SessionID); len(notices) != 1 || notices[0].State != "pending" {
				t.Fatal("hold consumed notice", notices)
			}
			if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,?,?)", loss.kind, loss.id, owner, raw); err != nil {
				t.Fatal(err)
			}
			pending, err = k.PendingWakeNotifications(ctx, call.SessionID, 100)
			if err != nil || len(pending) != 1 || pending[0].ID != n.ID {
				t.Fatal("exact source restore did not release notice", pending, err)
			}
		})
	}
}

func TestProductSplitCompletionKnownWatchLossIsNotRepairedByReplayOrFanout(t *testing.T) {
	ctx := context.Background()
	k, _, call, reply, r := splitNoticeFixture(t)
	id := reply.SubscriptionIDs[0]
	var session, wave, resource string
	var scope, plan int
	var cursor int64
	var raw []byte
	if err := k.DB.QueryRow("SELECT session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload FROM kernel_subscriptions WHERE id=?", id).Scan(&session, &wave, &scope, &plan, &resource, &cursor, &raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_subscriptions WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("cached adoption repaired missing watch", err)
	}
	if saved, err := k.RegisterProductSplitChildren(ctx, r); !errors.Is(err, ErrProductSplitHeld) || saved.ID != "" {
		t.Fatal("missing watch authorized registration/notice", saved, err)
	}
	assertNoRegisteredSplitChildren(t, k)
	if _, err := k.DB.Exec("INSERT INTO kernel_subscriptions(id,session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload) VALUES(?,?,?,?,?,?,?,?)", id, session, wave, scope, plan, resource, cursor, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	n := splitNotices(t, k, call.SessionID)[0]
	if _, err := k.DB.Exec("DELETE FROM kernel_subscriptions WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='notification_fanout' AND id=?", n.EventID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReconcileNotifications(ctx, 100); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("fanout repaired lost subscription", err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_subscriptions WHERE id=?", id).Scan(&count); err != nil || count != 0 {
		t.Fatal("fanout recreated watch", count, err)
	}
	if _, err := k.DB.Exec("INSERT INTO kernel_subscriptions(id,session_id,wave_id,scope_revision,plan_revision,resource,cursor,payload) VALUES(?,?,?,?,?,?,?,?)", id, session, wave, scope, plan, resource, cursor, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReconcileNotifications(ctx, 100); err != nil {
		t.Fatal("fanout could not resume", err)
	}
	if len(splitNotices(t, k, call.SessionID)) != 1 {
		t.Fatal("fanout duplicated notice")
	}
}

func TestProductSplitCompletionInvalidNoticeCannotShareGateOrWakeTask(t *testing.T) {
	ctx := context.Background()
	k, w, call, reply, r := splitNoticeFixture(t)
	bad := Notification{ID: "bad-split-notice", SubscriptionID: reply.SubscriptionIDs[0], SessionID: call.SessionID, Target: w.Target(), SourceTarget: w.Target(), EventID: "bad-split-event", SourceEventID: "bad-split-event", Kind: "split_registered", OperationID: reply.OperationID, OperationState: "completed", State: "pending", Split: &ProductSplitCompletionSummary{Stage: "registered"}}
	raw, err := json.Marshal(bad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,run_id,payload) VALUES(?,?,?,?,'pending','',?)", bad.ID, bad.SubscriptionID, bad.EventID, bad.SessionID, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	pending, err := k.PendingWakeNotifications(ctx, call.SessionID, 100)
	if err != nil || len(pending) != 1 || pending[0].ID == bad.ID {
		t.Fatal("invalid notice poisoned healthy gate or woke parent", pending, err)
	}
	plan := storagePlanFixture()
	for i := range plan.Phases {
		for j := range plan.Phases[i].Tasks {
			plan.Phases[i].Tasks[j].Criteria = []string{"AC", "extra"}
		}
	}
	admitted, err := k.AdoptPlan(ctx, w.Target(), "task-cannot-receive-split-completion", plan)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	forged := pending[0]
	forged.ID = "task-split-notice"
	forged.SessionID = task.SessionID
	forged.EventID = "task-split-event"
	raw, err = json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("INSERT INTO kernel_notifications(id,subscription_id,event_id,session_id,state,run_id,payload) VALUES(?,?,?,?,'pending','',?)", forged.ID, forged.SubscriptionID, forged.EventID, forged.SessionID, raw); err != nil {
		t.Fatal(err)
	}
	pending, err = k.PendingWakeNotifications(ctx, task.SessionID, 100)
	if err != nil || len(pending) != 0 {
		t.Fatal("Task received parent-only completion", pending, err)
	}
}

func TestProductSplitCompletionLegacyAdoptionKeepsOriginalReply(t *testing.T) {
	ctx := context.Background()
	k, w, call, reply, r := splitNoticeFixture(t)
	var adoption ProductSplitToolAdoption
	if err := k.Record(ctx, "product_split_tool_adoption", reply.OperationID, &adoption); err != nil {
		t.Fatal(err)
	}
	adoption.SubscriptionID = ""
	if err := k.Put(ctx, "product_split_tool_adoption", reply.OperationID, w.ID, adoption); err != nil {
		t.Fatal(err)
	}
	legacy := reply
	legacy.SubscriptionIDs = []string{}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("UPDATE kernel_tool_operations SET result=? WHERE id=?", raw, reply.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_subscriptions WHERE id=?", reply.SubscriptionIDs[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal("authenticated legacy adoption could not notify", err)
	}
	if len(splitNotices(t, k, call.SessionID)) != 1 {
		t.Fatal("legacy source did not get implicit completion")
	}
	again, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil || !reflect.DeepEqual(legacy, again) {
		t.Fatal("legacy reply was rewritten", again, err)
	}
	var stored ProductSplitToolAdoption
	if err := k.Record(ctx, "product_split_tool_adoption", reply.OperationID, &stored); err != nil || stored.SubscriptionID != "" {
		t.Fatal("legacy adoption mutated", stored, err)
	}
}

func TestProductSplitCompletionPauseAndDirectionNeverGrantOldWake(t *testing.T) {
	ctx := context.Background()
	k, w, call, reply, r := splitNoticeFixture(t)
	finishSplitMainWait(t, k, call, reply, w.Target())
	if _, err := k.RegisterProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-split-notice", Target: w.Target(), Action: "pause", Reason: "operator"}); err != nil {
		t.Fatal(err)
	}
	pending, err := k.PendingWakeNotifications(ctx, call.SessionID, 100)
	if err != nil || len(pending) != 0 {
		t.Fatal("pause did not fence completion wake", pending, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "resume-split-notice", Target: w.Target(), Action: "resume", Reason: "operator"}); err != nil {
		t.Fatal(err)
	}
	pending, err = k.PendingWakeNotifications(ctx, call.SessionID, 100)
	if err != nil || len(pending) != 1 {
		t.Fatal("resume lost unconsumed completion", pending, err)
	}
	d, err := k.RequestWaveDirection(ctx, WaveDirectionRequest{Key: "after-registered-split-notice", Target: w.Target(), Instruction: "new scope", NextPolicyRevision: w.Policy.Revision + 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	pending, err = k.PendingWakeNotifications(ctx, call.SessionID, 100)
	if err != nil || len(pending) != 0 {
		t.Fatal("old completion entered new epoch", pending, err)
	}
	if len(splitNotices(t, k, call.SessionID)) != 1 {
		t.Fatal("direction erased historical notice")
	}
}

func TestProductSplitCompletionAloneRetainsAllNegativeWitnesses(t *testing.T) {
	ctx := context.Background()
	k, w, call, reply, r := splitNoticeFixture(t, true)
	budget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || budget.SplitCreateRetries != 1 {
		t.Fatal("fixture did not consume retry", budget, err)
	}
	original, err := k.ReadProductSplitProposal(ctx, w.Target(), r.ParentWorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.RegisterProductSplitChildren(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind LIKE 'product_split_%' AND kind<>'product_split_completion'"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind IN ('product_work_item_registration','product_work_item_registration_version','wave_work_item') AND (json_type(payload,'$.value.split_scope') IS NOT NULL OR json_type(payload,'$.split_scope') IS NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_budget_ledger WHERE kind IN ('split_children','split_create_retry')"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("DELETE FROM kernel_resource_leases WHERE resource LIKE 'product-split-mutation:%'"); err != nil {
		t.Fatal(err)
	}
	for i, check := range []func() error{
		func() error { _, e := k.BindProductSplitChildScopes(ctx, r); return e },
		func() error { _, e := k.RegisterProductSplitChildren(ctx, r); return e },
		func() error {
			_, e := k.ReadProductSplitCreate(ctx, saved.Value.Children[0].Value.CreationID)
			return e
		},
		func() error { _, e := k.WaveOperationBudget(ctx, w.ID); return e },
		func() error {
			_, e := k.RegisterProductWorkItem(ctx, w.Target(), "", ProductWorkItemDefinition{ID: "new-after-completion-loss", Repository: "owner/repo", RepositoryID: 17, Criteria: []string{"AC", "extra"}})
			return e
		},
	} {
		if err := check(); !readinessHoldError(err) {
			t.Fatal("completion-only proof recreated/refunded state", i, err)
		}
	}
	original.Value.Request.Key = "new-after-completion-only"
	if _, err := k.PrepareProductSplitProposal(ctx, original.Value.Request); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("completion lost original parent split", err)
	}
	held, err := productWorkItemExecutionHeld(ctx, k.DB, w, r.ParentWorkItemID)
	if err != nil || !held {
		t.Fatal("completion released parent", held, err)
	}
	known, err := k.IsKnownProductSplitChildIssue(ctx, 17, 20)
	if err != nil || !known {
		t.Fatal("completion released numeric child source", known, err)
	}
	page, err := k.SplitCreationReconciliationTargets(ctx, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, target := range page.Targets {
		found = found || target.ID == saved.Value.Children[0].Value.CreationID
	}
	if !found {
		t.Fatal("completion disappeared from reconciliation work list", page)
	}
	pending, err := k.PendingWakeNotifications(ctx, call.SessionID, 100)
	if err != nil || len(pending) != 0 {
		t.Fatal("completion alone granted parent wake", pending, err)
	}
	if _, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call); !errors.Is(err, ErrProductSplitHeld) {
		t.Fatal("completion repaired lost tool adoption", reply, err)
	}
}
