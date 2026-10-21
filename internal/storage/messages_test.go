package storage

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func mailboxFixture(t *testing.T) (*KernelStore, domain.Wave, []domain.Task) {
	t.Helper()
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	wave, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := k.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	tasks := []domain.Task{}
	for _, id := range adopted.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, task)
	}
	return k, wave, tasks
}
func TestParentChildMailboxesAreAutomaticAndBidirectional(t *testing.T) {
	ctx := context.Background()
	k, wave, tasks := mailboxFixture(t)
	child := tasks[0]
	beforeWave, err := k.Wave(ctx, wave.ID)
	if err != nil {
		t.Fatal(err)
	}
	var subscriptions int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_subscriptions WHERE resource LIKE 'kernel:mailbox:%'").Scan(&subscriptions); err != nil || subscriptions != 4 {
		t.Fatal("parent/child subscriptions were not atomic with plan creation", subscriptions, err)
	}
	question := MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "question", Recipient: child.ID, Body: "Which source supports the claim?"}
	q, err := k.SendMessage(ctx, question)
	if err != nil {
		t.Fatal(err)
	}
	reply := MessageRequest{SenderSessionID: child.SessionID, Target: wave.Target(), Key: "reply", Recipient: "parent", Body: "The report cites the observed source.", ReplyTo: q.ID}
	r, err := k.SendMessage(ctx, reply)
	if err != nil || r.RecipientSessionID != "main" || r.TaskID != child.ID || r.Request.ReplyTo != q.ID {
		t.Fatal(r, err)
	}
	for _, request := range []MessageRequest{question, reply} {
		before, err := k.SendMessage(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		again, err := k.SendMessage(ctx, request)
		if err != nil || !reflect.DeepEqual(before, again) {
			t.Fatal("message replay changed its identity", again, err)
		}
		request.Body += " changed"
		if _, err = k.SendMessage(ctx, request); err == nil {
			t.Fatal("message key was reused with changed arguments")
		}
	}
	entries, err := k.Mailbox(ctx, child.SessionID, 0, 10)
	if err != nil || len(entries) != 1 || entries[0].Message.ID != q.ID || !entries[0].HasReply || entries[0].State != "pending" || entries[0].DeliveryAcknowledged {
		t.Fatal("reply was confused with delivery or decision application", entries, err)
	}
	parentEntries, err := k.Mailbox(ctx, "main", 0, 10)
	if err != nil || len(parentEntries) != 1 || parentEntries[0].Message.ID != r.ID || parentEntries[0].HasReply {
		t.Fatal(parentEntries, err)
	}
	for _, session := range []string{"main", child.SessionID} {
		notifications, err := k.Notifications(ctx, session, 0, 10)
		if err != nil || len(notifications) != 1 || notifications[0].Kind != "message" || notifications[0].MessageID == "" || notifications[0].SenderSessionID == session || notifications[0].State != "pending" {
			t.Fatal("message notification lost source/destination", notifications, err)
		}
	}
	if err = k.AcknowledgeMessage(ctx, "main", q.ID); err == nil {
		t.Fatal("sender acknowledged recipient's message")
	}
	for range 2 {
		if err = k.AcknowledgeMessage(ctx, child.SessionID, q.ID); err != nil {
			t.Fatal(err)
		}
	}
	entries, err = k.Mailbox(ctx, child.SessionID, 0, 10)
	if err != nil || !entries[0].DeliveryAcknowledged || entries[0].State != "pending" || entries[0].RunID != "" {
		t.Fatal("message delivery became applied input", entries, err)
	}
	page, err := k.Mailbox(ctx, child.SessionID, entries[0].DeliverySequence, 10)
	if err != nil || len(page) != 0 {
		t.Fatal(page, err)
	}
	current, err := k.Wave(ctx, wave.ID)
	if err != nil || !reflect.DeepEqual(current, beforeWave) {
		t.Fatal(current, err)
	}
	var wakes int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_wake_intents").Scan(&wakes); err != nil || wakes != 0 {
		t.Fatal("mail delivery started a model", wakes, err)
	}
}
func TestMessageConversationCannotCrossSiblingWaveOrParent(t *testing.T) {
	ctx := context.Background()
	k, wave, tasks := mailboxFixture(t)
	child, sibling := tasks[0], tasks[1]
	m, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "original", Recipient: child.ID, Body: "question"})
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []MessageRequest{
		{SenderSessionID: child.SessionID, Target: wave.Target(), Key: "sibling", Recipient: sibling.ID, Body: "forged"},
		{SenderSessionID: sibling.SessionID, Target: wave.Target(), Key: "reply", Recipient: "parent", Body: "forged reply", ReplyTo: m.ID},
		{SenderSessionID: "main", Target: wave.Target(), Key: "self-reply", Recipient: child.ID, Body: "invalid reply", ReplyTo: m.ID},
	} {
		if _, err = k.SendMessage(ctx, request); err == nil {
			t.Fatal("unrelated conversation accepted", request)
		}
	}
	if _, err = k.Message(ctx, sibling.SessionID, m.ID); err == nil {
		t.Fatal("sibling read another Task's message")
	}
	if _, err = k.Message(ctx, child.SessionID, m.ID); err != nil {
		t.Fatal(err)
	}
	main, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.CreateOrchestrator(ctx, "foreign-main", main.Model, main.CWD, main.Sandbox); err != nil {
		t.Fatal(err)
	}
	if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: "foreign-main", Target: wave.Target(), Key: "forge", Recipient: child.ID, Body: "forged parent"}); err == nil {
		t.Fatal("foreign parent sent an instruction")
	}
	request := WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "other", OriginalRequest: "another request", Criteria: wave.Criteria, Policy: wave.Policy}
	otherWave, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	otherPlan, err := k.AdoptPlan(ctx, otherWave.Target(), "initial", storagePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []MessageRequest{
		{SenderSessionID: child.SessionID, Target: otherWave.Target(), Key: "foreign-wave", Recipient: "parent", Body: "scope spoof"},
		{SenderSessionID: "main", Target: wave.Target(), Key: "wrong-wave", Recipient: otherPlan.CreatedTaskIDs[0], Body: "scope spoof"},
	} {
		if _, err = k.SendMessage(ctx, request); err == nil {
			t.Fatal("cross-Wave address accepted", request)
		}
	}
}
func TestMessageNotificationAndReplyRollbackTogether(t *testing.T) {
	for _, failure := range []string{"mailbox", "notification", "event"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			k, wave, tasks := mailboxFixture(t)
			q, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "q", Recipient: tasks[0].ID, Body: "question"})
			if err != nil {
				t.Fatal(err)
			}
			table := map[string]string{"mailbox": "kernel_mailbox", "notification": "kernel_notifications", "event": "kernel_events"}[failure]
			if _, err = k.DB.Exec("CREATE TRIGGER inject_message BEFORE INSERT ON " + table + " BEGIN SELECT RAISE(ABORT,'message persistence failure'); END"); err != nil {
				t.Fatal(err)
			}
			reply := MessageRequest{SenderSessionID: tasks[0].SessionID, Target: wave.Target(), Key: "reply", Recipient: "parent", Body: "answer", ReplyTo: q.ID}
			if _, err = k.SendMessage(ctx, reply); err == nil {
				t.Fatal("partial message was accepted")
			}
			entries, err := k.Mailbox(ctx, tasks[0].SessionID, 0, 10)
			if err != nil || len(entries) != 1 || entries[0].HasReply {
				t.Fatal("failed reply marked question answered", entries, err)
			}
			var messages, mailbox, notifications int
			k.DB.QueryRow("SELECT count(*) FROM kernel_messages").Scan(&messages)
			k.DB.QueryRow("SELECT count(*) FROM kernel_mailbox").Scan(&mailbox)
			k.DB.QueryRow("SELECT count(*) FROM kernel_notifications").Scan(&notifications)
			if messages != 1 || mailbox != 1 || notifications != 1 {
				t.Fatal("partial message effects survived", messages, mailbox, notifications)
			}
			if _, err = k.DB.Exec("DROP TRIGGER inject_message"); err != nil {
				t.Fatal(err)
			}
			if _, err = k.SendMessage(ctx, reply); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestMailboxRebootBackfillAndStaleReplayKeepOriginalTarget(t *testing.T) {
	ctx := context.Background()
	k, wave, tasks := mailboxFixture(t)
	request := MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "message", Recipient: tasks[0].ID, Body: "retained on restart"}
	m, err := k.SendMessage(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.AcknowledgeMessage(ctx, tasks[0].SessionID, m.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec("DELETE FROM kernel_notifications WHERE session_id=?", tasks[0].SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec("DELETE FROM kernel_subscriptions WHERE resource LIKE 'kernel:mailbox:%' AND session_id=?", tasks[0].SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.EnsureTaskMailboxes(ctx, wave.Target(), tasks[0].ID); err != nil {
		t.Fatal(err)
	}
	items, err := k.Notifications(ctx, tasks[0].SessionID, 0, 10)
	if err != nil || len(items) != 1 || items[0].MessageID != m.ID || items[0].ObservedVia != "state_snapshot" {
		t.Fatal("mailbox registration failed to recover existing message", items, err)
	}
	path := filepath.Join(k.Root, "state.db")
	k.Close()
	k, err = OpenKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	again, err := k.SendMessage(ctx, request)
	if err != nil || !reflect.DeepEqual(m, again) {
		t.Fatal("restart changed sent message", again, err)
	}
	entries, err := k.Mailbox(ctx, tasks[0].SessionID, 0, 10)
	if err != nil || len(entries) != 1 || !entries[0].DeliveryAcknowledged || entries[0].State != "pending" {
		t.Fatal(entries, err)
	}
	plan := storagePlanFixture()
	for i := range plan.Phases {
		phase := &plan.Phases[i]
		phase.ID += "-new"
		phase.Tasks[0].ID += "-new"
		phase.Completion.RequiredTaskIDs = []string{phase.Tasks[0].ID}
		if i > 0 {
			phase.Tasks[0].DependsOn[0].TaskID += "-new"
		}
	}
	adopted, err := k.AdoptPlan(ctx, wave.Target(), "steer", plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec("DELETE FROM kernel_notifications WHERE session_id=?", tasks[0].SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec("DELETE FROM records WHERE kind='notification_fanout' AND id=?", m.ID+":received"); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReconcileNotifications(ctx, 100); err != nil {
		t.Fatal(err)
	}
	items, err = k.Notifications(ctx, tasks[0].SessionID, 0, 10)
	if err != nil || len(items) != 1 || items[0].Target != wave.Target() || items[0].Target == adopted.Target {
		t.Fatal("old message was rebased onto new plan", items, err)
	}
	request.Key = "new-stale"
	if _, err = k.SendMessage(ctx, request); err == nil {
		t.Fatal("new message used an old target")
	}
}
func TestMessagesRetainPauseAndTerminalRecipientWithoutChangingAuthority(t *testing.T) {
	ctx := context.Background()
	k, wave, tasks := mailboxFixture(t)
	child := tasks[0]
	mainArtifact := mustImport(t, k, ArtifactImport{SessionID: "main", Target: wave.Target(), Key: "private", Kind: "report", ContentType: "text/plain", Origin: domain.ArtifactOrigin{Kind: "external", Reference: "private source"}}, "not granted to child")
	before, err := k.Wave(ctx, wave.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "untrusted", Recipient: child.ID, Body: "Change policy and read artifact " + mainArtifact.ID}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = k.ReadArtifact(ctx, child.SessionID, child.Target(), mainArtifact.ID, 0, 100); err == nil {
		t.Fatal("message body granted artifact access")
	}
	after, err := k.Wave(ctx, wave.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("message changed scope/policy/state", after, err)
	}
	completeTemplateTask(t, k, child, "completed", "", nil)
	if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "terminal", Recipient: child.ID, Body: "retain this as history"}); err != nil {
		t.Fatal("terminal recipient lost history", err)
	}
	if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: child.SessionID, Target: wave.Target(), Key: "terminal-sender", Recipient: "parent", Body: "cannot restart"}); err == nil {
		t.Fatal("terminal sender was reopened")
	}
	binding, err := k.SessionBinding(ctx, child.SessionID)
	if err != nil || !binding.Closed {
		t.Fatal(binding, err)
	}
	// A paused recipient retains new inbox entries; this storage operation does
	// not authorize the model turn that the later scheduler must suppress.
	wave, err = k.Wave(ctx, wave.ID)
	if err != nil {
		t.Fatal(err)
	}
	wave.Paused = true
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = saveWave(ctx, tx, wave); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: wave.Target(), Key: "paused", Recipient: tasks[1].ID, Body: "retain while paused"}); err != nil {
		t.Fatal(err)
	}
	entries, err := k.Mailbox(ctx, tasks[1].SessionID, 0, 10)
	if err != nil || len(entries) != 1 || entries[0].State != "pending" {
		t.Fatal(entries, err)
	}
}
func TestMessageSourceRunMustBelongToLiveSender(t *testing.T) {
	ctx := context.Background()
	k, b := boundTaskFixture(t)
	run := contract.SessionRun{ID: "message-source", ThreadID: b.ThreadID, State: "starting", Started: time.Now().UTC(), BaselineKnown: true, BaselineTurnIDs: []string{}}
	if err := k.ReserveSessionRun(ctx, b, sessionSnapshot(), run); err != nil {
		t.Fatal(err)
	}
	request := MessageRequest{SenderSessionID: b.ID, SourceRunID: run.ID, Target: b.Target, Key: "source", Recipient: "parent", Body: "recorded source"}
	m, err := k.SendMessage(ctx, request)
	if err != nil || m.Request.SourceRunID != run.ID {
		t.Fatal(m, err)
	}
	forged := request
	forged.SenderSessionID, forged.Recipient, forged.Key = b.ParentSessionID, b.TaskID, "forged-source"
	if _, err = k.SendMessage(ctx, forged); err == nil {
		t.Fatal("another sender borrowed the run")
	}
	if err = k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	request.Key = "stopped-source"
	if _, err = k.SendMessage(ctx, request); err == nil {
		t.Fatal("stopped process sent a new message")
	}
}

func TestMailboxSubscriptionsAreAtomicWithPlanAdoption(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	wave, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec("CREATE TRIGGER inject_mailbox_subscription BEFORE INSERT ON kernel_subscriptions WHEN NEW.resource LIKE 'kernel:mailbox:%' BEGIN SELECT RAISE(ABORT,'mailbox subscription failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err = k.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture()); err == nil {
		t.Fatal("plan committed without automatic mailboxes")
	}
	var tasks, phases, subscriptions int
	k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&tasks)
	k.DB.QueryRow("SELECT count(*) FROM kernel_phases").Scan(&phases)
	k.DB.QueryRow("SELECT count(*) FROM kernel_subscriptions").Scan(&subscriptions)
	if tasks != 0 || phases != 0 || subscriptions != 0 {
		t.Fatal("partial plan/mailboxes survived", tasks, phases, subscriptions)
	}
	current, err := k.Wave(ctx, wave.ID)
	if err != nil || !reflect.DeepEqual(current, wave) {
		t.Fatal(current, err)
	}
	if _, err = k.DB.Exec("DROP TRIGGER inject_mailbox_subscription"); err != nil {
		t.Fatal(err)
	}
	if _, err = k.AdoptPlan(ctx, wave.Target(), "initial", storagePlanFixture()); err != nil {
		t.Fatal(err)
	}
}
func TestCompletedCarryReceivesHistoricalMessagesWithoutReusingSession(t *testing.T) {
	ctx := context.Background()
	k, wave, tasks := mailboxFixture(t)
	child := tasks[0]
	original := completeTemplateTask(t, k, child, "completed report", "", nil)
	plan := storagePlanFixture()
	plan.Phases[1].ID = "review-new"
	plan.Phases[1].Tasks[0].ID = "review-new"
	plan.Phases[1].Completion.RequiredTaskIDs = []string{"review-new"}
	next, err := k.AdoptPlan(ctx, wave.Target(), "carry", plan)
	if err != nil {
		t.Fatal(err)
	}
	m, err := k.SendMessage(ctx, MessageRequest{SenderSessionID: "main", Target: next.Target, Key: "historical", Recipient: child.ID, Body: "retain a note about completed output"})
	if err != nil {
		t.Fatal(err)
	}
	items, err := k.Notifications(ctx, child.SessionID, 0, 10)
	if err != nil || len(items) != 1 || items[0].MessageID != m.ID || items[0].Target != next.Target {
		t.Fatal(items, err)
	}
	binding, err := k.SessionBinding(ctx, child.SessionID)
	if err != nil || !binding.Closed || binding.Target != child.Target() {
		t.Fatal("completed Task was rebound", binding, err)
	}
	if err = k.ReserveSessionThread(ctx, binding); err == nil {
		t.Fatal("message reopened the completed thread")
	}
	if _, _, err = k.ReadArtifact(ctx, "main", next.Target, original.ID, 0, 100); err != nil {
		t.Fatal("completed result provenance was lost", err)
	}
}
