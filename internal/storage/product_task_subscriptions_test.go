package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
)

func assignedPRFixture(t *testing.T, registerBefore bool) (*KernelStore, domain.Wave, []domain.Task, ProductTargetSnapshot) {
	t.Helper()
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.Policy.Stage, request.Policy.Product = "P", true
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	p := ProductTarget{Target: w.Target(), Repository: "owner/repo", BaseBranch: "master", WorkItem: domain.WorkItem{ID: "work", RepositoryID: 17, ScopeRevision: 1, PolicyRevision: 1, Version: 1, Phase: domain.Reviewing, Candidate: "candidate", Branch: "harness/work"}, Candidate: runner.Candidate{ID: "candidate", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: 1, PolicyRevision: 1}}
	var saved ProductTargetSnapshot
	if registerBefore {
		saved, err = k.SetProductTarget(ctx, "", p)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		// WorkItem ownership is registered before planning; candidate projection
		// can arrive later from the trusted product controller.
		if err = k.Put(ctx, "wave_work_item", p.WorkItem.ID, w.ID, map[string]any{"target": w.Target(), "candidate_id": ""}); err != nil {
			t.Fatal(err)
		}
	}
	plan := storagePlanFixture()
	for i := range plan.Phases {
		for j := range plan.Phases[i].Tasks {
			plan.Phases[i].Tasks[j].WorkItemIDs = []string{p.WorkItem.ID}
		}
	}
	adopted, err := k.AdoptPlan(ctx, w.Target(), "initial", plan)
	if err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	p.Target = w.Target()
	if !registerBefore {
		for _, id := range adopted.CreatedTaskIDs {
			var watch PRSubscriptionIntent
			if err = k.Record(ctx, "product_pr_subscription_intent", structureID("product-assignment-sub-", id, p.WorkItem.ID), &watch); err != nil || watch.RepositoryID != 0 || watch.State != "pending" {
				t.Fatal(watch, err)
			}
		}
	}
	saved, err = k.SetProductTarget(ctx, saved.Digest, p)
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
	return k, w, tasks, saved
}
func assignedWatch(t *testing.T, k *KernelStore, task domain.Task) PRSubscriptionIntent {
	t.Helper()
	var watch PRSubscriptionIntent
	if err := k.Record(context.Background(), "product_pr_subscription_intent", structureID("product-assignment-sub-", task.ID, "work"), &watch); err != nil {
		t.Fatal(err)
	}
	return watch
}
func bindAssignedPRFixture(t *testing.T, k *KernelStore, w domain.Wave, p ProductTargetSnapshot) (string, PRDispatchClaim, PRResourceObservation) {
	t.Helper()
	ctx := context.Background()
	b, run, _ := reserveBudgetMain(t, k, "publication", w.Target())
	call := kernelCall(t, b, run, "pr_publish", contract.PublishPRRequest{SchemaVersion: 1, Target: w.Target(), Candidate: contract.CandidateReference{WorkItemID: p.Value.WorkItem.ID, CandidateID: p.Value.Candidate.ID, SHA: p.Value.Candidate.SHA}})
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.PreparePRDispatch(ctx, reply.OperationID, "publisher")
	if err != nil {
		t.Fatal(err)
	}
	o := PRResourceObservation{ProofID: "observed", RepositoryID: 17, HeadRepositoryID: 17, BaseRepositoryID: 17, Number: 42, MarkerWorkItemID: p.Value.WorkItem.ID, HeadSHA: p.Value.Candidate.SHA, HeadBranch: p.Value.WorkItem.Branch, BaseBranch: p.Value.BaseBranch, State: "open", ObservedAt: k.clock()}
	return reply.OperationID, claim, o
}
func recordAssignedWaitFixture(t *testing.T, k *KernelStore, watch PRSubscriptionIntent) {
	t.Helper()
	ctx := context.Background()
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Exercise the storage portion of an already-authorized wait. Product turn
	// readiness remains held until Task_11; this is not native dispatch evidence.
	if err = k.awaitPRSubscriptionsTx(ctx, tx, watch.SessionID, "validated-wait-fixture", watch.Target, []string{watch.ID}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func TestTaskPRSubscriptionsPersistWithPlanAndRegistrationWithoutFabricatedOperations(t *testing.T) {
	k, w, tasks, p := assignedPRFixture(t, true)
	ctx := context.Background()
	for _, task := range tasks {
		watch := assignedWatch(t, k, task)
		if watch.SourceKind != "assignment" || watch.OperationID != "" || watch.SessionID != task.SessionID || watch.WorkItemID != p.Value.WorkItem.ID || watch.State != "pending" || watch.RepositoryID != 17 {
			t.Fatal(watch)
		}
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tool_operations").Scan(&count); err != nil || count != 0 {
		t.Fatal("assignment fabricated a model call", count, err)
	}
	if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
		t.Fatal(err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_pr_subscription_intent'").Scan(&count); err != nil || count != 2 {
		t.Fatal(count, err)
	}
	ready, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()})
	if err != nil || len(ready.Tasks) != 0 {
		t.Fatal("subscription bypassed product readiness", ready, err)
	}
}

func TestTaskPRSubscriptionsResolvePendingRepositoryAfterCandidateRegistration(t *testing.T) {
	k, _, tasks, _ := assignedPRFixture(t, false)
	for _, task := range tasks {
		watch := assignedWatch(t, k, task)
		if watch.RepositoryID != 17 || watch.State != "pending" || watch.ResourceKey != "" {
			t.Fatal(watch)
		}
	}
}
func TestTaskPRSubscriptionsBindAtomicallyAndReceiveRawAndSnapshotUpdates(t *testing.T) {
	k, w, tasks, p := assignedPRFixture(t, true)
	ctx := context.Background()
	first := assignedWatch(t, k, tasks[0])
	recordAssignedWaitFixture(t, k, first)
	receivePRComment(t, k, "before-publication", 42, "created")
	id, claim, o := bindAssignedPRFixture(t, k, w, p)
	if _, err := k.BindObservedPRPublication(ctx, id, PRTransportProof{ID: "publication", OwnerID: claim.OwnerID, Disposition: "not_started"}, o); err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		watch := assignedWatch(t, k, task)
		if watch.State != "bound" || watch.ResourceKey != "github:17:pull:42" || watch.PRNumber != 42 {
			t.Fatal(watch)
		}
	}
	if _, err := k.ReconcilePRWebhookNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	for i, task := range tasks {
		watch := assignedWatch(t, k, task)
		var state string
		if err := k.DB.QueryRow("SELECT state FROM kernel_notifications WHERE subscription_id=? AND json_extract(payload,'$.observed_via')='webhook'", watch.ID).Scan(&state); err != nil || (i == 0 && state != "pending") || (i != 0 && state != "deferred") {
			t.Fatal(state, err)
		}
	}
	scan := PRResourceScan{ID: "snapshot", RepositoryID: 17, PRNumber: 42, HeadSHA: p.Value.Candidate.SHA, StartedAt: k.clock()}
	for _, kind := range prSectionKinds {
		scan.Sections = append(scan.Sections, PRResourceSection{Kind: kind, Availability: "complete", Entities: []PRResourceEntity{}})
	}
	if _, err := k.RecordPRResourceScan(ctx, "", scan); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReconcilePRResourceNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	var taskWake WakeReservation
	var taskWakeRequest WakeRequest
	for _, task := range tasks {
		watch := assignedWatch(t, k, task)
		var count int
		if err := k.DB.QueryRow("SELECT count(*) FROM kernel_notifications WHERE subscription_id=? AND json_extract(payload,'$.observed_via')='resource_scan'", watch.ID).Scan(&count); err != nil || count != 1 {
			t.Fatal(count, err)
		}
		var notificationID string
		if err := k.DB.QueryRow("SELECT id FROM kernel_notifications WHERE subscription_id=? AND json_extract(payload,'$.observed_via')='resource_scan'", watch.ID).Scan(&notificationID); err != nil {
			t.Fatal(err)
		}
		notice, err := notificationTx(ctx, k.DB, notificationID)
		if err != nil || notice.TaskID != task.ID || notice.SessionID != task.SessionID || notice.Target != task.Target() || notice.SourceTarget != w.Target() {
			t.Fatal("PR update lost its assigned Task/Wave ownership", notice, err)
		}
		if task.ID == tasks[0].ID {
			taskWakeRequest = WakeRequest{SessionID: task.SessionID, Key: "task-pr-update-" + scan.ID, NotificationIDs: []string{notice.ID}}
			pending, err := k.PendingWakeNotifications(ctx, task.SessionID, 16)
			found := false
			for _, item := range pending {
				found = found || item.ID == notice.ID
			}
			if err != nil || !found {
				t.Fatal("Task wake did not expose its assigned PR update", notice, pending, err)
			}
			taskWake, err = k.ReserveWake(ctx, taskWakeRequest)
			if err != nil {
				t.Fatal("assigned Task could not reserve a durable wake for its PR update", err)
			}
		} else if notice.State != "deferred" {
			t.Fatal("unawaited sibling Task was woken by another Task's PR update", notice)
		}
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err := OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err = k.ReconcilePRWebhookNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReconcilePRResourceNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_notifications WHERE subscription_id=?", first.ID).Scan(&count); err != nil || count != 2 {
		t.Fatal("restart duplicated assignment inputs", count, err)
	}
	replayedWake, err := k.ReserveWake(ctx, taskWakeRequest)
	if err != nil || replayedWake.Intent.ID != taskWake.Intent.ID {
		t.Fatal("restart changed the durable assigned Task wake", replayedWake, err)
	}
}
func TestTaskPRSubscriptionsRollbackWithPlanAndPublicationBinding(t *testing.T) {
	ctx := context.Background()
	k, request := waveStoreFixture(t)
	request.Policy.Stage, request.Policy.Product = "P", true
	w, err := k.CreateWave(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.Put(ctx, "wave_work_item", "work", w.ID, map[string]any{"target": w.Target(), "candidate_id": ""}); err != nil {
		t.Fatal(err)
	}
	if _, err = k.DB.Exec(`CREATE TRIGGER reject_assignment BEFORE INSERT ON records WHEN NEW.kind='product_pr_subscription_intent' AND json_extract(NEW.payload,'$.source_kind')='assignment' BEGIN SELECT RAISE(FAIL,'assignment rollback'); END`); err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	plan.Phases[0].Tasks[0].WorkItemIDs = []string{"work"}
	if _, err = k.AdoptPlan(ctx, w.Target(), "plan", plan); err == nil || !strings.Contains(err.Error(), "assignment rollback") {
		t.Fatal("plan did not reach injected subscription failure", err)
	}
	var count int
	if err = k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	k, w, tasks, p := assignedPRFixture(t, true)
	id, claim, o := bindAssignedPRFixture(t, k, w, p)
	if _, err = k.DB.Exec(`CREATE TRIGGER reject_assignment_binding BEFORE UPDATE ON records WHEN NEW.kind='product_pr_subscription_intent' AND json_extract(NEW.payload,'$.source_kind')='assignment' AND json_extract(NEW.payload,'$.state')='bound' BEGIN SELECT RAISE(FAIL,'assignment binding rollback'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err = k.BindObservedPRPublication(ctx, id, PRTransportProof{ID: "publication", OwnerID: claim.OwnerID, Disposition: "not_started"}, o); err == nil {
		t.Fatal("partial resource binding committed")
	}
	if err = k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_pr_resource'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if watch := assignedWatch(t, k, tasks[0]); watch.State != "pending" {
		t.Fatal(watch)
	}
	if _, err = k.DB.Exec("DROP TRIGGER reject_assignment_binding"); err != nil {
		t.Fatal(err)
	}
	if _, err = k.BindObservedPRPublication(ctx, id, PRTransportProof{ID: "publication", OwnerID: claim.OwnerID, Disposition: "not_started"}, o); err != nil {
		t.Fatal(err)
	}
}
func TestTaskPRSubscriptionsRejectUnassignedIdentityAndForeignWait(t *testing.T) {
	k, _, tasks, _ := assignedPRFixture(t, true)
	ctx := context.Background()
	watch := assignedWatch(t, k, tasks[0])
	for _, mode := range []string{"session", "work", "operation"} {
		bad := watch
		switch mode {
		case "session":
			bad.SessionID = tasks[1].SessionID
		case "work":
			bad.WorkItemID = "unassigned"
		case "operation":
			bad.OperationID = "fabricated-call"
		}
		tx, err := k.kernelTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = prWatchWorkItemTx(ctx, tx, bad)
		tx.Rollback()
		if err == nil {
			t.Fatal("forged assignment accepted", mode)
		}
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = k.awaitPRSubscriptionsTx(ctx, tx, tasks[1].SessionID, "fixture", watch.Target, []string{watch.ID})
	tx.Rollback()
	if err == nil {
		t.Fatal("sibling waited on another Task subscription")
	}
}

func TestTaskPRSubscriptionsNewTasksObserveExistingPRAndOldScopeArchives(t *testing.T) {
	k, w, tasks, p := assignedPRFixture(t, true)
	ctx := context.Background()
	id, claim, o := bindAssignedPRFixture(t, k, w, p)
	if _, err := k.BindObservedPRPublication(ctx, id, PRTransportProof{ID: "observed", OwnerID: claim.OwnerID, Disposition: "not_started"}, o); err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	plan.Phases[0].ID = "new-work-phase"
	plan.Phases[1].ID = "new-review-phase"
	plan.Phases[0].Tasks[0].ID = "new-work"
	plan.Phases[0].Completion.RequiredTaskIDs = []string{"new-work"}
	plan.Phases[1].Tasks[0].ID = "new-review"
	plan.Phases[1].Tasks[0].DependsOn = []contract.TaskDependency{{TaskID: "new-work"}}
	plan.Phases[1].Completion.RequiredTaskIDs = []string{"new-review"}
	for i := range plan.Phases {
		plan.Phases[i].Tasks[0].WorkItemIDs = []string{"work"}
	}
	adopted, err := k.AdoptPlan(ctx, w.Target(), "new-plan", plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range adopted.CreatedTaskIDs {
		task, err := k.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		watch := assignedWatch(t, k, task)
		if watch.State != "bound" || watch.PRNumber != 42 || watch.Target != adopted.Target {
			t.Fatal(watch)
		}
	}
	receivePRComment(t, k, "after-new-plan", 42, "edited")
	if _, err = k.ReconcilePRWebhookNotifications(ctx, 16); err != nil {
		t.Fatal(err)
	}
	old := assignedWatch(t, k, tasks[0])
	var state string
	if err = k.DB.QueryRow("SELECT state FROM kernel_notifications WHERE subscription_id=? AND json_extract(payload,'$.observed_via')='webhook'", old.ID).Scan(&state); err != nil || state != "archived" {
		t.Fatal("old Task was resumed", state, err)
	}
}
