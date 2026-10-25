package product

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func splitWorkerRegistrationFixture(t *testing.T) (*storage.KernelStore, []storage.ProductSplitCreate, map[string]*splitGETFixtureAPI, string) {
	t.Helper()
	ctx := context.Background()
	k, first, policy, bin := splitHTTPFixture(t, "direct", true)
	plan, err := k.ReadProductSplitChildPlan(ctx, first.Value.Request)
	if err != nil {
		t.Fatal(err)
	}
	jobs := []storage.ProductSplitCreate{}
	apis := map[string]*splitGETFixtureAPI{}
	for i, child := range plan.Children {
		job, err := k.ReadLatestProductSplitCreate(ctx, child.ID)
		if err != nil {
			t.Fatal(err)
		}
		stopped, err := splitTestTransport(t, k, policy).Start(ctx, job.ID)
		if err != nil || stopped.State != "stopped" {
			t.Fatal("fixture physical sender not stopped", stopped.State, err)
		}
		jobs = append(jobs, stopped)
		apis[stopped.ID] = &splitGETFixtureAPI{job: stopped, number: 11 + i}
	}
	return k, jobs, apis, bin
}

func splitRegistrationWorker(k *storage.KernelStore, apis map[string]*splitGETFixtureAPI) SplitCreationReconciliationWorker {
	return SplitCreationReconciliationWorker{Reconciler: SplitCreationReconciler{Store: k, MaxPages: 2, API: func(job storage.ProductSplitCreate) ResourceAPI { return apis[job.ID] }}, Workers: 2, Every: time.Second}
}

func TestSplitWorkerRegistersAllChildrenAfterGETAndReplaysWithoutIO(t *testing.T) {
	ctx := context.Background()
	k, jobs, apis, bin := splitWorkerRegistrationFixture(t)
	before, err := k.WaveOperationBudget(ctx, jobs[0].Value.Request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	worker := splitRegistrationWorker(k, apis)
	first := storage.SplitCreationReconciliationTarget{ID: jobs[0].ID}
	changed, err := worker.process(ctx, first)
	if !changed || !errors.Is(err, storage.ErrProductSplitHeld) {
		t.Fatal("first GET did not wait for whole set", changed, err)
	}
	if _, err := k.ReadProductSplitRegistrations(ctx, jobs[0].Value.Request); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("partial set registered", err)
	}
	changed, err = worker.process(ctx, storage.SplitCreationReconciliationTarget{ID: jobs[1].ID})
	if !changed || err != nil {
		t.Fatal("last GET did not register whole set", changed, err)
	}
	saved, err := k.ReadProductSplitRegistrations(ctx, jobs[0].Value.Request)
	if err != nil || len(saved.Value.Registrations) != 2 {
		t.Fatal(saved, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = storage.OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	worker = splitRegistrationWorker(k, apis)
	worker.Reconciler.API = func(storage.ProductSplitCreate) ResourceAPI {
		t.Error("observed adoption reread external source")
		return nil
	}
	for _, job := range jobs {
		changed, err := worker.process(ctx, storage.SplitCreationReconciliationTarget{ID: job.ID})
		if changed || err != nil {
			t.Fatal("exact replay rewrote metadata", changed, err)
		}
		if len(apis[job.ID].reads()) != 4 {
			t.Fatal("duplicate/missing GET bookends", apis[job.ID].reads())
		}
	}
	if strings.Count(splitHTTPCalls(t, bin), "POST\n") != 2 {
		t.Fatal("registration issued duplicate POST", splitHTTPCalls(t, bin))
	}
	after, err := k.WaveOperationBudget(ctx, jobs[0].Value.Request.Target.WaveID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("metadata reset/charged budgets", before, after, err)
	}
	var n int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_tasks").Scan(&n); err != nil || n != 0 {
		t.Fatal("metadata dispatched model Tasks", n, err)
	}
}

func TestSplitWorkerResumesObservedSetAfterRegistrationFailureAndPause(t *testing.T) {
	ctx := context.Background()
	k, jobs, apis, _ := splitWorkerRegistrationFixture(t)
	worker := splitRegistrationWorker(k, apis)
	for _, job := range jobs {
		if _, _, err := worker.Reconciler.Reconcile(ctx, job.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := k.BindProductSplitChildScopes(ctx, jobs[0].Value.Request); err != nil {
		t.Fatal(err)
	}
	if _, err := k.DB.Exec("CREATE TRIGGER fail_registered_receipt BEFORE INSERT ON records WHEN NEW.kind='product_split_registrations_admission' BEGIN SELECT RAISE(ABORT,'failed registration commit'); END"); err != nil {
		t.Fatal(err)
	}
	changed, err := worker.process(ctx, storage.SplitCreationReconciliationTarget{ID: jobs[0].ID})
	if changed || err == nil {
		t.Fatal("failed transaction emitted success/hint", changed, err)
	}
	if _, err := k.ReadProductSplitRegistrations(ctx, jobs[0].Value.Request); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("failed transaction retained partial registration", err)
	}
	if _, err := k.DB.Exec("DROP TRIGGER fail_registered_receipt"); err != nil {
		t.Fatal(err)
	}
	target := jobs[0].Value.Request.Target
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause-observed-registration", Target: target, Action: "pause", Reason: "operator"}); err != nil {
		t.Fatal(err)
	}
	changed, err = worker.process(ctx, storage.SplitCreationReconciliationTarget{ID: jobs[0].ID})
	if changed || !errors.Is(err, storage.ErrProductSplitHeld) {
		t.Fatal("pause bypassed registration gate", changed, err)
	}
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "resume-observed-registration", Target: target, Action: "resume", Reason: "operator"}); err != nil {
		t.Fatal(err)
	}
	changed, err = worker.process(ctx, storage.SplitCreationReconciliationTarget{ID: jobs[0].ID})
	if !changed || err != nil {
		t.Fatal("observed set could not resume", changed, err)
	}
	for _, api := range apis {
		if len(api.reads()) != 4 {
			t.Fatal("resume reread/recreated issues", api.reads())
		}
	}
}

func TestSplitWorkerConcurrentGETsNotifyOnlyCommittedRegistrationAndJoin(t *testing.T) {
	k, jobs, apis, _ := splitWorkerRegistrationFixture(t)
	for _, kind := range []string{"product_split_child_scopes_history", "product_work_item_registration_version"} {
		if _, err := k.DB.Exec("INSERT INTO records(kind,id,work_item_id,payload) VALUES(?,?,'other-wave','{')", kind, strings.Repeat(kind, 30)); err != nil {
			t.Fatal(err)
		}
	}
	worker := splitRegistrationWorker(k, apis)
	registered := make(chan struct{})
	badRows := make(chan struct{})
	var reports atomic.Int64
	var once sync.Once
	worker.OnMutation = func() {
		saved, err := k.ReadProductSplitRegistrations(context.Background(), jobs[0].Value.Request)
		if err == nil {
			if len(saved.Value.Registrations) != 2 {
				t.Error("hint exposed partial registered set")
			}
			once.Do(func() { close(registered) })
		}
	}
	worker.Report = func(err error) {
		if !errors.Is(err, storage.ErrTaskEvidence) {
			t.Error("healthy worker reported fault", err)
			return
		}
		if reports.Add(1) == 2 {
			close(badRows)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-registered:
	case <-time.After(8 * time.Second):
		cancel()
		<-done
		t.Fatal("whole set was not registered")
	}
	select {
	case <-badRows:
	case <-time.After(3 * time.Second):
		cancel()
		<-done
		t.Fatal("corrupt foreign witnesses were hidden", reports.Load())
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not join")
	}
	for _, api := range apis {
		if len(api.reads()) != 4 {
			t.Fatal("independent witness duplicated GET", api.reads())
		}
	}
}
