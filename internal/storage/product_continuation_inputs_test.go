package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func continuationTaskFixture(t *testing.T) (*KernelStore, domain.Wave, domain.Task, ProductContinuation) {
	t.Helper()
	ctx := context.Background()
	k, request, _ := productContinuationFixture(t)
	lineage, err := k.RegisterProductContinuation(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	plan := storagePlanFixture()
	for i := range plan.Phases {
		phase := &plan.Phases[i]
		phase.ID = "continued-" + phase.ID
		for j := range phase.Tasks {
			task := &phase.Tasks[j]
			task.ID = "continued-" + task.ID
			task.WorkItemIDs = []string{request.Definition.ID}
			for d := range task.DependsOn {
				task.DependsOn[d].TaskID = "continued-" + task.DependsOn[d].TaskID
			}
		}
		for j := range phase.Completion.RequiredTaskIDs {
			phase.Completion.RequiredTaskIDs[j] = "continued-" + phase.Completion.RequiredTaskIDs[j]
		}
	}
	admitted, err := k.AdoptPlan(ctx, request.Target, "continued-plan", plan)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	task, err := k.Task(ctx, admitted.CreatedTaskIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	return k, w, task, lineage
}

func TestProductContinuationBindsNewTaskAndBoundedRoleInput(t *testing.T) {
	ctx := context.Background()
	k, w, task, node := continuationTaskFixture(t)
	prep, err := k.ReadProductTaskPreparation(ctx, task.ID)
	if err != nil || len(prep.Continuations) != 1 || prep.Continuations[0].Digest != node.Digest || prep.Product != nil {
		t.Fatal(prep, err)
	}
	manifest := registrationManifest(t, k, task.SessionID, w.Target())
	if len(manifest.ProductContinuations) != 1 || manifest.ProductContinuations[0].Digest != node.Digest || manifest.ProductContinuations[0].ParentMergeSHA != node.Value.ParentMergeSHA {
		t.Fatal(manifest.ProductContinuations)
	}
	raw, err := json.Marshal(manifest.ProductContinuations)
	if err != nil || strings.Contains(string(raw), k.Root) {
		t.Fatal("private lineage data in model summary", string(raw), err)
	}
	input, err := productTaskDigest(t, k, w, task.Spec)
	if err != nil || input != task.InputDigest {
		t.Fatal(input, task.InputDigest, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_continuation' AND id=?", node.Value.Request.Definition.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := productTaskDigest(t, k, w, task.Spec); err == nil {
		t.Fatal("lost lineage retained captured Task input")
	}
	if _, err := k.ReadProductTaskPreparation(ctx, task.ID); err == nil {
		t.Fatal("lost lineage released preparation")
	}
	if ready, err := k.ReadyTasks(ctx, ReadyRequest{Target: w.Target()}); err != nil || len(ready.Tasks) != 0 {
		t.Fatal("lost lineage became a scheduler error or dispatch", ready, err)
	}
	if err := k.Put(ctx, "product_continuation", node.Value.Request.Definition.ID, w.ID, node); err != nil {
		t.Fatal(err)
	}
	if restored, err := productTaskDigest(t, k, w, task.Spec); err != nil || restored != input {
		t.Fatal("restored proof rewrote input", restored, err)
	}
	current, err := k.Task(ctx, task.ID)
	if err != nil || !reflect.DeepEqual(current, task) {
		t.Fatal("lineage read restarted Task", current, err)
	}
}

func TestProductContinuationWriterRequiresFixedBaseAndOriginalMergeAncestor(t *testing.T) {
	ctx := context.Background()
	k, w, task, node := continuationTaskFixture(t)
	policy, err := k.ReadProductVerificationPolicy(ctx, w.Target(), node.Value.Request.Definition.RepositoryID)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := workspace.Resolve(ctx, policy.Value.Repository, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	request := ProductWorkspaceRequest{TaskID: task.ID, RepositoryID: policy.Value.RepositoryID, Repository: policy.Value.Repository, BaseSHA: initial}
	if _, err := k.ReserveProductWorkspace(ctx, request); !errors.Is(err, ErrProductBasePlanHeld) {
		t.Fatal("continuation bypassed fresh base selection", err)
	}
	if _, err := k.ProductWorkspace(ctx, task.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("held continuation persisted checkout", err)
	}
	input, err := k.ReadProductBaseInput(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	ancestors, err := input.RequiredAncestors(initial)
	if err != nil || !slices.Contains(ancestors, node.Value.ParentMergeSHA) {
		t.Fatal("base planning lost original merge", ancestors, err)
	}
	// The storage observation is trusted fixture metadata. It does not claim
	// these hashes were fetched or inspected by a real GitHub ancestry reader.
	observation := ProductBaseObservation{Head: ProductBaseHead{RepositoryID: request.RepositoryID, Repository: node.Value.Request.Definition.Repository, BaseBranch: "master", InitialSHA: initial, HeadSHA: strings.Repeat("f", 40), Remote: "https://github.com/owner/repo.git", Ancestors: ancestors}, ObservedAt: k.clock()}
	plan, err := k.PrepareProductBasePlan(ctx, task.ID, input.Digest, observation)
	if err != nil {
		t.Fatal(err)
	}
	request.BasePlanID, request.BasePlanDigest, request.BaseSHA = plan.ID, plan.Digest, plan.Value.Head.HeadSHA
	saved, err := k.ReserveProductWorkspace(ctx, request)
	if err != nil || saved.State != "prepared" {
		t.Fatal(saved, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_continuation_history' AND id=?", node.ID); err != nil {
		t.Fatal(err)
	}
	if claim, err := k.ClaimProductWorkspace(ctx, task.ID, saved.Digest); err == nil || claim.Start {
		t.Fatal("lost lineage started Git", claim, err)
	}
}

func TestProductContinuationHistoricalNodeDoesNotBecomeCurrentInput(t *testing.T) {
	ctx := context.Background()
	k, request, _ := productContinuationFixture(t)
	node, err := k.RegisterProductContinuation(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	direction := directionRequest(w)
	direction.Key = "later-contract"
	d, err := k.RequestWaveDirection(ctx, direction)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	w, err = k.Wave(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := k.checkProductContinuationInWaveTx(ctx, tx, node, w, false); err != nil {
		t.Fatal("original parent origin no longer auditable", err)
	}
	if _, err := k.productContinuationInputsTx(ctx, tx, w, request.Definition.ID); err == nil || !readinessHoldError(err) {
		t.Fatal("historical node authorized current source input")
	}
}
