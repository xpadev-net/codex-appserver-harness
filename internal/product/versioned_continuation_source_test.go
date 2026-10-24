package product

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Local Git/SQL quality and registered HTTP stop are real; source GETs,
// formal approvals/CI/rules and the remote merge response are fixtures.
func checkVersionedContinuationSource(t *testing.T, e Executor, g KernelMergeGate, saved storage.ProductTargetSnapshot, requestID string, adoptRequest bool) {
	t.Helper()
	ctx := context.Background()
	k := e.Store
	job, err := k.PrepareProductMergeHTTP(ctx, requestID, "kernel-http-owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	checkMergeHTTPWrapper(t, g, saved, requestID, "success")
	parent, changed, err := k.RecordProductMergedWorkItem(ctx, job.ID)
	if err != nil || !changed || parent.Value.Projection == nil {
		t.Fatal(parent, changed, err)
	}
	w, err := k.Wave(ctx, saved.Value.Target.WaveID)
	if err != nil || w.Source != "github" || w.SourceID != saved.Value.WorkItem.ID {
		t.Fatal("initial Issue ownership missing", w, err)
	}
	b, err := k.SessionBinding(ctx, w.OrchestratorSessionID)
	if err != nil {
		t.Fatal(err)
	}
	var active string
	if err := k.DB.QueryRow("SELECT active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	var run contract.SessionRun
	if err := k.Record(ctx, "agent_run", active, &run); err != nil {
		t.Fatal(err)
	}
	run.State = "completed" // Preserve the actual turn already used by PR tools.
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := k.ArchiveSessionRun(ctx, b.ID, run.ID, "fixture source controller observes settled main; keep tool/budget audit"); err != nil {
		t.Fatal(err)
	}
	source, api := issueAdmissionFixture(t)
	instruction := "implement the next registered version with fresh independent verification"
	if adoptRequest {
		api.issue.Body += "\nexplicitly adopted next version of acceptance"
		api.issue.UpdatedAt = api.issue.UpdatedAt.Add(time.Second)
		instruction = api.issue.Title + "\n\n" + api.issue.Body
	}
	direction, err := k.RequestWaveDirection(ctx, storage.WaveDirectionRequest{Key: "versioned-source", Target: w.Target(), Instruction: instruction})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ApplyWaveDirection(ctx, direction.ID); err != nil {
		t.Fatal(err)
	}
	// The same deterministic source fixture used at initial admission. Repoint
	// only the observer's store; do not alter saved Wave/source/registration facts.
	observer := IssueSourceObserver{Store: k, API: api, Policy: source.Policy, Attempts: 1}
	observe := func(want string) domain.Wave {
		t.Helper()
		current, err := k.Wave(ctx, w.ID)
		if err != nil {
			t.Fatal(err)
		}
		reads := len(api.reads)
		update, ack, err := observer.observe(ctx, ctx, api, current, 5*time.Second)
		status := update.Snapshot.Value.Status
		if update.Hold != nil && update.Hold.Reason == "state_invalid" {
			status = "state_invalid"
		}
		if (err != nil && !(want == "state_invalid" && errors.Is(err, storage.ErrTaskEvidence))) || !ack || status != want || len(api.reads)-reads != 4 {
			t.Fatal("fresh source bookends/status", update, ack, err, api.reads[reads:])
		}
		current, err = k.Wave(ctx, w.ID)
		if err != nil || (current.SourceHold == nil) != (want == "ready") {
			t.Fatal("source hold", current, err)
		}
		return current
	}
	expectedBefore := "scope_unconfirmed"
	if adoptRequest {
		expectedBefore = "request_changed"
	}
	w = observe(expectedBefore)
	node, err := k.RegisterProductContinuation(ctx, storage.ProductContinuationRequest{Target: w.Target(), Key: "source-successor", ParentMergeID: job.ID, ParentReceiptDigest: parent.Digest, Definition: storage.ProductWorkItemDefinition{ID: "issue-successor", Repository: saved.Value.Repository, RepositoryID: saved.Value.WorkItem.RepositoryID, Criteria: []string{"AC"}}})
	if err != nil {
		t.Fatal(err)
	}
	held, err := k.Wave(ctx, w.ID)
	if err != nil || held.SourceHold == nil {
		t.Fatal("metadata registration cleared source hold", held, err)
	}
	budget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if adoptRequest {
		checkSourceRequestAdoption(t, k, w, node)
	}
	w = observe("ready")
	if w.ScopeRevision != 2 || w.OriginalRequest != held.OriginalRequest {
		t.Fatal("source read rewrote request or scope", w)
	}
	original, err := k.ReadProductWorkItemRegistration(ctx, w.SourceID)
	if err != nil || original.Digest != node.Value.ParentRegistrationDigest || original.Value.ScopeRevision != 1 {
		t.Fatal("source confirmation refreshed old PR registration", original, err)
	}
	oldProjection, err := k.ReadProductTarget(ctx, saved.Value.Target, saved.Value.WorkItem.ID)
	if err != nil || !reflect.DeepEqual(oldProjection, parent.Value.Projection.After) {
		t.Fatal("old PR changed", oldProjection, err)
	}
	if _, err := k.ReadProductTarget(ctx, w.Target(), node.Value.Request.Definition.ID); err == nil {
		t.Fatal("source ready manufactured a candidate")
	}
	coverage, err := k.ProductCoverage(ctx, w.Target())
	if err != nil || len(coverage.Value.Retired) != 1 || len(coverage.Value.Merged) != 0 || len(coverage.Value.Residual) != 1 || !reflect.DeepEqual(coverage.Value.UnmergedWorkItems, []string{node.Value.Request.Definition.ID}) {
		t.Fatal("source ready accepted old quality", coverage, err)
	}
	// Every new read still applies normal source gates.
	api.issue.State = "closed"
	observe("closed")
	api.issue.State = "open"
	originalBody := api.issue.Body
	api.issue.Body += "\nchanged request is not silently adopted"
	observe("request_changed")
	api.issue.Body = originalBody
	observe("ready")
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_continuation' AND id=?", node.Value.Request.Definition.ID); err != nil {
		t.Fatal(err)
	}
	if adoptRequest {
		observe("state_invalid")
	} else {
		observe("scope_unconfirmed")
	}
	if err := k.Put(ctx, "product_continuation", node.Value.Request.Definition.ID, w.ID, node); err != nil {
		t.Fatal(err)
	}
	w = observe("ready")
	afterBudget, err := k.WaveOperationBudget(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(afterBudget, budget) || w.State == domain.WaveCompleted || w.MainTurns != held.MainTurns || w.TasksCreated != held.TasksCreated || w.Replans != held.Replans || w.Deadline != held.Deadline {
		t.Fatal("source reads reset budget or accepted Wave", w, afterBudget, err)
	}
	if adoptRequest {
		checkAdoptedRequestTaskEvidence(t, k, w, node)
	}
}
