package storage

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
)

func productContinuationFixture(t *testing.T) (*KernelStore, ProductContinuationRequest, ProductMergedWorkItem) {
	t.Helper()
	ctx := context.Background()
	k, job := knownCoverageFixture(t)
	parent, _, err := k.RecordProductMergedWorkItem(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, job.Value.Product.Value.Target.WaveID)
	if err != nil {
		t.Fatal(err)
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
	// Preserve the turn already associated by the publication/review tools.
	run.State = "completed"
	if err := k.Put(ctx, "agent_run", run.ID, b.ID, run); err != nil {
		t.Fatal(err)
	}
	if err := k.MarkSessionStopped(ctx, b.ID, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := k.ArchiveSessionRun(ctx, b.ID, run.ID, "fixture main stopped without another judgment; preserve tool and budget history"); err != nil {
		t.Fatal(err)
	}
	d, err := k.RequestWaveDirection(ctx, directionRequest(w))
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
	p := parent.Value.Product.Value
	return k, ProductContinuationRequest{Target: w.Target(), Key: "adopt-new-version", ParentMergeID: job.ID, ParentReceiptDigest: parent.Digest, Definition: ProductWorkItemDefinition{ID: "work-v2", Repository: p.Repository, RepositoryID: p.WorkItem.RepositoryID, Criteria: []string{"AC"}}}, parent
}

func TestProductContinuationRegistrationPreservesOldMergeAndWaveBudget(t *testing.T) {
	ctx := context.Background()
	k, request, parent := productContinuationFixture(t)
	before, err := k.Wave(ctx, request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := k.RegisterProductContinuation(ctx, request)
	if err != nil || saved.Value.ParentWorkItemID != parent.Value.Product.Value.WorkItem.ID || saved.Value.ParentTarget != parent.Value.Product.Value.Target || saved.Value.ParentMergeSHA != parent.Value.MergeSHA || saved.Value.Registration.Value.ScopeRevision != request.Target.ScopeRevision {
		t.Fatal(saved, err)
	}
	if read, err := k.ReadProductContinuation(ctx, request.Definition.ID); err != nil || !reflect.DeepEqual(read, saved) {
		t.Fatal(read, err)
	}
	if replay, err := k.RegisterProductContinuation(ctx, request); err != nil || !reflect.DeepEqual(replay, saved) {
		t.Fatal("continuation replay changed lineage", replay, err)
	}
	original, err := k.ReadProductMergedWorkItem(ctx, request.ParentMergeID)
	if err != nil || !reflect.DeepEqual(original, parent) {
		t.Fatal("new version changed original merge", original, err)
	}
	current, err := k.ReadProductTarget(ctx, parent.Value.Product.Value.Target, parent.Value.Product.Value.WorkItem.ID)
	if err != nil || !reflect.DeepEqual(current, parent.Value.Projection.After) {
		t.Fatal("new work changed merged PR projection", current, err)
	}
	if _, err := k.ReadProductTarget(ctx, request.Target, request.Definition.ID); err == nil {
		t.Fatal("registration manufactured a candidate")
	}
	after, err := k.Wave(ctx, before.ID)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("lineage changed Wave/Task budget", after, err)
	}
	afterBudget, err := k.WaveOperationBudget(ctx, before.ID)
	if err != nil || !reflect.DeepEqual(afterBudget, budget) {
		t.Fatal(afterBudget, err)
	}
	path := k.Root + "/state.db"
	k.Close()
	k, err = OpenExistingKernel(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if read, err := k.ReadProductContinuation(ctx, request.Definition.ID); err != nil || !reflect.DeepEqual(read, saved) {
		t.Fatal("reopen lost lineage", read, err)
	}
}

func TestProductContinuationRequiresNewVersionOriginalParentAndAtomicSave(t *testing.T) {
	for _, mode := range []string{"same_version", "wrong_receipt", "same_work_item", "repository", "unknown_criterion", "pause", "changed_parent", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, request, parent := productContinuationFixture(t)
			var err error
			switch mode {
			case "same_version":
				w, e := k.Wave(ctx, request.Target.WaveID)
				if e != nil {
					t.Fatal(e)
				}
				w.ScopeRevision = parent.Value.Product.Value.Target.ScopeRevision
				tx, e := k.kernelTx(ctx)
				if e != nil {
					t.Fatal(e)
				}
				err = saveWave(ctx, tx, w)
				if err == nil {
					err = tx.Commit()
				} else {
					tx.Rollback()
				}
				request.Target = w.Target()
			case "wrong_receipt":
				request.ParentReceiptDigest = strings.Repeat("a", 64)
			case "same_work_item":
				request.Definition.ID = parent.Value.Product.Value.WorkItem.ID
			case "repository":
				request.Definition.RepositoryID++
			case "unknown_criterion":
				request.Definition.Criteria = []string{"foreign"}
			case "pause":
				_, err = k.ApplyOperatorControl(ctx, OperatorControl{Key: "pause-continuation", Target: request.Target, Action: "pause", Reason: "fixture"})
			case "changed_parent":
				p := parent.Value.Projection.After
				p.Value.WorkItem.Version++
				p.Digest, err = contentDigest(p.Value)
				if err == nil {
					err = k.Put(ctx, "product_target", productTargetID(parent.Value.Product.Value.Target, p.Value.WorkItem.ID), request.Target.WaveID, p)
				}
			case "rollback":
				_, err = k.DB.Exec("CREATE TRIGGER fail_continuation BEFORE INSERT ON records WHEN NEW.kind='product_continuation' BEGIN SELECT RAISE(FAIL,'continuation rollback'); END")
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.RegisterProductContinuation(ctx, request); err == nil {
				t.Fatal("invalid continuation admitted", mode)
			}
			var count int
			if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE (kind LIKE 'product_continuation%') OR (kind IN ('product_work_item_registration','wave_work_item') AND id='work-v2') OR (kind='product_work_item_registration_version' AND json_extract(payload,'$.value.definition.id')='work-v2')").Scan(&count); err != nil || count != 0 {
				t.Fatal("partial continuation registration committed", count, err)
			}
		})
	}
}

func TestProductContinuationMissingEvidenceCannotBeReconstructed(t *testing.T) {
	for _, mode := range []string{"head", "history", "admission", "request_key", "head_and_admission", "registration", "registration_version", "parent_receipt", "parent_history"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, request, _ := productContinuationFixture(t)
			saved, err := k.RegisterProductContinuation(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "head":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_continuation' AND id=?", request.Definition.ID)
			case "history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_continuation_history' AND id=?", saved.ID)
			case "admission":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_continuation_admission' AND id=?", request.Definition.ID)
			case "request_key":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_continuation_request' AND id=?", productContinuationRequestID(request))
			case "head_and_admission":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind IN ('product_continuation','product_continuation_admission') AND id=?", request.Definition.ID)
			case "registration":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration' AND id=?", request.Definition.ID)
			case "registration_version":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration_version' AND id=?", structureID("product-work-", request.Definition.ID, saved.Value.Registration.Digest))
			case "parent_receipt":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_merged_work_item' AND id=?", request.ParentMergeID)
			case "parent_history":
				_, err = k.DB.Exec("DELETE FROM records WHERE kind='product_merged_work_item_history' AND id=?", request.ParentMergeID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ReadProductContinuation(ctx, request.Definition.ID); err == nil {
				t.Fatal("lost lineage evidence accepted")
			}
			if _, err := k.RegisterProductContinuation(ctx, request); err == nil {
				t.Fatal("lost lineage evidence reconstructed")
			}
		})
	}
}

func TestProductContinuationRequestKeyCannotCreateDifferentSuccessors(t *testing.T) {
	ctx := context.Background()
	k, request, _ := productContinuationFixture(t)
	requests := []ProductContinuationRequest{request, request}
	requests[1].Definition.ID = "different-successor"
	var group sync.WaitGroup
	results := make([]ProductContinuation, 2)
	errs := make([]error, 2)
	for i := range requests {
		group.Add(1)
		go func(i int) { defer group.Done(); results[i], errs[i] = k.RegisterProductContinuation(ctx, requests[i]) }(i)
	}
	group.Wait()
	winner := -1
	for i, err := range errs {
		if err == nil {
			if winner != -1 {
				t.Fatal("same key registered multiple successors", results)
			}
			winner = i
		}
	}
	if winner == -1 {
		t.Fatal(errs)
	}
	if replay, err := k.RegisterProductContinuation(ctx, requests[winner]); err != nil || !reflect.DeepEqual(replay, results[winner]) {
		t.Fatal(replay, err)
	}
	var count int
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_continuation'").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	if _, err := k.ReadProductWorkItemRegistration(ctx, requests[1-winner].Definition.ID); err == nil {
		t.Fatal("loser key request left ownership behind")
	}
}

func TestProductContinuationMetadataRefreshRetainsSourceHoldAndControls(t *testing.T) {
	// Exercise the private metadata-refresh boundary with a real GitHub-source
	// Wave. Receipt authentication is covered by the public-port tests above;
	// this fixture does not assert a native source observation or merge.
	ctx := context.Background()
	k, w, definition := productRegistrationFixture(t, "github")
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err != nil {
		t.Fatal(err)
	}
	d, err := k.RequestWaveDirection(ctx, directionRequest(w))
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
	w.SourceHold = &domain.WaveSourceHold{Reason: "source_changed", Digest: strings.Repeat("a", 64), ObservedAt: k.clock()}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveWave(ctx, tx, w); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	definition.ID = "work-v2"
	if _, err := k.RegisterProductWorkItem(ctx, w.Target(), "", definition); err == nil {
		t.Fatal("ordinary registration bypassed source hold")
	}
	if !productContinuationRegistrationAllowed(w, w.Target(), k.clock()) {
		t.Fatal("held source metadata could not be refreshed")
	}
	for _, mode := range []string{"pause", "direction", "cancel", "deadline"} {
		blocked := w
		when := k.clock()
		switch mode {
		case "pause":
			blocked.Paused = true
		case "direction":
			blocked.PendingDirectionID = "pending"
		case "cancel":
			blocked.State = domain.WaveCanceled
		case "deadline":
			when = w.Deadline
		}
		if productContinuationRegistrationAllowed(blocked, w.Target(), when) {
			t.Fatal("continuation bypassed control", mode)
		}
	}
	tx, err = k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.registerProductWorkItemSourceTx(ctx, tx, w.Target(), "", definition, true); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after, err := k.Wave(ctx, w.ID)
	if err != nil || !reflect.DeepEqual(after, w) || !after.DispatchPaused() {
		t.Fatal("metadata refresh cleared source prohibition", after, err)
	}
}

func TestProductContinuationRetainsOriginalLineageAfterRegistrationRefresh(t *testing.T) {
	ctx := context.Background()
	k, request, _ := productContinuationFixture(t)
	saved, err := k.RegisterProductContinuation(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	w, err := k.Wave(ctx, request.Target.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	direction := directionRequest(w)
	direction.Key = "next-source-revision"
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
	updated, err := k.RegisterProductWorkItem(ctx, w.Target(), saved.Value.Registration.Digest, request.Definition)
	if err != nil || updated.Digest == saved.Value.Registration.Digest {
		t.Fatal(updated, err)
	}
	read, err := k.ReadProductContinuation(ctx, request.Definition.ID)
	if err != nil || !reflect.DeepEqual(read, saved) {
		t.Fatal("legitimate metadata refresh lost original lineage", read, err)
	}
	if replay, err := k.RegisterProductContinuation(ctx, request); err != nil || !reflect.DeepEqual(replay, saved) {
		t.Fatal("refresh changed operation replay", replay, err)
	}
	var original ProductContinuation
	if err := k.Record(ctx, "product_continuation_history", saved.ID, &original); err != nil || !reflect.DeepEqual(original, saved) {
		t.Fatal("refresh rewrote historical registration", original, err)
	}
	if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_work_item_registration_version' AND id=?", structureID("product-work-", request.Definition.ID, saved.Value.Registration.Digest)); err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReadProductContinuation(ctx, request.Definition.ID); err == nil {
		t.Fatal("current metadata replaced lost original version evidence")
	}
}
