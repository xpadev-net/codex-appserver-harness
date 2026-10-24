package product

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Invoked from the actual Git/runner/independent-review quality fixture after
// its real gate packet consumed the fence. HTTP/native facts remain injected.
func checkMergeReconciler(t *testing.T, k *storage.KernelStore, job storage.ProductMerge, api *cloudReadAPI) {
	t.Helper()
	ctx := context.Background()
	reader := MergeReconciler{Store: k, API: func(storage.ProductTarget) ResourceAPI { return api }}
	reads := api.metadata
	if _, err := reader.Reconcile(ctx, job.ID); !errors.Is(err, storage.ErrProductMergeHeld) || api.metadata != reads {
		t.Fatal("native read certified missing physical stop", err, api.metadata, reads)
	}
	stop := storage.ProductMergeStop{MergeID: job.ID, ProofID: "merge-read-fixture-stop", Reason: "fixture actual synchronous owner joined", StartedAt: job.ClaimedAt, FinishedAt: time.Now().UTC(), Disposition: "outcome_unknown"}
	if _, err := k.RecordProductMergeStopped(ctx, job.ID, job.Value.OwnerID, job.Digest, stop); err != nil {
		t.Fatal(err)
	}
	baselinePR, baselineRepo := api.pr, api.repository
	baselinePR.State, baselinePR.Merged, baselinePR.MergeCommitSHA = "closed", true, strings.Repeat("e", 40)
	for _, mode := range []string{"open", "closed_unmerged", "new_head", "base_ref", "wrong_head_repo", "missing_sha", "wrong_repository", "wrong_marker", "changed_repository", "changed_pr", "read_error"} {
		t.Run("merge_reconcile_"+mode, func(t *testing.T) {
			api.pr, api.repository, api.pulls, api.metadata, api.onFinal, api.fail = baselinePR, baselineRepo, 0, 0, nil, "never"
			switch mode {
			case "open":
				api.pr.State = "open"
				api.pr.Merged = false
			case "closed_unmerged":
				api.pr.Merged = false
			case "new_head":
				api.pr.Head.SHA = strings.Repeat("f", 40)
			case "base_ref":
				api.pr.Base.Ref = "other"
			case "wrong_head_repo":
				api.pr.Head.Repo.ID++
			case "missing_sha":
				api.pr.MergeCommitSHA = ""
			case "wrong_repository":
				api.repository.Name = "other/repo"
			case "wrong_marker":
				api.pr.Body = "unrelated"
			case "changed_repository":
				api.onFinal = func() { api.repository.DefaultBranch = "other" }
			case "changed_pr":
				api.onFinal = func() { api.pr.Head.SHA = strings.Repeat("f", 40) }
			case "read_error":
				api.fail = ""
			}
			if _, err := reader.Reconcile(ctx, job.ID); err == nil {
				t.Fatal("unproven merged result released work", mode)
			}
			saved, err := k.ReadProductMerge(ctx, job.ID)
			if err != nil || saved.State != "stopped_unknown" {
				t.Fatal(saved, err)
			}
		})
	}
	api.pr, api.repository, api.pulls, api.metadata, api.onFinal, api.fail = baselinePR, baselineRepo, 0, 0, nil, "never"
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "cancel-before-merge-read", Target: job.Value.Product.Value.Target, Action: "cancel", Reason: "still record historical native fact"}); err != nil {
		t.Fatal(err)
	}
	p := job.Value.Product.Value
	before, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := checkMergeReconciliationWorker(t, reader, job, api)
	if err != nil || merged.State != "merged" || merged.Reconciliation == nil || merged.Reconciliation.MergeSHA != baselinePR.MergeCommitSHA || api.pulls != 2 || api.metadata != 2 {
		t.Fatal(merged, err, api.pulls, api.metadata)
	}
	after, err := k.ReadProductTarget(ctx, p.Target, p.WorkItem.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("native fact overwrote current Product", after, err)
	}
	api.fail = ""
	again, err := reader.Reconcile(ctx, job.ID)
	if err != nil || !reflect.DeepEqual(merged, again) || api.metadata != 2 {
		t.Fatal("completed reconciliation rescanned", again, err, api.metadata)
	}
}
