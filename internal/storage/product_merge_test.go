package storage

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
)

func mergeJournalFixture(t *testing.T) (*KernelStore, string) {
	t.Helper()
	ctx := context.Background()
	k, publishID, owner, _ := branchPublicationFixture(t)
	var claim PRDispatchClaim
	if err := k.Record(ctx, "product_pr_dispatch", publishID, &claim); err != nil {
		t.Fatal(err)
	}
	p := claim.Product.Value
	if _, err := k.BindObservedPRPublication(ctx, publishID, PRTransportProof{ID: "merge-fixture-publish", OwnerID: owner, Disposition: "not_started"}, PRResourceObservation{ProofID: "merge-fixture-pr", RepositoryID: p.WorkItem.RepositoryID, HeadRepositoryID: p.WorkItem.RepositoryID, BaseRepositoryID: p.WorkItem.RepositoryID, Number: 42, MarkerWorkItemID: p.WorkItem.ID, HeadSHA: p.Candidate.SHA, HeadBranch: p.WorkItem.Branch, BaseBranch: p.BaseBranch, State: "open", ObservedAt: k.clock()}); err != nil {
		t.Fatal(err)
	}
	_, err := k.PrepareProductPublication(ctx, p.Target, p.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	var runID string
	if err := k.DB.QueryRow("SELECT active_run_id FROM kernel_sessions WHERE id=?", b.ID).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	request := contract.RequestPRReviewRequest{SchemaVersion: 1, Target: p.Target, Candidate: contract.CandidateReference{WorkItemID: p.WorkItem.ID, CandidateID: p.Candidate.ID, SHA: p.Candidate.SHA}}
	call := kernelCall(t, b, contract.SessionRun{ID: runID}, "pr_review_request", request)
	// Use the same reserved turn that publication already bound.
	call.CallID = "merge-review"
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	reviewOwner := "merge-fixture-review"
	if _, err := k.PreparePRDispatch(ctx, reply.OperationID, reviewOwner); err != nil {
		t.Fatal(err)
	}
	packet, err := k.SavePRReviewRequestPayload(ctx, reply.OperationID, reviewOwner, "@review please review", 71, []int64{91})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.BindObservedPRReviewRequest(ctx, reply.OperationID, PRTransportProof{ID: "merge-fixture-request", OwnerID: reviewOwner, Disposition: "not_started"}, PRReviewRequestObservation{ProofID: "merge-fixture-comment", RepositoryID: p.WorkItem.RepositoryID, PRNumber: 42, RoundID: packet.RoundID, CommentID: 8, RequesterID: 71, Body: packet.Body(), ObservedAt: k.clock()}); err != nil {
		t.Fatal(err)
	}
	return k, reply.OperationID
}

func TestProductMergeJournalClaimsPhysicalOwnerAndSendFenceOnce(t *testing.T) {
	ctx := context.Background()
	k, id := mergeJournalFixture(t)
	job, err := k.PrepareProductMerge(ctx, id, "merge-owner", "merge")
	if err != nil || job.State != "prepared" {
		t.Fatal(job, err)
	}
	again, err := k.PrepareProductMerge(ctx, id, "merge-owner", "merge")
	if err != nil || !reflect.DeepEqual(job, again) {
		t.Fatal("prepare replay changed identity", again, err)
	}
	for _, args := range [][2]string{{"foreign", "merge"}, {"merge-owner", "squash"}} {
		if _, err := k.PrepareProductMerge(ctx, id, args[0], args[1]); !errors.Is(err, ErrProductMergeHeld) {
			t.Fatal("changed owner/method", err)
		}
	}
	if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, "merge-owner", job.Digest, mergeGateObservationFixture(k, job)); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("send without physical owner", err)
	}
	var starts atomic.Int64
	var wg sync.WaitGroup
	failures := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claim, err := k.ClaimProductMerge(ctx, job.ID, "merge-owner", job.Digest)
			if err != nil {
				failures <- err
			}
			if claim.Start {
				starts.Add(1)
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if starts.Load() != 1 {
		t.Fatal("multiple physical launches", starts.Load())
	}
	sent, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, "merge-owner", job.Digest, mergeGateObservationFixture(k, job))
	if err != nil || sent.State != "unknown" || sent.MayHaveSentAt.IsZero() {
		t.Fatal(sent, err)
	}
	if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, "merge-owner", job.Digest, mergeGateObservationFixture(k, job)); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("duplicate send fence", err)
	}
	root := k.Root
	k.Close()
	k, err = OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	read, err := k.ReadProductMerge(ctx, job.ID)
	if err != nil || !reflect.DeepEqual(read, sent) {
		t.Fatal("unknown not retained after reopen", read, err)
	}
	if claim, err := k.ClaimProductMerge(ctx, job.ID, "merge-owner", job.Digest); err != nil || claim.Start {
		t.Fatal("unknown relaunched", claim, err)
	}
	if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, "merge-owner", job.Digest, mergeGateObservationFixture(k, job)); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("reopened owner reused fence", err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := claimResourceTx(ctx, tx, "product-pr-mutation:"+productMergeResource(job.Value.Product.Value), "foreign", job.Value.Product.Value.Target.WaveID); !errors.Is(err, ErrResourceBusy) {
		t.Fatal("unknown mutation lease released", err)
	}
}

func TestProductMergeJournalControlsQualityAndLostEvidenceHold(t *testing.T) {
	for _, mode := range []string{"pause", "cancel", "merge_hold", "new_candidate", "quality_lost", "head_lost", "history_lost", "claim_lost", "send_lost", "foreign_fence", "reopen_before_send", "rollback_prepare", "rollback_send"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, id := mergeJournalFixture(t)
			if mode == "rollback_prepare" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_merge BEFORE INSERT ON records WHEN NEW.kind='product_merge' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
					t.Fatal(err)
				}
				if _, err := k.PrepareProductMerge(ctx, id, "owner", "merge"); err == nil {
					t.Fatal("failed prepare succeeded")
				}
				var n int
				if err := k.DB.QueryRow("SELECT count(*) FROM kernel_resource_leases WHERE run_id=?", productMergeID(id)).Scan(&n); err != nil || n != 0 {
					t.Fatal("prepare leaked lease", n, err)
				}
				return
			}
			job, err := k.PrepareProductMerge(ctx, id, "owner", "merge")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "head_lost" || mode == "history_lost" {
				kind := "product_merge"
				if mode == "history_lost" {
					kind = "product_merge_history"
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, job.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := k.PrepareProductMerge(ctx, id, "owner", "merge"); err == nil {
					t.Fatal("lost evidence regenerated")
				}
				return
			}
			if _, err := k.ClaimProductMerge(ctx, job.ID, "owner", job.Digest); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "pause", "cancel":
				if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "merge-control", Target: job.Value.Product.Value.Target, Action: mode, Reason: "fence must refresh controls"}); err != nil {
					t.Fatal(err)
				}
			case "merge_hold", "new_candidate":
				p := job.Value.Product.Value
				if mode == "merge_hold" {
					p.WorkItem.MergeHold = true
				} else {
					p.Candidate.ID = "next"
					p.WorkItem.Candidate = "next"
					p.WorkItem.Version++
				}
				if _, err := k.SetProductTarget(ctx, job.Value.Product.Digest, p); err != nil {
					t.Fatal(err)
				}
			case "quality_lost":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_publication_readiness_history' AND id=?", job.Value.ReadinessID); err != nil {
					t.Fatal(err)
				}
			case "claim_lost":
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_claim' AND id=?", job.ID); err != nil {
					t.Fatal(err)
				}
			case "send_lost":
				if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, "owner", job.Digest, mergeGateObservationFixture(k, job)); err != nil {
					t.Fatal(err)
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_merge_send' AND id=?", job.ID); err != nil {
					t.Fatal(err)
				}
			case "foreign_fence":
				if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, "foreign", job.Digest, mergeGateObservationFixture(k, job)); !errors.Is(err, ErrProductMergeHeld) {
					t.Fatal(err)
				}
				return
			case "reopen_before_send":
				root := k.Root
				k.Close()
				k, err = OpenExistingKernel(root + "/state.db")
				if err != nil {
					t.Fatal(err)
				}
				defer k.Close()
			case "rollback_send":
				if _, err := k.DB.Exec("CREATE TRIGGER fail_merge_send BEFORE INSERT ON records WHEN NEW.kind='product_merge_send' BEGIN SELECT RAISE(FAIL,'rollback'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, "owner", job.Digest, mergeGateObservationFixture(k, job)); err == nil {
				t.Fatal("unsafe send fence", mode)
			}
			if mode == "rollback_send" {
				read, err := k.ReadProductMerge(ctx, job.ID)
				if err != nil || read.State != "launching" || !read.MayHaveSentAt.IsZero() {
					t.Fatal("failed send spent fence", read, err)
				}
			}
		})
	}
}

func TestProductMergeJournalCancelsOnlyUnclaimedPreparedWorkAndProtectsNextLease(t *testing.T) {
	ctx := context.Background()
	k, id := mergeJournalFixture(t)
	job, err := k.PrepareProductMerge(ctx, id, "owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	canceled, err := k.CancelPreparedProductMerge(ctx, job.ID, "owner", job.Digest, "not-started", "prepared owner canceled")
	if err != nil || canceled.State != "canceled" {
		t.Fatal(canceled, err)
	}
	tx, err := k.kernelTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next, err := claimResourceTx(ctx, tx, job.MutationLease.Resource, "next-owner", job.Value.Product.Value.Target.WaveID)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if next.Generation <= job.MutationLease.Generation {
		t.Fatal("lease generation reused")
	}
	again, err := k.CancelPreparedProductMerge(ctx, job.ID, "owner", job.Digest, "not-started", "prepared owner canceled")
	if err != nil || !reflect.DeepEqual(canceled, again) {
		t.Fatal("cancellation replay changed", again, err)
	}
	if claim, err := k.ClaimProductMerge(ctx, job.ID, "owner", job.Digest); err != nil || claim.Start {
		t.Fatal("canceled job relaunched", claim, err)
	}
	var state, owner string
	if err := k.DB.QueryRow("SELECT state,run_id FROM kernel_resource_leases WHERE resource=?", job.MutationLease.Resource).Scan(&state, &owner); err != nil || state != "held" || owner != "next-owner" {
		t.Fatal("old cancellation released next owner", state, owner, err)
	}
}

func mergeGateObservationFixture(k *KernelStore, job ProductMerge) ProductMergeGateObservation {
	p := job.Value.Product.Value
	report := contract.NewReport(p.Repository)
	report.RepositoryID, report.DefaultBranch = p.WorkItem.RepositoryID, p.BaseBranch
	for _, name := range contract.MergeRequirements {
		report.Capabilities[name] = contract.Capability{State: contract.Verified, Evidence: "injected storage fixture"}
	}
	return ProductMergeGateObservation{ProductDigest: job.Value.Product.Digest, OperationID: job.Value.RequestID, RoundDigest: job.Value.RoundDigest, HeadSHA: p.Candidate.SHA, BaseSHA: p.Candidate.BaseSHA, TestedSHA: p.Candidate.SHA, ReviewIDs: []int64{101}, CheckIDs: []int64{201}, ObservedAt: k.clock(), Report: report}
}

func TestProductMergeJournalRequiresFreshBoundedGateObservationAtFence(t *testing.T) {
	ctx := context.Background()
	k, id := mergeJournalFixture(t)
	job, err := k.PrepareProductMerge(ctx, id, "owner", "merge")
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := k.ClaimProductMerge(ctx, job.ID, "owner", job.Digest)
	if err != nil {
		t.Fatal(err)
	}
	job = claimed.Merge
	for _, mode := range []string{"empty", "product", "request", "round", "head", "base", "tested", "reviews", "checks", "duplicate_reviews", "future", "old", "before_claim", "unknown_capability", "report_identity", "report_future"} {
		t.Run(mode, func(t *testing.T) {
			proof := mergeGateObservationFixture(k, job)
			switch mode {
			case "empty":
				proof = ProductMergeGateObservation{}
			case "product":
				proof.ProductDigest = "other"
			case "request":
				proof.OperationID = "other"
			case "round":
				proof.RoundDigest = "other"
			case "head":
				proof.HeadSHA = "other"
			case "base":
				proof.BaseSHA = "other"
			case "tested":
				proof.TestedSHA = "other"
			case "reviews":
				proof.ReviewIDs = nil
			case "checks":
				proof.CheckIDs = nil
			case "duplicate_reviews":
				proof.ReviewIDs = []int64{101, 101}
			case "future":
				proof.ObservedAt = k.clock().Add(time.Hour)
			case "old":
				proof.ObservedAt = k.clock().Add(-time.Minute)
				proof.Report.ObservedAt = proof.ObservedAt
			case "before_claim":
				proof.ObservedAt = job.ClaimedAt.Add(-time.Nanosecond)
				proof.Report.ObservedAt = proof.ObservedAt
			case "unknown_capability":
				proof.Report.Capabilities["cloud_native_approval"] = contract.Capability{State: contract.Unknown}
			case "report_identity":
				proof.Report.RepositoryID++
			case "report_future":
				proof.Report.ObservedAt = proof.ObservedAt.Add(time.Hour)
			}
			if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, "owner", job.Digest, proof); !errors.Is(err, ErrProductMergeHeld) {
				t.Fatal("invalid gate packet admitted", mode, err)
			}
			read, err := k.ReadProductMerge(ctx, job.ID)
			if err != nil || read.State != "launching" || !read.MayHaveSentAt.IsZero() {
				t.Fatal("failed packet spent fence", read, err)
			}
		})
	}
	if _, err := k.CancelPreparedProductMerge(ctx, job.ID, "owner", job.Digest, "fake-stop", "already claimed"); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("claimed work treated as never started", err)
	}
	if _, err := k.MarkProductMergeMayHaveSent(ctx, job.ID, "owner", job.Digest, mergeGateObservationFixture(k, job)); err != nil {
		t.Fatal("valid host observation rejected", err)
	}
	if _, err := k.CancelPreparedProductMerge(ctx, job.ID, "owner", job.Digest, "fake-stop", "unknown"); !errors.Is(err, ErrProductMergeHeld) {
		t.Fatal("unknown work treated as never started", err)
	}
}
