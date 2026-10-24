package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

func branchPublicationFixture(t *testing.T) (*KernelStore, string, string, ProductBranchRemote) {
	t.Helper()
	ctx := context.Background()
	k, w := publicationQualityFixture(t, true, true)
	ready, err := k.PrepareProductPublication(ctx, w.Target(), "work")
	if err != nil {
		t.Fatal(err)
	}
	b, run, _ := reserveBudgetMain(t, k, "branch-publication-main", w.Target())
	p := ready.Value.Product.Value
	request := contract.PublishPRRequest{SchemaVersion: 1, Target: w.Target(), Candidate: contract.CandidateReference{WorkItemID: "work", CandidateID: p.Candidate.ID, SHA: p.Candidate.SHA}}
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, kernelCall(t, b, run, "pr_publish", request))
	if err != nil {
		t.Fatal(err)
	}
	owner := "branch-owner"
	if _, err := k.PreparePRDispatch(ctx, reply.OperationID, owner); err != nil {
		t.Fatal(err)
	}
	return k, reply.OperationID, owner, ProductBranchRemote{RepositoryID: p.WorkItem.RepositoryID, Repository: p.Repository, DefaultBranch: p.BaseBranch, URL: "https://github.com/" + p.Repository + ".git"}
}

func branchStopFixture(k *KernelStore, job ProductBranchPublication, started time.Time, attempted, published bool) ProductBranchStop {
	head := ""
	if published {
		head = job.Value.Source.SHA
	}
	return ProductBranchStop{PublicationID: job.ID, ProofID: job.ID + ":stopped", Reason: "fixture trusted Git owner joined", StartedAt: started, FinishedAt: k.clock(), Observation: workspace.BranchPublication{Source: job.Value.Source, Remote: job.Value.Remote.URL, Branch: job.Value.Branch, RemoteSHA: head, Attempted: attempted}}
}

func TestProductBranchPublicationBindsCanonicalSourceAndClaimsBothBoundariesOnce(t *testing.T) {
	ctx := context.Background()
	k, id, owner, remote := branchPublicationFixture(t)
	job, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
	if err != nil || job.State != "prepared" || job.Value.Source.SHA != job.Value.Product.Value.Candidate.SHA || job.Value.Source.CWD != job.Value.Product.Value.WorkItem.Worktree || job.Value.Plan.ID == "" {
		t.Fatal(job, err)
	}
	again, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
	if err != nil || !reflect.DeepEqual(job, again) {
		t.Fatal("reservation replay changed source", again, err)
	}
	if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, job.Digest); !errors.Is(err, ErrProductBranchHeld) {
		t.Fatal("send without physical claim", err)
	}
	claim, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest)
	if err != nil || !claim.Start || !claim.Publication.OwnerProcess.Valid() {
		t.Fatal(claim, err)
	}
	started := k.clock()
	if repeat, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil || repeat.Start {
		t.Fatal("duplicate physical start", repeat, err)
	}
	if _, err := k.MarkProductBranchMayHaveSent(ctx, id, "other-owner", job.Digest); !errors.Is(err, ErrProductBranchHeld) {
		t.Fatal("foreign send owner", err)
	}
	if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, job.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, job.Digest); !errors.Is(err, ErrProductBranchHeld) {
		t.Fatal("duplicate remote-write fence", err)
	}
	proof := branchStopFixture(k, job, started, true, true)
	finished, err := k.RecordProductBranchStopped(ctx, id, owner, job.Digest, proof)
	if err != nil || finished.State != "published" || finished.Published == nil {
		t.Fatal(finished, err)
	}
	reservation, err := k.OperationReservation(ctx, job.ID)
	if err != nil || reservation.State != "released" {
		t.Fatal(reservation, err)
	}
	if _, err := k.PreparePRDispatch(ctx, id, owner); err != nil {
		t.Fatal("branch facts altered HTTP claim", err)
	}
	replayed, err := k.RecordProductBranchStopped(ctx, id, owner, job.Digest, proof)
	if err != nil || !reflect.DeepEqual(replayed, finished) {
		t.Fatal("stop replay changed immutable facts", replayed, err)
	}
}

func TestProductBranchPublicationUnknownRetainsGitOwnershipAcrossReopen(t *testing.T) {
	ctx := context.Background()
	k, id, owner, remote := branchPublicationFixture(t)
	job, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, job.Digest); err != nil {
		t.Fatal(err)
	}
	root := k.Root
	k.Close()
	reopened, err := OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	claim, err := reopened.ClaimProductBranchPublication(ctx, id, owner, job.Digest)
	if err != nil || claim.Start || claim.Publication.State != "launching" {
		t.Fatal("restart repeated unknown Git", claim, err)
	}
	if _, err := reopened.ConfirmProductBranchRemote(ctx, id, owner, job.Digest, ProductBranchRemoteProof{PublicationID: job.ID, ProofID: "remote-only", StopProofID: "missing", RemoteSHA: job.Value.Source.SHA, ReadAt: reopened.clock()}); !errors.Is(err, ErrProductBranchHeld) {
		t.Fatal("remote head alone settled physical owner", err)
	}
	r, err := reopened.OperationReservation(ctx, job.ID)
	if err != nil || r.State != "held" {
		t.Fatal("unknown Git lost capacity", r, err)
	}
}

func TestProductBranchPublicationStoppedUnknownCanBindOnlyExactFreshRemote(t *testing.T) {
	ctx := context.Background()
	k, id, owner, remote := branchPublicationFixture(t)
	job, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil {
		t.Fatal(err)
	}
	started := k.clock()
	if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, job.Digest); err != nil {
		t.Fatal(err)
	}
	proof := branchStopFixture(k, job, started, true, false)
	stopped, err := k.RecordProductBranchStopped(ctx, id, owner, job.Digest, proof)
	if err != nil || stopped.State != "stopped" || stopped.Published != nil {
		t.Fatal(stopped, err)
	}
	observation := ProductBranchRemoteProof{PublicationID: job.ID, ProofID: "fresh-read", StopProofID: proof.ProofID, RemoteSHA: strings.Repeat("e", 40), ReadAt: k.clock()}
	if _, err := k.ConfirmProductBranchRemote(ctx, id, owner, job.Digest, observation); !errors.Is(err, ErrProductBranchHeld) {
		t.Fatal("other SHA was adopted", err)
	}
	observation.RemoteSHA = job.Value.Source.SHA
	observation.ReadAt = stopped.StoppedAt.Add(-time.Nanosecond)
	if _, err := k.ConfirmProductBranchRemote(ctx, id, owner, job.Digest, observation); !errors.Is(err, ErrProductBranchHeld) {
		t.Fatal("pre-stop observation settled unknown", err)
	}
	observation.ReadAt = k.clock()
	published, err := k.ConfirmProductBranchRemote(ctx, id, owner, job.Digest, observation)
	if err != nil || published.State != "published" || !reflect.DeepEqual(published.Stop, stopped.Stop) {
		t.Fatal(published, err)
	}
	if claim, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil || claim.Start {
		t.Fatal("remote binding granted another push", claim, err)
	}
}

func TestProductBranchPublicationRejectsMetadataDriftAndLostHistory(t *testing.T) {
	for _, mode := range []string{"remote", "credentials", "owner", "pause", "head", "history", "claim_history", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, id, owner, remote := branchPublicationFixture(t)
			if mode == "remote" {
				remote.RepositoryID++
			}
			if mode == "credentials" {
				remote.URL = "https://secret@github.com/" + remote.Repository + ".git"
			}
			if mode == "remote" || mode == "credentials" {
				if _, err := k.ReserveProductBranchPublication(ctx, id, owner, remote); !errors.Is(err, ErrProductBranchHeld) {
					t.Fatal(err)
				}
				return
			}
			job, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "owner" {
				owner = "foreign"
			}
			if mode == "pause" {
				p := job.Value.Product
				p.Value.WorkItem.Paused = true
				if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "head" || mode == "history" {
				kind := "product_branch_publication"
				if mode == "history" {
					kind = "product_branch_history"
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind=? AND id=?", kind, job.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := k.ReserveProductBranchPublication(ctx, id, owner, remote); err == nil {
					t.Fatal("lost branch evidence regenerated")
				}
				return
			}
			if mode == "claim_history" {
				if _, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil {
					t.Fatal(err)
				}
				if _, err := k.DB.Exec("DELETE FROM records WHERE kind='product_branch_claim' AND id=?", job.ID); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_branch_claim BEFORE INSERT ON records WHEN NEW.kind='product_branch_claim' BEGIN SELECT RAISE(FAIL,'branch rollback'); END"); err != nil {
					t.Fatal(err)
				}
			}
			if claim, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err == nil || claim.Start {
				t.Fatal("invalid claim granted start", claim, err)
			}
		})
	}
}

func TestProductBranchPublicationNewControllerCannotSendOrStopOldPhysicalOwner(t *testing.T) {
	ctx := context.Background()
	k, id, owner, remote := branchPublicationFixture(t)
	job, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil {
		t.Fatal(err)
	}
	started := k.clock()
	root := k.Root
	k.Close()
	reopened, err := OpenExistingKernel(root + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.MarkProductBranchMayHaveSent(ctx, id, owner, job.Digest); !errors.Is(err, ErrProductBranchHeld) {
		t.Fatal("reopened instance acquired old send fence", err)
	}
	proof := branchStopFixture(reopened, job, started, false, true)
	if _, err := reopened.RecordProductBranchStopped(ctx, id, owner, job.Digest, proof); !errors.Is(err, ErrProductBranchHeld) {
		t.Fatal("new reader certified old process stopping", err)
	}
	r, err := reopened.OperationReservation(ctx, job.ID)
	if err != nil || r.State != "held" {
		t.Fatal("unknown old owner was released", r, err)
	}
}

func TestProductBranchPublicationStopBoundaryAndRollback(t *testing.T) {
	for _, mode := range []string{"no_send_fence", "wrong_source", "old_time", "stop_rollback", "noop", "paused_after_claim"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, id, owner, remote := branchPublicationFixture(t)
			job, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil {
				t.Fatal(err)
			}
			started := k.clock()
			if mode != "no_send_fence" && mode != "noop" && mode != "paused_after_claim" {
				if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, job.Digest); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "paused_after_claim" {
				p := job.Value.Product
				p.Value.WorkItem.Paused = true
				if _, err := k.SetProductTarget(ctx, p.Digest, p.Value); err != nil {
					t.Fatal(err)
				}
				if _, err := k.MarkProductBranchMayHaveSent(ctx, id, owner, job.Digest); err == nil {
					t.Fatal("pause did not revoke send fence")
				}
			}
			attempted := mode != "noop" && mode != "paused_after_claim"
			proof := branchStopFixture(k, job, started, attempted, mode != "paused_after_claim")
			if mode == "wrong_source" {
				proof.Observation.Source.SHA = strings.Repeat("e", 40)
			}
			if mode == "old_time" {
				proof.StartedAt = job.CreatedAt.Add(-time.Nanosecond)
			}
			if mode == "stop_rollback" {
				if _, err := k.DB.Exec("CREATE TRIGGER fail_branch_stop BEFORE INSERT ON records WHEN NEW.kind='product_branch_stop' BEGIN SELECT RAISE(FAIL,'branch stop rollback'); END"); err != nil {
					t.Fatal(err)
				}
			}
			stopped, err := k.RecordProductBranchStopped(ctx, id, owner, job.Digest, proof)
			if mode == "noop" || mode == "paused_after_claim" {
				if err != nil || stopped.Stop == nil || !stopped.MayHaveSentAt.IsZero() {
					t.Fatal(stopped, err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid/failed stop transition succeeded")
			}
			r, err := k.OperationReservation(ctx, job.ID)
			if err != nil || r.State != "held" {
				t.Fatal("failed stop released capacity", r, err)
			}
			if mode == "stop_rollback" {
				if _, err := k.DB.Exec("DROP TRIGGER fail_branch_stop"); err != nil {
					t.Fatal(err)
				}
				if stopped, err := k.RecordProductBranchStopped(ctx, id, owner, job.Digest, proof); err != nil || stopped.State != "published" {
					t.Fatal("retrying saved stop fact failed", stopped, err)
				}
			}
		})
	}
}

func TestProductBranchPublicationCancelOnlyBeforePhysicalClaim(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unclaimed", true: "unknown_physical_owner"}[claimed], func(t *testing.T) {
			ctx := context.Background()
			k, id, owner, remote := branchPublicationFixture(t)
			job, err := k.ReserveProductBranchPublication(ctx, id, owner, remote)
			if err != nil {
				t.Fatal(err)
			}
			var started time.Time
			if claimed {
				if _, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil {
					t.Fatal(err)
				}
				started = k.clock()
			}
			if _, err := k.ApplyOperatorControl(ctx, OperatorControl{Key: "cancel-branch-wave", Target: job.Value.Product.Value.Target, Action: "cancel", Reason: "preserve physical branch owner"}); err != nil {
				t.Fatal(err)
			}
			canceled, err := k.CancelPreparedProductBranch(ctx, id, owner, "known unclaimed intent")
			if claimed {
				if !errors.Is(err, ErrProductBranchHeld) {
					t.Fatal("unknown physical owner canceled as unsent", err)
				}
				proof := branchStopFixture(k, job, started, false, false)
				if stopped, err := k.RecordProductBranchStopped(ctx, id, owner, job.Digest, proof); err != nil || stopped.State != "stopped" {
					t.Fatal("canceled Wave could not record its stopped Git owner", stopped, err)
				}
			} else {
				if err != nil || canceled.State != "canceled" {
					t.Fatal(canceled, err)
				}
				if replay, err := k.CancelPreparedProductBranch(ctx, id, owner, "known unclaimed intent"); err != nil || !reflect.DeepEqual(replay, canceled) {
					t.Fatal(replay, err)
				}
			}
			r, err := k.OperationReservation(ctx, job.ID)
			if err != nil || r.State != "released" {
				t.Fatal("settled branch owner retained capacity", r, err)
			}
			if claim, err := k.ClaimProductBranchPublication(ctx, id, owner, job.Digest); err != nil || claim.Start {
				t.Fatal("terminal branch intent granted start", claim, err)
			}
		})
	}
}
