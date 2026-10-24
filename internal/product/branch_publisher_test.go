package product

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/workspace"
)

// Reuses the real checkout/seal/runner/local-review fixture. Model run facts and
// GitHub/Git publication transports are fixtures, not native P acceptance.
func checkBranchPublisher(t *testing.T, e Executor, saved storage.ProductTargetSnapshot, mode string) {
	t.Helper()
	ctx := context.Background()
	publishCtx, cancelPublish := context.WithCancel(ctx)
	defer cancelPublish()
	k := e.Store
	b, err := k.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err = k.ReserveSessionThread(ctx, b); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = k.BindSessionThread(ctx, b, "branch-main-thread"); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	input, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{saved.Value.Target})
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "branch-main-run", ThreadID: b.ThreadID, State: "starting", BaselineKnown: true, Started: time.Now().UTC()}
	if err = k.ReserveSessionRun(ctx, b, input, run); err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(contract.PublishPRRequest{SchemaVersion: 1, Target: saved.Value.Target, Candidate: contract.CandidateReference{WorkItemID: saved.Value.WorkItem.ID, CandidateID: saved.Value.Candidate.ID, SHA: saved.Value.Candidate.SHA}})
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, contract.ToolInvocation{SessionID: b.ID, RunID: run.ID, ThreadID: b.ThreadID, TurnID: "branch-main-turn", CallID: "branch-publish", Name: "pr_publish", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	id, owner := reply.OperationID, "branch-owner"
	_, err = k.PreparePRDispatch(ctx, id, owner)
	if err != nil {
		t.Fatal(err)
	}
	api := &fakeAPI{repository: Repository{ID: saved.Value.WorkItem.RepositoryID, Name: saved.Value.Repository, DefaultBranch: saved.Value.BaseBranch, CloneURL: "https://github.com/" + saved.Value.Repository + ".git"}}
	factory := func(_ storage.ProductTarget, before func(context.Context, string, string) error) API {
		// Each logical transport owns its guard; metadata must not replace the
		// PR transport's separate write fence in this fixture.
		return &branchTestAPI{fakeAPI: api, guard: before}
	}
	pushes := 0
	branch := BranchPublisher{Store: k, Gate: e.PublicationGate, API: factory}
	branch.Push = func(pushCtx context.Context, frozen workspace.FrozenCheckout, remote, ref string, before func(context.Context) error) (workspace.BranchPublication, error) {
		pushes++
		job, err := k.ProductBranchPublication(ctx, id)
		if err != nil || job.State != "launching" || frozen != job.Value.Source || remote != job.Value.Remote.URL || ref != saved.Value.WorkItem.Branch {
			t.Fatal("Git ran without fixed physical ownership", job, err)
		}
		o := workspace.BranchPublication{Source: frozen, Remote: remote, Branch: ref}
		if mode == "publication_noop" {
			o.RemoteSHA = frozen.SHA
			api.head = frozen.SHA
			return o, nil
		}
		if mode == "publication_metadata" {
			api.repository.CloneURL = "https://other.invalid/" + saved.Value.Repository + ".git"
		}
		if mode == "publication_cancel" {
			cancelPublish()
			return o, publishCtx.Err()
		}
		if mode == "publication_pause" {
			if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause-before-push", Target: saved.Value.Target, Action: "pause", Reason: "fixture operator pause"}); err != nil {
				t.Fatal(err)
			}
		}
		if mode != "publication_fence" {
			if err := before(pushCtx); err != nil {
				return o, err
			}
		}
		if mode != "publication_fence" {
			sent, err := k.ProductBranchPublication(ctx, id)
			if err != nil || sent.MayHaveSentAt.IsZero() {
				t.Fatal("push preceded durable send fence", sent, err)
			}
		}
		o.Attempted = true
		if mode == "publication_unknown" {
			api.head = frozen.SHA
			return o, workspace.ErrGitStopUnconfirmed
		}
		if mode == "publication_stopped" {
			return o, workspace.ErrPublicationUnconfirmed
		}
		o.RemoteSHA = frozen.SHA
		api.head = frozen.SHA
		if mode == "publication_partial" {
			return o, workspace.ErrPublicationUnconfirmed
		}
		return o, nil
	}
	p := Publisher{Store: k, Gate: e.PublicationGate, API: factory, Branch: branch.Publish}
	_, err = p.Publish(publishCtx, id, owner)
	job, readErr := k.ProductBranchPublication(ctx, id)
	if readErr != nil {
		t.Fatal(readErr)
	}
	r, readErr := k.OperationReservation(ctx, job.ID)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if mode == "publication_push" || mode == "publication_noop" {
		if err != nil || job.State != "published" || r.State != "released" || api.writes != 1 {
			t.Fatal(job.State, r.State, api.writes, err)
		}
		if mode == "publication_noop" && !job.MayHaveSentAt.IsZero() {
			t.Fatal("no-op consumed send fence")
		}
		if _, err = p.Publish(ctx, id, owner); err != nil || pushes != 1 || api.writes != 1 {
			t.Fatal("replay repeated publication", pushes, api.writes, err)
		}
		return
	}
	if err == nil || api.writes != 0 {
		t.Fatal("failed branch allowed HTTP", api.writes, err)
	}
	if mode == "publication_unknown" || mode == "publication_fence" {
		if job.State != "launching" || r.State != "held" || job.Stop != nil {
			t.Fatal("unknown writer released", job, r)
		}
	} else if job.State != "stopped" || r.State != "released" || job.Published != nil {
		t.Fatal("failed transport became publication", job, r)
	}
	if mode == "publication_partial" {
		// Only a new read after the saved stop may settle a failed push.
		if job.Stop.RemoteVerified {
			t.Fatal("partial observation verified")
		}
		if _, err = p.Publish(ctx, id, owner); err != nil || pushes != 1 || api.writes != 1 {
			t.Fatal("known stop did not reconcile by read", pushes, api.writes, err)
		}
	} else {
		if _, err = p.Publish(ctx, id, owner); err == nil || pushes != 1 || api.writes != 0 {
			t.Fatal("held branch relaunched", pushes, api.writes, err)
		}
	}
	var httpClaim storage.PRDispatchClaim
	if err = k.Record(ctx, "product_pr_dispatch", id, &httpClaim); err != nil {
		t.Fatal(err)
	}
	if mode != "publication_partial" && httpClaim.State != "prepared" {
		t.Fatal("branch consumed HTTP write permission", httpClaim.State)
	}
}

type branchTestAPI struct {
	*fakeAPI
	guard func(context.Context, string, string) error
}

func (a *branchTestAPI) Publish(ctx context.Context, base, branch, work, title, body string) (pr github.PR, err error) {
	// fakeAPI is synchronous and is only used serially by this fixture.
	a.fakeAPI.before = a.guard
	return a.fakeAPI.Publish(ctx, base, branch, work, title, body)
}

func TestBranchPublisherRequiresExplicitPorts(t *testing.T) {
	if err := (BranchPublisher{}).Publish(context.Background(), storage.PRDispatchClaim{}); err == nil || errors.Is(err, storage.ErrProductBranchHeld) {
		t.Fatal(err)
	}
}
