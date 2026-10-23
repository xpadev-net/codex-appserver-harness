package product

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/github"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

type fakeAPI struct {
	repository   Repository
	head         string
	pr           *github.PR
	before       func(context.Context, string, string) error
	onPublish    func()
	afterGuard   func()
	loseResponse bool
	writes       int
}

func (f *fakeAPI) Repository(context.Context) (Repository, error)     { return f.repository, nil }
func (f *fakeAPI) BranchHead(context.Context, string) (string, error) { return f.head, nil }
func (f *fakeAPI) Pull(context.Context, int) (github.PR, error) {
	if f.pr == nil {
		return github.PR{}, errors.New("missing PR")
	}
	return *f.pr, nil
}
func (f *fakeAPI) FindPR(context.Context, string, string) (*github.PR, error) { return f.pr, nil }
func (f *fakeAPI) Publish(ctx context.Context, base, branch, work, title, body string) (github.PR, error) {
	if f.onPublish != nil {
		f.onPublish()
	}
	method, endpoint := "POST", "pulls"
	if f.pr != nil {
		method, endpoint = "PATCH", fmt.Sprintf("pulls/%d", f.pr.Number)
	}
	if err := f.before(ctx, method, endpoint); err != nil {
		return github.PR{}, err
	}
	if f.afterGuard != nil {
		f.afterGuard()
	}
	f.writes++
	pr := github.PR{Number: 42, State: "open", Title: title, Body: github.Marker(work) + "\n" + body}
	pr.Head.Ref, pr.Head.SHA, pr.Head.Repo.ID = branch, f.head, f.repository.ID
	pr.Base.Ref, pr.Base.Repo.ID = base, f.repository.ID
	f.pr = &pr
	if f.loseResponse {
		return github.PR{}, errors.New("response lost after request")
	}
	return pr, nil
}
func publicationFixture(t *testing.T) (Publisher, *fakeAPI, storage.ProductTargetSnapshot, string) {
	t.Helper()
	ctx := context.Background()
	k, err := storage.OpenKernel(filepath.Join(t.TempDir(), "private", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { k.Close() })
	if _, err = k.CreateOrchestrator(ctx, "main", "unavailable-model", t.TempDir(), "read-only"); err != nil {
		t.Fatal(err)
	}
	w, err := k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: "main", Source: "direct", SourceID: "product", OriginalRequest: "publish verified candidate", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "verified code"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: domain.DefaultKernelLimits()}})
	if err != nil {
		t.Fatal(err)
	}
	p := storage.ProductTarget{Target: w.Target(), Repository: "owner/repo", BaseBranch: "trunk", WorkItem: domain.WorkItem{ID: "work", RepositoryID: 17, ScopeRevision: 1, PolicyRevision: 1, Version: 1, Phase: domain.Reviewing, Candidate: "candidate", Branch: "harness/work"}, Candidate: runner.Candidate{ID: "candidate", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: 1, PolicyRevision: 1}}
	saved, err := k.SetProductTarget(ctx, "", p)
	if err != nil {
		t.Fatal(err)
	}
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
	if err = k.BindSessionThread(ctx, b, "thread"); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(map[string]any{"schema_version": 1, "session_id": "main", "targets": []contract.Target{w.Target()}})
	run := contract.SessionRun{ID: "model-run", ThreadID: b.ThreadID, State: "starting", BaselineKnown: true, Started: time.Now().UTC()}
	if err = k.ReserveSessionRun(ctx, b, contract.InputSnapshot{ID: "input", Input: input, Digest: fmt.Sprintf("%x", sha256.Sum256(input))}, run); err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(contract.PublishPRRequest{SchemaVersion: 1, Target: w.Target(), Candidate: contract.CandidateReference{WorkItemID: p.WorkItem.ID, CandidateID: p.Candidate.ID, SHA: p.Candidate.SHA}})
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, contract.ToolInvocation{SessionID: b.ID, RunID: run.ID, ThreadID: b.ThreadID, TurnID: "turn", CallID: "publish", Name: "pr_publish", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	api := &fakeAPI{repository: Repository{ID: 17, DefaultBranch: "trunk"}, head: p.Candidate.SHA}
	publisher := Publisher{Store: k, Gate: func(context.Context, storage.ProductTargetSnapshot) (Publication, error) {
		return Publication{Title: "Verified implementation", Body: "Validation evidence with safe references."}, nil
	}, API: func(_ storage.ProductTarget, before func(context.Context, string, string) error) API {
		api.before = before
		return api
	}}
	return publisher, api, saved, reply.OperationID
}
func TestPublisherUsesCanonicalGateAndMarksUnknownBeforeSingleWrite(t *testing.T) {
	p, api, _, id := publicationFixture(t)
	api.afterGuard = func() {
		var claim storage.PRDispatchClaim
		if err := p.Store.Record(context.Background(), "product_pr_dispatch", id, &claim); err != nil || claim.State != "unknown" {
			t.Fatal("HTTP write preceded durable unknown", claim.State, err)
		}
	}
	result, err := p.Publish(context.Background(), id, "owner")
	if err != nil || result.Resource.Number != 42 || api.writes != 1 {
		t.Fatal(result, api.writes, err)
	}
	if !strings.Contains(api.pr.Body, operationMarker(id)) || api.pr.Base.Ref != "trunk" {
		t.Fatal("operation correlation/default base missing")
	}
	if _, err = p.Publish(context.Background(), id, "restart-owner"); err != nil || api.writes != 1 {
		t.Fatal("completed operation repeated write", api.writes, err)
	}
}
func TestPublisherRejectsLatestMetadataGateAndPauseBeforeHTTP(t *testing.T) {
	for _, mode := range []string{"default_branch", "repository", "remote_sha", "gate_changed", "gate_failed", "pause", "projection_changed", "closing_text", "unexpected_endpoint"} {
		t.Run(mode, func(t *testing.T) {
			p, api, saved, id := publicationFixture(t)
			calls := 0
			initial := p.Gate
			p.Gate = func(ctx context.Context, s storage.ProductTargetSnapshot) (Publication, error) {
				calls++
				v, err := initial(ctx, s)
				if mode == "gate_failed" && calls > 1 {
					return v, errors.New("validation invalidated")
				}
				if mode == "gate_changed" && calls > 1 {
					v.Body = "changed canonical evidence"
				}
				if mode == "closing_text" {
					v.Body = "Fixes #42"
				}
				return v, err
			}
			api.onPublish = func() {
				switch mode {
				case "default_branch":
					api.repository.DefaultBranch = "changed"
				case "repository":
					api.repository.ID++
				case "remote_sha":
					api.head = strings.Repeat("c", 40)
				case "pause":
					next := saved.Value
					next.WorkItem.Paused = true
					if _, err := p.Store.SetProductTarget(context.Background(), saved.Digest, next); err != nil {
						t.Fatal(err)
					}
				case "unexpected_endpoint":
					guard := api.before
					api.before = func(ctx context.Context, _, _ string) error { return guard(ctx, "PUT", "pulls/42/merge") }
				}
			}
			if mode == "projection_changed" {
				next := saved.Value
				next.PRNumber = 42
				updated, err := p.Store.SetProductTarget(context.Background(), saved.Digest, next)
				if err != nil {
					t.Fatal(err)
				}
				saved = updated
			}
			if _, err := p.Publish(context.Background(), id, "owner"); err == nil {
				t.Fatal("invalid publication allowed", mode)
			}
			if api.writes != 0 {
				t.Fatal("guard failure still wrote HTTP", api.writes)
			}
		})
	}
}

func TestPreparedPublicationRetryKeepsExactPayloadAndDoesNotSpendAnotherSeat(t *testing.T) {
	p, api, _, id := publicationFixture(t)
	api.onPublish = func() { api.head = strings.Repeat("c", 40) }
	if _, err := p.Publish(context.Background(), id, "owner"); err == nil {
		t.Fatal("remote mismatch allowed publication")
	}
	var packet storage.PRPublicationPayload
	if err := p.Store.Record(context.Background(), "product_pr_publication_payload", id, &packet); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Store.SavePRPublicationPayload(context.Background(), id, "owner", packet.Title, packet.Body+"changed"); err == nil {
		t.Fatal("changed durable publication packet")
	}
	api.onPublish = nil
	api.head = strings.Repeat("a", 40)
	if _, err := p.Publish(context.Background(), id, "owner"); err != nil || api.writes != 1 {
		t.Fatal("known-unsent retry failed", api.writes, err)
	}
	var held int
	if err := p.Store.DB.QueryRow("SELECT count(*) FROM kernel_dispatch_intents WHERE state='held' AND json_extract(payload,'$.kind')='pr'").Scan(&held); err != nil || held != 1 {
		t.Fatal("retry spent another open PR seat", held, err)
	}
}
func TestLostPublicationResponseReconcilesOnlyItsOwnMarkerWithoutResend(t *testing.T) {
	p, api, saved, id := publicationFixture(t)
	api.loseResponse = true
	if _, err := p.Publish(context.Background(), id, "owner"); !errors.Is(err, storage.ErrPRReconciliationRequired) {
		t.Fatal(err)
	}
	if _, err := p.Publish(context.Background(), id, "new-owner"); !errors.Is(err, storage.ErrPRReconciliationRequired) {
		t.Fatal("unknown request retried", err)
	}
	// Current policy changes cannot prevent collecting the already admitted fact.
	next := saved.Value
	next.WorkItem.Paused = true
	if _, err := p.Store.SetProductTarget(context.Background(), saved.Digest, next); err != nil {
		t.Fatal(err)
	}
	p.Gate = func(context.Context, storage.ProductTargetSnapshot) (Publication, error) {
		t.Fatal("read-only recovery ran current publication gates")
		return Publication{}, nil
	}
	proof := storage.PRTransportProof{ID: "verified-transport-stop", OwnerID: "owner", Disposition: "reconciled_after_stop"}
	result, err := p.ReconcilePublication(context.Background(), id, proof)
	if err != nil || result.Resource.Number != 42 || api.writes != 1 {
		t.Fatal(result, api.writes, err)
	}
}
func TestUnknownPublicationAbsenceOrAnotherOperationMarkerDoesNotClearUnknown(t *testing.T) {
	for _, mode := range []string{"absent", "other_marker", "wrong_sha", "wrong_body"} {
		t.Run(mode, func(t *testing.T) {
			p, api, _, id := publicationFixture(t)
			api.loseResponse = true
			if _, err := p.Publish(context.Background(), id, "owner"); err == nil {
				t.Fatal("expected missing response")
			}
			switch mode {
			case "absent":
				api.pr = nil
			case "other_marker":
				api.pr.Body = strings.ReplaceAll(api.pr.Body, operationMarker(id), operationMarker("other"))
			case "wrong_sha":
				api.pr.Head.SHA = strings.Repeat("c", 40)
			case "wrong_body":
				api.pr.Body += "changed"
			}
			if _, err := p.ReconcilePublication(context.Background(), id, storage.PRTransportProof{ID: "stopped", OwnerID: "owner", Disposition: "reconciled_after_stop"}); !errors.Is(err, storage.ErrPRReconciliationRequired) {
				t.Fatal("ambiguous observation resolved unknown", mode, err)
			}
			if api.writes != 1 {
				t.Fatal("reconciliation resent write", api.writes)
			}
		})
	}
}
func TestLiveFactoryReadsRepositoryAndBranchThroughGhWithoutBaseGuess(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "calls")
	t.Setenv("HARNESS_TEST_GH_CALLS", log)
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$HARNESS_TEST_GH_CALLS"
case "$*" in
 *"git/ref/heads/"*) printf '%s' '{"object":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}' ;;
 *) printf '%s' '{"id":17,"default_branch":"trunk"}' ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	api := GitHubAPI(storage.ProductTarget{Repository: "owner/repo", WorkItem: domain.WorkItem{RepositoryID: 17}}, func(context.Context, string, string) error { return errors.New("read only") })
	repo, err := api.Repository(context.Background())
	if err != nil || repo.DefaultBranch != "trunk" || repo.ID != 17 {
		t.Fatal(repo, err)
	}
	head, err := api.BranchHead(context.Background(), "harness/work")
	if err != nil || head != strings.Repeat("a", 40) {
		t.Fatal(head, err)
	}
	data, err := os.ReadFile(log)
	if err != nil || !strings.Contains(string(data), "api --method GET repos/owner/repo\n") || !strings.Contains(string(data), "git/ref/heads/harness%2Fwork") {
		t.Fatal(string(data), err)
	}
}
