package product

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/splitting"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
	"github.com/xpadev-net/codex-appserver-harness/internal/verification"
)

func nestedSplitHTTPFixture(t *testing.T) (*storage.KernelStore, storage.ProductSplitCreate, IssueSourcePolicy, *splitSourceFixtureAPI, string) {
	t.Helper()
	ctx := context.Background()
	observer, api, registered, bin := splitChildSourceFixture(t)
	k := observer.Store
	parent := registered.Value.Registrations[1]
	binding := registered.Value.Children[1]
	if _, ack, err := observer.Observe(ctx, registered.Value.Request.Target, parent.Value.Definition.ID); err != nil || !ack {
		t.Fatal(err)
	}
	proof, err := k.PrepareProductSplitParentScope(ctx, registered.Value.Request.Target, parent.Value.Definition.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(binding.Value.Scope.Criteria) != 1 {
		t.Fatal("fixture requires one local obligation")
	}
	criterion := binding.Value.Scope.Criteria[0]
	proposal := splitting.Proposal{SchemaVersion: 1, WorkItemID: parent.Value.Definition.ID, ScopeRevision: registered.Value.Request.Target.ScopeRevision, Outcome: "split", Children: []splitting.Child{}}
	for _, key := range []string{"one", "two"} {
		proposal.Children = append(proposal.Children, splitting.Child{Key: key, Title: "nested " + key, Requirement: criterion.Requirement, Criteria: []verification.Criterion{{ID: "nested-" + key, Requirement: criterion.Requirement}}, Coverage: []splitting.Coverage{{ParentCriterion: criterion.ID, ChildCriterion: "nested-" + key, PartID: key, PartCount: 2, Basis: "local obligation partition"}}, DependsOn: []string{}})
	}
	admitted, err := k.PrepareProductNestedSplitProposal(ctx, storage.ProductSplitProposalRequest{Target: registered.Value.Request.Target, Key: "nested-http", ParentWorkItemID: parent.Value.Definition.ID, RegistrationDigest: parent.Digest, Proposal: proposal}, storage.ProductSplitParentScopeReference{ID: proof.ID, Digest: proof.Digest})
	if err != nil {
		t.Fatal(err)
	}
	r := storage.ProductSplitChildPlanRequest{Target: registered.Value.Request.Target, ParentWorkItemID: parent.Value.Definition.ID, ProposalID: admitted.Value.Proposal.ID, ProposalDigest: admitted.Value.Proposal.Digest}
	plan, err := k.PrepareProductSplitChildPlan(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReserveProductSplitChildren(ctx, r); err != nil {
		t.Fatal(err)
	}
	job, err := k.PrepareProductSplitCreate(ctx, r, plan.Children[0].ID, "nested-http-controller")
	if err != nil {
		t.Fatal(err)
	}
	// The subprocess uses native gh against this local deterministic transport.
	script, err := os.ReadFile(filepath.Join(bin, "gh"))
	if err != nil {
		t.Fatal(err)
	}
	script = []byte(strings.ReplaceAll(strings.ReplaceAll(string(script), "repos/owner/repo/issues/42", "repos/owner/repo/issues/12"), `{"id":101,"number":11}`, `{"id":501,"number":51}`))
	if err := os.WriteFile(filepath.Join(bin, "gh"), script, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(api.issues["issues/12"])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "source.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return k, job, observer.Policy, api, bin
}

func TestNestedSplitHTTPDirectWaveRequiresFreshOwnParentAndOnlyOnePOST(t *testing.T) {
	for _, mode := range []string{"success", "worker", "missing_policy", "wrong_policy", "closed", "hold", "edited", "identity"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			k, job, policy, api, bin := nestedSplitHTTPFixture(t)
			before := strings.Count(splitHTTPCalls(t, bin), "POST")
			selected := &policy
			if mode == "missing_policy" {
				selected = nil
			}
			if mode == "wrong_policy" {
				copy := policy
				copy.HoldLabel = "different-hold"
				selected = &copy
			}
			source := api.issues["issues/12"]
			switch mode {
			case "closed":
				source.State = "closed"
			case "hold":
				source.Labels = append(source.Labels, struct {
					Name string `json:"name"`
				}{"hold"})
			case "edited":
				source.Body += "changed"
			case "identity":
				source.ID++
			}
			raw, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "source.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			transport := splitTestTransport(t, k, selected)
			var stopped storage.ProductSplitCreate
			if mode == "worker" {
				worker := splitDispatchFixture(t, k)
				if changed, err := worker.process(ctx, job.ID); changed || err == nil {
					t.Fatal("worker admitted missing own policy", changed, err)
				}
				worker.SourcePolicies = map[int64]IssueSourcePolicy{17: policy}
				changed, dispatchErr := worker.process(ctx, job.ID)
				if !changed || dispatchErr != nil {
					t.Fatal("worker failed nested direct dispatch", changed, dispatchErr)
				}
				stopped, err = k.ReadProductSplitCreate(ctx, job.ID)
			} else {
				stopped, err = transport.Start(ctx, job.ID)
			}
			after := strings.Count(splitHTTPCalls(t, bin), "POST")
			if mode != "success" && mode != "worker" {
				if after != before || err == nil && (stopped.State != "aborted" || stopped.HTTP != nil) {
					t.Fatal("unsafe own source sent Issue", mode, err, after-before)
				}
				return
			}
			if err != nil || stopped.State != "stopped" || stopped.HTTP == nil || stopped.Value.Ancestry == nil || after-before != 1 {
				t.Fatal("nested sender failed", err, stopped.State, after-before)
			}
			if strings.Count(splitHTTPCalls(t, bin), "GET:source") < 2 {
				t.Fatal("direct root skipped own Issue GET")
			}
			if _, err := transport.Start(ctx, job.ID); err == nil {
				t.Fatal("stopped nested creation resent")
			}
			if strings.Count(splitHTTPCalls(t, bin), "POST") != after {
				t.Fatal("replay duplicated POST")
			}
		})
	}
}
