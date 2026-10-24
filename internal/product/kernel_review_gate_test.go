package product

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Called after the real local quality fixture has bound its injected PR.
// Capability reports and remote resources below are deliberately fixtures.
func checkKernelReviewGate(t *testing.T, e Executor, old storage.ProductTargetSnapshot, api *fakeAPI) {
	t.Helper()
	ctx := context.Background()
	current, err := e.Store.ReadProductTarget(ctx, old.Value.Target, old.Value.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	api.pr.Base.SHA = current.Value.Candidate.BaseSHA
	report := contract.NewReport(current.Value.Repository)
	report.RepositoryID, report.DefaultBranch = current.Value.WorkItem.RepositoryID, current.Value.BaseBranch
	report.Capabilities["cloud_request"] = contract.Capability{State: contract.Verified, Evidence: "fixture observed request comment"}
	reads := 0
	route := CloudReviewRoute{CloudReviewerIDs: []int64{91}, Trigger: "@review please review", RequesterID: 71, ReadReport: func(context.Context) (contract.Report, error) { reads++; return report, nil }}
	g := KernelReviewGate{Publication: e.PublicationGate, Routes: map[int64]CloudReviewRoute{report.RepositoryID: route}, API: func(_ storage.ProductTarget, before func(context.Context, string, string) error) ReviewAPI {
		return &fakeReviewAPI{fakeAPI: api, guard: before}
	}}
	if _, err = g.Check(ctx, current); err == nil {
		t.Fatal("PR projection change bypassed readiness refresh")
	}
	ready, err := e.Store.PrepareProductPublication(ctx, current.Value.Target, current.Value.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	current = ready.Value.Product
	request, err := g.Check(ctx, current)
	if err != nil || request.Trigger != route.Trigger || request.RequesterID != 71 || reads != 1 {
		t.Fatal(request, reads, err)
	}
	// Native approval is deliberately unknown: a verified request route permits
	// requesting review, but cannot manufacture approval or merge readiness.
	if report.AutoMergeReady() {
		t.Fatal("request-only fixture authorized merge")
	}
	baselinePR, baselineRepository := *api.pr, api.repository
	for _, mode := range []string{"unknown", "unsupported", "missing_evidence", "wrong_repository_id", "wrong_repository", "wrong_base", "wrong_version", "missing_time", "read_error", "missing_route", "missing_reader", "wrong_requester", "empty_trigger", "oversized_trigger", "closing_trigger", "wrong_pr", "wrong_head", "wrong_head_branch", "wrong_head_repo", "wrong_base_sha", "wrong_base_ref", "wrong_base_repo", "missing_marker", "closed", "draft", "merged", "metadata_id", "metadata_base", "local_changed"} {
		t.Run("review_gate_"+mode, func(t *testing.T) {
			candidate := report
			candidate.Capabilities = map[string]contract.Capability{"cloud_request": report.Capabilities["cloud_request"]}
			changedRoute := route
			changedRoute.ReadReport = func(context.Context) (contract.Report, error) {
				if mode == "read_error" {
					return candidate, errors.New("report unreadable")
				}
				return candidate, nil
			}
			changed := g
			changed.Routes = map[int64]CloudReviewRoute{report.RepositoryID: changedRoute}
			pr := baselinePR
			api.pr = &pr
			api.repository = baselineRepository
			defer func() { *api.pr = baselinePR; api.repository = baselineRepository }()
			switch mode {
			case "unknown", "unsupported":
				candidate.Capabilities["cloud_request"] = contract.Capability{State: contract.State(mode), Evidence: "fixture"}
			case "missing_evidence":
				candidate.Capabilities["cloud_request"] = contract.Capability{State: contract.Verified, Evidence: " \n"}
			case "wrong_repository_id":
				candidate.RepositoryID++
			case "wrong_repository":
				candidate.Repository = "other/repo"
			case "wrong_base":
				candidate.DefaultBranch = "other"
			case "wrong_version":
				candidate.Version++
			case "missing_time":
				candidate.ObservedAt = time.Time{}
			case "missing_route":
				changed.Routes = nil
			case "missing_reader":
				changedRoute.ReadReport = nil
			case "wrong_requester":
				changedRoute.RequesterID = 0
			case "empty_trigger":
				changedRoute.Trigger = " \n"
			case "oversized_trigger":
				changedRoute.Trigger = strings.Repeat("x", 16385)
			case "closing_trigger":
				changedRoute.Trigger = "Closes #42"
			case "wrong_pr":
				pr.Number++
			case "wrong_head":
				pr.Head.SHA = strings.Repeat("c", 40)
			case "wrong_head_branch":
				pr.Head.Ref = "other"
			case "wrong_head_repo":
				pr.Head.Repo.ID++
			case "wrong_base_sha":
				pr.Base.SHA = strings.Repeat("c", 40)
			case "wrong_base_ref":
				pr.Base.Ref = "other"
			case "wrong_base_repo":
				pr.Base.Repo.ID++
			case "missing_marker":
				pr.Body = "unrelated PR"
			case "closed":
				pr.State = "closed"
			case "draft":
				pr.Draft = true
			case "merged":
				pr.Merged = true
			case "metadata_id":
				api.repository.ID++
			case "metadata_base":
				api.repository.DefaultBranch = "other"
			case "local_changed":
				calls := 0
				changed.Publication = func(ctx context.Context, s storage.ProductTargetSnapshot) (Publication, error) {
					v, err := e.PublicationGate(ctx, s)
					calls++
					if calls > 1 {
						v.Body += "changed"
					}
					return v, err
				}
			}
			if mode != "missing_route" {
				changed.Routes[report.RepositoryID] = changedRoute
			}
			if _, err := changed.Check(ctx, current); err == nil {
				t.Fatal("changed gate admitted", mode)
			}
		})
	}
	// Exercise the actual requester with this gate and an admitted gateway
	// operation. A capability revocation between preparation and send prevents
	// the write, then a prepared retry may use a fresh verified report.
	b, err := e.Store.SessionBinding(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(contract.PublishPRRequest{SchemaVersion: 1, Target: current.Value.Target, Candidate: contract.CandidateReference{WorkItemID: current.Value.WorkItem.ID, CandidateID: current.Value.Candidate.ID, SHA: current.Value.Candidate.SHA}})
	reply, err := (contract.KernelGateway{Backend: e.Store}).Handle(ctx, contract.ToolInvocation{SessionID: b.ID, RunID: "branch-main-run", ThreadID: b.ThreadID, TurnID: "branch-main-turn", CallID: "kernel-review", Name: "pr_review_request", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	requestAPI := &fakeReviewAPI{fakeAPI: api}
	r := ReviewRequester{Store: e.Store, Gate: g.Check, API: func(_ storage.ProductTarget, before func(context.Context, string, string) error) ReviewAPI {
		requestAPI.guard = before
		return requestAPI
	}}
	requestAPI.onRequest = func() { report.Capabilities["cloud_request"] = contract.Capability{State: contract.Unknown} }
	if _, err = r.Request(ctx, reply.OperationID, "review-owner"); err == nil || requestAPI.requestWrites != 0 {
		t.Fatal("revoked route sent request", err)
	}
	var claim storage.PRDispatchClaim
	if err = e.Store.Record(ctx, "product_pr_dispatch", reply.OperationID, &claim); err != nil || claim.State != "prepared" {
		t.Fatal(claim, err)
	}
	report.Capabilities["cloud_request"] = contract.Capability{State: contract.Verified, Evidence: "fixture restored request observation"}
	requestAPI.onRequest = nil
	receipt, err := r.Request(ctx, reply.OperationID, "review-owner")
	if err != nil || receipt.Outcome != "review_requested" || requestAPI.requestWrites != 1 {
		t.Fatal(receipt, err)
	}
	if _, err = r.Request(ctx, reply.OperationID, "other-owner"); err != nil || requestAPI.requestWrites != 1 {
		t.Fatal("review replay wrote again", err)
	}
	checkKernelMergeWithQuality(t, e, current, reply.OperationID, requestAPI)
}
