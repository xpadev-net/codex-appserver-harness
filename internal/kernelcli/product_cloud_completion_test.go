package kernelcli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/control"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestProductPublicationPollsSavedCloudRequestWithoutModelTurn(t *testing.T) {
	ctx := context.Background()
	k, main, err := openKernel(ctx, cliRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	binding := boundPublicationObservationFixture(t, k, main)
	wave, err := k.Wave(ctx, binding.WaveID)
	if err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(contract.RequestPRReviewRequest{SchemaVersion: 1, Target: wave.Target(), Candidate: contract.CandidateReference{WorkItemID: binding.WorkItemID, CandidateID: "candidate", SHA: strings.Repeat("a", 40)}})
	b, err := k.SessionBinding(ctx, main.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	var runID string
	if err := k.DB.QueryRow("SELECT run_id FROM kernel_run_bindings WHERE session_id=?", b.ID).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, contract.ToolInvocation{SessionID: b.ID, RunID: runID, ThreadID: b.ThreadID, TurnID: "observation-turn", CallID: "review", Name: "pr_review_request", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := k.PreparePRDispatch(ctx, reply.OperationID, "cloud-owner")
	if err != nil {
		t.Fatal(err)
	}
	packet, err := k.SavePRReviewRequestPayload(ctx, reply.OperationID, claim.OwnerID, "@review please review", 71, []int64{91})
	if err != nil {
		t.Fatal(err)
	}
	_, err = k.BindObservedPRReviewRequest(ctx, reply.OperationID, storage.PRTransportProof{ID: "fixture-no-send", OwnerID: claim.OwnerID, Disposition: "not_started"}, storage.PRReviewRequestObservation{ProofID: "fixture-comment", RepositoryID: 17, PRNumber: 42, RoundID: packet.RoundID, CommentID: 8, RequesterID: 71, Body: packet.Body(), ObservedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	// Cancel affects future business effects, not completion of known old work.
	if _, err := k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "cloud-cancel", Target: claim.Product.Value.Target, Action: "cancel", Reason: "still join original work"}); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	calls := filepath.Join(bin, "calls")
	comment, _ := json.Marshal([][]any{{map[string]any{"id": 8, "body": packet.Body(), "user": map[string]int{"id": 71}}}})
	reviews, _ := json.Marshal([][]any{{map[string]any{"id": 101, "user": map[string]int{"id": 91}, "state": "COMMENTED", "commit_id": strings.Repeat("a", 40), "submitted_at": time.Now().UTC()}}})
	if err := os.WriteFile(filepath.Join(bin, "comment"), comment, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "reviews"), reviews, 0600); err != nil {
		t.Fatal(err)
	}
	// shell builtins keep PATH isolated to this fake read-only gh.
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$CLOUD_WORKER_CALLS"
case "$*" in
 'api --method GET repos/owner/repo') printf '%s' '{"id":17,"full_name":"owner/repo","default_branch":"trunk"}' ;;
 'api --method GET repos/owner/repo/pulls/42') printf '%s' '{"number":42,"state":"closed","head":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ref":"harness/work","repo":{"id":17}},"base":{"ref":"trunk","sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","repo":{"id":17}}}' ;;
 *'issues/42/comments?per_page=100') while IFS= read -r line || [ -n "$line" ]; do printf '%s' "$line"; done < "$CLOUD_WORKER_DATA/comment" ;;
 *'pulls/42/reviews?per_page=100') while IFS= read -r line || [ -n "$line" ]; do printf '%s' "$line"; done < "$CLOUD_WORKER_DATA/reviews" ;;
 *) exit 49 ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("CLOUD_WORKER_CALLS", calls)
	t.Setenv("CLOUD_WORKER_DATA", bin)
	config := publicationConfigFixture(t)
	report := contract.NewReport("owner/repo")
	report.RepositoryID, report.DefaultBranch = 17, "trunk"
	for _, key := range []string{"cloud_request", "review_sha"} {
		report.Capabilities[key] = contract.Capability{State: contract.Verified, Evidence: "fixture route"}
	}
	config.Publication.ReviewRoutes[0].CapabilitiesPath = operatorFile(t, report)
	config.Publication.ReviewRoutes[0].CloudReviewerIDs = []int64{91}
	// Standalone PR collection does not replace request-specific completion.
	worker, _, err := loadProductExecutorWithObserver(operatorFile(t, config), k, control.IsolatedConfig{}, true)
	if err != nil {
		t.Fatal(err)
	}
	workCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	hints := make(chan struct{}, 8)
	failures := make(chan error, 8)
	done := make(chan error, 1)
	go func() {
		done <- worker(workCtx, k, func() { hints <- struct{}{} }, func(err error) { failures <- err })
	}()
	select {
	case <-hints:
		if _, err := k.ReadProductCloudReviewCompletion(ctx, reply.OperationID); err != nil {
			t.Fatal("hint preceded completion commit", err)
		}
	case err := <-failures:
		t.Fatal(err)
	case <-workCtx.Done():
		t.Fatal("saved request not polled")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(calls)
	if err != nil || !strings.Contains(string(raw), "pulls/42/reviews") || strings.Contains(string(raw), "POST") || strings.Contains(string(raw), "PATCH") {
		t.Fatal(string(raw), err)
	}
	var runs, approvals int
	if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 1 {
		t.Fatal("completion launched model", runs, err)
	}
	if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind IN ('review_approval','wave_completion')").Scan(&approvals); err != nil || approvals != 0 {
		t.Fatal("completion became approval", approvals, err)
	}
}
