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
	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/runner"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// This binds an admitted operation using injected transport facts. The worker
// below uses real fake-gh subprocesses; neither is native model/P acceptance.
func boundPublicationObservationFixture(t *testing.T, k *storage.KernelStore, main Config) storage.PRResourceBinding {
	t.Helper()
	ctx := context.Background()
	w, err := k.CreateWave(ctx, storage.WaveRequest{OrchestratorSessionID: main.SessionID, Source: "direct", SourceID: "observation-fixture", OriginalRequest: "observe published resources", Criteria: []domain.WaveCriterion{{ID: "AC", Requirement: "track PR updates"}}, Policy: domain.WavePolicy{Revision: 1, Stage: "P", Product: true, Limits: main.Limits}})
	if err != nil {
		t.Fatal(err)
	}
	p := storage.ProductTarget{Target: w.Target(), Repository: "owner/repo", BaseBranch: "trunk", WorkItem: domain.WorkItem{ID: "observation-work", RepositoryID: 17, ScopeRevision: 1, PolicyRevision: 1, Version: 1, Phase: domain.Reviewing, Candidate: "candidate", Branch: "harness/work"}, Candidate: runner.Candidate{ID: "candidate", SHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40), ScopeRevision: 1, PolicyRevision: 1}}
	if _, err := k.SetProductTarget(ctx, "", p); err != nil {
		t.Fatal(err)
	}
	b, err := k.SessionBinding(ctx, main.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := k.ReserveSessionThread(ctx, b); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := k.BindSessionThread(ctx, b, "observation-main-thread"); err != nil {
		t.Fatal(err)
	}
	b, err = k.SessionBinding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	input, err := k.BuildSessionInput(ctx, b.ID, []contract.Target{w.Target()})
	if err != nil {
		t.Fatal(err)
	}
	run := contract.SessionRun{ID: "observation-main-run", ThreadID: b.ThreadID, State: "starting", BaselineKnown: true, Started: time.Now().UTC()}
	if err := k.ReserveSessionRun(ctx, b, input, run); err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(contract.PublishPRRequest{SchemaVersion: 1, Target: w.Target(), Candidate: contract.CandidateReference{WorkItemID: p.WorkItem.ID, CandidateID: p.Candidate.ID, SHA: p.Candidate.SHA}})
	reply, err := (contract.KernelGateway{Backend: k}).Handle(ctx, contract.ToolInvocation{SessionID: b.ID, RunID: run.ID, ThreadID: b.ThreadID, TurnID: "observation-turn", CallID: "publish", Name: "pr_publish", Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.PreparePRDispatch(ctx, reply.OperationID, "fixture-owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.MarkPRMayHaveSent(ctx, reply.OperationID, "fixture-owner"); err != nil {
		t.Fatal(err)
	}
	receipt, err := k.BindObservedPRPublication(ctx, reply.OperationID, storage.PRTransportProof{ID: "fixture-response", OwnerID: "fixture-owner", Disposition: "response_received"}, storage.PRResourceObservation{ProofID: "fixture-response", RepositoryID: 17, HeadRepositoryID: 17, BaseRepositoryID: 17, Number: 42, MarkerWorkItemID: p.WorkItem.ID, HeadSHA: p.Candidate.SHA, HeadBranch: p.WorkItem.Branch, BaseBranch: p.BaseBranch, State: "open", ObservedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	return receipt.Resource
}

func TestProductPublicationAutomaticallyObservesBoundPRUnlessStandaloneConfigured(t *testing.T) {
	for _, standalone := range []bool{false, true} {
		name := "automatic"
		if standalone {
			name = "standalone"
		}
		t.Run(name, func(t *testing.T) {
			root := cliRoot(t)
			k, main, err := openKernel(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			binding := boundPublicationObservationFixture(t, k, main)
			bin := t.TempDir()
			calls := filepath.Join(bin, "calls")
			script := `#!/bin/sh
printf '%s\n' "$*" >> "$PUBLICATION_OBSERVATION_CALLS"
case "$*" in
 'api --method GET repos/owner/repo') printf '%s' '{"id":17,"default_branch":"trunk"}' ;;
 'api --method GET repos/owner/repo/pulls/42') printf '%s' '{"number":42,"state":"open","head":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ref":"harness/work","repo":{"id":17}},"base":{"ref":"trunk","repo":{"id":17}}}' ;;
 'api --method GET --paginate --slurp repos/owner/repo/issues/42/comments?per_page=100') printf '%s' '[[{"id":11,"body":"updated after publication","user":{"id":7}}]]' ;;
 *'check-suites?per_page=100') printf '%s' '[{"check_suites":[]}]' ;;
 'api --method GET --paginate --slurp repos/owner/repo/'*) printf '%s' '[[]]' ;;
 *) exit 49 ;;
esac
`
			if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin)
			t.Setenv("PUBLICATION_OBSERVATION_CALLS", calls)
			config := publicationConfigFixture(t)
			worker, _, err := loadProductExecutorWithObserver(operatorFile(t, config), k, control.IsolatedConfig{}, standalone)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			changed := make(chan struct{}, 8)
			reported := make(chan error, 16)
			done := make(chan error, 1)
			go func() {
				done <- worker(ctx, k, func() {
					select {
					case changed <- struct{}{}:
					default:
					}
				}, func(err error) {
					select {
					case reported <- err:
					default:
					}
				})
			}()
			if standalone {
				select {
				case <-changed:
					t.Fatal("publication duplicated standalone observer")
				case <-time.After(150 * time.Millisecond):
				}
			} else {
				select {
				case <-changed:
				case err := <-reported:
					t.Fatal("automatic observation failed", err)
				case <-ctx.Done():
					t.Fatal("bound PR was not automatically observed")
				}
				state, err := k.PRResourceState(context.Background(), binding.ResourceKey)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, section := range state.Sections {
					if section.Kind == "issue_comment" {
						for _, entity := range section.Entities {
							found = found || entity.ID == "11" && strings.Contains(string(entity.Data), "updated after publication")
						}
					}
				}
				if !found {
					t.Fatal("hint preceded resource commit", state)
				}
				var pending int
				if err := k.DB.QueryRow("SELECT count(*) FROM records WHERE kind='product_pr_scan' AND json_extract(payload,'$.resource_key')=? AND json_extract(payload,'$.notification_state')='pending'", binding.ResourceKey).Scan(&pending); err != nil || pending < 1 {
					t.Fatal("hint preceded durable notification outbox", pending, err)
				}
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("publication observer not joined")
			}
			select {
			case err := <-reported:
				t.Fatal(err)
			default:
			}
			raw, err := os.ReadFile(calls)
			if standalone {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatal("standalone observer duplicated GETs", string(raw), err)
				}
			} else if err != nil || !strings.Contains(string(raw), "issues/42/comments") || strings.Contains(string(raw), "POST") || strings.Contains(string(raw), "PATCH") {
				t.Fatal("unexpected observation transport", string(raw), err)
			}
			var runs int
			if err := k.DB.QueryRow("SELECT count(*) FROM kernel_run_bindings").Scan(&runs); err != nil || runs != 1 {
				t.Fatal("observation started another model run", runs, err)
			}
		})
	}
}
