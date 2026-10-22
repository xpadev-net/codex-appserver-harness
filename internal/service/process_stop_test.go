package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/worker"
)

func TestRetryRequiresPositiveStopProofEvenForCompletedRun(t *testing.T) {
	for _, withRun := range []bool{false, true} {
		t.Run(map[bool]string{false: "startup-without-run", true: "completed-run"}[withRun], func(t *testing.T) {
			ctx := context.Background()
			c, _, _, _ := setup(t)
			w, err := c.Store.Get(ctx, "w")
			if err != nil {
				t.Fatal(err)
			}
			if err := c.block(ctx, w, "process stopping is unconfirmed"); err != nil {
				t.Fatal(err)
			}
			if withRun {
				if err := c.Store.Put(ctx, "agent_run", "completed-run", w.ID, worker.Run{ID: "completed-run", State: "completed"}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := c.Store.Put(ctx, "agent_process", "completed-run", w.ID, AgentProcess{Generation: "current-process", OperationID: "completed-run", WorkItemID: w.ID}); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(filepath.Dir(c.Config.Root), "trusted-config.json")
			c.Config.RequiredChecks = []gate.RequiredCheck{}
			raw, err := json.Marshal(c.Config)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"retry", "--config", path, "--id", w.ID, "--reason", "operator repaired execution"}
			for _, state := range []string{"missing", "false"} {
				if state == "false" {
					if err := c.Store.Put(ctx, "agent_quiesced", "completed-run", w.ID, false); err != nil {
						t.Fatal(err)
					}
				}
				if err := CLI(ctx, args); err == nil || !strings.Contains(err.Error(), "quiescence is unverified") {
					t.Fatal(state, err)
				}
				current, err := c.Store.Get(ctx, w.ID)
				if err != nil || current.Phase != domain.Blocked || current.RepairRounds != 0 {
					t.Fatal(current, err)
				}
			}
			if err := c.Store.Put(ctx, "agent_quiesced", "completed-run", w.ID, true); err != nil {
				t.Fatal(err)
			}
			if !withRun {
				if err := CLI(ctx, args); err == nil {
					t.Fatal("legacy boolean stopped a new process generation")
				}
				p := AgentProcess{Generation: "old-process", OperationID: "completed-run", WorkItemID: w.ID}
				if err := recordAgentStop(ctx, c.Store, p, "not_started"); err != nil {
					t.Fatal(err)
				}
				if err := CLI(ctx, args); err == nil {
					t.Fatal("old generation proof stopped current process")
				}
				p.Generation = "current-process"
				if err := recordAgentStop(ctx, c.Store, p, "not_started"); err != nil {
					t.Fatal(err)
				}
			}
			if err := CLI(ctx, args); err != nil {
				t.Fatal("verified stop did not permit operator retry", err)
			}
		})
	}

}

type unconfirmedStopAgent struct{ calls int }

func (a *unconfirmedStopAgent) Run(_ context.Context, _ domain.WorkItem, op domain.Operation, _ string, _ any) (worker.Run, error) {
	a.calls++
	return worker.Run{ID: op.ID, State: "unknown"}, process.ErrUnconfirmed
}
func TestUnconfirmedStopCannotAutomaticallyLaunchReconciliation(t *testing.T) {
	ctx := context.Background()
	c, _, _, _ := setup(t)
	fake := &unconfirmedStopAgent{}
	c.Agents = fake
	drive(t, c, 2)
	if fake.calls != 1 {
		t.Fatal("second process launched before stop proof", fake.calls)
	}
	w, err := c.Store.Get(ctx, "w")
	if err != nil || w.Phase != domain.Blocked {
		t.Fatal(w, err)
	}
	var intent ReconcileIntent
	if err := c.Store.Record(ctx, "reconcile_needed", w.ID, &intent); err != nil || intent.Active {
		t.Fatal(intent, err)
	}
	if err := c.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if fake.calls != 1 {
		t.Fatal("poll restarted unknown process", fake.calls)
	}
}
