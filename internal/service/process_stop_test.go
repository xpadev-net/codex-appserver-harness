package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xpadev-net/codex-appserver-harness/internal/domain"
	"github.com/xpadev-net/codex-appserver-harness/internal/gate"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
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

type heldUnconfirmedStopAgent struct{ calls int }

func (a *heldUnconfirmedStopAgent) Run(context.Context, domain.WorkItem, domain.Operation, string, any) (worker.Run, error) {
	a.calls++
	return worker.Run{}, errors.Join(ErrDispatchHeld, process.ErrUnconfirmed)
}
func TestUnconfirmedStoppingTakesPriorityOverHeldDispatch(t *testing.T) {
	c, _, _, _ := setup(t)
	fake := &heldUnconfirmedStopAgent{}
	c.Agents = fake
	drive(t, c, 4)
	if fake.calls != 1 {
		t.Fatal("held dispatch discarded stopping failure", fake.calls)
	}
	w, err := c.Store.Get(context.Background(), "w")
	if err != nil || w.Phase != domain.Blocked || !strings.Contains(w.Reason, "stopping is unconfirmed") {
		t.Fatal(w, err)
	}
}

type startupUnconfirmedStopAgent struct{ calls int }

func (a *startupUnconfirmedStopAgent) Run(_ context.Context, _ domain.WorkItem, op domain.Operation, _ string, _ any) (worker.Run, error) {
	a.calls++
	if a.calls <= 2 {
		return worker.Run{ID: op.ID, ThreadID: "thread", State: "unknown"}, errors.New("lost response")
	}
	return worker.Run{}, process.ErrUnconfirmed
}
func TestEmptyRunStoppingFailureClearsOldReconciliationIntent(t *testing.T) {
	c, _, _, _ := setup(t)
	fake := &startupUnconfirmedStopAgent{}
	c.Agents = fake
	drive(t, c, 2)
	if fake.calls != 2 {
		t.Fatal(fake.calls)
	}
	drive(t, c, 2)
	if fake.calls != 3 {
		t.Fatal(fake.calls)
	}
	var intent ReconcileIntent
	if err := c.Store.Record(context.Background(), "reconcile_needed", "w", &intent); err != nil || intent.Active {
		t.Fatal(intent, err)
	}
	drive(t, c, 2)
	if fake.calls != 3 {
		t.Fatal("empty startup result retained restart authority", fake.calls)
	}
}
func TestLiveAgentAdmissionRefusesUnconfirmedPreviousGenerationBeforeLaunch(t *testing.T) {
	ctx := context.Background()
	c, _, _, _ := setup(t)
	w, err := c.Store.Get(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{ID: "previous-startup"}
	p := AgentProcess{Generation: "unconfirmed-generation", OperationID: op.ID, WorkItemID: w.ID}
	if err := c.Store.Put(ctx, "agent_process", op.ID, w.ID, p); err != nil {
		t.Fatal(err)
	}
	// The configured executable/launcher are intentionally absent. Admission
	// must reject before probing or launching them, including after restart.
	run, err := (LiveAgents{Config: c.Config, Store: c.Store}).Run(ctx, w, op, "unused", nil)
	if !errors.Is(err, process.ErrUnconfirmed) || run.ID != "" {
		t.Fatal(run, err)
	}
	old := p
	old.Generation = "older-generation"
	if err := recordAgentStop(ctx, c.Store, old, "not_started"); err != nil {
		t.Fatal(err)
	}
	if _, err := (LiveAgents{Config: c.Config, Store: c.Store}).Run(ctx, w, op, "unused", nil); !errors.Is(err, process.ErrUnconfirmed) {
		t.Fatal(err)
	}
}

func TestLiveAgentRefusesLauncherThatExposesControllerSocket(t *testing.T) {
	ctx := context.Background()
	c, _, _, _ := setup(t)
	c.Config.Root = wakeRoot(t)
	socket, err := ipc.Listen(c.Config.Root, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	launcher := filepath.Join(t.TempDir(), "partial-isolation")
	script := `#!/bin/sh
if [ "$1" = sh ] && [ "$2" = -c ] && [ "$4" = probe ]; then
 case "$5" in
  */controller.sock) exec "$@" ;;
  *) exit 0 ;;
 esac
fi
exit 98
`
	if err = os.WriteFile(launcher, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	c.Config.Launcher = []string{launcher}
	w, err := c.Store.Get(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	w.Worktree = c.Config.RepositoryPath
	result, err := (LiveAgents{Config: c.Config, Store: c.Store}).Run(ctx, w, domain.Operation{ID: "socket-probe"}, "unused", nil)
	if err == nil || !strings.Contains(err.Error(), "worker isolation probe failed") || result.ID != "" {
		t.Fatal("socket exposure passed worker launch probes", result, err)
	}
}
