package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// A real two-process initialize/stop observation, without model/thread turns.
// The notification reference workload separately stubs processes and WorkSource.
func TestLiveParallelNativeCapacityAndStop(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_FACTORY_PARALLEL") != "1" {
		t.Skip("explicit model-free parallel native probe only")
	}
	binary := os.Getenv("HARNESS_LIVE_CODEX_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute native binary required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	k, first := taskFixture(t)
	tasks, err := k.Tasks(ctx, first.Target.WaveID)
	if err != nil || len(tasks) != 2 {
		t.Fatal("two independent Tasks required", err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Clean(filepath.Join(cwd, "../../scripts/isolate-linux"))
	script, err := os.ReadFile(launcher)
	if err != nil {
		t.Fatal(err)
	}
	auth := t.TempDir()
	if err = os.Chmod(auth, 0700); err != nil {
		t.Fatal(err)
	}
	runtime, err := NewIsolatedRuntime(k, IsolatedConfig{PrivateRoot: k.Root, Model: first.Model, CodexBinary: binary, WorkerCodexHome: auth, Launcher: []string{launcher}, LauncherContract: "argv_passthrough_v1", IsolationVerified: true})
	if err != nil {
		t.Fatal(err)
	}
	reservations := []storage.ManagedReservation{}
	for i, task := range tasks {
		binding, err := k.SessionBinding(ctx, task.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.MkdirAll(binding.CWD, 0700); err != nil {
			t.Fatal(err)
		}
		reservation, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: fmt.Sprintf("parallel-%d", i), RunID: fmt.Sprintf("parallel-run-%d", i), Mode: "execute", Binding: binding})
		if err != nil {
			t.Fatal(err)
		}
		reservations = append(reservations, reservation)
	}
	assertCapacity(t, k, 2)
	main, err := k.SessionBinding(ctx, first.ParentSessionID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "third", RunID: "third-run", Mode: "execute", Binding: main}); !errors.Is(err, storage.ErrExecutionCapacity) {
		t.Fatal("third process was not denied before launch", err)
	}
	type initialized struct {
		reservation       storage.ManagedReservation
		p                 Process
		rpc               *codex.SessionRPC
		probe, initialize int64
		err               error
	}
	ready := make(chan initialized, 2)
	var group sync.WaitGroup
	start := make(chan struct{})
	for _, reservation := range reservations {
		group.Go(func() {
			<-start
			result := initialized{reservation: reservation}
			defer func() { ready <- result }()
			launch, err := k.BeginManagedProcessLaunch(ctx, reservation.Process.Request.OwnerID)
			if err != nil {
				result.err = err
				return
			}
			before := time.Now()
			result.p, result.err = runtime.Factory.Open(ctx, Launch{Binding: reservation.Binding, Process: launch})
			if result.err != nil {
				return
			}
			result.probe = result.p.StartedAt().Sub(before).Nanoseconds()
			if err = k.RecordManagedProcessStarted(ctx, launch.Request.OwnerID, result.p.ProcessIdentity()); err != nil {
				result.err = err
				return
			}
			result.rpc = codex.NewSessionRPC(ctx, result.p, nil)
			if err = result.rpc.Initialize(ctx); err != nil {
				result.err = err
				return
			}
			result.initialize = time.Since(result.p.StartedAt()).Nanoseconds()
		})
	}
	close(start)
	// All probe operations inherit the finite context; drain both before touching
	// the store's test cleanup, including a failed launch/initialize.
	group.Wait()
	close(ready)
	results := []initialized{}
	for result := range ready {
		results = append(results, result)
		if result.p != nil {
			t.Cleanup(func() {
				if result.rpc != nil {
					result.rpc.Close()
				}
				_ = result.p.Close()
			})
		}
	}
	for _, result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
	}
	if len(results) != 2 || results[0].p.ProcessIdentity() == results[1].p.ProcessIdentity() {
		t.Fatal("independent native processes missing")
	}
	observations := []map[string]any{}
	combinedRSS := int64(0)
	rssKnown := true
	for _, result := range results {
		if !process.Alive(result.p.ProcessIdentity()) || result.p.GroupStopped() {
			t.Fatal("both app-servers must be live at the same boundary")
		}
		rss, err := process.TreeRSS(ctx, result.p.ProcessIdentity())
		obs := map[string]any{"probe_nanos": result.probe, "initialize_nanos": result.initialize, "initialized": true, "rss_method": "linux_proc_tree_rss_sampled_sum"}
		if err == nil {
			obs["rss_sample_bytes"] = rss
			combinedRSS += rss
		} else {
			obs["rss_sample_bytes"] = nil
			rssKnown = false
		}
		observations = append(observations, obs)
	}
	assertCapacity(t, k, 2)
	for i, result := range results {
		before := time.Now()
		result.rpc.Close()
		_ = result.p.Close()
		observations[i]["stop_nanos"] = time.Since(before).Nanoseconds()
		if !result.p.GroupStopped() {
			t.Fatal("native group stop was not confirmed")
		}
		if err = k.ConfirmManagedProcessStopped(ctx, storage.ManagedStopProof{OwnerID: result.reservation.Process.Request.OwnerID, Disposition: "group_stopped", Identity: result.p.ProcessIdentity()}); err != nil {
			t.Fatal(err)
		}
		observations[i]["group_stopped"] = true
		assertCapacity(t, k, len(results)-i-1)
	}
	if output := os.Getenv("HARNESS_PARALLEL_EVIDENCE"); output != "" {
		if !filepath.IsAbs(output) {
			t.Fatal("absolute sanitized evidence path required")
		}
		var total any
		if rssKnown {
			total = combinedRSS
		}
		evidence := map[string]any{"stage": "Task_5 model-free parallel process probe", "observed_at": time.Now().UTC(), "native_version": "codex-cli 0.163.0", "launcher_sha256": fmt.Sprintf("%x", sha256.Sum256(script)), "independent_tasks": 2, "simultaneously_live_app_servers": 2, "third_prelaunch_reservation_denied": true, "final_held_process_slots": 0, "authentication": "empty template", "model_calls": 0, "thread_or_turn_started": false, "k_workflow_acceptance": false, "combined_rss_sample_bytes": total, "rss_sampling": "one observation while both are alive; sum may count shared pages per process; not a peak or memory limit", "observations": observations}
		raw, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(output, append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
