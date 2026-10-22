package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// Opt-in real launcher/native initialize probe, with an EMPTY auth template.
// It invokes no thread/model turn and supplies no K workflow acceptance claim.
func TestLiveSessionAccessTokenInitializeAndStop(t *testing.T) {
	if os.Getenv("HARNESS_LIVE_FACTORY_AUTH") != "1" {
		t.Skip("explicit model-free native factory probe only")
	}
	binary := os.Getenv("HARNESS_LIVE_CODEX_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute native binary required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	k, task := taskFixture(t)
	main, err := k.SessionBinding(ctx, task.ParentSessionID)
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Clean(filepath.Join(cwd, "../../scripts/isolate-linux"))
	source, err := os.ReadFile(launcher)
	if err != nil {
		t.Fatal(err)
	}
	auth := t.TempDir()
	if err = os.Chmod(auth, 0700); err != nil {
		t.Fatal(err)
	}
	authFile := os.Getenv("HARNESS_LIVE_SESSION_AUTH_FILE")
	if authFile == "" {
		home := os.Getenv("CODEX_HOME")
		if home == "" {
			user, err := os.UserHomeDir()
			if err != nil {
				t.Fatal(err)
			}
			home = filepath.Join(user, ".codex")
		}
		authFile = filepath.Join(home, "auth.json")
	}
	authFile, err = filepath.EvalSymlinks(authFile)
	if err != nil {
		t.Fatal("session auth source missing")
	}
	config := IsolatedConfig{SessionAuthFile: authFile, PrivateRoot: k.Root, Model: "configured", CodexBinary: binary, WorkerCodexHome: auth, Launcher: []string{launcher}, LauncherContract: "argv_passthrough_v1", IsolationVerified: true}
	runtime, err := NewIsolatedRuntime(k, config)
	if err != nil {
		t.Fatal(err)
	}
	observations := []map[string]any{}
	for _, binding := range []contract.SessionBinding{main, task} {
		owner := binding.Kind + "-probe"
		r, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: owner, RunID: owner, Mode: "execute", Binding: binding})
		if err != nil {
			t.Fatal(err)
		}
		launch, err := k.BeginManagedProcessLaunch(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		before := time.Now()
		p, err := runtime.Factory.Open(ctx, Launch{Binding: r.Binding, Process: launch})
		if err != nil {
			t.Fatal(err)
		}
		identity := p.ProcessIdentity()
		t.Cleanup(func() {
			_ = p.Close()
			stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = process.StopGroup(stop, identity)
		})
		if err = k.RecordManagedProcessStarted(ctx, owner, identity); err != nil {
			t.Fatal(err)
		}
		provider, ok := p.(interface {
			TokenProvider() codex.ChatGPTTokenProvider
		})
		if !ok {
			t.Fatal("runtime omitted access-token provider")
		}
		rpc, err := codex.NewAuthenticatedSessionRPC(ctx, p, nil, provider.TokenProvider())
		if err != nil {
			t.Fatal(err)
		}
		if err = rpc.Initialize(ctx); err != nil {
			t.Fatal(err)
		}
		ready := time.Now()
		rss, rssErr := process.TreeRSS(ctx, identity)
		stop := time.Now()
		rpc.Close()
		_ = p.Close()
		stopped := time.Now()
		if !p.GroupStopped() {
			t.Fatal("native managed group did not stop")
		}
		if err = k.ConfirmManagedProcessStopped(ctx, storage.ManagedStopProof{OwnerID: owner, Disposition: "group_stopped", Identity: identity}); err != nil {
			t.Fatal(err)
		}
		obs := map[string]any{"role": binding.Kind, "initialized": true, "external_auth_login": true, "group_stopped": true, "probe_nanos": p.StartedAt().Sub(before).Nanoseconds(), "initialize_nanos": ready.Sub(p.StartedAt()).Nanoseconds(), "stop_nanos": stopped.Sub(stop).Nanoseconds(), "rss_method": "linux_proc_tree_rss_sampled_sum"}
		if rssErr == nil {
			obs["rss_sample_bytes"] = rss
		} else {
			obs["rss_sample_bytes"] = nil
		}
		var saved storage.SessionAuthentication
		if err = k.Record(ctx, "session_authentication", binding.ID, &saved); err != nil || saved.SessionID != binding.ID || saved.AccountID == "" || saved.BoundAt.IsZero() {
			t.Fatal("durable logical account binding missing")
		}
		obs["logical_account_binding"] = true
		if _, err = os.Lstat(filepath.Join(isolatedHome(config, binding.ID), "codex", "auth.json")); !os.IsNotExist(err) {
			t.Fatal("session authentication persisted in worker profile", err)
		}
		observations = append(observations, obs)
	}
	assertCapacity(t, k, 0)
	if output := os.Getenv("HARNESS_LIVE_FACTORY_AUTH_EVIDENCE"); output != "" {
		if !filepath.IsAbs(output) {
			t.Fatal("absolute sanitized evidence path required")
		}
		data, err := json.MarshalIndent(map[string]any{"stage": "Task_5 session access-token authentication probe", "observed_at": time.Now().UTC(), "launcher_sha256": fmt.Sprintf("%x", sha256.Sum256(source)), "native_version": "codex-cli 0.163.0", "authentication": "authorized session access token; refresh remains source-owned", "credential_values_recorded": false, "thread_or_turn_started": false, "model_calls": 0, "k_workflow_acceptance": false, "observations": observations}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(output, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
