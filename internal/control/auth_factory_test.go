package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestIsolatedFactoryUsesSessionAccessSourceForBothRoles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	k, task := taskFixture(t)
	main, err := k.SessionBinding(ctx, task.ParentSessionID)
	if err != nil {
		t.Fatal(err)
	}
	config := isolatedConfig(t, k.Root)
	config.SessionAuthFile = filepath.Join(t.TempDir(), "auth.json")
	access := sessionSourceFixture(t, "account", time.Now().Add(time.Hour).Unix())
	raw, _ := json.Marshal(map[string]any{"auth_mode": "chatgpt", "tokens": map[string]any{"access_token": access, "account_id": "account", "refresh_token": "fixture-never-clone"}})
	if err = os.WriteFile(config.SessionAuthFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	// Even an old worker template containing OAuth must not clone its refresh
	// token when the external source is selected.
	if err = os.WriteFile(filepath.Join(config.WorkerCodexHome, "auth.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENAI_API_KEY", "fixture-must-not-override-session-source")
	runtime, err := NewIsolatedRuntime(k, config)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []contract.SessionBinding{main, task} {
		r := request(b)
		r.RunID = "auth-" + b.ID
		r.Snapshot.ID = "snapshot-" + b.ID
		if b.Kind == "orchestrator" {
			input, err := json.Marshal(map[string]any{"schema_version": 1, "session_id": b.ID, "targets": []contract.Target{task.Target}})
			if err != nil {
				t.Fatal(err)
			}
			r.Snapshot.Input = input
			r.Snapshot.Digest = fmt.Sprintf("%x", sha256.Sum256(input))
		}
		out, err := runtime.Execute(ctx, r)
		if err != nil || !out.Stopped {
			t.Fatal("authenticated managed turn failed", err)
		}
		data, err := os.ReadFile(filepath.Join(isolatedHome(config, b.ID), "observed.json"))
		if err != nil {
			t.Fatal(err)
		}
		var observation isolatedObservation
		if json.Unmarshal(data, &observation) != nil || !observation.Authenticated || observation.HasAPIKey {
			t.Fatal("source authentication was not applied before thread/start")
		}
		if _, err = os.Lstat(filepath.Join(isolatedHome(config, b.ID), "codex", "auth.json")); !os.IsNotExist(err) {
			t.Fatal("worker persisted copied credentials", err)
		}
	}
	after, err := os.ReadFile(config.SessionAuthFile)
	if err != nil || string(after) != string(raw) {
		t.Fatal("controller changed the source owner's credentials", err)
	}
	assertCapacity(t, k, 0)
}

func TestSessionAccountBindingSurvivesDatabaseAndProcessRecreation(t *testing.T) {
	ctx := context.Background()
	k, b := taskFixture(t)
	config := isolatedConfig(t, k.Root)
	config.SessionAuthFile = filepath.Join(t.TempDir(), "auth.json")
	reservation, err := k.ReserveManagedProcess(ctx, storage.ManagedProcessRequest{OwnerID: "account-launch", RunID: "account-run", Mode: "execute", Binding: b})
	if err != nil {
		t.Fatal(err)
	}
	launch, err := k.BeginManagedProcessLaunch(ctx, "account-launch")
	if err != nil {
		t.Fatal(err)
	}
	if err = k.BindSessionAuthentication(ctx, reservation.Binding, "account"); err != nil {
		t.Fatal(err)
	}
	stale := reservation.Binding
	stale.Generation++
	if err = k.BindSessionAuthentication(ctx, stale, "account"); err == nil {
		t.Fatal("stale preparation changed authentication binding")
	}
	dbPath := filepath.Join(k.Root, "state.db")
	if err = k.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.OpenKernel(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err = reopened.BindSessionAuthentication(ctx, reservation.Binding, "account"); err != nil {
		t.Fatal("same account did not replay after restart", err)
	}
	token := sessionSourceFixture(t, "replacement-account", time.Now().Add(time.Hour).Unix())
	raw, _ := json.Marshal(map[string]any{"auth_mode": "chatgpt", "tokens": map[string]any{"access_token": token, "account_id": "replacement-account"}})
	if err = os.WriteFile(config.SessionAuthFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	runtime, err := NewIsolatedRuntime(reopened, config)
	if err != nil {
		t.Fatal(err)
	}
	p, err := runtime.Factory.Open(ctx, Launch{Binding: reservation.Binding, Process: launch})
	if p != nil {
		_ = p.Close()
		t.Fatal("replacement account started a native process")
	}
	if !errors.Is(err, storage.ErrSessionAccountChanged) || !errors.Is(err, ErrNotStarted) {
		t.Fatal("logical session adopted a replacement account", err)
	}
}
