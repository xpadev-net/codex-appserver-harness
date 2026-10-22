package control

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/codex"
	"github.com/xpadev-net/codex-appserver-harness/internal/process"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

// This model-free fixture checks the real launcher, stdio and process owner. It
// is trusted test code, not evidence that a production sandbox is configured.
func TestCommandHelper(t *testing.T) {
	if os.Getenv("HARNESS_TEST_RPC") != "1" {
		return
	}
	cwd, err := os.Getwd()
	if err != nil {
		os.Exit(2)
	}
	out := json.NewEncoder(os.Stdout)
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		var m codex.Message
		if json.Unmarshal(scan.Bytes(), &m) != nil {
			os.Exit(3)
		}
		if len(m.ID) == 0 {
			continue
		}
		var result any
		switch m.Method {
		case "initialize":
			result = map[string]any{}
		case "thread/start":
			result = map[string]any{"thread": map[string]any{"id": "thread", "cwd": cwd, "turns": []any{}}, "model": "configured"}
		case "turn/start":
			result = map[string]any{"turn": map[string]any{"id": "turn"}}
		default:
			os.Exit(4)
		}
		if out.Encode(map[string]any{"id": m.ID, "result": result}) != nil {
			os.Exit(5)
		}
		if m.Method == "turn/start" {
			if out.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn", "status": "completed", "items": []any{map[string]string{"type": "agentMessage", "text": `{"ok":true}`}}}}}) != nil {
				os.Exit(6)
			}
		}
	}
	os.Exit(0)
}
func TestCommandRuntimeOwnsRealProcessAndFixedCWD(t *testing.T) {
	k, b := taskFixture(t)
	program, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	factory := CommandFactory{Build: func(ctx context.Context, l Launch) (Command, error) {
		assertCapacity(t, k, 1)
		return Command{Program: program, Arguments: []string{"-test.run=^TestCommandHelper$"}, Environment: []string{"HARNESS_TEST_RPC=1"}, ContainmentVerified: true}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := (Runtime{Store: k, Factory: factory}).Execute(ctx, request(b))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Stopped || out.Run.State != "completed" {
		t.Fatal(out)
	}
	assertCapacity(t, k, 0)
	managed, err := k.ManagedProcess(ctx, out.OwnerID)
	if err != nil || !managed.Identity.Valid() {
		t.Fatal(managed, err)
	}
	if alive, err := process.GroupAlive(managed.Identity); err != nil || alive {
		t.Fatal(alive, err)
	}
	metrics, err := k.ProcessMetrics(ctx, out.PreparationID)
	if err != nil || metrics.PeakRSSBytes == nil || *metrics.PeakRSSBytes <= 0 || metrics.RSSMethod != storage.RSSProcTree || metrics.RSSIntervalNanos != int64(RSSSampleInterval) {
		t.Fatal("real owned process RSS was not recorded", metrics, err)
	}
}
func TestCommandFactoryRefusesUnverifiedOrInheritedLaunch(t *testing.T) {
	f := CommandFactory{Build: func(context.Context, Launch) (Command, error) {
		return Command{Program: "/bin/false", ContainmentVerified: true}, nil
	}}
	p, err := f.Open(context.Background(), Launch{})
	if p != nil || !errors.Is(err, ErrNotStarted) {
		t.Fatal(p, err)
	}
}
