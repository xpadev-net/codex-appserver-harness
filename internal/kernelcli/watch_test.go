package kernelcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpadev-net/codex-appserver-harness/internal/contract"
	"github.com/xpadev-net/codex-appserver-harness/internal/ipc"
	"github.com/xpadev-net/codex-appserver-harness/internal/storage"
)

func TestKernelCLIWatchOncePaginatesWithoutDaemonOrModel(t *testing.T) {
	root, w := controlWave(t)
	pause := storage.OperatorControl{Key: "pause", Target: w.Target(), Action: "pause", Reason: "observe saved operation"}
	if _, err := cliCall(t, "pause", "--root", root, "--file", operatorFile(t, pause)); err != nil {
		t.Fatal(err)
	}
	after := int64(0)
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		raw, err := cliCall(t, "watch", "--root", root, "--once", "--limit", "1", "--cursor", fmt.Sprint(after))
		if err != nil {
			t.Fatal(err)
		}
		var page contract.StateEventPage
		if err = json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		if err = page.Validate(after, 1); err != nil || len(page.Events) != 1 || seen[page.Events[0].ID] {
			t.Fatal(page, err)
		}
		seen[page.Events[0].ID] = true
		after = page.Cursor
	}
	raw, err := cliCall(t, "watch", "--root", root, "--once", "--cursor", fmt.Sprint(after))
	if err != nil {
		t.Fatal(err)
	}
	var page contract.StateEventPage
	if err = json.Unmarshal(raw, &page); err != nil || len(page.Events) != 0 || page.Cursor != after {
		t.Fatal(page, err)
	}
	if _, err = cliCall(t, "watch", "--root", root, "--once", "--cursor", fmt.Sprint(after+1)); err == nil {
		t.Fatal("future cursor accepted")
	}
	if _, err = cliCall(t, "watch", "--root", root, "--once", "--limit", "33"); err == nil {
		t.Fatal("unsupported state page size accepted")
	}
}

type pageWriter struct{ pages chan []byte }

func (w pageWriter) Write(raw []byte) (int, error) {
	copy := append([]byte{}, raw...)
	w.pages <- copy
	return len(raw), nil
}
func awaitWatchPage(t *testing.T, output <-chan []byte) contract.StateEventPage {
	t.Helper()
	select {
	case raw := <-output:
		var page contract.StateEventPage
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		return page
	case <-time.After(3 * time.Second):
		t.Fatal("state stream stalled")
	}
	return contract.StateEventPage{}
}
func TestKernelCLIWatchReconnectsWithoutWakeOrAcknowledgement(t *testing.T) {
	root, w := controlWave(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	var wakes atomic.Int32
	server, err := ipc.Listen(root, k, func() { wakes.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	server.PollEvery = 5 * time.Millisecond
	defer server.Close()
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(ctx) }()
	output := make(chan []byte, 8)
	watchCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(watchCtx, []string{"watch", "--root", root, "--limit", "1"}, pageWriter{output}, io.Discard)
	}()
	first := awaitWatchPage(t, output)
	if err = first.Validate(0, 1); err != nil || len(first.Events) != 1 {
		t.Fatal(first, err)
	}
	stop()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watch did not cancel")
	}
	if _, err = k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "pause-stream", Target: w.Target(), Action: "pause", Reason: "event after disconnect"}); err != nil {
		t.Fatal(err)
	}
	resumedCtx, resumeStop := context.WithCancel(context.Background())
	defer resumeStop()
	go func() {
		done <- Run(resumedCtx, []string{"watch", "--root", root, "--limit", "1", "--cursor", fmt.Sprint(first.Cursor)}, pageWriter{output}, io.Discard)
	}()
	second := awaitWatchPage(t, output)
	if err = second.Validate(first.Cursor, 1); err != nil || len(second.Events) != 1 || second.Events[0].Kind != "operator_control" || second.Events[0].ID == first.Events[0].ID {
		t.Fatal(second, err)
	}
	resumeStop()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("resumed watch did not cancel")
	}
	if wakes.Load() != 0 {
		t.Fatal("watch woke a model", wakes.Load())
	}
	budget, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil || budget.MainTurns != 0 {
		t.Fatal(budget, err)
	}
	cancel()
	select {
	case <-serving:
	case <-time.After(3 * time.Second):
		t.Fatal("state server did not stop")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestKernelCLIWatchOutputFailureRetainsCursorForReconnect(t *testing.T) {
	root, _ := controlWave(t)
	k, _, err := openKernel(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if err = watchEvents(context.Background(), k, root, 0, 1, true, failingWriter{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = watchEvents(context.Background(), k, root, 0, 1, true, &out); err != nil {
		t.Fatal(err)
	}
	var page contract.StateEventPage
	if err = json.Unmarshal(out.Bytes(), &page); err != nil || len(page.Events) != 1 {
		t.Fatal(page, err)
	}
}

func TestKernelCLIWatchReferencesLargeEventPayload(t *testing.T) {
	root, w := controlWave(t)
	reason := strings.Repeat("e", contract.MaxInline)
	request := storage.OperatorControl{Key: "large-watch", Target: w.Target(), Action: "pause", Reason: reason}
	if _, err := cliCall(t, "pause", "--root", root, "--file", operatorFile(t, request)); err != nil {
		t.Fatal(err)
	}
	raw, err := cliCall(t, "watch", "--root", root, "--once", "--cursor", "1")
	if err != nil {
		t.Fatal(err)
	}
	var page contract.StateEventPage
	if err = json.Unmarshal(raw, &page); err != nil || len(page.Events) != 1 || !page.Events[0].PayloadOmitted || page.Events[0].PayloadBytes <= contract.MaxInline || len(page.Events[0].Payload) != 0 {
		t.Fatal(page, err)
	}
	if bytes.Contains(raw, []byte(reason)) {
		t.Fatal("stream copied oversized event body")
	}
}

func TestKernelCLIWatchDoesNotAcknowledgeOrApplyPendingNotifications(t *testing.T) {
	ctx := context.Background()
	root, w := controlWave(t)
	k, _, err := openKernel(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if _, err = k.Subscribe(ctx, storage.SubscriptionRequest{SessionID: w.OrchestratorSessionID, Target: w.Target(), Key: "terminal", Kind: "wave_terminal"}); err != nil {
		t.Fatal(err)
	}
	if _, err = k.ApplyOperatorControl(ctx, storage.OperatorControl{Key: "cancel", Target: w.Target(), Action: "cancel", Reason: "saved terminal observation"}); err != nil {
		t.Fatal(err)
	}
	before, err := k.Notifications(ctx, w.OrchestratorSessionID, 0, 100)
	if err != nil || len(before) != 1 || before[0].DeliveryAcknowledged {
		t.Fatal(before, err)
	}
	if _, err = cliCall(t, "watch", "--root", root, "--once"); err != nil {
		t.Fatal(err)
	}
	after, err := k.Notifications(ctx, w.OrchestratorSessionID, 0, 100)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("watch changed notification delivery/application", before, after, err)
	}
	budget, err := k.WaveRunBudget(ctx, w.ID)
	if err != nil || budget.MainTurns != 0 {
		t.Fatal(budget, err)
	}
}
