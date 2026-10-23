package kernelcli

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type blockedDiagnosticWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockedDiagnosticWriter) Write(raw []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(raw), nil
}
func TestKernelDaemonDiagnosticsNeverBlockControllerOrCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &blockedDiagnosticWriter{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(w.release)
	report := diagnosticReporter(ctx, w)
	report(errors.New("first diagnostic"))
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not receive diagnostic")
	}
	done := make(chan struct{})
	go func() {
		for range 10000 {
			report(errors.New("blocked stderr must not stall controller"))
		}
		cancel()
		report(errors.New("after shutdown"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full diagnostics queue blocked controller or cancellation")
	}
}
